package http1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestConformance_RFC9110_Sec8_6_BodyMustMatchDeclaredContentLength pins that a
// request body is reconciled against the Content-Length its head declared.
//
// RFC 9110 §8.6 forbids a sender from emitting a Content-Length it knows to be
// incorrect. An over-run puts octets on the wire that the peer reads as the start
// of the next request; an under-run leaves the peer waiting for octets that never
// come. Nothing reconciled the two, so a caller streaming more or fewer bytes
// than it declared emitted a smuggling primitive and the connection was still
// treated as reusable.
func TestConformance_RFC9110_Sec8_6_BodyMustMatchDeclaredContentLength(t *testing.T) {
	t.Run("over-run refused before the excess reaches the wire", func(t *testing.T) {
		ex, capture := rawCapture(t)
		require.NoError(t, ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("5")}), false),
			"WriteRequest")

		err := ex.WriteBody(context.Background(), []byte("0123456789"), true)

		require.ErrorIsf(t, err, http1.ErrInvalidRequest,
			"WriteBody(10 octets under Content-Length 5) = %v, want ErrInvalidRequest", err)
		wire := capture()
		assert.NotContainsf(t, wire, "0123456789",
			"the excess body reached the wire — the desync is already done:\n%q", wire)
	})

	t.Run("under-run refused at fin", func(t *testing.T) {
		ex, _ := rawCapture(t)
		require.NoError(t, ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("5")}), false),
			"WriteRequest")

		err := ex.WriteBody(context.Background(), []byte("abc"), true)

		require.ErrorIsf(t, err, http1.ErrInvalidRequest,
			"WriteBody(3 octets under Content-Length 5, fin) = %v, want ErrInvalidRequest", err)
	})

	t.Run("exact match accepted", func(t *testing.T) {
		ex, capture := rawCapture(t)
		require.NoError(t, ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("5")}), false),
			"WriteRequest")

		err := ex.WriteBody(context.Background(), []byte("HELLO"), true)

		require.NoErrorf(t, err, "WriteBody(exact) = %v, want nil", err)
		wire := capture()
		assert.Truef(t, strings.HasSuffix(wire, "HELLO"), "body missing from the wire:\n%q", wire)
	})

	t.Run("split writes summing to the declaration accepted", func(t *testing.T) {
		ex, _ := rawCapture(t)
		require.NoError(t, ex.WriteRequest(context.Background(),
			reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte("5")}), false),
			"WriteRequest")

		err1 := ex.WriteBody(context.Background(), []byte("HE"), false)
		err2 := ex.WriteBody(context.Background(), []byte("LLO"), true)

		require.NoErrorf(t, err1, "WriteBody(part 1) = %v, want nil", err1)
		require.NoErrorf(t, err2, "WriteBody(part 2, fin) = %v, want nil", err2)
	})

	t.Run("chunked body is unaffected", func(t *testing.T) {
		ex, _ := rawCapture(t)
		// No Content-Length → chunked framing → nothing to reconcile against.
		require.NoError(t, ex.WriteRequest(context.Background(), reqCL("POST"), false), "WriteRequest")

		err := ex.WriteBody(context.Background(), []byte("any length at all"), true)

		require.NoErrorf(t, err, "WriteBody(chunked) = %v, want nil", err)
	})
}

// TestConformance_RFC9110_Sec8_6_RequestContentLengthMustBe1DIGIT pins that a
// caller Content-Length is parsed as §8.6's `Content-Length = 1*DIGIT` on the
// bodied path too, not only when no body follows.
//
// A comma-folded "5, 10" is what RFC 9110 §5.3 makes of two field lines, i.e.
// the CL.CL smuggling primitive on one line — and this client's own receive side
// already refuses that shape. Emitting what it would reject on arrival is the
// asymmetry that lets a request smuggle.
func TestConformance_RFC9110_Sec8_6_RequestContentLengthMustBe1DIGIT(t *testing.T) {
	for _, cl := range []string{"5, 10", "5,5", "+5", "-5", "abc", "5x", "", "0x5"} {
		t.Run(cl, func(t *testing.T) {
			ex, capture := rawCapture(t)

			err := ex.WriteRequest(context.Background(),
				reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte(cl)}), false)

			require.ErrorIsf(t, err, http1.ErrInvalidRequest,
				"WriteRequest(Content-Length %q) = %v, want ErrInvalidRequest", cl, err)
			wire := capture()
			assert.Emptyf(t, wire, "a rejected request must put no bytes on the wire, got:\n%q", wire)
		})
	}
	// Over-rejection guard: a plain decimal, with or without surrounding OWS, is
	// legal (a field value's edges are not part of it).
	for _, cl := range []string{"5", " 5 ", "0"} {
		t.Run("accepted "+cl, func(t *testing.T) {
			ex, _ := rawCapture(t)

			err := ex.WriteRequest(context.Background(),
				reqCL("POST", header.Field{Name: []byte("Content-Length"), Value: []byte(cl)}), false)

			require.NoErrorf(t, err, "WriteRequest(Content-Length %q) = %v, want nil", cl, err)
		})
	}
}

// TestConformance_RFC9112_Sec6_3_Rule6_PrematureEOFNotPoolable pins that a body
// that ends before its Content-Length is satisfied leaves the connection
// unusable. RFC 9112 §6.3 rule 6 makes the message incomplete; the stream
// position is then indeterminate, so reuse would begin the next response
// somewhere inside this one, and KeepAlive()'s contract promises it does not.
//
// What this holds down is ReadBodyChunk's deferred condemn, which fires on any
// non-nil err. Worth naming, because it did not use to hold down anything: the
// premature-EOF branch condemned a second time on its own, so the assertion
// below passed with either site deleted and caught neither. The duplicate is
// gone; deleting what remains fails this test.
func TestConformance_RFC9112_Sec6_3_Rule6_PrematureEOFNotPoolable(t *testing.T) {
	ex := wireExchange(t, "GET", "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nHELLO")
	_, _, rerr := ex.ReadResponse(context.Background())
	require.NoError(t, rerr, "ReadResponse")

	var err error
	for {
		var done bool
		_, done, err = ex.ReadBodyChunk(make([]byte, 64))
		if err != nil || done {
			break
		}
	}

	require.Error(t, err, "a body ending before Content-Length must report an error")
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true after a premature EOF, want false — the stream "+
			"position is indeterminate, so the connection must not be reused (RFC 9112 §6.3 rule 6)")
}
