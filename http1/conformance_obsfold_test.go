package http1_test

// Conformance tests for RFC 9112 §5.2 obsolete line folding.
//
//	obs-fold = OWS CRLF RWS
//
// A field line beginning with SP or HTAB continues the PREVIOUS field's value.
// Reading it as a field line of its own lets a server smuggle a header the
// message does not contain — most damagingly a Content-Length, which reframes
// the body and strands the remainder on a connection this client then pools.
//
// The remedy is prescribed per role, and a user agent gets no choice:
//
//	"A user agent that receives an obs-fold in a response message that is not
//	 within a 'message/http' container MUST replace each received obs-fold with
//	 one or more SP octets prior to interpreting the field value."
//
// Rejecting is a server's duty on a request (400) and a proxy's on a response
// (502). We are a user agent: we unfold.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

func fieldValue(hdrs []header.Field, name string) (string, bool) {
	for _, h := range hdrs {
		if string(h.Name) == name {
			return string(h.Value), true
		}
	}
	return "", false
}

// TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleContentLength is the
// attack this fix exists to close.
//
// The wire carries ONE field — X-Junk, whose value continues onto the next line
// — and NO Content-Length. Splitting the continuation on ':' invented one. The
// body must therefore be framed by RFC 9112 §6.3 rule 7 (read until the server
// closes), delivering all 10 octets; framing at 5 leaves "EXTRA" for the next
// response on a pooled connection to read as its status line.
func TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleContentLength(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nX-Junk: a\r\n\tContent-Length: 5\r\n\r\nhelloEXTRA")
	_, hdrs, err := ex.ReadResponse(context.Background())
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if v, ok := fieldValue(hdrs, "content-length"); ok {
		t.Errorf("a content-length field appeared with value %q — the wire had none; "+
			"the obs-fold continuation was read as a field line of its own", v)
	}
	if v, _ := fieldValue(hdrs, "x-junk"); v != "a Content-Length: 5" {
		t.Errorf("x-junk = %q, want %q — the fold must be replaced by SP and joined "+
			"to the previous field's value", v, "a Content-Length: 5")
	}
	if body := readAllTolerant(ex); body != "helloEXTRA" {
		t.Errorf("body = %q, want %q — with no Content-Length the body runs to "+
			"connection close; %d octets were left on the wire for the next "+
			"response to parse as a status line (KeepAlive()=%v)",
			body, "helloEXTRA", len("helloEXTRA")-len(body), ex.KeepAlive())
	}
}

// TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleTransferEncoding is the
// same primitive aimed at the other framing header.
func TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleTransferEncoding(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Junk: a\r\n Transfer-Encoding: chunked\r\n\r\nhelloEXTRA")
	_, hdrs, err := ex.ReadResponse(context.Background())
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if _, ok := fieldValue(hdrs, "transfer-encoding"); ok {
		t.Error("a transfer-encoding field appeared; the wire had none, only a fold")
	}
	// The real Content-Length: 5 governs, so exactly "hello" and no more.
	if body := drainBody(t, ex); body != "hello" {
		t.Errorf("body = %q, want %q (the genuine Content-Length: 5 frames it)", body, "hello")
	}
}

// TestConformance_RFC9112_Sec5_2_ObsFoldUnfoldsToSP pins the unfolding itself,
// across the shapes §5.2's `obs-fold = OWS CRLF RWS` admits: SP or HTAB, one or
// many, and several folds in a row.
func TestConformance_RFC9112_Sec5_2_ObsFoldUnfoldsToSP(t *testing.T) {
	for _, tc := range []struct {
		name, wire, want string
	}{
		{"htab", "X-A: one\r\n\ttwo\r\n", "one two"},
		{"space", "X-A: one\r\n two\r\n", "one two"},
		{"many_spaces", "X-A: one\r\n      two\r\n", "one two"},
		{"multiple_folds", "X-A: one\r\n two\r\n\tthree\r\n", "one two three"},
		{"fold_only_value", "X-A:\r\n one\r\n", "one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", "HTTP/1.1 200 OK\r\n"+tc.wire+"Content-Length: 0\r\n\r\n")
			_, hdrs, err := ex.ReadResponse(context.Background())
			if err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			got, ok := fieldValue(hdrs, "x-a")
			if !ok {
				t.Fatalf("x-a field missing entirely")
			}
			if got != tc.want {
				t.Errorf("x-a = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestConformance_RFC9112_Sec5_2_ObsFoldWithNoPrecedingField rejects a fold that
// has nothing to fold into. `obs-fold = OWS CRLF RWS` only exists after a field
// line, so a block opening with one is not a field block at all — and its
// remainder cannot be trusted to be fields either, which is why the connection
// is condemned rather than the line skipped.
func TestConformance_RFC9112_Sec5_2_ObsFoldWithNoPrecedingField(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\n Content-Length: 5\r\n\r\nhello")
	_, _, err := ex.ReadResponse(context.Background())
	if err == nil {
		t.Fatal("ReadResponse returned nil error for a header block opening with a fold")
	}
	if !errors.Is(err, http1.ErrInvalidHeaderBlock) {
		t.Errorf("error = %v, want it to wrap ErrInvalidHeaderBlock", err)
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after an uninterpretable header block, want false")
	}
}

// TestConformance_RFC9112_Sec5_2_ObsFoldInTrailerSection covers the other block
// consumeHeaders parses.
//
// A folded trailer CANNOT smuggle, and the reason is worth stating because it is
// not obvious: the trailer section is read with consumeHeaders(nil, false), so
// parseBody is false and out is nil. A Content-Length spliced in there reaches
// neither the framing switch nor the caller. The first version of this test
// claimed to pin trailer smuggling and PASSED with the obs-fold bug fully
// restored — it could not fail, because there was nothing there to fail.
//
// What IS reachable: a trailer section opening with a fold has no field line to
// continue, so it is refused — and #257 makes any non-EOF trailer error condemn
// the connection. Both halves are asserted; either alone is a silent desync.
func TestConformance_RFC9112_Sec5_2_ObsFoldInTrailerSection(t *testing.T) {
	t.Run("fold_with_no_preceding_trailer_is_refused", func(t *testing.T) {
		ex := wireExchange(t, "GET",
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
				"5\r\nhello\r\n0\r\n Content-Length: 99\r\n\r\n")
		if _, _, err := ex.ReadResponse(context.Background()); err != nil {
			t.Fatalf("ReadResponse: %v", err)
		}
		buf := make([]byte, 64)
		var err error
		for {
			_, done, e := ex.ReadBodyChunk(buf)
			err = e
			if e != nil || done {
				break
			}
		}
		if !errors.Is(err, http1.ErrInvalidHeaderBlock) {
			t.Errorf("error = %v, want it to wrap ErrInvalidHeaderBlock — a trailer "+
				"section opening with a fold has no field line to continue", err)
		}
		if ex.KeepAlive() {
			t.Error("KeepAlive() = true after an uninterpretable trailer section, want false")
		}
	})

	t.Run("well_formed_folded_trailer_stays_poolable", func(t *testing.T) {
		// The over-rejection guard. Since #257 condemns the conn on any non-EOF
		// trailer error, an unfolding that errored here would silently halve pool
		// reuse for every server that folds a trailer.
		ex := wireExchange(t, "GET",
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
				"5\r\nhello\r\n0\r\nX-Trailer: a\r\n\tcontinued\r\n\r\n")
		if _, _, err := ex.ReadResponse(context.Background()); err != nil {
			t.Fatalf("ReadResponse: %v", err)
		}
		if body := drainBody(t, ex); body != "hello" {
			t.Errorf("body = %q, want %q", body, "hello")
		}
		if !ex.KeepAlive() {
			t.Error("KeepAlive() = false — a well-formed chunked response with a folded " +
				"trailer is reusable; over-condemning it costs a connection per response")
		}
	})
}
