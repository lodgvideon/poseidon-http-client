package grpc

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInvokeInto_ReusesTheBuffer is the point: a load generator hammering one
// unary method reuses the response buffer instead of allocating per call.
func TestInvokeInto_ReusesTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := bytes.Repeat([]byte{'q'}, 256)
	buf := make([]byte, 0, 256)
	firstCap := cap(buf)

	for i := 0; i < 20; i++ {
		var err error
		buf, err = cc.InvokeInto(ctx, "/bench.Svc/Echo", req, buf[:0], nil)
		require.NoErrorf(t, err, "call %d", i)
		require.Truef(t, bytes.Equal(buf, req), "call %d: echo differs", i)
	}

	assert.Equalf(t, firstCap, cap(buf),
		"buffer capacity grew %d -> %d: the loop reallocated", firstCap, cap(buf))
}

// TestInvokeInto_DiscardsLength pins that only dst's capacity is used, so a
// dirty buffer cannot prepend its old contents to the response.
func TestInvokeInto_DiscardsLength(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	dirty := bytes.Repeat([]byte{'Z'}, 1000)

	got, err := cc.InvokeInto(context.Background(), "/bench.Svc/Echo", []byte("hi"), dirty, nil)

	require.NoError(t, err, "InvokeInto")
	assert.Equalf(t, "hi", string(got),
		"response = %q, want \"hi\" — dst's length or contents survived", got)
}

// TestInvokeInto_TwoMessageAnswerReturnsTheBufferEmpty covers the drain — the
// second read InvokeInto performs to reach the terminal event, which is how a
// unary method answering with two messages is caught.
//
// It replaces a test named for the drain that never called InvokeInto. That one
// opened a stream by hand, pushed the two-message payload with s.s.SendData and
// then called RecvInto and Recv itself: a replica of InvokeInto's shape rather
// than InvokeInto, so no change inside InvokeInto could fail it. Its assertion
// was also conditional on the drain succeeding and re-checked something the
// three lines above it had already required. What it did cover — that Recv does
// not reuse a buffer handed to an earlier RecvInto — is TestRecv_ContractUnchanged's
// job, and that test asserts it unconditionally, on the backing arrays.
//
// What the drain decides is observable at the return, and only there: the call
// must be reported, and the slice handed back must be dst[:0] rather than the
// first message. TestIntegration_UnaryRejectsSecondMessage pins the status;
// nothing pinned the buffer, and a caller looping on one buffer would be handed
// an answer the client has just decided not to trust.
//
// conn.go's drain comment used to say the reason for reading with Recv rather
// than RecvInto(resp) is corruption of the answer about to be returned, and this
// comment used to reply that it would not matter, because the path returns
// resp[:0] either way and the overwritten bytes are never read. Both were wrong,
// and #803 was filed on the second one.
//
// The overwrite lands in the CALLER'S dst array, which the caller still holds
// after the call. The final assertion below is what pins it: with the drain
// changed to RecvInto(resp[:0]) the buffer comes back holding the second
// message's 'B'x64 where the first message's 'A'x64 belongs, 2 runs out of 2.
// Without that assertion the substitution is invisible here.
func TestInvokeInto_TwoMessageAnswerReturnsTheBufferEmpty(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, bytes.Repeat([]byte{'A'}, 64))
		_ = srvWriteMessage(w, bytes.Repeat([]byte{'B'}, 64))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	buf := make([]byte, 0, 512)

	got, err := cc.InvokeInto(t.Context(), "/t.S/Chatty", []byte("x"), buf[:0], nil)

	var st *Status
	require.Truef(t, errors.As(err, &st), "InvokeInto = %v (%T), want a *Status", err, err)
	require.Equalf(t, Internal, st.Code,
		"code = %v, want INTERNAL — a unary method sent two messages", st.Code)
	require.NotNil(t, got, "InvokeInto returned nil, discarding the caller's buffer")
	assert.Emptyf(t, got,
		"InvokeInto returned %d bytes (%q) alongside %v — the documented return on "+
			"error is dst[:0], and a caller that reuses the buffer would carry a "+
			"rejected answer into the next call", len(got), got, st.Code)
	assert.Equalf(t, cap(buf), cap(got),
		"returned capacity %d, want the caller's %d", cap(got), cap(buf))
	assert.Equalf(t, bytes.Repeat([]byte{'A'}, 64), buf[:64],
		"the caller's buffer holds %q, want the FIRST message", buf[:64])
}

// TestInvokeInto_MatchesInvoke pins that the two forms agree on the happy path,
// so routing Invoke through InvokeInto did not move its result.
func TestInvokeInto_MatchesInvoke(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := []byte("payload")

	a, aErr := cc.Invoke(ctx, "/bench.Svc/Echo", req, nil)
	b, bErr := cc.InvokeInto(ctx, "/bench.Svc/Echo", req, make([]byte, 0, 8), nil)

	require.NoError(t, aErr, "Invoke")
	require.NoError(t, bErr, "InvokeInto")
	assert.Truef(t, bytes.Equal(a, b), "Invoke = %q, InvokeInto = %q", a, b)
}

// TestInvokeInto_ErrorKeepsTheBuffer pins the same decision RecvInto makes: a
// caller looping unary calls on one buffer must not lose it on the first
// failure.
func TestInvokeInto_ErrorKeepsTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	// Deliberately not buf[:0]. dst arriving with a previous call's bytes still
	// in it is the state a reuse loop actually produces, and the only state in
	// which the truncation InvokeInto documents is observable at all: hand it a
	// zero-length slice and the length check below passes with the truncation
	// removed.
	buf := append(make([]byte, 0, 512), bytes.Repeat([]byte{'Z'}, 300)...)

	// A method name without a leading slash is rejected before any I/O.
	got, err := cc.InvokeInto(context.Background(), "bad-method", nil, buf, nil)

	require.Error(t, err, "InvokeInto accepted a malformed method name")
	require.NotNil(t, got, "InvokeInto returned nil on error, discarding the caller's buffer")
	// Length as well as capacity, the way TestRecvInto_TerminalErrorKeepsTheBuffer
	// checks it. Only the pre-I/O error returns are exposed here — every later one
	// goes through RecvInto, which truncates dst itself — so without this a caller
	// doing `buf, err = cc.InvokeInto(...); if err != nil { continue }` would carry
	// the previous call's bytes into the next iteration.
	assert.Emptyf(t, got,
		"InvokeInto returned %d bytes on an error raised before any I/O, want dst[:0]", len(got))
	assert.Equalf(t, cap(buf), cap(got),
		"returned capacity %d, want the caller's %d", cap(got), cap(buf))
}
