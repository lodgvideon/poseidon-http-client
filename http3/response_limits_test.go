package http3

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClient_RejectsOversizedResponseFrame: a single response frame declaring a
// length past the budget is refused before its payload is buffered, and the
// request stream is aborted with H3_EXCESSIVE_LOAD (RFC 9114 §7.1, §8.1).
func TestClient_RejectsOversizedResponseFrame(t *testing.T) {
	// Only the frame header is needed — the cap fires on the declared length before
	// any payload is buffered, so this allocates nothing large.
	huge := AppendFrameHeader(nil, FrameHeaders, maxResponseBytes+1)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{huge}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}

	_, _, doErr := client.Do(context.Background(), req)

	require.ErrorIsf(t, doErr, ErrResponseTooLarge, "Do = %v, want ErrResponseTooLarge", doErr)
	require.Truef(t, conn.req.reset,
		"stream reset = %v, want the request stream aborted: a refused response must not "+
			"leave the server sending into a stream nobody reads", conn.req.reset)
	assert.Equalf(t, H3ExcessiveLoad, conn.req.resetCode,
		"stream reset code %#x, want H3_EXCESSIVE_LOAD (%#x) — with the default code the peer "+
			"cannot tell an over-budget response from an ordinary cancellation",
		conn.req.resetCode, H3ExcessiveLoad)
}

// TestFrameReader_OversizedFrameRefusedBeforeBuffering pins the ORDERING that makes
// a request stream's FrameReader safe against a server that declares a huge frame
// length and then dribbles the payload. ReadFrame consults the cap on the DECLARED
// length the instant the frame header parses — before the "is the payload buffered
// yet" check — so the dribble never begins: the reader errors on the first pass and
// its buffer never grows past the frame header it already held.
//
// Swapping those two checks would silently turn the cap into a no-op against a
// dribbling peer (the reader would return ErrNeedMore and buffer toward the declared
// length forever), and every caller-level test would still pass, because they all
// deliver the payload. Hence a direct test at this level.
func TestFrameReader_OversizedFrameRefusedBeforeBuffering(t *testing.T) {
	var r FrameReader
	r.SetMaxFrameLen(maxResponseBytes)
	// A frame header declaring 2^40 bytes, then a dribble of payload that will never
	// reach the declared length.
	r.Feed(AppendFrameHeader(nil, FrameData, 1<<40))
	header := r.Buffered()
	const dribbles, dribbleSize = 8, 4096

	errs := make([]error, 0, dribbles)
	for i := 0; i < dribbles; i++ {
		_, _, err := r.ReadFrame()
		errs = append(errs, err)
		r.Feed(bytes.Repeat([]byte("x"), dribbleSize)) // the server dribbles on regardless
	}
	grown := r.Buffered() - header

	for i, err := range errs {
		require.ErrorIsf(t, err, ErrH3FrameTooLarge,
			"pass %d: ReadFrame = %v, want ErrH3FrameTooLarge; the reader buffered %d bytes "+
				"toward a declared 2^40-byte frame", i, err, grown)
	}
	// The cap fired before any payload was consumed, so whatever is buffered is only
	// what the peer pushed at us — the reader never grew toward the declared length.
	assert.LessOrEqualf(t, grown, dribbles*dribbleSize,
		"reader buffered %d bytes beyond the frame header, want <= the %d dribbled: "+
			"the cap is being consulted after the payload accumulates", grown, dribbles*dribbleSize)
}

// TestClient_AcceptsLargeSingleDataFrame: a body delivered as one DATA frame up to
// the whole budget is accepted — the per-frame cap must not be tighter than the
// total cap (RFC 9114 places no per-frame size limit on DATA).
func TestClient_AcceptsLargeSingleDataFrame(t *testing.T) {
	saved := maxResponseBytes
	maxResponseBytes = 4096
	defer func() { maxResponseBytes = saved }()
	payload := bytes.Repeat([]byte("x"), 4000) // one DATA frame, comfortably under the cap
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	data := AppendData(nil, payload)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}

	_, body, doErr := client.Do(context.Background(), req)

	require.NoErrorf(t, doErr, "Do = %v, want nil (a single sub-cap DATA frame must be accepted)", doErr)
	assert.Truef(t, bytes.Equal(body, payload), "body len = %d, want %d", len(body), len(payload))
}

// TestClient_RejectsOversizedResponseTotal: header + body payloads that together
// exceed the budget abort the request, even when each frame is under the per-frame
// cap.
func TestClient_RejectsOversizedResponseTotal(t *testing.T) {
	saved := maxResponseBytes
	maxResponseBytes = 8
	defer func() { maxResponseBytes = saved }()
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	d1 := AppendData(nil, []byte("012345")) // 6 bytes, under the 8-byte per-frame cap
	d2 := AppendData(nil, []byte("678901")) // 6 more → cumulative over the cap
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(append(headers, d1...), d2...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}

	_, _, doErr := client.Do(context.Background(), req)

	assert.ErrorIsf(t, doErr, ErrResponseTooLarge,
		"Do = %v, want ErrResponseTooLarge — the cap is on what one response RETAINS, so "+
			"per-frame-legal chunks that add up past it must still be refused", doErr)
}

// TestClient_RejectsInterimFlood: a server that streams 1xx informational
// responses without ever sending a final response cannot grow the interim buffer
// without bound, even with empty frames that add no bytes (RFC 9114 §4.1).
func TestClient_RejectsInterimFlood(t *testing.T) {
	var chunk []byte
	for i := 0; i <= maxInterimResponses; i++ { // one past the cap
		chunk = append(chunk, AppendHeaders(nil, encodeSection(hf(":status", "103")))...)
	}
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{chunk}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}

	_, _, doErr := client.Do(context.Background(), req)

	assert.ErrorIsf(t, doErr, ErrResponseTooLarge,
		"Do = %v, want ErrResponseTooLarge — a 1xx costs no retained bytes, so only the "+
			"count cap stands between a flood and unbounded memory", doErr)
}
