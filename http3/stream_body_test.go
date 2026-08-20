package http3

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doStreamHead drives DoStream against a fake conn carrying the given response
// chunks, returning the response head and the streaming body. It fails the test on
// any DoStream error.
func doStreamHead(t *testing.T, conn *fakeConn, req *Request) (*Response, ResponseBody) {
	t.Helper()
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	require.NoError(t, err, "NewClientFake over the fake transport")

	resp, body, err := client.DoStream(context.Background(), req)
	require.NoError(t, err, "DoStream up to the response head")
	return resp, body
}

// TestClient_DoStream_IncrementalBody checks that DoStream returns the response
// head after HEADERS and then yields each DATA frame as its own BodyEvent — never
// buffering the whole body — followed by a clean End.
func TestClient_DoStream_IncrementalBody(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	d1 := AppendData(nil, []byte("chunk-one"))
	d2 := AppendData(nil, []byte("chunk-two"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, d1, d2}, fin: true}}
	resp, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	defer func() { _ = body.Close() }()
	// The buffered-body accumulator lives on the concrete reader, and reading it is
	// the only proof that the whole body is not retained.
	br, isReader := body.(*BodyReader)
	require.Truef(t, isReader, "DoStream returned %T, not *BodyReader — the accumulator "+
		"check below cannot see whether the body was buffered whole", body)
	ctx := context.Background()

	// Each payload is copied as it arrives: a DATA chunk aliases the frame reader's
	// buffer and is valid only until the next Next call.
	ev1, err1 := body.Next(ctx)
	got1 := append([]byte(nil), ev1.Data...)
	buffered := len(br.rb.body)
	ev2, err2 := body.Next(ctx)
	got2 := append([]byte(nil), ev2.Data...)
	ev3, err3 := body.Next(ctx)
	got3 := append([]byte(nil), ev3.Data...)

	require.Equalf(t, 200, resp.Status, "status = %d, want 200", resp.Status)
	require.NoErrorf(t, err1, "Next#1 = {%q end=%v} err=%v, want {chunk-one end=false}", got1, ev1.End, err1)
	assert.Equalf(t, []byte("chunk-one"), got1,
		"Next#1 = {%q end=%v}, want {chunk-one end=false}", got1, ev1.End)
	assert.Falsef(t, ev1.End, "Next#1 = {%q end=%v}, want end=false", got1, ev1.End)
	assert.Zerof(t, buffered,
		"streaming path buffered %d body bytes, want 0 — peak retained memory must be one "+
			"frame, not the whole body", buffered)
	require.NoErrorf(t, err2, "Next#2 = {%q} err=%v, want chunk-two", got2, err2)
	assert.Equalf(t, []byte("chunk-two"), got2, "Next#2 = {%q}, want chunk-two", got2)
	require.NoErrorf(t, err3, "Next#3 = {data=%q trailers=%v end=%v} err=%v, want clean End",
		got3, ev3.Trailers, ev3.End, err3)
	assert.Truef(t, got3 == nil && ev3.Trailers == nil && ev3.End,
		"Next#3 = {data=%q trailers=%v end=%v}, want clean End", got3, ev3.Trailers, ev3.End)
}

// TestClient_DoStream_Trailers checks the streaming path surfaces a trailer field
// section as a terminal trailer BodyEvent after the body.
func TestClient_DoStream_Trailers(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	data := AppendData(nil, []byte("payload"))
	trailer := AppendHeaders(nil, encodeSection(hf("x-checksum", "abc")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, data, trailer}, fin: true}}
	_, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	defer func() { _ = body.Close() }()
	ctx := context.Background()

	// The DATA payload is copied at once — it aliases the reader's buffer. Trailers
	// own their backing memory, so they survive the next call.
	dataEv, dataErr := body.Next(ctx)
	gotData := append([]byte(nil), dataEv.Data...)
	trailerEv, trailerErr := body.Next(ctx)
	viaAccessor := body.Trailers()

	require.NoErrorf(t, dataErr, "Next(data) = {%q} err=%v, want payload", gotData, dataErr)
	assert.Equalf(t, []byte("payload"), gotData, "Next(data) = {%q}, want payload", gotData)
	require.NoError(t, trailerErr, "Next(trailers) after the body")
	require.Lenf(t, trailerEv.Trailers, 1, "trailers = %+v, want x-checksum=abc", trailerEv.Trailers)
	assert.Equalf(t, "x-checksum", string(trailerEv.Trailers[0].Name),
		"trailers = %+v, want x-checksum=abc", trailerEv.Trailers)
	assert.Equalf(t, "abc", string(trailerEv.Trailers[0].Value),
		"trailers = %+v, want x-checksum=abc", trailerEv.Trailers)
	assert.True(t, trailerEv.End,
		"trailer event End = false, want true — trailers are the last thing on the stream, "+
			"so a caller draining until End would block on a body that already finished")
	require.Lenf(t, viaAccessor, 1, "Trailers() = %+v, want x-checksum", viaAccessor)
	assert.Equalf(t, "x-checksum", string(viaAccessor[0].Name), "Trailers() = %+v, want x-checksum", viaAccessor)
}

// TestClient_DoStream_Reset checks that a server RESET_STREAM mid-body surfaces
// from Next as a *StreamResetError carrying the application code, and that the
// stream is aborted.
func TestClient_DoStream_Reset(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	partial := AppendFrameHeader(nil, FrameData, 10) // declares 10 bytes
	partial = append(partial, []byte("abc")...)      // only 3 arrive, then reset
	conn := &fakeConn{req: &fakeStream{
		recvChunks:    [][]byte{append(headers, partial...)},
		recvReset:     true,
		recvResetCode: H3RequestRejected,
	}}
	_, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	defer func() { _ = body.Close() }()

	// The declared-but-incomplete DATA frame is not yet a full frame, so Next drives
	// the loop into the reset.
	var termErr error
	cleanEnd := false
	for {
		ev, err := body.Next(context.Background())
		if err != nil {
			termErr = err
			break
		}
		if ev.End {
			cleanEnd = true
			break
		}
	}

	require.False(t, cleanEnd, "clean End on a reset stream, want *StreamResetError")
	var rst *StreamResetError
	require.Truef(t, errors.As(termErr, &rst), "Next err = %v, want *StreamResetError", termErr)
	assert.Equalf(t, H3RequestRejected, rst.Code,
		"reset code = %#x, want H3_REQUEST_REJECTED (%#x)", rst.Code, H3RequestRejected)
	assert.Truef(t, rst.Retryable(),
		"reset code = %#x retryable=%v, want H3_REQUEST_REJECTED retryable — a request the "+
			"server never processed is the one case a retry is safe", rst.Code, rst.Retryable())
}

// TestClient_DoStream_ContentLengthMismatch checks that the streaming path applies
// the same Content-Length equality rule as buffered Do (RFC 9114 §4.1.2): a body
// shorter than the declared length is malformed at end, and the stream is aborted
// with H3_MESSAGE_ERROR.
func TestClient_DoStream_ContentLengthMismatch(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "5")))
	data := AppendData(nil, []byte("abc")) // 3 bytes ≠ declared 5
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, data}, fin: true}}
	_, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	defer func() { _ = body.Close() }()

	// First event is the 3-byte DATA chunk; the mismatch is only detectable at end.
	dataEv, dataErr := body.Next(context.Background())
	gotData := append([]byte(nil), dataEv.Data...)
	_, endErr := body.Next(context.Background())

	require.NoErrorf(t, dataErr, "Next(data) = {%q} err=%v, want abc", gotData, dataErr)
	assert.Equalf(t, []byte("abc"), gotData, "Next(data) = {%q}, want abc", gotData)
	require.ErrorIsf(t, endErr, ErrH3Message,
		"Next(end) err = %v, want ErrH3Message — a short body must be malformed on the "+
			"streaming path exactly as it is on the buffered one", endErr)
	assert.Equalf(t, H3MessageError, conn.req.resetCode,
		"abort code = %#x, want H3_MESSAGE_ERROR (%#x)", conn.req.resetCode, H3MessageError)
}

// TestClient_DoStream_CloseAbortsStream checks that abandoning the body before the
// end aborts the request stream with H3_REQUEST_CANCELLED.
func TestClient_DoStream_CloseAbortsStream(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	data := AppendData(nil, []byte("streamed-body"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, data}, fin: true}}
	_, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	// Read one chunk then abandon.
	_, nextErr := body.Next(context.Background())
	closeErr := body.Close()
	reset, resetCode := conn.req.reset, conn.req.resetCode
	secondCloseErr := body.Close()

	require.NoError(t, nextErr, "Next over the first body chunk")
	require.NoError(t, closeErr, "Close over an abandoned body")
	assert.Truef(t, reset,
		"Close abort = (reset=%v code=%#x), want reset with H3_REQUEST_CANCELLED — an "+
			"abandoned body leaves the server sending until the stream is reset", reset, resetCode)
	assert.Equalf(t, H3RequestCancelled, resetCode,
		"Close abort = (reset=%v code=%#x), want reset with H3_REQUEST_CANCELLED (%#x)",
		reset, resetCode, H3RequestCancelled)
	assert.NoError(t, secondCloseErr, "second Close: Close is documented idempotent")
}

// TestClient_DoStream_NoFinalResponse checks that a request stream that ends with
// only a 1xx interim response (no final response) is malformed on the streaming
// path, exactly as on the buffered path.
func TestClient_DoStream_NoFinalResponse(t *testing.T) {
	interim := AppendHeaders(nil, encodeSection(hf(":status", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{interim}, fin: true}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.DoStream(context.Background(),
		&Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Message,
		"DoStream err = %v, want ErrH3Message — a stream that ends after only an "+
			"informational response carries no response head to hand back", doErr)
}

// TestClient_DoStream_MatchesBufferedBody checks the streaming body reassembled
// chunk-by-chunk equals what buffered Do returns for the same response.
func TestClient_DoStream_MatchesBufferedBody(t *testing.T) {
	makeConn := func() *fakeConn {
		headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "11")))
		d1 := AppendData(nil, []byte("hello "))
		d2 := AppendData(nil, []byte("world"))
		return &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, d1, d2}, fin: true}}
	}
	req := &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}
	bufClient, err := NewClientFake(makeConn(), []Setting{{SettingQPACKMaxTableCapacity, 0}})
	require.NoError(t, err, "NewClientFake for the buffered arm")
	_, want, err := bufClient.Do(context.Background(), req)
	require.NoError(t, err, "buffered Do, the reference the streamed body is compared against")
	_, body := doStreamHead(t, makeConn(), req)
	defer func() { _ = body.Close() }()

	// append copies each chunk out before the next Next invalidates it.
	var got []byte
	var drainErr error
	for {
		ev, nerr := body.Next(context.Background())
		if nerr != nil {
			drainErr = nerr
			break
		}
		got = append(got, ev.Data...)
		if ev.End {
			break
		}
	}

	require.NoError(t, drainErr, "Next while draining the streamed body")
	assert.Truef(t, bytes.Equal(got, want),
		"streamed body = %q, want %q (buffered) — the two entry points must not disagree "+
			"about the bytes a response carried", got, want)
}

// TestClient_DataAfterTrailers_PathsDiverge is a drift tripwire, not an
// endorsement. The same bytes — HEADERS(:status 200) DATA HEADERS(trailer) DATA,
// FIN — get two different answers depending on which entry point read them
// (#816):
//
//   - buffered Do consumes every frame to end of stream, so dispatchFrame sees
//     the DATA after the trailer section and makes it a connection error of type
//     H3_FRAME_UNEXPECTED (RFC 9114 §4.1: nothing may follow the trailers);
//   - DoStream never reaches that frame. BodyReader.Next checks
//     `trailersSeen && !emittedTrailers` before reading another frame and finish()
//     sets done, so the remaining buffered bytes are never parsed — the caller is
//     told the body ended normally, the stream is not reset and the connection
//     stays open. The §7.1 truncated-final-frame check recvStep performs is
//     likewise skipped after trailers.
//
// A mutation cannot find this: both paths share dispatchFrame, so the RULE is well
// covered and only the streaming path's REACHABILITY differs. What is recorded
// here is the divergence itself, both halves, so neither can move in silence.
// Whichever way #816 is decided — teach the streaming path to reject, or document
// the shortcut as deliberate — this test goes red and has to be updated, which is
// the point of writing it before the decision rather than after.
func TestClient_DataAfterTrailers_PathsDiverge(t *testing.T) {
	wire := func() []byte {
		headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
		data := AppendData(nil, []byte("payload"))
		trailer := AppendHeaders(nil, encodeSection(hf("x-checksum", "abc")))
		extra := AppendData(nil, []byte("after-trailers")) // illegal: nothing follows the trailers
		return append(append(append(headers, data...), trailer...), extra...)
	}
	req := func() *Request {
		return &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}
	}

	bufConn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{wire()}, fin: true}}
	bufClient, bufErr := NewClientFake(bufConn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	require.NoError(t, bufErr, "NewClientFake over the fake transport")
	_, _, bufDoErr := bufClient.Do(context.Background(), req())

	streamConn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{wire()}, fin: true}}
	_, body := doStreamHead(t, streamConn, req())
	defer func() { _ = body.Close() }()
	var events []BodyEvent
	var streamErr error
	for i := 0; i < 8; i++ {
		ev, err := body.Next(context.Background())
		if err != nil {
			streamErr = err
			break
		}
		events = append(events, ev)
		if ev.End {
			break
		}
	}

	// The buffered half is the RFC answer, and the one the sibling protocols give.
	assert.ErrorIsf(t, bufDoErr, ErrH3Control,
		"buffered Do = %v, want ErrH3Control: a frame after the trailer section is an "+
			"invalid frame sequence, which §4.1 makes a connection error", bufDoErr)
	assert.Equalf(t, H3FrameUnexpected, bufConn.closeCode,
		"buffered close code = %#x, want H3_FRAME_UNEXPECTED (%#x)",
		bufConn.closeCode, H3FrameUnexpected)
	// The streaming half is TODAY'S answer, recorded so a change to it is visible.
	// If this arm fails because the streaming path now rejects, that is #816 being
	// fixed: assert ErrH3Control and H3FrameUnexpected here and delete this note.
	require.NoErrorf(t, streamErr,
		"DoStream err = %v. If this is now ErrH3Control the streaming path has been taught "+
			"to reject bytes after the trailer section (#816) — update this arm to match the "+
			"buffered one above rather than restoring the divergence", streamErr)
	require.NotEmpty(t, events, "the streaming reader produced no events at all")
	last := events[len(events)-1]
	assert.Truef(t, last.End,
		"DoStream ended with End=%v: today the trailer section ends the body and the "+
			"remaining buffered bytes are never parsed", last.End)
	assert.Lenf(t, last.Trailers, 1,
		"trailers = %+v, want the one field the section carried", last.Trailers)
	assert.Equalf(t, uint64(0), streamConn.closeCode,
		"streaming close code = %#x, want 0: the divergence is that the connection is NOT "+
			"closed on this path — recorded, not endorsed (#816)", streamConn.closeCode)
	assert.Falsef(t, streamConn.req.reset,
		"the streaming path reset the request stream; today it does not, and the buffered "+
			"path is the one that treats these bytes as an error")
}
