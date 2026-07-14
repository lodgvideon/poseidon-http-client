package http3

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// doStreamHead drives DoStream against a fake conn carrying the given response
// chunks, returning the response head and the streaming body. It fails the test on
// any DoStream error.
func doStreamHead(t *testing.T, conn *fakeConn, req *Request) (*Response, ResponseBody) {
	t.Helper()
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	if err != nil {
		t.Fatalf("NewClientFake: %v", err)
	}
	resp, body, err := client.DoStream(context.Background(), req)
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
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
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}

	ctx := context.Background()
	ev1, err := body.Next(ctx)
	if err != nil || !bytes.Equal(ev1.Data, []byte("chunk-one")) || ev1.End {
		t.Fatalf("Next#1 = {%q end=%v} err=%v, want {chunk-one end=false}", ev1.Data, ev1.End, err)
	}
	// The buffered-body accumulator must stay empty on the streaming path — the
	// proof that the whole body is not retained.
	if br, ok := body.(*BodyReader); ok {
		if br.rb.body != nil {
			t.Fatalf("streaming path buffered %d body bytes, want 0", len(br.rb.body))
		}
	}
	ev2, err := body.Next(ctx)
	if err != nil || !bytes.Equal(ev2.Data, []byte("chunk-two")) {
		t.Fatalf("Next#2 = {%q} err=%v, want chunk-two", ev2.Data, err)
	}
	ev3, err := body.Next(ctx)
	if err != nil || ev3.Data != nil || ev3.Trailers != nil || !ev3.End {
		t.Fatalf("Next#3 = {data=%q trailers=%v end=%v} err=%v, want clean End", ev3.Data, ev3.Trailers, ev3.End, err)
	}
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

	ev, err := body.Next(ctx)
	if err != nil || !bytes.Equal(ev.Data, []byte("payload")) {
		t.Fatalf("Next(data) = {%q} err=%v, want payload", ev.Data, err)
	}
	ev, err = body.Next(ctx)
	if err != nil {
		t.Fatalf("Next(trailers): %v", err)
	}
	if len(ev.Trailers) != 1 || string(ev.Trailers[0].Name) != "x-checksum" || string(ev.Trailers[0].Value) != "abc" {
		t.Fatalf("trailers = %+v, want x-checksum=abc", ev.Trailers)
	}
	if !ev.End {
		t.Fatal("trailer event End = false, want true")
	}
	if tr := body.Trailers(); len(tr) != 1 || string(tr[0].Name) != "x-checksum" {
		t.Fatalf("Trailers() = %+v, want x-checksum", tr)
	}
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
	var rst *StreamResetError
	for {
		ev, err := body.Next(context.Background())
		if err != nil {
			if !errors.As(err, &rst) {
				t.Fatalf("Next err = %v, want *StreamResetError", err)
			}
			break
		}
		if ev.End {
			t.Fatal("clean End on a reset stream, want *StreamResetError")
		}
	}
	if rst.Code != H3RequestRejected || !rst.Retryable() {
		t.Fatalf("reset code = %#x retryable=%v, want H3_REQUEST_REJECTED retryable", rst.Code, rst.Retryable())
	}
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
	if ev, err := body.Next(context.Background()); err != nil || !bytes.Equal(ev.Data, []byte("abc")) {
		t.Fatalf("Next(data) = {%q} err=%v, want abc", ev.Data, err)
	}
	if _, err := body.Next(context.Background()); !errors.Is(err, ErrH3Message) {
		t.Fatalf("Next(end) err = %v, want ErrH3Message", err)
	}
	if conn.req.resetCode != H3MessageError {
		t.Fatalf("abort code = %#x, want H3_MESSAGE_ERROR", conn.req.resetCode)
	}
}

// TestClient_DoStream_CloseAbortsStream checks that abandoning the body before the
// end aborts the request stream with H3_REQUEST_CANCELLED.
func TestClient_DoStream_CloseAbortsStream(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	data := AppendData(nil, []byte("streamed-body"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers, data}, fin: true}}

	_, body := doStreamHead(t, conn, &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	// Read one chunk then abandon.
	if _, err := body.Next(context.Background()); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !conn.req.reset || conn.req.resetCode != H3RequestCancelled {
		t.Fatalf("Close abort = (reset=%v code=%#x), want reset with H3_REQUEST_CANCELLED", conn.req.reset, conn.req.resetCode)
	}
	// Idempotent.
	if err := body.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestClient_DoStream_NoFinalResponse checks that a request stream that ends with
// only a 1xx interim response (no final response) is malformed on the streaming
// path, exactly as on the buffered path.
func TestClient_DoStream_NoFinalResponse(t *testing.T) {
	interim := AppendHeaders(nil, encodeSection(hf(":status", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{interim}, fin: true}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	if err != nil {
		t.Fatalf("NewClientFake: %v", err)
	}
	if _, _, err := client.DoStream(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); !errors.Is(err, ErrH3Message) {
		t.Fatalf("DoStream err = %v, want ErrH3Message", err)
	}
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

	bufClient, _ := NewClientFake(makeConn(), []Setting{{SettingQPACKMaxTableCapacity, 0}})
	_, want, err := bufClient.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	if err != nil {
		t.Fatalf("buffered Do: %v", err)
	}

	_, body := doStreamHead(t, makeConn(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	defer func() { _ = body.Close() }()
	var got []byte
	for {
		ev, err := body.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, ev.Data...)
		if ev.End {
			break
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("streamed body = %q, want %q (buffered)", got, want)
	}
}
