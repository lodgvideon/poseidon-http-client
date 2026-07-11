package http3

import (
	"bytes"
	"context"
	"errors"
	"testing"
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
	if err != nil {
		t.Fatal(err)
	}
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	if _, _, err := client.Do(context.Background(), req); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("Do = %v, want ErrResponseTooLarge", err)
	}
	if !conn.req.reset || conn.req.resetCode != H3ExcessiveLoad {
		t.Fatalf("stream reset = %v code %#x, want reset with H3_EXCESSIVE_LOAD (%#x)",
			conn.req.reset, conn.req.resetCode, H3ExcessiveLoad)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	_, body, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("Do = %v, want nil (a single sub-cap DATA frame must be accepted)", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("body len = %d, want %d", len(body), len(payload))
	}
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
	if err != nil {
		t.Fatal(err)
	}
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	if _, _, err := client.Do(context.Background(), req); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("Do = %v, want ErrResponseTooLarge", err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	if _, _, err := client.Do(context.Background(), req); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("Do = %v, want ErrResponseTooLarge", err)
	}
}
