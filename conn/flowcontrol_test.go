package conn

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestIntegration_LargeBody_RefundsRecvWindow_NoStall fetches a body
// well in excess of the default 65535-byte recv window. With B.2.1
// flow-control wiring the request would stall once peer's send window
// hits zero; with B.2.2 the reader emits WINDOW_UPDATE refunds and the
// download completes.
func TestIntegration_LargeBody_RefundsRecvWindow_NoStall(t *testing.T) {
	const bodySize = 200 * 1024
	body := make([]byte, bodySize)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/big")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	var got []byte
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv after %d bytes: %v", len(got), err)
		}
		if ev.Type == EventData {
			got = append(got, ev.Data...)
		}
		if ev.EndStream {
			break
		}
	}
	if len(got) != len(body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch")
	}
}

// TestConn_OnData_EmitsWindowUpdate_OnceThresholdReached drives the
// reader's flow-control accounting via a net.Pipe peer. After 32 KiB
// of DATA has been consumed by the user, the client must emit two
// WINDOW_UPDATE frames (per-stream + connection).
func TestConn_OnData_EmitsWindowUpdate_OnceThresholdReached(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := &Conn{
		opts:           ConnOptions{}.defaulted(),
		fr:             fr,
		streams:        map[uint32]*Stream{},
		readerDone:     make(chan struct{}),
		connRecvWindow: 65535,
	}
	s := newStream(1, 8, c, 65535)
	c.streams[1] = s

	if err := c.onDataReceived(s, recvWindowRefundThreshold); err != nil {
		t.Fatalf("onDataReceived: %v", err)
	}
	got := parseWindowUpdates(t, buf.Bytes())
	if len(got) != 2 {
		t.Fatalf("WINDOW_UPDATE count = %d, want 2 (stream + conn)", len(got))
	}
	var streamUpdate, connUpdate uint32
	for _, u := range got {
		switch u.streamID {
		case 0:
			connUpdate = u.increment
		case 1:
			streamUpdate = u.increment
		default:
			t.Fatalf("unexpected stream id %d", u.streamID)
		}
	}
	if streamUpdate != recvWindowRefundThreshold {
		t.Fatalf("stream WINDOW_UPDATE inc = %d, want %d", streamUpdate, recvWindowRefundThreshold)
	}
	if connUpdate != recvWindowRefundThreshold {
		t.Fatalf("conn WINDOW_UPDATE inc = %d, want %d", connUpdate, recvWindowRefundThreshold)
	}
}

// TestConn_OnData_PeerOverflowsConnWindow_ReturnsConnError verifies
// the OnData path surfaces a typed ConnError(FLOW_CONTROL_ERROR)
// when the peer exceeds our connection-level recv window.
func TestConn_OnData_PeerOverflowsConnWindow_ReturnsConnError(t *testing.T) {
	c := &Conn{
		opts:           ConnOptions{}.defaulted(),
		streams:        map[uint32]*Stream{},
		readerDone:     make(chan struct{}),
		connRecvWindow: 100,
	}
	s := newStream(1, 8, c, 1<<20)
	c.streams[1] = s

	if err := c.onDataReceived(s, 200); err == nil {
		t.Fatalf("want ConnError, got nil")
	} else {
		var ce *ConnError
		if !errors.As(err, &ce) || ce.Code != frame.ErrCodeFlowControlError {
			t.Fatalf("err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
		}
	}
}

// TestConn_OnData_PeerOverflowsStreamWindow_ReturnsStreamError covers
// the per-stream branch.
func TestConn_OnData_PeerOverflowsStreamWindow_ReturnsStreamError(t *testing.T) {
	c := &Conn{
		opts:           ConnOptions{}.defaulted(),
		streams:        map[uint32]*Stream{},
		readerDone:     make(chan struct{}),
		connRecvWindow: 1 << 20,
	}
	s := newStream(3, 8, c, 50)
	c.streams[3] = s

	if err := c.onDataReceived(s, 100); err == nil {
		t.Fatalf("want StreamError, got nil")
	} else {
		var se *StreamError
		if !errors.As(err, &se) || se.Code != frame.ErrCodeFlowControlError {
			t.Fatalf("err = %v, want StreamError(FLOW_CONTROL_ERROR)", err)
		}
	}
}

// windowUpdateCapture is a frame.Handler that funnels WINDOW_UPDATE
// FrameHeaders out via a channel so a test can synchronize on them.
type windowUpdateRecord struct {
	streamID  uint32
	increment uint32
}

// parseWindowUpdates extracts WINDOW_UPDATE frames from a raw stream
// of HTTP/2 frame bytes and returns each as (streamID, increment).
// Validates the WINDOW_UPDATE wire format (RFC 7540 §6.9): 9-byte
// header + 4-byte payload (top bit reserved).
func parseWindowUpdates(t *testing.T, b []byte) []windowUpdateRecord {
	t.Helper()
	var out []windowUpdateRecord
	for len(b) >= 9 {
		length := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		ftype := b[3]
		streamID := uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])
		streamID &^= 1 << 31
		body := b[9 : 9+length]
		if ftype == 0x8 { // WINDOW_UPDATE
			if len(body) != 4 {
				t.Fatalf("WINDOW_UPDATE payload = %d bytes, want 4", len(body))
			}
			inc := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
			inc &^= 1 << 31
			out = append(out, windowUpdateRecord{streamID: streamID, increment: inc})
		}
		b = b[9+length:]
	}
	return out
}
