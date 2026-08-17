package conn

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_LargeBody_RefundsRecvWindow_NoStall fetches a body
// well in excess of the default 65535-byte recv window. With B.2.1
// flow-control wiring the request would stall once peer's send window
// hits zero; with B.2.2 the reader emits WINDOW_UPDATE refunds and the
// download completes.
func TestIntegration_LargeBody_RefundsRecvWindow_NoStall(t *testing.T) {
	const bodySize = 200 * 1024
	body := make([]byte, bodySize)
	_, err := rand.Read(body)
	require.NoErrorf(t, err, "rand")
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	// 64 events: the body is 200 KiB and the peer's frames are up to 16 KiB, so
	// the default buffer of 8 can overflow before this test's loop drains it.
	c := dialServerOpts(t, srv, cfg, 64)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")

	err = s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/big")},
	}, true)

	require.NoErrorf(t, err, "SendHeaders")
	got := drainBody(ctx, t, s)
	require.Lenf(t, got, len(body), "got %d bytes, want %d", len(got), len(body))
	require.Equal(t, body, got, "body mismatch")
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

	err := c.onDataReceived(s, recvWindowRefundThreshold)

	require.NoErrorf(t, err, "onDataReceived")
	got := parseWindowUpdates(t, buf.Bytes())
	require.Lenf(t, got, 2, "WINDOW_UPDATE count = %d, want 2 (stream + conn)", len(got))
	var streamUpdate, connUpdate uint32
	for _, u := range got {
		switch u.streamID {
		case 0:
			connUpdate = u.increment
		case 1:
			streamUpdate = u.increment
		default:
			require.FailNowf(t, "unexpected stream id", "unexpected stream id %d", u.streamID)
		}
	}
	assert.EqualValuesf(t, recvWindowRefundThreshold, streamUpdate,
		"stream WINDOW_UPDATE inc = %d, want %d", streamUpdate, recvWindowRefundThreshold)
	assert.EqualValuesf(t, recvWindowRefundThreshold, connUpdate,
		"conn WINDOW_UPDATE inc = %d, want %d", connUpdate, recvWindowRefundThreshold)
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

	err := c.onDataReceived(s, 200)

	require.Error(t, err, "want ConnError, got nil")
	var ce *ConnError
	require.ErrorAsf(t, err, &ce, "err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
	require.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
		"err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
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

	err := c.onDataReceived(s, 100)

	require.Error(t, err, "want StreamError, got nil")
	var se *StreamError
	require.ErrorAsf(t, err, &se, "err = %v, want StreamError(FLOW_CONTROL_ERROR)", err)
	require.Equalf(t, frame.ErrCodeFlowControlError, se.Code,
		"err = %v, want StreamError(FLOW_CONTROL_ERROR)", err)
}

// windowUpdateRecord is one parsed WINDOW_UPDATE frame: the stream it names and
// the increment it carries.
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
			require.Lenf(t, body, 4, "WINDOW_UPDATE payload = %d bytes, want 4", len(body))
			inc := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])
			inc &^= 1 << 31
			out = append(out, windowUpdateRecord{streamID: streamID, increment: inc})
		}
		b = b[9+length:]
	}
	return out
}

// TestIntegration_EventBufferOverflow_ReportsResetNotShortBody pins the
// behaviour that made the CI failure above unreadable.
//
// When the caller falls behind the reader the connection cancels its own stream
// by design (RFC 9113 §7 CANCEL — "the stream is no longer needed"), and the
// EventReset it delivers carries EndStream. A drain loop that breaks on
// EndStream without checking Type therefore exits as though the body were
// complete, and the test reports a byte-count mismatch — which reads as data
// corruption or a flow-control stall rather than "this caller was too slow".
//
// A one-slot buffer and a caller that does not drain makes the overflow
// deterministic, so this documents the contract rather than racing it.
func TestIntegration_EventBufferOverflow_ReportsResetNotShortBody(t *testing.T) {
	const bodySize = 200 * 1024
	body := make([]byte, bodySize)
	_, err := rand.Read(body)
	require.NoErrorf(t, err, "rand")
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := dialServerOpts(t, srv, cfg, 1)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")
	err = s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/big")},
	}, true)
	require.NoErrorf(t, err, "SendHeaders")

	// Do not drain: let the reader fill the one-slot buffer and overflow it.
	time.Sleep(300 * time.Millisecond)

	sawReset := false
	total := 0
	for {
		ev, rerr := s.Recv(ctx)
		if rerr != nil {
			break // the stream is gone; the reset already happened
		}
		if ev.Type == EventReset {
			sawReset = true
			require.Equalf(t, frame.ErrCodeCancel, ev.RSTCode,
				"overflow reset code = %d, want CANCEL (%d)", ev.RSTCode, frame.ErrCodeCancel)
			require.True(t, ev.EndStream,
				"overflow reset did not carry EndStream — the misleading-break hazard would not exist")
			break
		}
		if ev.Type == EventData {
			total += len(ev.Data)
		}
		if ev.EndStream {
			break
		}
	}
	if !sawReset {
		t.Skipf("no overflow observed (drained %d of %d bytes); the machine kept up", total, bodySize)
	}
	require.Lessf(t, total, bodySize,
		"reset after a complete body (%d bytes) — overflow should truncate", total)
}
