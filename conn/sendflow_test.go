package conn

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestIntegration_LargePOST_RespectsPeerSendWindow uploads a body
// significantly larger than the default 65535-byte send window. It
// would have either deadlocked (peer never granted more credit) or
// triggered a FLOW_CONTROL_ERROR connection abort under B.2.2;
// B.2.3 chunks + waits for WINDOW_UPDATE.
func TestIntegration_LargePOST_RespectsPeerSendWindow(t *testing.T) {
	const bodySize = 200 * 1024
	body := make([]byte, bodySize)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if len(got) != len(body) {
			t.Errorf("server received %d bytes, want %d", len(got), len(body))
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/upload")},
	}, false); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if err := s.SendData(ctx, body, true); err != nil {
		t.Fatalf("SendData: %v", err)
	}
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.EndStream {
			break
		}
	}
}

// TestConn_AcquireSendCredits_BlocksUntilWindowUpdate verifies that an
// acquireSendCredits call blocks while the conn-level send window is
// zero and unblocks when onWindowUpdate(0, inc) is called from another
// goroutine.
func TestConn_AcquireSendCredits_BlocksUntilWindowUpdate(t *testing.T) {
	c := newOutFCConn(0, 0)
	s := newStream(1, 8, c, 65535)
	c.streams[1] = s
	s.sendWindow = 1024 // stream window OK; conn window is 0

	type result struct {
		n   int
		err error
	}
	out := make(chan result, 1)
	go func() {
		n, err := c.acquireSendCredits(context.Background(), s, 100)
		out <- result{n: n, err: err}
	}()

	select {
	case r := <-out:
		t.Fatalf("acquire returned without window update: n=%d err=%v", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := c.onWindowUpdate(0, 200); err != nil {
		t.Fatalf("onWindowUpdate: %v", err)
	}

	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("acquire err = %v", r.err)
		}
		if r.n != 100 {
			t.Fatalf("acquire n = %d, want 100", r.n)
		}
	case <-time.After(time.Second):
		t.Fatalf("acquire did not return after window update")
	}
}

// TestConn_AcquireSendCredits_HonorsCtxCancel verifies that ctx
// cancellation wakes a blocked acquireSendCredits call.
func TestConn_AcquireSendCredits_HonorsCtxCancel(t *testing.T) {
	c := newOutFCConn(0, 0)
	s := newStream(1, 8, c, 65535)
	c.streams[1] = s
	s.sendWindow = 1024

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() {
		_, err := c.acquireSendCredits(ctx, s, 100)
		out <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-out:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("acquire did not honor ctx cancel")
	}
}

// TestConn_OnWindowUpdate_OverflowsConn_ReturnsConnError covers the
// 2^31-1 cap on the connection send window.
func TestConn_OnWindowUpdate_OverflowsConn_ReturnsConnError(t *testing.T) {
	c := newOutFCConn(0, 1<<31-1)
	if err := c.onWindowUpdate(0, 1); err == nil {
		t.Fatalf("want ConnError, got nil")
	} else {
		var ce *ConnError
		if !errors.As(err, &ce) || ce.Code != frame.ErrCodeFlowControlError {
			t.Fatalf("err = %v, want ConnError(FLOW_CONTROL_ERROR)", err)
		}
	}
}

// TestConn_OnWindowUpdate_OverflowsStream_ReturnsStreamError covers
// the per-stream 2^31-1 cap.
func TestConn_OnWindowUpdate_OverflowsStream_ReturnsStreamError(t *testing.T) {
	c := newOutFCConn(0, 0)
	s := newStream(3, 8, c, 65535)
	s.sendWindow = 1<<31 - 1
	c.streams[3] = s
	if err := c.onWindowUpdate(3, 1); err == nil {
		t.Fatalf("want StreamError, got nil")
	} else {
		var se *StreamError
		if !errors.As(err, &se) || se.Code != frame.ErrCodeFlowControlError {
			t.Fatalf("err = %v, want StreamError(FLOW_CONTROL_ERROR)", err)
		}
	}
}

// TestConn_WriteData_ChunksByPeerMaxFrameSize verifies that a payload
// larger than the peer's MAX_FRAME_SIZE is split into multiple DATA
// frames on the wire.
func TestConn_WriteData_ChunksByPeerMaxFrameSize(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := &Conn{
		opts: ConnOptions{}.defaulted(),
		fr:   fr,
		peerSettings: frame.SettingsParams{
			Pairs: [16]frame.SettingPair{
				{ID: frame.SettingMaxFrameSize, Value: 4096},
			},
			N: 1,
		},
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		peerConnSendWindow: 65535,
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535
	c.streams[1] = s

	payload := make([]byte, 10000) // 3 chunks of 4096+4096+1808
	if err := c.writeData(context.Background(), s, payload, true); err != nil {
		t.Fatalf("writeData: %v", err)
	}
	frames := parseDataFrames(t, buf.Bytes())
	if len(frames) != 3 {
		t.Fatalf("data frames = %d, want 3", len(frames))
	}
	total := 0
	for i, f := range frames {
		total += int(f.length)
		if i < len(frames)-1 && f.endStream {
			t.Fatalf("intermediate frame %d carries END_STREAM", i)
		}
	}
	if !frames[len(frames)-1].endStream {
		t.Fatalf("final DATA frame missing END_STREAM")
	}
	if total != len(payload) {
		t.Fatalf("total payload = %d, want %d", total, len(payload))
	}
}

// newOutFCConn builds a *Conn wired only enough to exercise the
// outbound flow-control surface (acquireSendCredits, onWindowUpdate).
// streamWindow is unused — caller seeds Stream.sendWindow manually.
func newOutFCConn(_ int32, peerConnSendWindow int32) *Conn {
	c := &Conn{
		opts:               ConnOptions{}.defaulted(),
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		peerConnSendWindow: peerConnSendWindow,
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	return c
}

// dataFrameHeader is the per-frame summary parseDataFrames returns.
type dataFrameHeader struct {
	streamID  uint32
	length    uint32
	endStream bool
}

// parseDataFrames extracts DATA frame summaries from a raw frame
// stream. Validates RFC 7540 §6.1 wire format: 9-byte header, type
// 0x0, payload of `length` bytes (no padding handling — the test
// payload is unpadded).
func parseDataFrames(t *testing.T, b []byte) []dataFrameHeader {
	t.Helper()
	var out []dataFrameHeader
	for len(b) >= 9 {
		length := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		ftype := b[3]
		flags := b[4]
		streamID := uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])
		streamID &^= 1 << 31
		if ftype == 0x0 { // DATA
			out = append(out, dataFrameHeader{
				streamID:  streamID,
				length:    length,
				endStream: flags&0x1 != 0,
			})
		}
		b = b[9+length:]
	}
	return out
}
