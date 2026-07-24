package conn

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Batch 1 — frame-error SCOPE and typed error CODE at the reader-loop boundary.
//
// The Framer surfaces a malformed frame as a plain sentinel (ErrZeroIncrement,
// ErrPriorityWrongLength, ErrInvalidStreamID, …). The reader loop maps each to a
// *StreamError or *ConnError with the RFC 9113 scope and error code, so a
// single-stream fault (a stream WINDOW_UPDATE with a 0 increment, a wrong-length
// PRIORITY) resets ONE stream and leaves the pooled connection — and every other
// in-flight request — alive, and a connection-scoped fault tears the connection
// down with a typed GOAWAY the peer can read.

// framingProbe captures the client-originated RST_STREAM / GOAWAY error codes.
// Channels (not fields) give a happens-before edge to the asserting goroutine,
// so the reads are -race clean without a separate barrier.
type framingProbe struct {
	nilHandler
	rst  chan frame.ErrCode
	away chan frame.ErrCode
}

func newFramingProbe() *framingProbe {
	return &framingProbe{rst: make(chan frame.ErrCode, 4), away: make(chan frame.ErrCode, 4)}
}

func (p *framingProbe) OnRSTStream(_ frame.FrameHeader, code frame.ErrCode) error {
	p.rst <- code
	return nil
}

func (p *framingProbe) OnGoAway(_ frame.FrameHeader, _ uint32, code frame.ErrCode, _ []byte) error {
	p.away <- code
	return nil
}

// writeRawFrame writes a hand-built frame so a test can send exactly the bytes a
// conformant Framer would refuse to emit (a 0-increment WINDOW_UPDATE, a
// wrong-length PRIORITY, a stream-0 DATA).
func writeRawFrame(w io.Writer, typ frame.FrameType, flags byte, streamID uint32, payload []byte) error {
	n := len(payload)
	hdr := []byte{
		byte(n >> 16), byte(n >> 8), byte(n),
		byte(typ),
		flags,
		byte(streamID >> 24), byte(streamID >> 16), byte(streamID >> 8), byte(streamID),
	}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// recvCode reads one error code from ch or fails the test on timeout. A timeout
// is the RED signal on current code: the mapping does not exist, so the client
// never sends the expected RST_STREAM/GOAWAY.
func recvCode(t *testing.T, what string, ch chan frame.ErrCode) frame.ErrCode {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return 0
	}
}

// openHalfClosedStream opens stream 1 and sends END_STREAM, leaving it
// half-closed(local) and still registered so a later server frame targeting it
// resolves to a live *Stream.
func openHalfClosedStream(ctx context.Context, t *testing.T, c *Conn) *Stream {
	t.Helper()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, reqHeaders(), true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	return s
}

// TestConformance_RFC9113_Sec6_9_1_StreamWindowUpdateZero_StreamError pins that a
// WINDOW_UPDATE with a 0 increment on a STREAM is a stream error of type
// PROTOCOL_ERROR (RFC 9113 §6.9.1: "A receiver MUST treat the receipt of a
// WINDOW_UPDATE frame with a flow-control window increment of 0 as a stream
// error ... of type PROTOCOL_ERROR"), so only that stream is reset and the
// connection survives.
func TestConformance_RFC9113_Sec6_9_1_StreamWindowUpdateZero_StreamError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FrameWindowUpdate, 0, 1, []byte{0, 0, 0, 0}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s := openHalfClosedStream(ctx, t, c)
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventReset || ev.RSTCode != frame.ErrCodeProtocolError {
		t.Fatalf("event = {%s code=%v}, want EventReset PROTOCOL_ERROR", ev.Type, ev.RSTCode)
	}
	if code := recvCode(t, "RST_STREAM", probe.rst); code != frame.ErrCodeProtocolError {
		t.Errorf("RST_STREAM code = %v, want PROTOCOL_ERROR", code)
	}
	if !c.IsAlive() {
		t.Error("connection died after a single-stream WINDOW_UPDATE fault; a stream error must not kill the connection")
	}
	release()
}

// TestConformance_RFC9113_Sec6_9_ConnWindowUpdateZero_ConnError pins that a
// WINDOW_UPDATE with a 0 increment on the CONNECTION (stream 0) is a connection
// error of type PROTOCOL_ERROR (RFC 9113 §6.9: "errors on the connection
// flow-control window MUST be treated as a connection error"), emitting a
// typed GOAWAY.
func TestConformance_RFC9113_Sec6_9_ConnWindowUpdateZero_ConnError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FrameWindowUpdate, 0, 0, []byte{0, 0, 0, 0}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	if code := recvCode(t, "GOAWAY", probe.away); code != frame.ErrCodeProtocolError {
		t.Errorf("GOAWAY code = %v, want PROTOCOL_ERROR", code)
	}
	if aliveWithin(c, false, 2*time.Second) {
		t.Error("connection still alive after a connection-scoped WINDOW_UPDATE fault")
	}
	release()
}

// TestConformance_RFC9113_Sec6_3_PriorityWrongLength_StreamError pins that a
// PRIORITY frame whose length is not 5 octets is a stream error of type
// FRAME_SIZE_ERROR (RFC 9113 §6.3), resetting only the referenced stream.
func TestConformance_RFC9113_Sec6_3_PriorityWrongLength_StreamError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, probe)
		// length=4 (one short of the mandatory 5) on stream 1.
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FramePriority, 0, 1, []byte{0, 0, 0, 0}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s := openHalfClosedStream(ctx, t, c)
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventReset || ev.RSTCode != frame.ErrCodeFrameSizeError {
		t.Fatalf("event = {%s code=%v}, want EventReset FRAME_SIZE_ERROR", ev.Type, ev.RSTCode)
	}
	if code := recvCode(t, "RST_STREAM", probe.rst); code != frame.ErrCodeFrameSizeError {
		t.Errorf("RST_STREAM code = %v, want FRAME_SIZE_ERROR", code)
	}
	if !c.IsAlive() {
		t.Error("connection died after a wrong-length PRIORITY; §6.3 makes it a stream error, not a connection error")
	}
	release()
}

// TestConformance_RFC9113_Sec6_1_DataOnStreamZero_ConnError pins that a DATA
// frame on stream 0 is a connection error of type PROTOCOL_ERROR (RFC 9113 §6.1),
// and that the teardown carries a typed GOAWAY(PROTOCOL_ERROR) rather than a
// silent close.
func TestConformance_RFC9113_Sec6_1_DataOnStreamZero_ConnError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FrameData, 0, 0, []byte("body")) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	if code := recvCode(t, "GOAWAY", probe.away); code != frame.ErrCodeProtocolError {
		t.Errorf("GOAWAY code = %v, want PROTOCOL_ERROR", code)
	}
	if aliveWithin(c, false, 2*time.Second) {
		t.Error("connection still alive after DATA on stream 0")
	}
	release()
}

// aliveWithin polls IsAlive until it equals want or the deadline passes; returns
// the last observed value. Used to assert a connection has (not) died without a
// fixed sleep.
func aliveWithin(c *Conn, want bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		got := c.IsAlive()
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}
