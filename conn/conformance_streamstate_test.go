package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Batch 7 — RFC 9113 §5.1 / §6.4 stream-state rules on the receive side.
//
// An "idle" stream is one that has never been opened — a client-initiated (odd)
// id the client has not allocated, or a server-initiated (even) id no
// PUSH_PROMISE reserved. Any frame other than HEADERS/PRIORITY on an idle stream,
// a HEADERS on an idle server-initiated stream, and a RST_STREAM naming an idle
// stream are each a connection error PROTOCOL_ERROR (§5.1, §6.4). A DATA frame on
// a stream that has already received END_STREAM (half-closed(remote)) is a stream
// error STREAM_CLOSED (§5.1, §6.1). The reader previously treated an unknown
// stream id as a silent drop and never checked half-closed(remote).

// assertIdleStreamConnError drives a server that sends `send` — a frame targeting
// an idle stream — and asserts the client tears the connection down with a typed
// GOAWAY(PROTOCOL_ERROR).
func assertIdleStreamConnError(t *testing.T, send func(srvFr *frame.Framer, srv net.Conn) error) {
	t.Helper()
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return send(srvFr, srv) })
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
		t.Error("connection still alive after a frame on an idle stream")
	}
	release()
}

// TestConformance_RFC9113_Sec5_1_FrameOnIdleStream_ConnError pins the idle-stream
// connection errors: DATA, RST_STREAM, and WINDOW_UPDATE on an idle stream, and a
// HEADERS on an idle server-initiated (even) stream.
func TestConformance_RFC9113_Sec5_1_FrameOnIdleStream_ConnError(t *testing.T) {
	t.Run("data on idle even stream", func(t *testing.T) {
		assertIdleStreamConnError(t, func(_ *frame.Framer, srv net.Conn) error {
			return writeRawFrame(srv, frame.FrameData, 0, 2, []byte("x"))
		})
	})
	t.Run("rst_stream on idle stream", func(t *testing.T) {
		assertIdleStreamConnError(t, func(srvFr *frame.Framer, _ net.Conn) error {
			return srvFr.WriteRSTStream(5, frame.ErrCodeCancel)
		})
	})
	t.Run("window_update on idle stream", func(t *testing.T) {
		assertIdleStreamConnError(t, func(srvFr *frame.Framer, _ net.Conn) error {
			return srvFr.WriteWindowUpdate(5, 100)
		})
	})
	t.Run("headers on idle server-initiated stream", func(t *testing.T) {
		assertIdleStreamConnError(t, func(srvFr *frame.Framer, _ net.Conn) error {
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}})
			return srvFr.WriteHeaders(frame.WriteHeadersParams{StreamID: 2, BlockFragment: block, EndHeaders: true})
		})
	})
}

// TestConformance_RFC9113_Sec5_1_PriorityOnIdleStream_Accepted is the
// over-rejection guard: §5.1 exempts PRIORITY (and HEADERS) — a PRIORITY frame on
// an idle stream must NOT tear the connection down.
func TestConformance_RFC9113_Sec5_1_PriorityOnIdleStream_Accepted(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return srvFr.WritePriority(5, frame.Priority{Weight: 16}) })
		// A PING after; if the conn is alive the client echoes the ACK, proving the
		// PRIORITY was tolerated.
		<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{9}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	// No GOAWAY should arrive; the connection stays alive.
	select {
	case code := <-probe.away:
		t.Errorf("connection torn down (GOAWAY %v) by a PRIORITY on an idle stream", code)
	case <-time.After(300 * time.Millisecond):
	}
	if !c.IsAlive() {
		t.Error("connection died after a PRIORITY frame on an idle stream")
	}
	release()
}

// TestConformance_RFC9113_Sec5_1_DataOnHalfClosedRemote_StreamClosed pins that a
// DATA frame on a stream already in half-closed(remote) — the server sent
// END_STREAM while the client is still uploading — is a stream error STREAM_CLOSED
// that resets only that stream and leaves the connection alive.
func TestConformance_RFC9113_Sec5_1_DataOnHalfClosedRemote_StreamClosed(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}})
		// Full response with END_STREAM → the client's stream 1 becomes
		// half-closed(remote) (its upload is still open).
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{StreamID: 1, BlockFragment: block, EndHeaders: true, EndStream: true})
		})
		// Now a DATA frame on that half-closed(remote) stream → STREAM_CLOSED.
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FrameData, 0, 1, []byte("late")) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	// endStream=false leaves our upload open → half-closed(remote) after the
	// server's END_STREAM, not fully closed.
	if err := s.SendHeaders(ctx, reqHeaders(), false); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	// First the response headers (END_STREAM), then a reset for the late DATA.
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv headers: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("event = %s, want EventHeaders", ev.Type)
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}
	if code := recvCode(t, "RST_STREAM", probe.rst); code != frame.ErrCodeStreamClosed {
		t.Errorf("RST_STREAM code = %v, want STREAM_CLOSED", code)
	}
	if !c.IsAlive() {
		t.Error("connection died after a DATA frame on a half-closed(remote) stream; §5.1 makes it a stream error")
	}
	release()
}
