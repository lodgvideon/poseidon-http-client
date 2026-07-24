package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Batch 6 — RFC 9113 §6.10 field-block continuity. A HEADERS or PUSH_PROMISE
// frame that does not set END_HEADERS opens a field block that MUST be followed
// immediately by CONTINUATION frames on the SAME stream until one sets
// END_HEADERS. "A receiver MUST treat the receipt of any other type of frame or a
// frame on a different stream as a connection error … of type PROTOCOL_ERROR",
// and a CONTINUATION observed outside that sequence is the same error. The reader
// tracked the block only for HPACK reassembly and silently accepted interleaving.

// openFieldBlock has the server send a HEADERS frame without END_HEADERS on
// stream 1, opening a field block, then whatever `interleave` writes. It asserts
// the client tears the connection down with a typed GOAWAY(PROTOCOL_ERROR).
func assertFieldBlockViolation(t *testing.T, interleave func(srvFr *frame.Framer, srv net.Conn) error) {
	t.Helper()
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{StreamID: 1, BlockFragment: block, EndHeaders: false})
		})
		<-asyncWrite(func() error { return interleave(srvFr, srv) })
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
		t.Error("connection still alive after a §6.10 field-block violation")
	}
	release()
}

// TestConformance_RFC9113_Sec6_10_InterleavedFrameInFieldBlock_ConnError pins the
// three interleaving shapes: a different frame type on the same stream, a frame
// on a different stream, and an unknown (extension) frame type.
func TestConformance_RFC9113_Sec6_10_InterleavedFrameInFieldBlock_ConnError(t *testing.T) {
	t.Run("different type same stream", func(t *testing.T) {
		assertFieldBlockViolation(t, func(_ *frame.Framer, srv net.Conn) error {
			return writeRawFrame(srv, frame.FrameData, 0, 1, []byte("x"))
		})
	})
	t.Run("frame on different stream", func(t *testing.T) {
		assertFieldBlockViolation(t, func(srvFr *frame.Framer, _ net.Conn) error {
			return srvFr.WriteWindowUpdate(3, 100)
		})
	})
	t.Run("unknown extension frame", func(t *testing.T) {
		// §6.10: extension frames in the middle of a field block are forbidden.
		assertFieldBlockViolation(t, func(_ *frame.Framer, srv net.Conn) error {
			return writeRawFrame(srv, frame.FrameType(0x99), 0, 1, []byte{0, 0, 0, 0})
		})
	})
}

// TestConformance_RFC9113_Sec6_10_UnexpectedContinuation_ConnError pins that a
// CONTINUATION frame observed when no field block is open is a connection error
// PROTOCOL_ERROR.
func TestConformance_RFC9113_Sec6_10_UnexpectedContinuation_ConnError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}})
		// A CONTINUATION with no preceding HEADERS/PUSH_PROMISE-without-END_HEADERS.
		<-asyncWrite(func() error { return srvFr.WriteContinuation(1, true, block) })
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
		t.Error("connection still alive after an unexpected CONTINUATION")
	}
	release()
}

// TestConformance_RFC9113_Sec6_10_SplitHeaderBlock_Accepted is the over-rejection
// guard: a conformant HEADERS(no END_HEADERS)+CONTINUATION(END_HEADERS) response
// is reassembled and delivered, and the connection stays alive.
func TestConformance_RFC9113_Sec6_10_SplitHeaderBlock_Accepted(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		enc := hpack.NewEncoder()
		full := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}})
		// Split the block across HEADERS(no END_HEADERS) + CONTINUATION(END_HEADERS).
		half := len(full) / 2
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{StreamID: 1, BlockFragment: full[:half], EndHeaders: false})
		})
		<-asyncWrite(func() error { return srvFr.WriteContinuation(1, true, full[half:]) })
		<-finish // keep the pipe open so the IsAlive check sees the live conn, not EOF
	})
	defer release()

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
	if err := s.SendHeaders(ctx, reqHeaders(), true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("event = %s, want EventHeaders from a split header block", ev.Type)
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}
	if !c.IsAlive() {
		t.Error("connection died on a conformant split header block")
	}
}
