package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	code := recvCode(t, "GOAWAY", probe.away)

	assert.Equalf(t, frame.ErrCodeProtocolError, code, "GOAWAY code = %v, want PROTOCOL_ERROR", code)
	assert.False(t, aliveWithin(c, false, 2*time.Second),
		"connection still alive after a field-block continuity violation (RFC 9113 §6.10)")
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
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	code := recvCode(t, "GOAWAY", probe.away)

	assert.Equalf(t, frame.ErrCodeProtocolError, code, "GOAWAY code = %v, want PROTOCOL_ERROR", code)
	assert.False(t, aliveWithin(c, false, 2*time.Second),
		"connection still alive after an unexpected CONTINUATION")
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
	require.NoError(t, err, "NewClientConn")
	defer c.Close()
	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	require.NoError(t, s.SendHeaders(ctx, reqHeaders(), true), "SendHeaders")

	ev, rerr := s.Recv(ctx)
	require.NoError(t, rerr, "Recv")
	require.Equalf(t, EventHeaders, ev.Type,
		"event = %s, want EventHeaders from a split header block", ev.Type)
	ev.Release()
	assert.True(t, c.IsAlive(), "connection died on a conformant split header block")
}

// TestConformance_RFC9113_Sec6_10_SplitBlockStreamError_ConnSurvives is the
// regression guard for a §6.10 continuity-state escalation: a response that spans
// HEADERS+CONTINUATION and is stream-malformed (a forbidden `connection` header)
// is a STREAM error (RFC 9113 §8.1.2.6 — one stream), and the field block was
// closed on the wire by the CONTINUATION's END_HEADERS, so the very next frame
// MUST be processed normally. A regression left the Framer's expectContinuation
// flag stranded (it was updated only on a nil dispatch), so the next frame tripped
// a false interleaving error and killed the whole pooled connection.
func TestConformance_RFC9113_Sec6_10_SplitBlockStreamError_ConnSurvives(t *testing.T) {
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
		// A response carrying a connection-specific header is malformed (a STREAM
		// error), and it spans HEADERS+CONTINUATION.
		full := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
			{Name: []byte("connection"), Value: []byte("keep-alive")},
		})
		half := len(full) / 2
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{StreamID: 1, BlockFragment: full[:half], EndHeaders: false})
		})
		<-asyncWrite(func() error { return srvFr.WriteContinuation(1, true, full[half:]) })
		// The next frame after the stream reset must be handled, not rejected as an
		// interleaving violation.
		<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{7}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")
	defer c.Close()
	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	require.NoError(t, s.SendHeaders(ctx, reqHeaders(), true), "SendHeaders")

	ev, rerr := s.Recv(ctx)
	require.NoError(t, rerr, "Recv")
	require.Equalf(t, EventReset, ev.Type,
		"event = {%s %v}, want EventReset PROTOCOL_ERROR for the malformed response", ev.Type, ev.RSTCode)
	require.Equalf(t, frame.ErrCodeProtocolError, ev.RSTCode,
		"event = {%s %v}, want EventReset PROTOCOL_ERROR for the malformed response", ev.Type, ev.RSTCode)
	// The malformed response cost one stream; the connection — and the frame after
	// it — must survive.
	select {
	case code := <-probe.away:
		assert.Failf(t, "connection torn down",
			"GOAWAY %v after a stream error on a CONTINUATION-completed block", code)
	case <-time.After(300 * time.Millisecond):
	}
	assert.True(t, c.IsAlive(), "connection died after a stream-scoped malformed split response")
	release()
}
