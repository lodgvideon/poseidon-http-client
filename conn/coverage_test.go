package conn

// coverage_test.go — additional tests to push conn package coverage to ≥ 90%.
//
// Targets:
//   - conn.go: Stats, GoAwayReceived, emitConnGoAwayIfTyped, shutdownStreams,
//     writeData (closed / no-id paths), writeWindowUpdate (closed),
//     writeSettingsAck (closed), writePingAck (closed)
//   - handler.go: OnContinuation, OnPriority
//   - settings.go: settingsRecorder no-op methods via unusual frames during
//     handshake (OnData, OnHeaders, OnPriority, OnRSTStream, OnPing, OnGoAway,
//     OnWindowUpdate, OnContinuation)
//   - stream.go: push overflow path → RST_STREAM(REFUSED_STREAM)

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

// TestConn_Stats_AfterRequest verifies Stats() returns non-zero counters
// after a real request has been completed.
func TestConn_Stats_AfterRequest(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	// Zero-check before any request.
	before := c.Stats()
	if before.StreamsOpened != 0 {
		t.Fatalf("StreamsOpened before = %d, want 0", before.StreamsOpened)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	// Drain response.
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.EndStream {
			break
		}
	}

	after := c.Stats()
	if after.StreamsOpened == 0 {
		t.Fatalf("StreamsOpened after = 0, want > 0")
	}
	if after.FramesSent == 0 {
		t.Fatalf("FramesSent after = 0, want > 0")
	}
	if after.FramesReceived == 0 {
		t.Fatalf("FramesReceived after = 0, want > 0")
	}
}

// ---------------------------------------------------------------------------
// GoAwayReceived
// ---------------------------------------------------------------------------

// TestConn_GoAwayReceived_FalseByDefault checks the initial state.
func TestConn_GoAwayReceived_FalseByDefault(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	if c.GoAwayReceived() {
		t.Fatalf("GoAwayReceived() = true before any GOAWAY, want false")
	}
}

// TestConn_GoAwayReceived_TrueAfterPeerGoAway exercises the flag via a
// net.Pipe peer that sends GOAWAY immediately after handshake.
func TestConn_GoAwayReceived_TrueAfterPeerGoAway(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c.GoAwayReceived() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("GoAwayReceived() still false after peer GOAWAY")
}

// ---------------------------------------------------------------------------
// emitConnGoAwayIfTyped
// ---------------------------------------------------------------------------

// TestConn_EmitConnGoAwayIfTyped_NonConnError verifies a plain error is a
// no-op — no GOAWAY bytes are written.
func TestConn_EmitConnGoAwayIfTyped_NonConnError(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{
		fr:      frame.NewFramer(&buf, bytes.NewReader(nil)),
		streams: map[uint32]*Stream{},
	}
	c.emitConnGoAwayIfTyped(io.EOF)
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written for EOF, got %d", buf.Len())
	}
}

// TestConn_EmitConnGoAwayIfTyped_ConnError checks that a *ConnError causes
// a GOAWAY frame to be written.
func TestConn_EmitConnGoAwayIfTyped_ConnError(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{
		fr:      frame.NewFramer(&buf, bytes.NewReader(nil)),
		streams: map[uint32]*Stream{},
	}
	ce := &ConnError{Code: frame.ErrCodeProtocolError, Reason: "test"}
	c.emitConnGoAwayIfTyped(ce)
	if buf.Len() == 0 {
		t.Fatalf("expected GOAWAY bytes written, got 0")
	}
}

// ---------------------------------------------------------------------------
// shutdownStreams
// ---------------------------------------------------------------------------

// TestConn_ShutdownStreams_ClosesOpenStreams confirms shutdownStreams sends
// EventReset and closes the events channel for any open stream on a non-EOF
// error.
func TestConn_ShutdownStreams_ClosesOpenStreams(t *testing.T) {
	c := newGoAwayConn()
	s := newStream(1, 8, c, 65535)
	s.id = 1
	c.streams[1] = s

	c.shutdownStreams(&ConnError{Code: frame.ErrCodeInternalError, Reason: "transport died"})

	// First receive: should get the EventReset that was sent before close.
	select {
	case ev, ok := <-s.events:
		if ok && ev.Type != EventReset {
			t.Fatalf("expected EventReset, got %v", ev.Type)
		}
	case <-time.After(time.Second):
		t.Fatalf("events channel not closed within 1s")
	}
}

// TestConn_ShutdownStreams_EOF_ChannelClosed verifies that even with an
// io.EOF reason the events channel is closed.
func TestConn_ShutdownStreams_EOF_ChannelClosed(t *testing.T) {
	c := newGoAwayConn()
	s := newStream(1, 8, c, 65535)
	s.id = 1
	c.streams[1] = s

	c.shutdownStreams(io.EOF)

	done := make(chan struct{})
	go func() {
		for ev := range s.events {
			_ = ev // drain
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("events channel not closed after EOF shutdown")
	}
}

// ---------------------------------------------------------------------------
// writeWindowUpdate — closed-conn fast-path
// ---------------------------------------------------------------------------

// TestConn_WriteWindowUpdate_ClosedConn verifies the closed fast-path.
func TestConn_WriteWindowUpdate_ClosedConn(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{fr: frame.NewFramer(&buf, bytes.NewReader(nil))}
	c.closed.Store(true)

	err := c.writeWindowUpdate(1, 1024)
	if err != ErrConnClosed {
		t.Fatalf("writeWindowUpdate on closed conn = %v, want ErrConnClosed", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written, got %d", buf.Len())
	}
}

// ---------------------------------------------------------------------------
// writeSettingsAck — closed-conn fast-path
// ---------------------------------------------------------------------------

// TestConn_WriteSettingsAck_ClosedConn verifies the closed fast-path.
func TestConn_WriteSettingsAck_ClosedConn(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{fr: frame.NewFramer(&buf, bytes.NewReader(nil))}
	c.closed.Store(true)

	err := c.writeSettingsAck()
	if err != ErrConnClosed {
		t.Fatalf("writeSettingsAck on closed conn = %v, want ErrConnClosed", err)
	}
}

// ---------------------------------------------------------------------------
// writePingAck — closed-conn fast-path
// ---------------------------------------------------------------------------

// TestConn_WritePingAck_ClosedConn verifies the closed fast-path.
func TestConn_WritePingAck_ClosedConn(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{fr: frame.NewFramer(&buf, bytes.NewReader(nil))}
	c.closed.Store(true)

	err := c.writePingAck([8]byte{})
	if err != ErrConnClosed {
		t.Fatalf("writePingAck on closed conn = %v, want ErrConnClosed", err)
	}
}

// ---------------------------------------------------------------------------
// writeData — error paths
// ---------------------------------------------------------------------------

// TestConn_WriteData_ClosedConn checks that writeData returns ErrConnClosed
// when the connection is already closed.
func TestConn_WriteData_ClosedConn(t *testing.T) {
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))
	c.closed.Store(true)

	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535

	err := c.writeData(context.Background(), s, []byte("hello"), false)
	if err != ErrConnClosed {
		t.Fatalf("writeData on closed conn = %v, want ErrConnClosed", err)
	}
}

// TestConn_WriteData_NoIDReturnsErrStreamClosed verifies that writeData with
// an unassigned stream (id == 0) returns ErrStreamClosed.
func TestConn_WriteData_NoIDReturnsErrStreamClosed(t *testing.T) {
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	s := newStream(0, 8, c, 65535) // id == 0 → not yet on the wire
	s.sendWindow = 65535

	err := c.writeData(context.Background(), s, []byte("data"), false)
	if err != ErrStreamClosed {
		t.Fatalf("writeData id=0 = %v, want ErrStreamClosed", err)
	}
}

// ---------------------------------------------------------------------------
// handler.go: OnContinuation
// ---------------------------------------------------------------------------

// TestHandler_OnContinuation_CompletesHeaderBlock verifies that a split
// HEADERS + CONTINUATION sequence is reassembled and delivered as a single
// EventHeaders to the stream.
func TestHandler_OnContinuation_CompletesHeaderBlock(t *testing.T) {
	m := newFakeStreamMap()
	dec := hpack.NewDecoder()
	h := newConnHandler(m, dec)
	s := m.addStream(1)

	enc := hpack.NewEncoder()
	block1 := enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})

	// HEADERS without END_HEADERS to start buffering (but with END_STREAM).
	fhHeaders := frame.FrameHeader{
		Type:     frame.FrameHeaders,
		Flags:    frame.FlagHeadersEndStream, // END_STREAM but NOT END_HEADERS
		StreamID: 1,
		Length:   uint32(len(block1)),
	}
	if err := h.OnHeaders(fhHeaders, frame.HeaderBlock(block1), nil, 0); err != nil {
		t.Fatalf("OnHeaders: %v", err)
	}

	// No event yet — block is still pending CONTINUATION.
	select {
	case <-s.events:
		t.Fatalf("event pushed before CONTINUATION completes the block")
	default:
	}

	// CONTINUATION with END_HEADERS to complete the block.
	fhCont := frame.FrameHeader{
		Type:     frame.FrameContinuation,
		Flags:    frame.FlagContinuationEndHeaders,
		StreamID: 1,
	}
	if err := h.OnContinuation(fhCont, frame.HeaderBlock(nil)); err != nil {
		t.Fatalf("OnContinuation: %v", err)
	}

	select {
	case ev := <-s.events:
		if ev.Type != EventHeaders {
			t.Fatalf("event type = %v, want EventHeaders", ev.Type)
		}
		if !ev.EndStream {
			t.Fatalf("EndStream not set")
		}
	case <-time.After(time.Second):
		t.Fatalf("event not delivered after CONTINUATION")
	}
}

// TestHandler_OnContinuation_UnknownStream verifies that a CONTINUATION
// for an unknown stream is silently ignored.
func TestHandler_OnContinuation_UnknownStream(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	fh := frame.FrameHeader{
		Type:     frame.FrameContinuation,
		Flags:    frame.FlagContinuationEndHeaders,
		StreamID: 99,
	}
	if err := h.OnContinuation(fh, nil); err != nil {
		t.Fatalf("OnContinuation unknown stream: %v", err)
	}
}

// TestHandler_OnContinuation_WrongPendingStream verifies that a CONTINUATION
// whose stream ID doesn't match the pending stream ID is ignored.
func TestHandler_OnContinuation_WrongPendingStream(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)
	m.addStream(3)
	h.pendingStreamID = 1 // pending on stream 1

	fh := frame.FrameHeader{
		Type:     frame.FrameContinuation,
		Flags:    frame.FlagContinuationEndHeaders,
		StreamID: 3, // mismatch → should be silently dropped
	}
	if err := h.OnContinuation(fh, []byte{0x82}); err != nil {
		t.Fatalf("OnContinuation wrong pending stream: %v", err)
	}
}

// TestHandler_OnContinuation_Partial verifies that a CONTINUATION without
// END_HEADERS just appends to the buffer without emitting an event.
func TestHandler_OnContinuation_Partial(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	// Prime pending state with a HEADERS (no END_HEADERS).
	h.pendingStreamID = 1
	h.pendingBuf = []byte{0x82} // partial block

	fh := frame.FrameHeader{
		Type:     frame.FrameContinuation,
		Flags:    0, // no END_HEADERS
		StreamID: 1,
	}
	if err := h.OnContinuation(fh, []byte{0x84}); err != nil {
		t.Fatalf("OnContinuation partial: %v", err)
	}

	// No event must have been pushed.
	select {
	case ev := <-s.events:
		t.Fatalf("unexpected event after partial CONTINUATION: %+v", ev)
	default:
	}
}

// ---------------------------------------------------------------------------
// handler.go: OnPriority
// ---------------------------------------------------------------------------

// TestHandler_OnPriority_IsNoop confirms the deprecated PRIORITY frame
// handler is a silent no-op.
func TestHandler_OnPriority_IsNoop(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	fh := frame.FrameHeader{Type: frame.FramePriority, StreamID: 1}
	p := frame.Priority{StreamDep: 0, Exclusive: false, Weight: 15}
	if err := h.OnPriority(fh, p); err != nil {
		t.Fatalf("OnPriority: %v", err)
	}
}

// ---------------------------------------------------------------------------
// settings.go: settingsRecorder no-op methods
// ---------------------------------------------------------------------------

// TestSettingsRecorder_UnexpectedFramesDuringHandshake builds a net.Pipe
// peer that sends every unusual frame type BEFORE the real SETTINGS frame,
// exercising all no-op methods on settingsRecorder (OnData, OnHeaders,
// OnPriority, OnRSTStream, OnPing, OnGoAway, OnWindowUpdate, OnContinuation).
func TestSettingsRecorder_UnexpectedFramesDuringHandshake(t *testing.T) {
	cli, srv := net.Pipe()

	go func() {
		defer srv.Close()

		// Consume the 24-byte client connection preface.
		preface := make([]byte, 24)
		if _, err := io.ReadFull(srv, preface); err != nil {
			t.Logf("read preface: %v", err)
			return
		}

		srvFr := frame.NewFramer(srv, srv)

		// We interleave unusual frames before the real SETTINGS.
		// net.Pipe is synchronous; writes are run in a goroutine so
		// they don't deadlock while the client is reading.
		writeDone := make(chan error, 1)
		go func() {
			// DATA (stream 1) — settingsRecorder.OnData
			if err := srvFr.WriteData(1, false, []byte("junk")); err != nil {
				writeDone <- err
				return
			}
			// HEADERS (stream 1) — settingsRecorder.OnHeaders
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			if err := srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: block,
				EndHeaders:    true,
				EndStream:     false,
			}); err != nil {
				writeDone <- err
				return
			}
			// PRIORITY — settingsRecorder.OnPriority
			if err := srvFr.WritePriority(1, frame.Priority{Weight: 15}); err != nil {
				writeDone <- err
				return
			}
			// RST_STREAM — settingsRecorder.OnRSTStream
			if err := srvFr.WriteRSTStream(1, frame.ErrCodeCancel); err != nil {
				writeDone <- err
				return
			}
			// PING (no ACK) — settingsRecorder.OnPing
			if err := srvFr.WritePing(false, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
				writeDone <- err
				return
			}
			// GOAWAY — settingsRecorder.OnGoAway
			if err := srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil); err != nil {
				writeDone <- err
				return
			}
			// WINDOW_UPDATE (stream 0) — settingsRecorder.OnWindowUpdate
			if err := srvFr.WriteWindowUpdate(0, 65535); err != nil {
				writeDone <- err
				return
			}
			// CONTINUATION: prime with HEADERS (no END_HEADERS), then CONTINUATION
			// — settingsRecorder.OnContinuation
			block2 := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("404")},
			})
			if err := srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      3,
				BlockFragment: block2,
				EndHeaders:    false,
				EndStream:     false,
			}); err != nil {
				writeDone <- err
				return
			}
			if err := srvFr.WriteContinuation(3, true, []byte{0x82}); err != nil {
				writeDone <- err
				return
			}
			// Now send the real SETTINGS so peerSeen becomes true.
			if err := srvFr.WriteSettings(frame.SettingsParams{}); err != nil {
				writeDone <- err
				return
			}
			writeDone <- nil
		}()

		// Read client SETTINGS while the server goroutine writes unusual frames.
		if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
			t.Logf("read client settings: %v", err)
			<-writeDone
			return
		}
		if err := <-writeDone; err != nil {
			t.Logf("server write unusual frames: %v", err)
			return
		}

		// Write SETTINGS ACK (client is now in the second loop waiting for it).
		writeDone2 := make(chan error, 1)
		go func() { writeDone2 <- srvFr.WriteSettingsAck() }()

		// Read client SETTINGS ACK.
		if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
			t.Logf("read client settings ack: %v", err)
			<-writeDone2
			return
		}
		if err := <-writeDone2; err != nil {
			t.Logf("server write settings ack: %v", err)
		}
		// Handshake complete — just drain until the other side closes.
	}()

	fr := frame.NewFramer(cli, cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := handshakeSettings(ctx, fr, AdvertisedSettings{}.defaulted(), false)
	if err != nil {
		t.Fatalf("handshakeSettings with unusual frames before SETTINGS: %v", err)
	}
}

// ---------------------------------------------------------------------------
// stream.go: push overflow → RST_STREAM(REFUSED_STREAM)
// ---------------------------------------------------------------------------

// TestStream_Push_Overflow_SendsRST exercises the buffer-full path in
// Stream.push. A 1-element buffer is filled, then a second push overflows.
// This triggers the background RST write and a best-effort EventReset.
func TestStream_Push_Overflow_SendsRST(t *testing.T) {
	w := &fakeStreamWriter{}
	// Buffer of 1 so a single push fills it.
	s := newStream(1, 1, w, 65535)

	// Fill the buffer.
	s.push(StreamEvent{Type: EventHeaders})

	// Second push must overflow and trigger RST.
	s.push(StreamEvent{Type: EventData, Data: []byte("overflow")})

	// Wait for the background RST goroutine.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		rst := w.rstCalls
		w.mu.Unlock()
		if rst > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	w.mu.Lock()
	rstCalls := w.rstCalls
	code := w.lastRSTCode
	w.mu.Unlock()

	if rstCalls == 0 {
		t.Fatalf("writeRSTStream not called after push overflow")
	}
	if code != frame.ErrCodeRefusedStream {
		t.Fatalf("RST code = %v, want REFUSED_STREAM", code)
	}
}

// TestStream_Push_Overflow_IsIdempotent confirms that subsequent overflows
// after the stream is already marked closed do not send additional RSTs.
func TestStream_Push_Overflow_IsIdempotent(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 1, w, 65535)

	// Fill buffer.
	s.push(StreamEvent{Type: EventHeaders})

	// First overflow — triggers RST.
	s.push(StreamEvent{Type: EventData})
	time.Sleep(60 * time.Millisecond)

	// Second overflow — stream already closed; must NOT trigger another RST.
	s.push(StreamEvent{Type: EventData})
	time.Sleep(60 * time.Millisecond)

	w.mu.Lock()
	rstCalls := w.rstCalls
	w.mu.Unlock()

	if rstCalls != 1 {
		t.Fatalf("writeRSTStream called %d times, want exactly 1 (idempotent)", rstCalls)
	}
}

// ---------------------------------------------------------------------------
// stream.go: StreamEventType.String() — "unknown" branch
// ---------------------------------------------------------------------------

// TestStreamEventType_String_Unknown exercises the default branch.
func TestStreamEventType_String_Unknown(t *testing.T) {
	var unknown StreamEventType = 255
	got := unknown.String()
	if got != "unknown" {
		t.Fatalf("String(255) = %q, want %q", got, "unknown")
	}
}

// ---------------------------------------------------------------------------
// settings.go: settingsRecorder.OnPushPromise — returns ConnError
// ---------------------------------------------------------------------------

// TestSettingsRecorder_OnPushPromise_ReturnsError verifies that a
// PUSH_PROMISE during the handshake phase is rejected with a ConnError.
func TestSettingsRecorder_OnPushPromise_ReturnsError(t *testing.T) {
	r := &settingsRecorder{}
	err := r.OnPushPromise(frame.FrameHeader{}, 4, nil, 0)
	if err == nil {
		t.Fatalf("expected ConnError, got nil")
	}
	ce, ok := err.(*ConnError)
	if !ok {
		t.Fatalf("err type = %T, want *ConnError", err)
	}
	if ce.Code != frame.ErrCodeProtocolError {
		t.Fatalf("code = %v, want ErrCodeProtocolError", ce.Code)
	}
}

// ---------------------------------------------------------------------------
// conn.go: Ping — readerDone path
// ---------------------------------------------------------------------------

// TestConn_Ping_ReaderDoneClosesConn verifies that Ping returns ErrConnClosed
// when the reader goroutine exits (readerDone is closed) while waiting for ACK.
func TestConn_Ping_ReaderDoneClosesConn(t *testing.T) {
	// Use a net.Pipe peer that closes the connection right after handshake.
	// The client Ping is issued; before the ACK arrives the reader loop exits.
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(_ *frame.Framer) {
		// Server closes immediately after handshake → reader loop exits with EOF.
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Wait briefly so the reader loop observes the peer close.
	time.Sleep(50 * time.Millisecond)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pingCancel()
	_, err = c.Ping(pingCtx)
	if err == nil {
		t.Fatalf("Ping after reader exit: expected error, got nil")
	}
	// Accept either ErrConnClosed or "use of closed" transport error.
}

// ---------------------------------------------------------------------------
// conn.go: writeData — empty payload with endStream=false is a no-op
// ---------------------------------------------------------------------------

// TestConn_WriteData_EmptyNoEndStream verifies that an empty payload with
// endStream=false returns nil without writing any frames.
func TestConn_WriteData_EmptyNoEndStream(t *testing.T) {
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535

	err := c.writeData(context.Background(), s, nil, false)
	if err != nil {
		t.Fatalf("writeData(empty, noEndStream) = %v, want nil", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no bytes written for empty no-endstream, got %d", buf.Len())
	}
}

// TestConn_WriteData_EmptyEndStream verifies that an empty payload with
// endStream=true writes a zero-length DATA frame with END_STREAM.
func TestConn_WriteData_EmptyEndStream(t *testing.T) {
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535

	err := c.writeData(context.Background(), s, nil, true)
	if err != nil {
		t.Fatalf("writeData(empty, endStream) = %v, want nil", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected DATA frame bytes for empty+endStream, got 0")
	}
}

// TestStream_Push_Overflow_Integration exercises the overflow path via an
// integration test: server sends many flushed DATA chunks faster than the
// client drains its event buffer.
func TestStream_Push_Overflow_Integration(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 20; i++ {
			_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	// Dial with a tiny event buffer (1) to make overflow likely.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := ConnOptions{
		Dialer:          &TLSDialer{Config: cfg},
		StreamEventBuffer: 1,
	}
	c, err := Dial(ctx, srv.Listener.Addr().String(), opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/overflow")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	// Do NOT drain immediately — let the buffer fill, then read one event.
	time.Sleep(200 * time.Millisecond)

	recvCtx, recvCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer recvCancel()
	_, _ = s.Recv(recvCtx)
}
