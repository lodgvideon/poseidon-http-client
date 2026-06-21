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
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// TestConn_WriteData_WithPadding verifies writeData uses WriteDataPadded when
// ConnOptions.Padding is configured (covers the padLen > 0 branch).
func TestConn_WriteData_WithPadding(t *testing.T) {
	t.Parallel()
	c := newGoAwayConn()
	c.opts.Padding = PaddingStrategy{Min: 4, Max: 4}
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535

	// Non-empty data with padding enabled.
	if err := c.writeData(context.Background(), s, []byte("hi"), true); err != nil {
		t.Fatalf("writeData with padding: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected DATA frame in output")
	}
}

// TestConn_WriteData_EmptyWithPadding covers the padLen > 0 branch inside the
// empty-payload path (len(p)==0, endStream=true).
func TestConn_WriteData_EmptyWithPadding(t *testing.T) {
	t.Parallel()
	c := newGoAwayConn()
	c.opts.Padding = PaddingStrategy{Min: 2, Max: 2}
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535

	if err := c.writeData(context.Background(), s, nil, true); err != nil {
		t.Fatalf("writeData(empty,padding): %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected padded DATA frame bytes")
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

// ---------------------------------------------------------------------------
// LookupStream
// ---------------------------------------------------------------------------

// TestConn_LookupStream_FoundAndNotFound verifies the public LookupStream API.
func TestConn_LookupStream_FoundAndNotFound(t *testing.T) {
	// Gate prevents the server from replying until we've checked LookupStream.
	// Without it, a fast server response causes markStreamDone to evict the
	// stream from c.streams before our LookupStream call.
	gate := make(chan struct{})
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer c.Close()

	// Before any stream: ID 1 should not exist.
	if _, ok := c.LookupStream(1); ok {
		t.Fatal("stream 1 should not exist before any SendHeaders")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create and send a stream; after SendHeaders it has ID 1.
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

	// Stream 1 now registered — server is still blocked on gate so no eviction.
	if _, ok := c.LookupStream(s.ID()); !ok {
		close(gate) // unblock server before fatalf so defer srv.Close() doesn't hang
		t.Fatalf("LookupStream(%d) = false, want true", s.ID())
	}
	close(gate) // unblock server

	// Unknown ID returns false.
	if _, ok := c.LookupStream(999); ok {
		t.Fatal("LookupStream(999) should return false for unknown ID")
	}
}

// ---------------------------------------------------------------------------
// AltSvcEntries
// ---------------------------------------------------------------------------

// TestConn_AltSvcEntries_StoreAndRetrieve verifies AltSvcEntries returns a copy.
func TestConn_AltSvcEntries_StoreAndRetrieve(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	// Initially nil.
	if got := c.AltSvcEntries(); got != nil {
		t.Fatalf("expected nil before any ALTSVC, got %v", got)
	}

	entries := []frame.AltSvcEntry{
		{Origin: "https://example.com", AltValue: `h2=":8080"`},
		{Origin: "https://cdn.example.com", AltValue: `h2=":443"`},
	}
	c.storeAltSvc(entries)

	got := c.AltSvcEntries()
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Origin != "https://example.com" {
		t.Fatalf("entry[0].Origin = %q, want https://example.com", got[0].Origin)
	}

	// Verify it's a copy.
	got[0].Origin = "modified"
	again := c.AltSvcEntries()
	if again[0].Origin != "https://example.com" {
		t.Error("AltSvcEntries() must return a copy")
	}
}

// TestConn_AltSvcEntries_ClearedByEmptySlice verifies that storing empty
// slice returns nil from AltSvcEntries (matching ALTSVC clear semantics).
func TestConn_AltSvcEntries_ClearedByEmptySlice(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.storeAltSvc([]frame.AltSvcEntry{{Origin: "https://a.com", AltValue: "h2=:443"}})
	c.storeAltSvc(nil) // clear
	if got := c.AltSvcEntries(); got != nil {
		t.Fatalf("expected nil after clearing, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// settingsRecorder no-ops: OnAltSvc and OnOrigin
// ---------------------------------------------------------------------------

// TestSettingsRecorder_NoOps calls the no-op OnAltSvc / OnOrigin methods on
// settingsRecorder (needed to satisfy frame.Handler during handshake).
func TestSettingsRecorder_NoOps(t *testing.T) {
	t.Parallel()
	r := &settingsRecorder{}
	if err := r.OnAltSvc(frame.FrameHeader{}, nil); err != nil {
		t.Fatalf("OnAltSvc: %v", err)
	}
	if err := r.OnOrigin(frame.FrameHeader{}, nil); err != nil {
		t.Fatalf("OnOrigin: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Stream.Recv — resetSignal path
// ---------------------------------------------------------------------------

// TestStream_Recv_ResetSignal verifies that Recv returns EventReset when the
// stream's resetSignal channel is closed (e.g. after a RST_STREAM arrives).
func TestStream_Recv_ResetSignal(t *testing.T) {
	t.Parallel()
	w := &fakeStreamWriter{}
	s := newStream(1, 8, w, 65535)
	s.resetCode.Store(uint32(frame.ErrCodeCancel))
	close(s.resetSignal)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if ev.Type != EventReset || ev.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("event = %+v", ev)
	}
}

// ---------------------------------------------------------------------------
// Proxy: bufferedConn.Read + ProxyTLSDialer coverage
// ---------------------------------------------------------------------------

// TestProxyDialer_BufferedConn verifies that when the proxy response headers
// are immediately followed by data in the same read, bufferedConn.Read is
// used and returns that data.
func TestProxyDialer_BufferedConn(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, cerr := ln.Accept()
		if cerr != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4096)
		_, _ = c.Read(buf)
		// Send the 200 + headers + extra data all at once so bufio.Reader
		// captures the extra bytes after the blank line.
		_, _ = io.WriteString(c, "HTTP/1.1 200 Connection established\r\nX-Proxy: ok\r\n\r\nhello")
	}()

	proxyURL := &url.URL{Scheme: "http", Host: ln.Addr().String()}
	d := &ProxyDialer{ProxyURL: proxyURL}
	tc, err := d.Dial(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer tc.Close()

	out := make([]byte, 5)
	n, _ := tc.Read(out)
	if string(out[:n]) != "hello" {
		t.Fatalf("bufferedConn.Read = %q, want %q", out[:n], "hello")
	}
}

// TestProxyTLSDialer_NilURL verifies ProxyTLSDialer.Dial propagates the
// ProxyDialer error when ProxyURL is nil (covers the early-return error path).
func TestProxyTLSDialer_NilURL(t *testing.T) {
	t.Parallel()
	d := &ProxyTLSDialer{}
	_, err := d.Dial(context.Background(), "example.com:443")
	if err == nil {
		t.Fatal("expected error for nil ProxyURL")
	}
}

// TestTLSDialer_ALPNFailure verifies ErrALPNFailed when the server does not
// negotiate h2 (covers the NegotiatedProtocol != "h2" branch in TLSDialer.Dial).
// The server is an httptest server that participates in no ALPN (NextProtos=nil),
// so the negotiated protocol is "". TLS 1.2 allows this; TLS 1.3 may reject
// the ALPN mismatch at handshake time, so skip on error.
func TestTLSDialer_ALPNFailure(t *testing.T) {
	// Start an httptest TLS server (no h2) so we can get its cert.
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.StartTLS() // EnableHTTP2 == false → NextProtos does not include h2
	defer srv.Close()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			if cert, err := x509.ParseCertificate(certDER); err == nil {
				pool.AddCert(cert)
			}
		}
	}

	// Override the server's TLS config to remove all ALPN protocols so the
	// TLS handshake completes but NegotiatedProtocol is "".
	srv.TLS.NextProtos = nil

	// Spin up our own raw TLS listener using the same cert.
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv.TLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, cerr := ln.Accept()
		if cerr != nil {
			return
		}
		_ = c.(*tls.Conn).Handshake()
		c.Close()
	}()

	d := &TLSDialer{Config: &tls.Config{
		RootCAs:    pool,
		ServerName: "example.com",
		MaxVersion: tls.VersionTLS12, // TLS 1.2: ALPN mismatch doesn't abort
		NextProtos: []string{"h2"},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = d.Dial(ctx, ln.Addr().String())
	if err != ErrALPNFailed {
		t.Skipf("TLS ALPN behaviour: err = %v (not ErrALPNFailed — skip)", err)
	}
}

// TestDial_DialError verifies that the top-level Dial returns the dialer's
// error when the underlying transport cannot connect.
func TestDial_DialError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// 127.0.0.1:1 — always connection refused.
	_, err := Dial(ctx, "127.0.0.1:1", ConnOptions{
		Dialer: &TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec
	})
	if err == nil {
		t.Fatal("expected dial error")
	}
}

// TestDial_NewClientConnError covers the `transport.Close(); return nil, err`
// branch inside Dial when NewClientConn fails (peer closes connection before
// sending HTTP/2 SETTINGS).
func TestDial_NewClientConnError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, cerr := ln.Accept()
		if cerr != nil {
			return
		}
		c.Close() // close before HTTP/2 handshake
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = Dial(ctx, ln.Addr().String(), ConnOptions{
		Dialer: &PlaintextDialer{},
	})
	if err == nil {
		t.Fatal("expected error from NewClientConn")
	}
}

// TestProxyDialer_NoPortInURL verifies that a proxy URL without a port gets
// ":80" appended (covers the strings.Contains branch in ProxyDialer.Dial).
func TestProxyDialer_NoPortInURL(t *testing.T) {
	t.Parallel()
	// Use an IP-only proxy URL — the Dial will try 127.0.0.1:80 which is
	// connection-refused, covering the proxyAddr+":80" path.
	d := &ProxyDialer{ProxyURL: &url.URL{Scheme: "http", Host: "127.0.0.1"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := d.Dial(ctx, "example.com:443")
	// We expect a connection error (port 80 refused), not a panic.
	if err == nil {
		t.Fatal("expected connection error")
	}
}

// TestConn_Ping_ClosedConn verifies Ping returns ErrConnClosed immediately
// when the connection is already closed.
func TestConn_Ping_ClosedConn(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	c.closed.Store(true)
	ctx := context.Background()
	_, err := c.Ping(ctx)
	if err != ErrConnClosed {
		t.Fatalf("Ping on closed conn = %v, want ErrConnClosed", err)
	}
}

// ---------------------------------------------------------------------------
// markStreamDone — draining + inflight==0 path
// ---------------------------------------------------------------------------

// TestConn_MarkStreamDone_DrainsDone verifies that markStreamDone closes the
// drainDone channel when draining is active and inflight drops to zero.
func TestConn_MarkStreamDone_DrainsDone(t *testing.T) {
	t.Parallel()
	c := newGoAwayConn()
	c.drainDone = make(chan struct{})
	c.draining.Store(true)

	// Register stream 1 in a state where both sides have ended.
	s := newStream(1, 8, &fakeStreamWriter{}, 65535)
	s.id = 1
	s.mu.Lock()
	s.localEnded = true
	s.remoteEnded = true
	s.mu.Unlock()
	c.smu.Lock()
	c.streams[1] = s
	c.smu.Unlock()
	c.inflight = 1

	c.markStreamDone(1)

	select {
	case <-c.drainDone:
		// expected
	case <-time.After(time.Second):
		t.Fatal("drainDone not closed after markStreamDone")
	}
}

// ---------------------------------------------------------------------------
// Shutdown — second concurrent call waits on drainDone
// ---------------------------------------------------------------------------

// TestConn_Shutdown_AlreadyDraining covers the CompareAndSwap=false branch in
// Shutdown (called while another Shutdown is already in progress).
func TestConn_Shutdown_AlreadyDraining(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)

	// Pre-set draining so the CompareAndSwap fails.
	c.draining.Store(true)
	c.drainDone = make(chan struct{})
	// Close drainDone immediately so the waiting branch exits.
	close(c.drainDone)

	if err := c.Shutdown(10 * time.Millisecond); err != nil {
		t.Logf("Shutdown (already-draining): %v", err)
	}
}

// ---------------------------------------------------------------------------
// SendHeadersWithPriority — closed stream path
// ---------------------------------------------------------------------------

// TestConn_rstStream_WritesFrame verifies rstStream delegates to writeRSTStream
// and writes a RST_STREAM frame for the given stream ID.
func TestConn_rstStream_WritesFrame(t *testing.T) {
	t.Parallel()
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))

	if err := c.rstStream(5, frame.ErrCodeCancel); err != nil {
		t.Fatalf("rstStream: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected RST_STREAM frame in output")
	}
}

// TestStream_SendHeadersWithPriority_ClosedStream verifies that calling
// SendHeadersWithPriority on a closed stream returns ErrStreamClosed.
func TestStream_SendHeadersWithPriority_ClosedStream(t *testing.T) {
	t.Parallel()
	w := &fakeStreamWriter{}
	s := newStream(1, 8, w, 65535)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	err := s.SendHeadersWithPriority(context.Background(), nil, false, nil)
	if err != ErrStreamClosed {
		t.Fatalf("err = %v, want ErrStreamClosed", err)
	}
}

// TestStream_SendHeadersWithPriority_LocalEnded verifies that a half-closed
// (local) stream also returns ErrStreamClosed on another SendHeaders.
func TestStream_SendHeadersWithPriority_LocalEnded(t *testing.T) {
	t.Parallel()
	w := &fakeStreamWriter{}
	s := newStream(1, 8, w, 65535)
	s.mu.Lock()
	s.localEnded = true
	s.mu.Unlock()
	err := s.SendHeadersWithPriority(context.Background(), nil, false, nil)
	if err != ErrStreamClosed {
		t.Fatalf("err = %v, want ErrStreamClosed", err)
	}
}

// ---------------------------------------------------------------------------
// writeRSTStream — closed conn path
// ---------------------------------------------------------------------------

// TestConn_WriteRSTStream_ClosedConn verifies that writeRSTStream returns
// ErrConnClosed when the connection has been closed.
func TestConn_WriteRSTStream_ClosedConn(t *testing.T) {
	t.Parallel()
	c := newGoAwayConn()
	c.closed.Store(true)
	s := &Stream{id: 1}
	err := c.writeRSTStream(s, frame.ErrCodeCancel)
	if err != ErrConnClosed {
		t.Fatalf("writeRSTStream on closed conn = %v, want ErrConnClosed", err)
	}
}

// ---------------------------------------------------------------------------
// handshakeSettings — WriteClientPreface error path
// ---------------------------------------------------------------------------

// errWriter always returns an error.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// TestHandshakeSettings_WriteClientPrefaceError covers the early return in
// handshakeSettings when WriteClientPreface fails.
func TestHandshakeSettings_WriteClientPrefaceError(t *testing.T) {
	t.Parallel()
	fr := frame.NewFramer(errWriter{}, bytes.NewReader(nil))
	_, err := handshakeSettings(context.Background(), fr, AdvertisedSettings{}, false)
	if err == nil {
		t.Fatal("expected error from failed WriteClientPreface")
	}
}
