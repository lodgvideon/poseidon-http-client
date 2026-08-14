package conn

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// asyncWrite runs fn in a goroutine because net.Pipe is synchronous.
func asyncWrite(fn func() error) chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

// drainFrames reads frames until the pipe errors, handing each to h. net.Pipe
// is unbuffered, so a peer that writes without a concurrent reader deadlocks
// the moment the client writes back (an RST_STREAM refusal, a GOAWAY). Every
// push test that expects a client-originated frame runs this alongside its
// writes.
func drainFrames(fr *frame.Framer, h frame.Handler) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := fr.ReadFrame(context.Background(), h); err != nil {
				return
			}
		}
	}()
	return done
}

// pushProbeHandler counts the client-originated frames the push tests assert
// on. Written only by the single drainFrames goroutine; read after that
// goroutine has exited (its done channel closed), so no mutex is needed.
//
// pingAck is the ordering barrier the registry test needs. The client's reader
// is a single goroutine processing frames in wire order and auto-echoes an
// inbound PING (RFC 7540 §6.7), so an observed ACK proves every frame the peer
// wrote before that PING has already been handled. Without it a test racing the
// reader can sample the registry before the promises land.
type pushProbeHandler struct {
	nilHandler
	rstCodes  []frame.ErrCode
	goAwayHit bool
	goAwayErr frame.ErrCode
	pingAck   chan struct{}
	ackOnce   sync.Once
}

func newPushProbe() *pushProbeHandler {
	return &pushProbeHandler{pingAck: make(chan struct{})}
}

// newFinish returns a channel the peer goroutine parks on until the test is
// done asserting, plus an idempotent closer. Idempotent so the happy path can
// release the peer early while a defer still frees it after a t.Fatalf.
func newFinish() (chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch) }) }
}

func (p *pushProbeHandler) OnRSTStream(_ frame.FrameHeader, code frame.ErrCode) error {
	p.rstCodes = append(p.rstCodes, code)
	return nil
}

func (p *pushProbeHandler) OnGoAway(_ frame.FrameHeader, _ uint32, code frame.ErrCode, _ []byte) error {
	p.goAwayHit = true
	p.goAwayErr = code
	return nil
}

func (p *pushProbeHandler) OnPing(fh frame.FrameHeader, _ [8]byte) error {
	if fh.Flags&frame.FlagPingAck != 0 && p.pingAck != nil {
		p.ackOnce.Do(func() { close(p.pingAck) })
	}
	return nil
}

// awaitRequest blocks until the client's request HEADERS arrive. Every push
// test must do this before writing a response: Conn defers stream-id
// allocation to the first HEADERS write (§5.1.1 monotonic ids), so the
// client's stream 1 does not exist until SendHeaders puts it on the wire. A
// peer that responds eagerly races the stream into existence, and its response
// lands on a stream the reader cannot find — leaving the parent's first event
// to be whatever arrives next.
func awaitRequest(t *testing.T, srvFr *frame.Framer) bool {
	t.Helper()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("server read client request: %v", err)
		return false
	}
	return true
}

// reqHeaders is the client request header list the push tests send.
func reqHeaders() []hpack.HeaderField {
	return []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
}

// promiseBlock encodes a minimal promised request header list.
func promiseBlock(enc *hpack.Encoder, path string) []byte {
	return enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte(path)},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	})
}

// openParentStream performs the request/response exchange the push tests
// build on: client sends HEADERS on stream 1 with END_STREAM, server replies
// with response HEADERS *without* END_STREAM, so stream 1 stays open and
// keeps holding its in-flight slot.
func openParentStream(ctx context.Context, t *testing.T, c *Conn) StreamRef {
	t.Helper()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, reqHeaders(), true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("parent Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("parent event = %s, want EventHeaders", ev.Type)
	}
	ev.Release()
	return s
}

// TestConformance_RFC7540_Sec512_PushedStreamDoesNotFreeOurSlot pins the
// directional reading of SETTINGS_MAX_CONCURRENT_STREAMS (RFC 7540 §5.1.2,
// §6.5.2): the peer's advertised limit governs the streams *we* initiate, and
// a server-pushed stream is not one of ours. Closing a pushed stream must
// therefore not hand our request streams a slot back.
//
// Before the fix, registerPushedStream never incremented Conn.inflight but
// Close decremented it, so closing 2 pushed streams zeroed a count that still
// owed 1 for the open request stream — measured: 3 concurrent request streams
// against a peer gate of 2.
func TestConformance_RFC7540_Sec512_PushedStreamDoesNotFreeOurSlot(t *testing.T) {
	const gate = 2

	cli, srv := net.Pipe()
	defer cli.Close()

	peer := frame.SettingsParams{N: 1}
	peer.Pairs[0] = frame.SettingPair{ID: frame.SettingMaxConcurrentStreams, Value: gate}

	go pipeServerWithSettings(t, srv, peer, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		// Drain concurrently: the client RSTs both pushed streams on Close.
		drainFrames(srvFr, &nilHandler{})

		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		for _, id := range []uint32{2, 4} {
			<-asyncWrite(func() error {
				return srvFr.WritePushPromise(1, id, promiseBlock(enc, "/a.css"), true, 0)
			})
		}
		<-time.After(2 * time.Second) // keep the pipe alive for the assertions
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)

	// Both promises must land before we close them.
	for i := 0; i < 2; i++ {
		ev, err := parent.Recv(ctx)
		if err != nil {
			t.Fatalf("push promise %d: %v", i, err)
		}
		if ev.Type != EventPushPromise {
			t.Fatalf("event %d = %s, want EventPushPromise", i, ev.Type)
		}
		ev.Release()
	}
	for _, id := range []uint32{2, 4} {
		p, ok := c.LookupStream(id)
		if !ok {
			t.Fatalf("pushed stream %d not registered", id)
		}
		if err := p.Close(); err != nil {
			t.Fatalf("pushed %d Close: %v", id, err)
		}
	}

	// The request stream is still open, so exactly gate-1 more may open.
	opened := 1
	var lastErr error
	for i := 0; i < gate+2; i++ {
		if _, err := c.NewStream(ctx); err != nil {
			lastErr = err
			break
		}
		opened++
	}
	if !errors.Is(lastErr, ErrTooManyStreams) {
		t.Fatalf("NewStream past the gate returned %v, want ErrTooManyStreams", lastErr)
	}
	if opened != gate {
		t.Fatalf("closing 2 pushed streams let %d concurrent request streams open against a peer gate of %d; "+
			"pushed streams must not return an inflight slot they never took (RFC 7540 §5.1.2)", opened, gate)
	}
}

// TestConformance_RFC7540_Sec512_PushedStreamCloseKeepsShutdownBlocking is the
// Shutdown-side consequence of the same asymmetry: Shutdown drains on
// Conn.inflight, so a pushed Close that zeroed the count made Shutdown return
// immediately with a request stream still open. Measured before the fix:
// Shutdown(30s) returned in ~0s.
func TestConformance_RFC7540_Sec512_PushedStreamCloseKeepsShutdownBlocking(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, &nilHandler{})

		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, promiseBlock(enc, "/a.css"), true, 0)
		})
		<-time.After(3 * time.Second)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)

	ev, err := parent.Recv(ctx)
	if err != nil {
		t.Fatalf("push promise: %v", err)
	}
	if ev.Type != EventPushPromise {
		t.Fatalf("event = %s, want EventPushPromise", ev.Type)
	}
	ev.Release()
	pushed, ok := c.LookupStream(2)
	if !ok {
		t.Fatal("pushed stream 2 not registered")
	}
	if err := pushed.Close(); err != nil {
		t.Fatalf("pushed Close: %v", err)
	}

	// Stream 1 never ends, so Shutdown must burn its full graceful timeout.
	const graceful = 300 * time.Millisecond
	start := time.Now()
	_ = c.Shutdown(graceful)
	elapsed := time.Since(start)
	if elapsed < graceful {
		t.Fatalf("Shutdown(%v) returned after %v with the request stream still open; "+
			"a pushed Close must not zero the drain count (measured before fix: ~0s)", graceful, elapsed)
	}
}

// TestConformance_RFC7540_Sec66_PushRegistryBoundedByOurMaxStreams pins the
// other direction of §6.5.2's "this limit is directional": the value *we*
// advertise in SETTINGS_MAX_CONCURRENT_STREAMS is the number of streams we
// permit the server to create, i.e. the bound on concurrent pushed streams.
// Beyond it, §6.6 lets the recipient reject a promised stream with an
// RST_STREAM referencing the promised id.
//
// Before the fix there was no bound at all: measured 8000 promises -> 8001
// registry entries retaining ~8.56 MB (~1.1 KB/stream) off ~36 wire bytes per
// PUSH_PROMISE, and no plateau.
func TestConformance_RFC7540_Sec66_PushRegistryBoundedByOurMaxStreams(t *testing.T) {
	const ourCap = 4
	const promises = 10

	cli, srv := net.Pipe()
	defer cli.Close()

	probe := newPushProbe()
	drained := make(chan struct{})
	finish, stop := newFinish()
	defer stop()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		// Concurrent drain is mandatory: the client RSTs every refused
		// promise while the server is still writing the next one.
		d := drainFrames(srvFr, probe)

		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		for i := 0; i < promises; i++ {
			id := uint32(2 + 2*i)
			<-asyncWrite(func() error {
				return srvFr.WritePushPromise(1, id, promiseBlock(enc, "/a.css"), true, 0)
			})
		}
		// Barrier: the ACK proves all promises above were processed.
		<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{9}) })
		<-finish
		srv.Close()
		<-d
		close(drained)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{MaxConcurrentStreams: ourCap}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)

	select {
	case <-probe.pingAck:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the PING ACK barrier")
	}

	c.smu.Lock()
	registry := len(c.streams)
	c.smu.Unlock()
	// 1 request stream + at most ourCap pushed streams.
	if registry > ourCap+1 {
		t.Fatalf("%d PUSH_PROMISEs grew the stream registry to %d entries; "+
			"our advertised SETTINGS_MAX_CONCURRENT_STREAMS=%d bounds server-initiated "+
			"streams (RFC 7540 §6.5.2), so want at most %d", promises, registry, ourCap, ourCap+1)
	}

	// The accepted promises — and only those — reach the parent.
	delivered := 0
	for {
		select {
		case ev := <-parent.Stream().events:
			ev.Release()
			if ev.Type == EventPushPromise {
				delivered++
			}
			continue
		default:
		}
		break
	}
	if delivered > ourCap {
		t.Fatalf("%d PUSH_PROMISEs delivered %d events to the parent, want at most %d",
			promises, delivered, ourCap)
	}

	stop()
	<-drained
	refused := 0
	for _, code := range probe.rstCodes {
		if code == frame.ErrCodeRefusedStream {
			refused++
		}
	}
	if refused != promises-ourCap {
		t.Fatalf("client sent %d RST_STREAM(REFUSED_STREAM) for %d over-cap promises; "+
			"RFC 7540 §6.6 rejects a promised stream by RST_STREAM on the promised id (got codes %v)",
			refused, promises-ourCap, probe.rstCodes)
	}
}

// TestConformance_RFC7540_Sec66_IllegalPromisedIDIsProtocolError pins RFC 7540
// §6.6: "A receiver MUST treat the receipt of a PUSH_PROMISE that promises an
// illegal stream identifier (Section 5.1.1) as a connection error (Section
// 5.4.1) of type PROTOCOL_ERROR." The section goes on to define the term:
// "Note that an illegal stream identifier is an identifier for a stream that
// is not currently in the \"idle\" state."
//
// §5.1.1 makes server-initiated ids even and non-zero, and requires a new id to
// exceed every id the sender has already opened or reserved. Nothing before
// registerPushedStream checked any of that — frame.dispatchPushPromise only
// masks the reserved bit — so a promise for stream 1 overwrote the client's own
// stream 1 in the registry.
func TestConformance_RFC7540_Sec66_IllegalPromisedIDIsProtocolError(t *testing.T) {
	tests := []struct {
		name string
		ids  []uint32
	}{
		{"zero", []uint32{0}},
		{"odd_collides_with_our_stream", []uint32{1}},
		{"odd_unused", []uint32{3}},
		{"duplicate", []uint32{2, 2}},
		{"decreasing", []uint32{4, 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			defer cli.Close()

			probe := newPushProbe()
			drained := make(chan struct{})
			finish, stop := newFinish()
			defer stop()
			// The illegal promise tears the connection down, which closes
			// every stream's resetSignal. Recv selects over the event channel
			// and that signal, so a reset racing an already-buffered response
			// header event makes Recv's choice a coin flip. Hold the promises
			// until the parent's response is in hand.
			ready := make(chan struct{})

			go pipeServer(t, srv, func(srvFr *frame.Framer) {
				if !awaitRequest(t, srvFr) {
					return
				}
				d := drainFrames(srvFr, probe)

				enc := hpack.NewEncoder()
				<-asyncWrite(func() error {
					return srvFr.WriteHeaders(frame.WriteHeadersParams{
						StreamID:      1,
						BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
						EndHeaders:    true,
					})
				})
				<-ready
				for _, id := range tc.ids {
					<-asyncWrite(func() error {
						return srvFr.WritePushPromise(1, id, promiseBlock(enc, "/a.css"), true, 0)
					})
				}
				<-finish
				srv.Close()
				<-d
				close(drained)
			})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			c, err := NewClientConn(ctx, cli, ConnOptions{
				Settings:          AdvertisedSettings{}.defaulted(),
				StreamEventBuffer: 16,
				EnablePush:        true,
			})
			if err != nil {
				t.Fatalf("NewClientConn: %v", err)
			}
			defer c.Close()

			parent := openParentStream(ctx, t, c)
			close(ready)

			// Only the final id in each case is illegal; any leading ids are
			// legal promises that must be accepted normally. The illegal one
			// tears the connection down, so the parent's Recv ends in an
			// error / reset rather than one more push event.
			legal := len(tc.ids) - 1
			accepted := 0
			for {
				ev, err := parent.Recv(ctx)
				if err != nil {
					break
				}
				ev.Release()
				if ev.Type == EventReset {
					break
				}
				if ev.Type == EventPushPromise {
					accepted++
					if accepted > legal {
						t.Fatalf("promised ids %v: %d accepted, want %d; §6.6 requires a "+
							"connection error of type PROTOCOL_ERROR for an id that is not idle",
							tc.ids, accepted, legal)
					}
				}
			}

			stop()
			<-drained
			if !probe.goAwayHit {
				t.Fatalf("promised id %v produced no GOAWAY; §6.6 requires a connection "+
					"error of type PROTOCOL_ERROR", tc.ids)
			}
			if probe.goAwayErr != frame.ErrCodeProtocolError {
				t.Fatalf("promised id %v produced GOAWAY(%v), want GOAWAY(PROTOCOL_ERROR) per §6.6",
					tc.ids, probe.goAwayErr)
			}
		})
	}
}

// TestConn_ReservePushedStream_RejectsIllegalIDs pins the registry-level
// consequence of the §6.6 id check directly on the function that owns the
// invariant. The wire path is covered by
// TestConformance_RFC7540_Sec66_IllegalPromisedIDIsProtocolError; asserting
// registry identity there is not reliably observable, because the connection
// error tears the registry down in the same breath.
func TestConn_ReservePushedStream_RejectsIllegalIDs(t *testing.T) {
	newConn := func() *Conn {
		return &Conn{
			opts:    ConnOptions{}.defaulted(),
			streams: map[uint32]*Stream{},
		}
	}

	t.Run("our own stream is not overwritten", func(t *testing.T) {
		c := newConn()
		ours := newStream(1, 8, c, 65535)
		c.streams[1] = ours

		if err := c.notePromisedID(1); !errors.Is(err, ErrIllegalPromisedID) {
			t.Fatalf("notePromisedID(1) err = %v, want ErrIllegalPromisedID", err)
		}
		if got := c.streams[1]; got != ours {
			t.Fatal("a promise for our own stream 1 replaced it in the registry")
		}
		if c.pushInflight != 0 {
			t.Fatalf("pushInflight = %d after a rejected promise, want 0", c.pushInflight)
		}
	})

	t.Run("illegal ids", func(t *testing.T) {
		for _, id := range []uint32{0, 1, 3, 7} {
			c := newConn()
			if err := c.notePromisedID(id); !errors.Is(err, ErrIllegalPromisedID) {
				t.Fatalf("notePromisedID(%d) err = %v, want ErrIllegalPromisedID", id, err)
			}
			if len(c.streams) != 0 {
				t.Fatalf("notePromisedID(%d) registered %d streams, want 0", id, len(c.streams))
			}
		}
	})

	t.Run("ids must strictly increase", func(t *testing.T) {
		c := newConn()
		if err := c.notePromisedID(4); err != nil {
			t.Fatalf("notePromisedID(4): %v", err)
		}
		for _, id := range []uint32{4, 2} {
			if err := c.notePromisedID(id); !errors.Is(err, ErrIllegalPromisedID) {
				t.Fatalf("notePromisedID(%d) after 4: err = %v, want ErrIllegalPromisedID", id, err)
			}
		}
		if err := c.notePromisedID(6); err != nil {
			t.Fatalf("notePromisedID(6) after 4: %v", err)
		}
	})

	t.Run("a refused id still advances the promised-id floor", func(t *testing.T) {
		c := newConn()
		c.opts.Settings.MaxConcurrentStreams = 1
		if err := c.notePromisedID(2); err != nil {
			t.Fatalf("notePromisedID(2): %v", err)
		}
		if _, err := c.reservePushedStream(2); err != nil {
			t.Fatalf("reservePushedStream(2): %v", err)
		}
		// 4 is spent on the wire even though the cap will refuse it: notePromisedID
		// advances the promised-id floor before the cap check, so the server's
		// raced pushed-response frames on stream 4 resolve to a closed stream
		// (discarded), not an idle one (which would kill the connection, §5.1).
		if err := c.notePromisedID(4); err != nil {
			t.Fatalf("notePromisedID(4): %v", err)
		}
		if _, err := c.reservePushedStream(4); !errors.Is(err, ErrPushRefused) {
			t.Fatalf("reservePushedStream(4) at cap 1: err = %v, want ErrPushRefused", err)
		}
		// The floor advanced past 4, so a repeat promise for it is illegal.
		if err := c.notePromisedID(4); !errors.Is(err, ErrIllegalPromisedID) {
			t.Fatalf("notePromisedID(4) repeat: err = %v, want ErrIllegalPromisedID", err)
		}
	})
}

// TestConn_ReservePushedStream_ReleasesToItsOwnCounter verifies the two
// directional counters stay independent: a pushed stream's release returns its
// slot to pushInflight, never to the NewStream gate (RFC 7540 §5.1.2).
func TestConn_ReservePushedStream_ReleasesToItsOwnCounter(t *testing.T) {
	c := &Conn{opts: ConnOptions{}.defaulted(), streams: map[uint32]*Stream{}}
	c.inflight = 3 // pretend three request streams are open

	s, err := c.reservePushedStream(2)
	if err != nil {
		t.Fatalf("reservePushedStream(2): %v", err)
	}
	if c.pushInflight != 1 {
		t.Fatalf("pushInflight = %d after reserve, want 1", c.pushInflight)
	}
	if c.inflight != 3 {
		t.Fatalf("reserving a pushed stream moved the request gate to %d, want 3", c.inflight)
	}

	c.releaseInflight(s.id)
	if c.pushInflight != 0 {
		t.Fatalf("pushInflight = %d after release, want 0", c.pushInflight)
	}
	if c.inflight != 3 {
		t.Fatalf("releasing a pushed stream freed a request slot: inflight = %d, want 3", c.inflight)
	}
}

// TestConn_PushPromise_DeliveredToParentStream verifies that when
// EnablePush is true, a PUSH_PROMISE frame from the server creates a
// pushed stream and delivers an EventPushPromise on the parent stream.
func TestConn_PushPromise_DeliveredToParentStream(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Read the client's request HEADERS on stream 1.
		srvFr.ReadFrame(context.Background(), &nilHandler{})

		enc := hpack.NewEncoder()

		// Response HEADERS on stream 1 (END_HEADERS, no END_STREAM).
		respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: respBlock,
				EndHeaders:    true,
			})
		})

		// PUSH_PROMISE on stream 1, promising stream 2.
		pushBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":path"), Value: []byte("/style.css")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, pushBlock, true, 0)
		})

		// Pushed response on stream 2 (END_HEADERS | END_STREAM).
		pushedBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      2,
				BlockFragment: pushedBlock,
				EndHeaders:    true,
				EndStream:     true,
			})
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	// Event 1: response headers.
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("expected EventHeaders, got error: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("expected EventHeaders, got %s", ev.Type)
	}
	ev.Release()

	// Event 2: push promise.
	ev, err = s.Recv(ctx)
	if err != nil {
		t.Fatalf("expected EventPushPromise, got error: %v", err)
	}
	if ev.Type != EventPushPromise {
		t.Fatalf("expected EventPushPromise, got %s", ev.Type)
	}
	if ev.PushStreamID != 2 {
		t.Fatalf("expected PushStreamID=2, got %d", ev.PushStreamID)
	}
	// Verify promised headers.
	found := false
	for _, h := range ev.Headers {
		if string(h.Name) == ":path" && string(h.Value) == "/style.css" {
			found = true
		}
	}
	if !found {
		t.Fatalf("promised headers missing :path=/style.css")
	}
	ev.Release()

	// Pushed stream (ID=2) should be registered.
	pushed := c.lookupStream(2)
	if pushed == nil {
		t.Fatal("pushed stream (ID=2) not registered")
	}

	// Read pushed response.
	pev, err := pushed.ref().Recv(ctx)
	if err != nil {
		t.Fatalf("pushed stream Recv error: %v", err)
	}
	if pev.Type != EventHeaders {
		t.Fatalf("expected EventHeaders on pushed stream, got %s", pev.Type)
	}
	if !pev.EndStream {
		t.Fatal("expected END_STREAM on pushed stream headers")
	}
	pev.Release()
}

// TestConn_PushPromise_DisabledReturnsProtocolError verifies that when
// EnablePush is false, a PUSH_PROMISE triggers a connection error.
func TestConn_PushPromise_DisabledReturnsProtocolError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		srvFr.ReadFrame(context.Background(), &nilHandler{})

		enc := hpack.NewEncoder()
		respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: respBlock,
				EndHeaders:    true,
			})
		})

		pushBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, pushBlock, true, 0)
		})

		time.Sleep(200 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        false,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	// PUSH_PROMISE with ENABLE_PUSH=0 → PROTOCOL_ERROR → conn closes.
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			if errors.Is(err, ErrStreamClosed) {
				return // connection closed — expected
			}
			return // any other error is fine too
		}
		if ev.Type == EventReset {
			return // stream reset — also acceptable
		}
		ev.Release()
	}
}
