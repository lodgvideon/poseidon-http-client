package conn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// parkedH2Server returns a server whose handler flushes a response header and
// then parks, so every stream opened against it stays open — headers delivered,
// END_STREAM never sent — until the test kills the connection. That is the only
// shape in which a stream is both *registered* and *mid-response* when the
// reader dies, which is what shutdownStreams needs to have anything to do.
func parkedH2Server(t *testing.T) *httptest.Server {
	t.Helper()
	quit := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-quit:
		case <-r.Context().Done():
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	// LIFO: release the parked handlers before srv.Close waits on them.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(quit) })
	return srv
}

// openParkedStream sends a GET and returns the stream. The response headers are
// on their way but not consumed; what the caller does with them is the point of
// each test below.
func openParkedStream(t *testing.T, c *Conn) StreamRef {
	t.Helper()
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
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	return s
}

// terminalOf pumps Recv until the stream reaches a terminal, and reports what
// it saw. It returns (nil, true) only for an event carrying EndStream on a
// non-reset event — a *clean* end, which is precisely the outcome no caller may
// observe on a connection that died mid-response.
//
// Pumping rather than reading once is not slack: when shutdownStreams takes its
// default: arm, it closes resetSignal while a buffered event is still in the
// channel, leaving Recv's select with two ready arms. Which one wins is a
// genuine coin flip (conn/stream.go:349-361), so a caller may see its buffered
// HEADERS first or be handed the reset straight away. Both are correct; the
// invariant is that a terminal arrives, bounded, and is never a clean end.
func terminalOf(ctx context.Context, s StreamRef) (err error, cleanEnd bool) {
	for range 8 {
		ev, rerr := s.Recv(ctx)
		if rerr != nil {
			return rerr, false
		}
		if ev.Type == EventReset {
			return &connDeathReset{code: ev.RSTCode}, false
		}
		if ev.EndStream {
			return nil, true
		}
	}
	return errors.New("stream never reached a terminal within 8 events"), false
}

// connDeathReset carries the RST code out of terminalOf so the assertion can
// name it without re-deriving it.
type connDeathReset struct{ code frame.ErrCode }

func (e *connDeathReset) Error() string {
	return fmt.Sprintf("stream reset: code %d", uint32(e.code))
}

// isTerminalConnDeath accepts the set of terminals conn may hand a caller when
// the connection dies underneath an open stream. Three members, deliberately:
// shutdownStreams delivers through the event channel when it has room and
// through resetSignal when it does not, and close(s.events) can beat both to a
// blocked Recv. Pinning one exact code would be a nightly flake, so the class
// is what gets pinned.
func isTerminalConnDeath(err error) bool {
	if errors.Is(err, ErrStreamClosed) || errors.Is(err, ErrConnClosed) {
		return true
	}
	var r *connDeathReset
	if errors.As(err, &r) {
		switch r.code {
		case frame.ErrCodeRefusedStream, frame.ErrCodeInternalError:
			return true
		}
	}
	return false
}

// TestConn_ShutdownWithFullEventChannel_ReaderStillExits drives the one branch
// of shutdownStreams (conn/conn.go:1465-1479) that 1466 tests never reached:
// the default: arm of
//
//	select {
//	case s.events <- EventReset(INTERNAL_ERROR):
//	default:
//	    s.signalReset(INTERNAL_ERROR)
//	}
//
// Reaching it needs a stream whose event channel is *full* at the instant the
// connection dies — a slow consumer, exactly the hazard CLAUDE.md warns about
// ("Caller must drain Recv promptly or set larger buffer"). Every death test in
// this package drains promptly, so the send always had room and the default:
// arm never ran. StreamEventBuffer: 1 plus a consumer that has not called Recv
// yet makes a full channel deterministic rather than a matter of timing.
//
// What the arm buys is not the signalReset call — see the sibling test's note —
// it is that the send cannot block. shutdownStreams runs on the reader
// goroutine and holds c.smu across the whole fan-out. A send that blocks on a
// full channel wedges the reader inside its own shutdown: readerDone never
// closes, so the connection never admits it is dead, and a pool hands the
// corpse out forever. That is bug #241's shape, reached by a different road.
//
// The two streams differ only in drain depth, which is the crossing: one arm of
// the select per stream, both exercised in one shutdown.
func TestConn_ShutdownWithFullEventChannel_ReaderStillExits(t *testing.T) {
	srv := parkedH2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a test server's self-signed cert
			NextProtos:         []string{"h2"},
		}},
		// 1, not the default 8: the buffer size is what "full" means, so the
		// test sets it rather than hoping eight events show up. One flushed
		// HEADERS fills it exactly, with no second event to overflow it —
		// overflow is a different path (Stream.push), and it would evict the
		// stream from the registry before shutdownStreams ever saw it.
		StreamEventBuffer: 1,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// sStuck: nobody calls Recv, so its HEADERS sits in the channel and fills
	// it. shutdownStreams must take default: for this one.
	sStuck := openParkedStream(t, c)
	// sLive: a prompt consumer. Its channel is empty, so shutdownStreams takes
	// the ordinary send arm. Its terminal is the observable that proves the
	// fan-out got past sStuck.
	sLive := openParkedStream(t, c)

	liveTerm := make(chan error, 1)
	// Closed once the consumer has actually received the response HEADERS. The
	// drained check below cannot stand in for it: an empty channel means
	// "consumed" and "never delivered" alike, and openParkedStream returns as
	// soon as the request is written, without waiting for the response. Kill
	// the connection before sLive's HEADERS are parsed and shutdownStreams
	// delivers its reset into an empty channel, so this Recv legitimately
	// returns a reset — which the assertion below then reports as a failure.
	// Rare enough to pass everywhere for months and to survive 100 local runs
	// under -race; it surfaced on Linux CI under coverage instrumentation.
	liveHeaders := make(chan struct{})
	go func() {
		ev, rerr := sLive.Recv(ctx) // the flushed HEADERS
		if rerr != nil {
			liveTerm <- rerr
			return
		}
		if ev.Type != EventHeaders {
			liveTerm <- errors.New("sLive: expected HEADERS first, got " + ev.Type.String())
			return
		}
		close(liveHeaders)
		term, clean := terminalOf(ctx, sLive)
		if clean {
			liveTerm <- errors.New("sLive: clean END_STREAM after the connection died")
			return
		}
		liveTerm <- term
	}()

	// Control 1 — the precondition. Without a full channel the default: arm
	// never runs and this test silently degrades into a duplicate of the
	// ordinary death tests. Poll rather than sleep so a slow machine does not
	// turn it into a flake.
	deadline := time.Now().Add(5 * time.Second)
	for len(sStuck.Stream().events) < cap(sStuck.Stream().events) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got, want := len(sStuck.Stream().events), cap(sStuck.Stream().events); got != want {
		t.Fatalf("sStuck event channel is %d/%d full; shutdownStreams would take its send arm and the default: branch would go untested", got, want)
	}
	// And sLive's must be drained — genuinely drained, which means waiting for
	// the consumer to say it got the HEADERS rather than only observing an
	// empty channel.
	select {
	case <-liveHeaders:
	case <-time.After(5 * time.Second):
		t.Fatal("sLive never received its response HEADERS; killing the connection now would test the empty-channel path, not the drained one")
	}
	for len(sLive.Stream().events) > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(sLive.Stream().events); got != 0 {
		t.Fatalf("sLive event channel holds %d events; it must be drained for its shutdown to take the send arm", got)
	}

	// Control 2 — both streams must still be registered. shutdownStreams
	// iterates c.streams; a stream already evicted is never visited, and every
	// assertion below would pass for the wrong reason.
	c.smu.Lock()
	_, stuckReg := c.streams[sStuck.Stream().id]
	_, liveReg := c.streams[sLive.Stream().id]
	nreg := len(c.streams)
	c.smu.Unlock()
	if !stuckReg || !liveReg || nreg != 2 {
		t.Fatalf("registry holds %d streams (sStuck=%v sLive=%v); shutdownStreams only visits registered streams", nreg, stuckReg, liveReg)
	}

	srv.CloseClientConnections()

	// Assertion 1 — the reader escaped its own shutdown. readerLoop closes
	// readerDone in a defer (conn/conn.go:1417), so the channel closing is
	// proof that shutdownStreams returned rather than blocking on sStuck's
	// full channel while holding c.smu. This is map-order-independent: no
	// iteration order lets the loop finish if one of its sends can block.
	select {
	case <-c.readerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("readerDone still open 5s after the peer died: the reader is wedged inside shutdownStreams on a full event channel, holding c.smu — the connection will never report itself dead")
	}

	// Assertion 2 — the fan-out reached the pump-drained stream too.
	select {
	case err := <-liveTerm:
		if err == nil {
			t.Error("sLive: nil terminal after the connection died")
		} else if !isTerminalConnDeath(err) {
			t.Errorf("sLive: terminal %v is outside the set conn delivers on connection death", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("sLive never terminated after the connection died")
	}

	// Assertion 3 — the slow consumer is not stranded. It comes back late, as
	// slow consumers do, and must find a terminal waiting rather than a hang or
	// a clean end. This is the caller the default: arm exists for.
	stuckCtx, stuckCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stuckCancel()
	term, clean := terminalOf(stuckCtx, sStuck)
	if clean {
		t.Fatal("sStuck: clean END_STREAM after the connection died — the server never finished this response")
	}
	if errors.Is(term, context.DeadlineExceeded) {
		t.Fatal("sStuck: the slow consumer hung; its terminal was never delivered")
	}
	if !isTerminalConnDeath(term) {
		t.Errorf("sStuck: terminal %v is outside the set conn delivers on connection death", term)
	}
}
