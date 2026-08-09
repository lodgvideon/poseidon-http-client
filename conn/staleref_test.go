package conn

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// These tests pin the three ways a stale *Stream reaches another request's
// stream (issue #370). All three are written to fail on the unfixed code and to
// become the guarantee once the lifetime guard lands.
//
// None of them depends on a sync.Pool draw. The test that pinned this before
// (TestStream_RecvAfterClose_ReallocWindowStillOpen) called allocStream and
// t.Skip'd when the pool handed back a different struct — which means the fix
// could ship with its own regression test never having run. reallocInPlace
// below reproduces allocStream's pool-hit arm directly instead, so the
// re-allocation is a fact of the test rather than a draw.

// syncBuf is an io.Writer that records what the Framer emits and can be read
// safely while a writer goroutine is still running.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// staleRefConn builds a hand-wired Conn whose writes land in a syncBuf.
func staleRefConn() (*Conn, *syncBuf) {
	var out syncBuf
	c := &Conn{
		opts:               ConnOptions{}.defaulted(),
		fr:                 frame.NewFramer(&out, bytes.NewReader(nil)),
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		connRecvWindow:     int32(connInitialRecvWindow),
		peerConnSendWindow: 1 << 20,
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	return c, &out
}

// reallocInPlace reproduces the pool-hit arm of Conn.allocStream (conn.go, the
// `if v := c.streamPool.Get(); v != nil` branch) against a specific struct, so a
// test can stage the re-allocation the bug needs without asking sync.Pool for a
// particular draw.
//
// It is white-box on purpose. If allocStream's re-arm ever grows a field this
// does not mirror, these tests stage a re-allocation that no longer matches the
// real one — so keep the two in step.
func reallocInPlace(c *Conn, s *Stream, id uint32, recvWindow int32) {
	s.w = c
	s.recvWindow = recvWindow
	s.released.Store(false)
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()
	c.smu.Lock()
	c.streams[id] = s
	c.smu.Unlock()
}

// retireAndRecycle drives a stream through the clean completion that returns it
// to the pool: both directions ended, the connection done with it, then Close.
func retireAndRecycle(t *testing.T, s *Stream) {
	t.Helper()
	s.mu.Lock()
	s.connDone = true
	s.mu.Unlock()
	if err := s.ref().Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStaleRef_RecvReachesTheNextRequest pins the receive-side hole: a reference
// kept past Close is admitted after the struct has been handed to another
// request, and returns that request's events.
func TestStaleRef_RecvReachesTheNextRequest(t *testing.T) {
	c, _ := staleRefConn()
	s := c.allocStream(4, 65535)
	reallocInPlace(c, s, 5, 65535)
	refA := s.ref() // the handle request A holds
	s.push(StreamEvent{Type: EventHeaders})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := refA.Recv(ctx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	retireAndRecycle(t, s)

	// Request B claims the same struct.
	reallocInPlace(c, s, 7, 65535)
	s.push(StreamEvent{Type: EventData})

	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	ev, err := refA.Recv(rctx) // the STALE handle, from request A
	if err == nil {
		t.Errorf("stale Recv returned %v with no error: request A's reader is being handed "+
			"request B's events", ev.Type)
	}
}

// TestStaleRef_CloseResetsTheNextRequest pins the destructive hole, and it needs
// no race at all.
//
// Close's idempotency latch is re-armed by the re-allocation, so a second Close
// from the finished request passes it, marks the new lifetime closed, wakes its
// blocked writer with an error, and puts RST_STREAM(CANCEL) on the wire for the
// new request's stream id.
func TestStaleRef_CloseResetsTheNextRequest(t *testing.T) {
	c, out := staleRefConn()
	s := c.allocStream(4, 65535)
	reallocInPlace(c, s, 5, 65535)
	refA := s.ref() // the handle request A holds
	retireAndRecycle(t, s)

	// Request B claims the struct and is live on the wire as stream 7.
	reallocInPlace(c, s, 7, 65535)
	before := len(out.snapshot())

	_ = refA.Close() // the STALE Close, from request A

	for _, fh := range parseFrameHeaders(t, out.snapshot()[before:]) {
		if fh.ftype == 0x3 { // RST_STREAM
			t.Errorf("a stale Close sent RST_STREAM on stream %d — request A tore down request B", fh.streamID)
		}
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		t.Error("a stale Close marked the next request's stream closed")
	}
}

// TestStaleRef_ParkedWriterWakesOntoTheNextRequest pins the send-side hole that
// a check at the method door cannot reach.
//
// SendData validates and then releases s.mu before writeData parks in
// acquireSendCredits waiting for the peer's credit. The wake loop re-reads only
// s.closed — and s.closed is false for every stream that reaches the pool,
// because markStreamDone pools only when it is false. So the woken writer
// debits the connection's live send window and emits DATA carrying whatever
// stream id the struct now holds.
func TestStaleRef_ParkedWriterWakesOntoTheNextRequest(t *testing.T) {
	c, out := staleRefConn()
	s := c.allocStream(4, 65535)
	reallocInPlace(c, s, 5, 65535)

	// Ten bytes of credit: writeData emits one short DATA frame and then parks
	// for the rest, which is the state this test needs and the only thing it
	// waits on.
	c.fcOutMu.Lock()
	s.mu.Lock()
	s.sendWindow = 10
	s.mu.Unlock()
	c.fcOutMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	refA := s.ref() // the handle request A holds
	done := make(chan error, 1)
	go func() { done <- refA.SendData(ctx, make([]byte, 100), false) }()

	// Wait for the writer to be parked, observed rather than timed: the first
	// chunk is on the wire exactly when it has run out of credit.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if frames := parseDataFrames(t, out.snapshot()); len(frames) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the writer never emitted its first chunk; it is not parked on credit")
		}
		time.Sleep(time.Millisecond)
	}
	before := len(out.snapshot())

	// The stream completes, is recycled, and request B claims the struct.
	retireAndRecycle(t, s)
	reallocInPlace(c, s, 7, 65535)

	// The peer grants credit — on request B's stream, as far as the connection
	// is concerned.
	c.fcOutMu.Lock()
	connWindowBefore := c.peerConnSendWindow
	s.mu.Lock()
	s.sendWindow = 1 << 20
	s.mu.Unlock()
	c.fcOutCond.Broadcast()
	c.fcOutMu.Unlock()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the parked writer never returned")
	}

	for _, fh := range parseDataFrames(t, out.snapshot()[before:]) {
		if fh.streamID == 7 {
			t.Errorf("request A's parked writer emitted DATA on stream 7: its body bytes were " +
				"spliced into request B's stream")
		}
	}

	// The second consequence, and the one the wake-loop check exists for on its
	// own: a writer that bails on a dead lifetime must not first spend the
	// connection's shared credit on it. That debit is permanent — every later
	// stream on this connection stalls sooner — and it leaves nothing on the
	// wire, so the frame assertion above cannot see it. Without this, disabling
	// the wake-loop check alone passed every test, because the pre-write check
	// still stopped the wrong-id frame.
	c.fcOutMu.Lock()
	connWindowAfter := c.peerConnSendWindow
	c.fcOutMu.Unlock()
	if connWindowAfter != connWindowBefore {
		t.Errorf("connection send window went %d -> %d: the woken writer debited shared credit "+
			"for a stream that no longer exists", connWindowBefore, connWindowAfter)
	}
}

// TestStaleRef_GenerationOnlyEverIncreases guards the trap the generation field
// sets for its own maintainers.
//
// resetForPoolLocked is a wall of zeroing assignments — id, w, closed,
// localEnded, every per-response counter — and gen is the one line in it that
// must INCREMENT. A future tidy-up that makes it match its neighbours and store
// 0 collapses every lifetime onto one value and silently reopens #370.
//
// The single-recycle tests do not catch that: the first Store(0) still moves gen
// off the seed, so the handle minted before it still mismatches. It only bites
// from the second recycle onward, which is why this asserts the invariant
// directly rather than another scenario.
func TestStaleRef_GenerationOnlyEverIncreases(t *testing.T) {
	c, _ := staleRefConn()
	s := c.allocStream(4, 65535)

	seen := []uint64{s.gen.Load()}
	if seen[0] == 0 {
		t.Fatal("a fresh stream has generation 0; the zero StreamRef would match it")
	}
	for i := 0; i < 4; i++ {
		reallocInPlace(c, s, uint32(5+2*i), 65535)
		refBefore := s.ref()
		retireAndRecycle(t, s)
		g := s.gen.Load()
		if g <= seen[len(seen)-1] {
			t.Fatalf("recycle %d left generation %d, previous %d — it must strictly increase, "+
				"or two lifetimes share a value and a stale handle passes",
				i, g, seen[len(seen)-1])
		}
		if g == 0 {
			t.Fatalf("recycle %d zeroed the generation", i)
		}
		seen = append(seen, g)
		// And the handle from the lifetime just retired stays refused, however
		// many recycles have happened.
		if _, err := refBefore.Recv(t.Context()); !errors.Is(err, ErrStaleStream) {
			t.Fatalf("recycle %d: handle from the retired lifetime = %v, want ErrStaleStream", i, err)
		}
	}
}
