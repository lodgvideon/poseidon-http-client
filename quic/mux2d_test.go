package quic

import (
	"context"
	"testing"
	"time"
)

// The PR 2d mandatory QUIC-level test (docs/HTTP3_DESIGN.md §5): OpenStreamContext
// blocks on the cumulative bidi-stream limit and wakes when the peer raises it with
// MAX_STREAMS (OnMaxStreams → signalStreamCredit), and its ctx / connection-close
// escapes work. This is the transport half of the concurrency acid test; the
// http3 half (N concurrent Do over waves that exceed the limit) rides on it.

// newCreditConn builds a minimal established Conn with a bidi-stream limit of one,
// the wake latch and stream-credit channel allocated as NewConn would.
func newCreditConn(t *testing.T) *Conn {
	t.Helper()
	dcid := []byte("credtest")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	return &Conn{
		pc: &capturePC{}, dcid: dcid, oneRTTSealer: sealer,
		now:          time.Now,
		peer:         TransportParams{InitialMaxStreamsBidi: 1},
		connRecvMax:  DefaultConnRecvWindow,
		done:         make(chan struct{}),
		streamCredit: make(chan struct{}, 1),
	}
}

// TestMux2d_OpenStreamWaitsOnStreamCredit proves the credit wake: a second
// OpenStreamContext parks while at the initial_max_streams_bidi limit and returns
// only once the peer's MAX_STREAMS raises it (RFC 9000 §4.6).
func TestMux2d_OpenStreamWaitsOnStreamCredit(t *testing.T) {
	c := newCreditConn(t)

	if _, err := c.OpenStreamContext(context.Background()); err != nil {
		t.Fatalf("first OpenStreamContext: %v", err) // fills the limit of 1
	}

	opened := make(chan *Stream, 1)
	errc := make(chan error, 1)
	go func() {
		s, err := c.OpenStreamContext(context.Background())
		if err != nil {
			errc <- err
			return
		}
		opened <- s
	}()

	// Parked: no stream yet, no error yet, at the cumulative limit.
	select {
	case s := <-opened:
		t.Fatalf("OpenStreamContext returned stream %d before MAX_STREAMS raised the limit", s.ID())
	case err := <-errc:
		t.Fatalf("OpenStreamContext errored while it should be parked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// The peer raises the limit; OnMaxStreams signals the parked opener.
	h := &connFrameHandler{c: c}
	c.mu.Lock()
	err := h.OnMaxStreams(false, 4)
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("OnMaxStreams: %v", err)
	}

	select {
	case s := <-opened:
		if s.ID() != 4 {
			t.Fatalf("woken stream id = %d, want 4 (the next client bidi id)", s.ID())
		}
	case err := <-errc:
		t.Fatalf("OpenStreamContext = %v, want success after MAX_STREAMS", err)
	case <-time.After(2 * time.Second):
		t.Fatal("OpenStreamContext did not wake on the MAX_STREAMS credit grant")
	}
}

// TestMux2d_OpenStreamCreditWakesManyWaiters proves the baton-pass: one MAX_STREAMS
// grant that raises the limit by several slots wakes several parked openers in
// turn, not just one (openStreamLocked re-signals while credit remains).
func TestMux2d_OpenStreamCreditWakesManyWaiters(t *testing.T) {
	c := newCreditConn(t)
	if _, err := c.OpenStreamContext(context.Background()); err != nil {
		t.Fatalf("first OpenStreamContext: %v", err) // fills the limit of 1
	}

	const waiters = 5
	got := make(chan uint64, waiters)
	errc := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			s, err := c.OpenStreamContext(context.Background())
			if err != nil {
				errc <- err
				return
			}
			got <- s.ID()
		}()
	}

	// One MAX_STREAMS granting exactly enough slots for every waiter (1 already
	// open + 5 waiting = 6).
	h := &connFrameHandler{c: c}
	c.mu.Lock()
	err := h.OnMaxStreams(false, 1+waiters)
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("OnMaxStreams: %v", err)
	}

	seen := map[uint64]bool{}
	for i := 0; i < waiters; i++ {
		select {
		case id := <-got:
			if seen[id] {
				t.Fatalf("duplicate stream id %d handed out", id)
			}
			seen[id] = true
		case err := <-errc:
			t.Fatalf("a parked OpenStreamContext errored: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d waiters woke from a single MAX_STREAMS grant (baton-pass broken)", i, waiters)
		}
	}
}

// TestMux2d_OpenStreamContext_CtxCancelWakes proves the ctx escape: a parked
// OpenStreamContext returns ctx.Err() when its context is cancelled, so a caller
// is never wedged waiting on a peer that never raises the limit.
func TestMux2d_OpenStreamContext_CtxCancelWakes(t *testing.T) {
	c := newCreditConn(t)
	if _, err := c.OpenStreamContext(context.Background()); err != nil {
		t.Fatalf("first OpenStreamContext: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.OpenStreamContext(ctx)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond) // let it park at the limit
	cancel()

	select {
	case err := <-errc:
		if err != context.Canceled {
			t.Fatalf("OpenStreamContext = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not wake the parked OpenStreamContext")
	}
}

// TestMux2d_OpenStreamContext_CloseWakes proves the connection-close escape: a
// parked OpenStreamContext returns the latched terminal error when the connection
// terminates (terminateLocked).
func TestMux2d_OpenStreamContext_CloseWakes(t *testing.T) {
	c := newCreditConn(t)
	if _, err := c.OpenStreamContext(context.Background()); err != nil {
		t.Fatalf("first OpenStreamContext: %v", err)
	}

	errc := make(chan error, 1)
	go func() {
		_, err := c.OpenStreamContext(context.Background())
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond) // let it park at the limit

	c.mu.Lock()
	c.terminateLocked(ErrConnClosed)
	c.mu.Unlock()

	select {
	case err := <-errc:
		if err != ErrConnClosed {
			t.Fatalf("OpenStreamContext = %v, want ErrConnClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection close did not wake the parked OpenStreamContext")
	}
}
