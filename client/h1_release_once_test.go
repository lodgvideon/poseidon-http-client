package client

import (
	"context"
	"testing"
	"time"
)

// Releasing an HTTP/1.1 exchange must happen exactly once however many times it
// is asked for. Each of the three H1 transports used to own that itself, with a
// sync.Once built per exchange and a closure wrapping the real release in
// once.Do — three copies of one idea, and two allocations each on a path that
// already allocates the exchange. It now lives on h1Exchange, as a CAS on the
// struct every release flows through (#476).
//
// The cost of being wrong differs by transport, which is why both are covered
// here. A pooled double-release decrements the conn's active count twice, so the
// pool believes a busy connection is free and puts a second exchange on it —
// HTTP/1.1 has no multiplexing, so that is interleaved requests on one socket.
// The single-conn one calls Unlock on an already-unlocked sync.Mutex, which
// panics the process.

// TestH1SingleConn_ReleaseIsIdempotent is the gate. A second release must be a
// no-op, not a panic.
func TestH1SingleConn_ReleaseIsIdempotent(t *testing.T) {
	s := &h1singleConn{
		addr:        "h:80",
		dialer:      newH1FakeDialer(),
		metrics:     &Metrics{},
		dialTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex, _, _, _, err := s.openExchange(ctx)
	if err != nil {
		t.Fatalf("openExchange: %v", err)
	}
	h1ex, ok := ex.(*h1Exchange)
	if !ok {
		t.Fatalf("openExchange returned %T, want *h1Exchange", ex)
	}

	h1ex.release(true)
	// Without the guard this is "unlock of unlocked mutex" and takes the process
	// with it, so there is nothing to assert afterwards — surviving IS the
	// assertion.
	h1ex.release(true)
}

// TestH1PoolTransport_ReleaseIsIdempotent covers the pooled path, where the
// symptom of a lost guard is silent rather than a panic.
//
// The assertion is the pool's capacity, not a counter: with MaxConnsPerHost 1,
// exactly one exchange may be outstanding at a time. A double release drives the
// conn's active count to -1, and the third acquire below — which must block
// until its deadline because the second exchange still holds the only
// connection — instead succeeds and hands two exchanges the same socket.
func TestH1PoolTransport_ReleaseIsIdempotent(t *testing.T) {
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	defer func() { _ = p.Close() }()
	pt := &h1PoolTransport{p: p}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex1, _, _, _, err := pt.openExchange(ctx)
	if err != nil {
		t.Fatalf("first openExchange: %v", err)
	}
	ex1.(*h1Exchange).release(true)
	ex1.(*h1Exchange).release(true) // the second must be a no-op

	// The slot really was freed — idempotent must not mean inert.
	ex2, _, _, _, err := pt.openExchange(ctx)
	if err != nil {
		t.Fatalf("second openExchange after release: %v", err)
	}

	// ex2 still holds the pool's only connection, so this must not be served.
	short, shortCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer shortCancel()
	if _, _, _, _, oerr := pt.openExchange(short); oerr == nil {
		t.Fatal("a third exchange was handed out while the only connection was " +
			"checked out — the double release decremented the active count twice, " +
			"so two exchanges now share one HTTP/1.1 socket")
	}
	ex2.(*h1Exchange).release(true)
}

// TestH1SingleConn_ReleaseStillFreesTheSlot is the control: idempotent must not
// mean inert. After a release the transport can open the next exchange, which is
// only true if inFlight was actually unlocked.
func TestH1SingleConn_ReleaseStillFreesTheSlot(t *testing.T) {
	s := &h1singleConn{
		addr:        "h:80",
		dialer:      newH1FakeDialer(),
		metrics:     &Metrics{},
		dialTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ex, _, _, _, err := s.openExchange(ctx)
	if err != nil {
		t.Fatalf("first openExchange: %v", err)
	}
	ex.(*h1Exchange).release(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ex2, _, _, _, oerr := s.openExchange(ctx)
		if oerr != nil {
			t.Errorf("second openExchange: %v", oerr)
			return
		}
		ex2.(*h1Exchange).release(true)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the second openExchange blocked: release wrapped the slot away " +
			"instead of freeing it")
	}
}
