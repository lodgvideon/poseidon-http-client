package client

import (
	"context"
	"testing"
	"time"
)

// The three HTTP/1.1 transports all hand back a release closure, and two of them
// wrapped it in a sync.Once while the single-connection one did not. What kept
// that safe was the e.done contract — Recv sets it on the terminal chunk, Close
// returns early once set — a convention spread across two files rather than a
// structural guard, and client.go says so where it explains the streaming path.
//
// The cost of being wrong differs too. A pooled double-release returns a
// connection to the pool twice: bad accounting. This one calls Unlock on an
// already-unlocked sync.Mutex, which panics the process.

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

	ex, _, _, err := s.openExchange(ctx)
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

	ex, _, _, err := s.openExchange(ctx)
	if err != nil {
		t.Fatalf("first openExchange: %v", err)
	}
	ex.(*h1Exchange).release(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ex2, _, _, oerr := s.openExchange(ctx)
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
