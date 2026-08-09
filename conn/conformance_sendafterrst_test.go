package conn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestConformance_RFC9113_Sec6_4_AcquireSendCreditsBailsOnResetStream pins RFC
// 9113 §5.1: "An endpoint MUST NOT send frames other than PRIORITY on a closed
// stream." A stream the peer reset with RST_STREAM (§6.4) is closed, so a DATA
// send that is (or would be) blocked in acquireSendCredits waiting for
// send-window credit must observe the reset and return, never hand back credit
// that would let writeData put DATA on it. Here the stream window is empty (a
// real send would block) and the stream is already reset, so acquireSendCredits
// bails immediately with ErrStreamClosed — the check runs at the top of the loop,
// before Wait, so no wakeup is needed for this case.
func TestConformance_RFC9113_Sec6_4_AcquireSendCreditsBailsOnResetStream(t *testing.T) {
	c := &Conn{}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 65535 // the connection window has room ...
	s := newStream(1, 8, nil, 65535)
	s.sendWindow = 0 // ... but the stream window is empty, so a send would block
	s.closed = true  // and the stream has been reset

	done := make(chan error, 1)
	go func() {
		_, err := c.acquireSendCredits(context.Background(), s, s.gen.Load(), 10, 0)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStreamClosed) {
			t.Errorf("acquireSendCredits on a reset stream = %v, want ErrStreamClosed (§6.4: no DATA after RST)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquireSendCredits blocked on a reset stream instead of bailing (§6.4)")
	}
}

// TestConformance_RFC9113_Sec6_4_ResetWakesBlockedSender covers the wake half: a
// writer already parked in acquireSendCredits when the reset arrives is woken by
// wakeSendWaiters (as OnRSTStream calls it) and then bails. The broadcast is
// retried until the writer is observed parked, so the test never races the
// goroutine into Wait.
func TestConformance_RFC9113_Sec6_4_ResetWakesBlockedSender(t *testing.T) {
	c := &Conn{}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 65535
	s := newStream(1, 8, nil, 65535)
	s.sendWindow = 0 // blocks in Wait; not yet closed

	done := make(chan error, 1)
	go func() {
		_, err := c.acquireSendCredits(context.Background(), s, s.gen.Load(), 10, 0)
		done <- err
	}()

	// Simulate the peer RST: mark closed, then broadcast (as OnRSTStream does).
	// Re-broadcast on a tick so a broadcast issued before the writer reaches Wait
	// is not lost — Cond.Broadcast only wakes goroutines already parked.
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			if !errors.Is(err, ErrStreamClosed) {
				t.Errorf("woken writer on a reset stream = %v, want ErrStreamClosed", err)
			}
			return
		case <-deadline:
			t.Fatal("wakeSendWaiters did not release the blocked writer after reset (§6.4)")
		case <-tick.C:
			c.wakeSendWaiters()
		}
	}
}
