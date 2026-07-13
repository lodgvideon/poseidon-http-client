package quic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The wake vocabulary added in PR 2b (docs/HTTP3_DESIGN.md §3.3) is additive and
// inert — the receive path produces the signals but nothing consumes them yet.
// These tests exercise the primitives in isolation, as §5 (PR 2b "Test")
// prescribes: signalReady wakes a WaitReadable; the cwnd broadcast wakes a
// WaitSendable; the blockPace timer fires with no signal; repeated signals
// coalesce to one token; terminateLocked wakes all waiters and is idempotent
// under concurrent callers (run under -race).

// newWakeConn builds a minimal Conn with the wake latch allocated, as NewConn
// would, without driving a real handshake. Hand-built test Conns elsewhere leave
// done nil; these tests need it live so terminateLocked and the Wait helpers work.
func newWakeConn() *Conn {
	return &Conn{
		now:          time.Now,
		done:         make(chan struct{}),
		streamCredit: make(chan struct{}, 1),
	}
}

// waitTimeout is generous so a slow CI machine does not flake; a working wake
// returns in microseconds.
const waitTimeout = 2 * time.Second

// TestSignalReady_WakesWaitReadable checks that signalReady delivers a token that
// unblocks a WaitReadable (§3.3 recv-readiness).
func TestSignalReady_WakesWaitReadable(t *testing.T) {
	c := newWakeConn()
	s := &Stream{id: 0, conn: c, ready: make(chan struct{}, 1)}

	got := make(chan error, 1)
	go func() { got <- s.WaitReadable(context.Background()) }()

	c.mu.Lock()
	c.signalReady(s) // producer runs under c.mu in the real receive path
	c.mu.Unlock()

	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("WaitReadable = %v, want nil", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("WaitReadable did not wake on signalReady")
	}
}

// TestMaybeBroadcastSendWindow_WakesWaitSendable checks the critical cwnd wake
// (§3.3, INV-5): freeing in-flight bytes flags the burst, and the end-of-burst
// broadcast wakes a sender parked on blockCong, which has no frame-driven wake.
func TestMaybeBroadcastSendWindow_WakesWaitSendable(t *testing.T) {
	c := newWakeConn()
	s := &Stream{id: 0, conn: c, ready: make(chan struct{}, 1), sendBlock: blockCong}
	c.streams = map[uint64]*Stream{0: s}

	got := make(chan error, 1)
	go func() { got <- s.WaitSendable(context.Background()) }()

	c.mu.Lock()
	c.removeInFlight(0) // the flight-freeing choke point sets sendWindowGrew
	if !c.sendWindowGrew {
		c.mu.Unlock()
		t.Fatal("removeInFlight did not set sendWindowGrew")
	}
	c.maybeBroadcastSendWindow() // end-of-burst sweep signals every open stream
	if c.sendWindowGrew {
		c.mu.Unlock()
		t.Fatal("maybeBroadcastSendWindow did not clear sendWindowGrew")
	}
	c.mu.Unlock()

	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("WaitSendable = %v, want nil", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("WaitSendable did not wake on the cwnd broadcast")
	}
}

// TestWaitSendable_PaceTimerFires checks that a pacing-limited sender (blockPace)
// wakes on its wall-clock timer with no inbound signal at all (§3.3): blockPace
// refills on elapsed time, so WaitSendable owns a time.After(pacingRefillDelay).
func TestWaitSendable_PaceTimerFires(t *testing.T) {
	c := newWakeConn()
	c.cwnd = kInitialWindow
	c.rtt.smoothedRTT = time.Millisecond // keep pacingRefillDelay tiny for a fast test
	s := &Stream{id: 0, conn: c, ready: make(chan struct{}, 1), sendBlock: blockPace}

	start := time.Now()
	got := make(chan error, 1)
	go func() { got <- s.WaitSendable(context.Background()) }()

	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("WaitSendable(blockPace) = %v, want nil (timer)", err)
		}
		if elapsed := time.Since(start); elapsed > waitTimeout {
			t.Fatalf("pacing timer took %v, want prompt", elapsed)
		}
	case <-time.After(waitTimeout):
		t.Fatal("WaitSendable(blockPace) did not fire its pacing timer with no signal")
	}
}

// TestSignalReady_Coalesces checks the cap-1 coalescing rule (§3.3): repeated
// signalReady with no consumer leaves exactly one buffered token, so a slow
// consumer never stalls the reader and a wake is never lost or doubled.
func TestSignalReady_Coalesces(t *testing.T) {
	c := newWakeConn()
	s := &Stream{id: 0, conn: c, ready: make(chan struct{}, 1)}

	for i := 0; i < 5; i++ {
		c.signalReady(s)
	}

	select {
	case <-s.ready:
	default:
		t.Fatal("expected exactly one buffered token after repeated signalReady, found none")
	}
	select {
	case <-s.ready:
		t.Fatal("expected exactly one token, found a second (no coalescing)")
	default:
	}
}

// TestSignalReady_NilChannelIsNoOp checks that signalReady on a stream with no
// ready channel (a hand-built test stream) neither blocks nor panics — the
// default branch of the non-blocking send covers a nil channel.
func TestSignalReady_NilChannelIsNoOp(t *testing.T) {
	c := newWakeConn()
	s := &Stream{id: 0, conn: c} // ready is nil
	c.signalReady(s)             // must not panic or block
}

var errTestTerminate = errors.New("quic: test terminate")

// TestTerminateLocked_WakesAllWaiters checks that the single-close latch wakes
// every blocked WaitReadable with the recorded error (§3.3).
func TestTerminateLocked_WakesAllWaiters(t *testing.T) {
	c := newWakeConn()
	const n = 8
	got := make(chan error, n)
	for i := 0; i < n; i++ {
		s := &Stream{id: uint64(i * 4), conn: c, ready: make(chan struct{}, 1)}
		go func() { got <- s.WaitReadable(context.Background()) }()
	}

	c.mu.Lock()
	c.terminateLocked(errTestTerminate)
	c.mu.Unlock()

	for i := 0; i < n; i++ {
		select {
		case err := <-got:
			if !errors.Is(err, errTestTerminate) {
				t.Fatalf("waiter %d got %v, want errTestTerminate", i, err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("a waiter did not wake on terminateLocked")
		}
	}
}

// TestTerminateLocked_IdempotentConcurrent checks that many concurrent callers
// close c.done exactly once (no panic), first error wins, terminated latches
// (§3.3). Run under -race, it also proves the terminated/closeErr/done writes are
// properly serialized by c.mu.
func TestTerminateLocked_IdempotentConcurrent(t *testing.T) {
	c := newWakeConn()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			c.mu.Lock()
			c.terminateLocked(fmt.Errorf("terminate-%d", k))
			c.mu.Unlock()
		}(i)
	}
	wg.Wait()

	// done closed exactly once: a receive succeeds and a second close would have
	// panicked before reaching here.
	select {
	case <-c.done:
	default:
		t.Fatal("c.done was not closed after terminateLocked")
	}
	c.mu.Lock()
	terminated, closeErr := c.terminated, c.closeErr
	c.mu.Unlock()
	if !terminated {
		t.Fatal("terminated flag not set")
	}
	if closeErr == nil {
		t.Fatal("closeErr not set (first error should win)")
	}
}

// TestRecvState_LockedSnapshot checks that RecvState mirrors Finished /
// ResetReceived / ResetCode under c.mu (§3.3, fix F5).
func TestRecvState_LockedSnapshot(t *testing.T) {
	c := newWakeConn()
	s := &Stream{id: 0, conn: c, ready: make(chan struct{}, 1)}

	if fin, reset, code := s.RecvState(); fin || reset || code != 0 {
		t.Fatalf("fresh RecvState = (%v,%v,%d), want (false,false,0)", fin, reset, code)
	}

	c.mu.Lock()
	s.recvReset = true
	s.recvResetCode = 0x0107
	c.mu.Unlock()

	fin, reset, code := s.RecvState()
	if !fin || !reset || code != 0x0107 {
		t.Fatalf("reset RecvState = (%v,%v,%#x), want (true,true,0x0107)", fin, reset, code)
	}
}
