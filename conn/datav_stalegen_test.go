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

// TestWriteDataVec_StaleGenerationAfterCreditIsRefused covers the re-check that
// #534 reported as untested: replacing
//
//	id, stale := s.id, s.gen.Load() != wantGen
//
// with `stale := false` in writeDataVec left the whole conn suite green.
//
// Why it survived everything else: acquireSendCredits performs the SAME
// generation check while parked (conn.go), so a stream recycled during the
// credit wait is caught there and never reaches this line. What this line
// guards is the narrower window AFTER credit is granted and before the frame is
// emitted — the loop releases s.mu, and the stream can be completed, recycled
// and handed to another request inside it. Without the re-check the woken
// writer emits DATA carrying the next request's stream id.
//
// The issue called that "a scheduling window rather than a state a hand-built
// Conn reaches on its own", and declined to fake a test for it. It is reachable
// deterministically, though, because the window has a lock in it: the loop takes
// c.wmu immediately after acquireSendCredits returns. A test that holds c.wmu
// owns the window.
//
//	writer                          test
//	------                          ----
//	parks in acquireSendCredits
//	                                observes the park, takes c.wmu
//	                                grants credit — generation still valid,
//	                                so acquireSendCredits returns normally
//	blocks taking c.wmu
//	                                bumps s.gen (the recycle), releases c.wmu
//	re-reads s.gen -> stale
//
// No production code changes and no sleeps decide the outcome; the ordering is
// carried entirely by the lock the loop already takes.
func TestWriteDataVec_StaleGenerationAfterCreditIsRefused(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}, opts: ConnOptions{}.defaulted()}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&buf, nil) // writer first
	c.peerConnSendWindow = 0          // no credit yet: the writer must park

	s := newStream(1, 8, c, 65535)
	s.sendWindow = 65535 // stream credit is fine; the conn window is the gate
	c.streams[1] = s
	gen := s.gen.Load()

	// A context far longer than the test, so a pass cannot come from it expiring.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.ref().SendDataV(ctx, [][]byte{[]byte("first"), []byte("second")}, false)
	}()

	// Wait for the writer to be parked on credit rather than racing the lock
	// against its entry. Parked is observable: it holds no wmu and the conn
	// window it is waiting on is still zero.
	if !waitParked(t, c) {
		t.Fatal("writer never parked on credit; the rest of this test would prove nothing")
	}

	// Own the window. From here the writer cannot get past c.wmu.Lock().
	c.wmu.Lock()

	// Grant credit. acquireSendCredits wakes, re-checks the generation — still
	// valid — and returns with credit, then blocks on the lock held above.
	const grant = 4096
	if err := c.onWindowUpdate(0, grant); err != nil {
		c.wmu.Unlock()
		t.Fatalf("onWindowUpdate: %v", err)
	}

	// Wait until the credit has actually been DEBITED before recycling.
	//
	// This is the whole test. Without it the generation changes while the writer
	// is still inside acquireSendCredits, whose own identical check catches it —
	// and the mutation this test exists to kill survives, because the line under
	// test is never reached. Verified: the first version of this test passed with
	// `stale := false` hardcoded.
	//
	// The debit is observable and the writer cannot proceed past it, because the
	// next thing it does is take the c.wmu held above.
	if !waitCreditTaken(t, c, grant) {
		c.wmu.Unlock()
		t.Fatal("credit was granted but never debited; the writer is not parked on " +
			"c.wmu, so the re-check under test would not be the one exercised")
	}

	// The recycle, inside the window this test owns.
	s.gen.Add(1)
	c.wmu.Unlock()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStaleStream) {
			t.Fatalf("SendDataV = %v, want ErrStaleStream — the stream was recycled "+
				"after credit was granted, so this write belongs to a request that no "+
				"longer owns the struct", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendDataV never returned after the generation changed")
	}

	// Nothing may have reached the wire: emitting here would carry the next
	// request's stream id.
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes for a recycled stream, want none: %q", buf.Len(), buf.Bytes())
	}
	if got := s.gen.Load(); got == gen {
		t.Fatal("the generation never changed; the recycle this test simulates did not happen")
	}
}

// waitParked reports whether a writer reached cond.Wait inside
// acquireSendCredits, polling the state it parks on rather than sleeping a
// fixed time and hoping.
func waitParked(t *testing.T, c *Conn) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.fcOutMu.Lock()
		parked := c.peerConnSendWindow == 0
		c.fcOutMu.Unlock()
		if parked {
			// The flag above is true before the writer parks as well, so give it
			// the moment it needs to actually reach cond.Wait. If it has not, the
			// onWindowUpdate below is simply a no-op broadcast and the test fails
			// on its timeout rather than passing for the wrong reason.
			time.Sleep(100 * time.Millisecond)
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// waitCreditTaken reports whether the writer has debited the granted credit and
// therefore left acquireSendCredits. It is the synchronisation that makes this
// test exercise the re-check in writeDataVec rather than the identical one
// inside acquireSendCredits.
func waitCreditTaken(t *testing.T, c *Conn, granted int32) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.fcOutMu.Lock()
		taken := c.peerConnSendWindow < granted
		c.fcOutMu.Unlock()
		if taken {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
