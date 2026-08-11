package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// syncSink records everything written and reports how much has arrived, so a
// test can look at the wire while a writer is parked.
type syncSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncSink) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncSink) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Len()
}

// TestWriteData_ReachesTheWireBeforeParkingOnCredit pins the property the send
// loop exists to guarantee: when the writer parks waiting for the peer's
// WINDOW_UPDATE, everything it has already written is on the wire. A frame left
// in the buffer is a frame the peer never sees, so the credit it would grant for
// it never comes and both sides wait.
//
// The suite had no test that parked at all — every stream in it has a window
// larger than its payload — so this was uncovered. A send window smaller than
// the payload makes it a one-iteration certainty rather than a race.
//
// It does NOT discriminate between the loop's two flushes, and that is not a
// weakness to fix: the flush at the TOP of the loop is redundant on the current
// code, because every iteration already flushes after emitting its frame and
// before releasing wmu. Removing the top flush is an equivalent mutant — the
// mutation survives because there is nothing to catch, not because the test is
// weak. It is defence in depth against a future writer that leaves bytes
// buffered under wmu, which is worth keeping and worth saying out loud.
func TestWriteData_ReachesTheWireBeforeParkingOnCredit(t *testing.T) {
	sink := &syncSink{}
	c, s := sinkConn(t, sink)

	// One frame's worth of credit for a two-frame payload: the first chunk goes
	// out, the second must park.
	const chunk = 512
	c.opts.Settings.MaxFrameSize = chunk
	s.sendWindow = chunk
	c.peerConnSendWindow = chunk

	payload := make([]byte, chunk*2)
	done := make(chan error, 1)
	go func() {
		done <- c.writeData(context.Background(), s, s.gen.Load(), payload, false)
	}()

	// The writer is now parked on credit for the second chunk. The first must
	// already be on the wire.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.len() > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("writeData returned early: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if got := sink.len(); got == 0 {
		t.Fatal("nothing reached the wire while the writer is parked waiting for credit — " +
			"the peer cannot see the frame it would grant credit for, and both sides wait")
	}

	// Release the writer so the test does not leak a goroutine: grant the credit
	// the way an inbound WINDOW_UPDATE would, then wake the parked writer.
	c.fcOutMu.Lock()
	s.sendWindow += chunk
	c.peerConnSendWindow += chunk
	c.fcOutMu.Unlock()
	c.fcOutCond.Broadcast()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Log("writer did not finish after the credit grant; the flush assertion above still held")
	}
}

// sinkConn wires a Conn's framer to w.
func sinkConn(t *testing.T, w *syncSink) (*Conn, *Stream) {
	t.Helper()
	c := newGoAwayConn()
	c.fr = frame.NewFramer(w, bytes.NewReader(nil))
	s := newStream(1, 8, c, 1<<20)
	s.id = 1
	return c, s
}
