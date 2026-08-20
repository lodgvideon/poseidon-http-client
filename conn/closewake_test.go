package conn

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// parkProbe reports the moment a writer is about to park on fcOutCond, and can
// widen the window before it gets there.
//
// acquireSendCredits registers a context watchdog with context.AfterFunc on its
// first actual block — after it has read s.closed for that iteration, and
// immediately before fcOutCond.Wait(). AfterFunc asks the context for its Done
// channel, so a Done call is the one observable event inside that window, and
// delay lands squarely in it.
//
// Observing it is what makes the handshake in the test deterministic. The writer
// holds c.fcOutMu from the top of acquireSendCredits until Wait releases it, and
// Wait registers on the cond's notify list BEFORE unlocking. So a test that sees
// the signal and then acquires fcOutMu is guaranteed the writer is already
// registered, and the Broadcast that follows cannot be lost.
type parkProbe struct {
	context.Context
	once   sync.Once
	signal chan struct{}
	delay  time.Duration
}

// Done reports the park point and then stalls for delay, so a test can arrange
// for Close to land in the window between the writer's last look at s.closed and
// its arrival on the cond's notify list.
func (p *parkProbe) Done() <-chan struct{} {
	p.once.Do(func() {
		close(p.signal)
		time.Sleep(p.delay)
	})
	return p.Context.Done()
}

// TestStream_CloseWakesASenderParkedOnCredit pins that abandoning a stream
// actually abandons its writer.
//
// acquireSendCredits already bails with ErrStreamClosed when it observes
// s.closed — but only on wake, and it is parked in cond.Wait(). That check and
// the broadcast that makes it observable were written together for the peer
// RST_STREAM path (RFC 9113 §6.4). Close sets the same flag and inherited
// neither, so a Send blocked on flow-control credit slept through the Close
// meant to abandon it and woke only when its own context expired — which for a
// long-lived request context means a stuck goroutine, on the one API documented
// as "how a client abandons an RPC early".
//
// The wait for the writer to park used to poll c.peerConnSendWindow == 0 — a
// field the Arrange block sets to 0 and nothing ever raises — so it broke on its
// first iteration and the only synchronisation left was a fixed 100 ms sleep
// (#802).
//
// That sleep was dead weight rather than the flake it looked like, and the
// measurement is worth recording because the obvious reading is wrong. Cond
// .Broadcast wakes only goroutines already parked, so a Close landing before the
// writer reaches Wait() looks like a lost wakeup — but it cannot be one:
// wakeSendWaiters takes c.fcOutMu, and the writer holds that mutex from the top
// of acquireSendCredits until Wait() releases it, having already registered on
// the notify list. Close therefore cannot broadcast until the writer is
// reachable. Running the old fixture with the 250 ms injection below confirms
// it: it passes.
//
// So the repair is not a flake fix. It replaces a wait that was doing nothing
// with one that waits for the thing it names, and it adds the case the old test
// could not express at all — a writer that parks well after Close was called.
func TestStream_CloseWakesASenderParkedOnCredit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
	}{
		{"parks immediately", 0},
		{"parks after Close would have been called", 250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := &Conn{streams: map[uint32]*Stream{}}
			c.fcOutCond = sync.NewCond(&c.fcOutMu)
			c.fr = frame.NewFramer(&buf, nil) // writer first
			c.peerConnSendWindow = 0          // no credit, and none is coming

			s := newStream(1, 8, c, 65535)
			s.sendWindow = 0
			c.streams[1] = s

			// A context far longer than the test, so passing cannot come from it.
			base, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			probe := &parkProbe{Context: base, signal: make(chan struct{}), delay: tc.delay}
			done := make(chan error, 1)
			go func() { done <- s.ref().SendData(probe, []byte("payload"), false) }()

			// Wait for the writer to reach the park point rather than guessing.
			select {
			case <-probe.signal:
			case <-time.After(5 * time.Second):
				require.FailNow(t, "the writer never reached the park point; "+
					"the fixture never produced the state this test is about")
			}
			// Acquiring fcOutMu IS the rest of the wait: the writer holds it until
			// Wait releases it, and Wait registers on the notify list first.
			c.fcOutMu.Lock()
			credit := c.peerConnSendWindow
			c.fcOutMu.Unlock()
			require.Zerof(t, credit,
				"peerConnSendWindow = %d — with credit available the writer would have "+
					"returned instead of parking, and Close would be waking nobody", credit)

			_ = s.ref().Close()

			select {
			case err := <-done:
				require.Truef(t, errors.Is(err, ErrStreamClosed), "Send = %v, want ErrStreamClosed", err)
			case <-time.After(3 * time.Second):
				require.FailNow(t, "Send still parked 3s after Close; only its own context would free it")
			}
		})
	}
}
