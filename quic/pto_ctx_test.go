package quic

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingReadPC wraps a deadline-capable net.Conn and counts Read calls, so a
// test can assert that a path returned WITHOUT reading. It forwards
// SetReadDeadline through the embedded conn, so readWithPTO still takes its
// deadline-capable branch — the fixture must not quietly change which path runs.
type countingReadPC struct {
	net.Conn
	reads atomic.Int64
}

func (p *countingReadPC) Read(b []byte) (int, error) {
	p.reads.Add(1)
	return p.Conn.Read(b)
}

// TestReadWithPTO_AlreadyCancelled: a context already done returns its error
// WITHOUT reading.
//
// The second half of that sentence used to be untested. Deleting both pre-Read
// ctx.Err() checks left the whole suite green: the read IS performed under that
// mutant, and the same context.Canceled comes back from the post-Read terminal
// check once the watchdog pokes the deadline. Two mechanisms produce the observed
// error and only one of them is this test's subject, so the Read count — not the
// error — is what has to be asserted. #836.
func TestReadWithPTO_AlreadyCancelled(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	pc := &countingReadPC{Conn: client}
	c := &Conn{pc: pc}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.readWithPTO(ctx, make([]byte, 64))

	require.ErrorIsf(t, err, context.Canceled,
		"readWithPTO with cancelled ctx = %v, want context.Canceled", err)
	assert.Zerof(t, pc.reads.Load(),
		"readWithPTO issued %d Read call(s) on an already-cancelled context. The error "+
			"is the same either way, so only this count says whether the pre-Read check "+
			"ran; on a live transport the difference is a syscall and a datagram "+
			"consumed for a caller that has already given up", pc.reads.Load())
}

// TestReadWithPTO_CancelUnblocksRead: a cancel during a blocking read interrupts
// it promptly VIA THE WATCHDOG — the goroutine that pokes the read deadline into
// the past. Run under -race, this also validates that concurrent SetReadDeadline
// against the in-flight Read.
//
// The old form asserted a flat 2-second tolerance, which is three times too loose
// to name a mechanism: with the watchdog deleted the read was still unblocked, at
// ~667ms, by the anti-amplification handshake PTO deadline (handshakeAntiDeadlock
// held, so computeReadDeadline returned now + 2*kInitialRtt). Two mechanisms
// satisfied the assertion. handshakeComplete closes that escape — with the
// handshake done and nothing in flight the read deadline is the 10s give-up
// bound, so the watchdog is the only thing that can return within the tolerance
// below. #836.
func TestReadWithPTO_CancelUnblocksRead(t *testing.T) {
	for _, delay := range []time.Duration{0, 20 * time.Millisecond, 200 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			client, server := net.Pipe() // Read blocks (server never writes) but honors deadlines
			defer client.Close()
			defer server.Close()
			c := &Conn{pc: client, handshakeComplete: true} // no anti-deadlock escape hatch
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var cancels atomic.Int64
			go func() {
				time.Sleep(delay)
				// Count the injection BEFORE performing it, not after. cancel() is what
				// unblocks readWithPTO, so the moment it runs the main goroutine is free
				// to return from the read and reach the assertion below -- and it beat
				// this goroutine to cancels.Add(1) on a loaded CI runner, reporting "the
				// injection never happened" for an injection that had. Incrementing first
				// is sound for what the counter means: nothing between the two lines can
				// return early, so a counter of 1 still implies the cancel follows.
				cancels.Add(1)
				cancel()
			}()

			start := time.Now()
			_, err := c.readWithPTO(ctx, make([]byte, 2048))
			elapsed := time.Since(start)

			require.ErrorIsf(t, err, context.Canceled, "readWithPTO = %v, want context.Canceled", err)
			require.EqualValuesf(t, 1, cancels.Load(),
				"the injection never happened (%d cancels): the timing below would then "+
					"be measuring something else entirely", cancels.Load())
			assert.Lessf(t, elapsed, delay+500*time.Millisecond,
				"the read returned %v after a cancel injected at %v. The only other way out "+
					"of this fixture is the 10s give-up deadline, so a figure near that "+
					"means the watchdog did not fire and the caller's cancel is not what "+
					"freed the read", elapsed, delay)
			t.Logf("cancel injected at %v (1 injection): readWithPTO returned after %v", delay, elapsed)
		})
	}

	// Control: with NOTHING injected the fixture cannot unblock itself, so the
	// timings above are attributable to the cancel rather than to a pipe that
	// happens to return.
	t.Run("control_no_cancel", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		c := &Conn{pc: client, handshakeComplete: true}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_, _ = c.readWithPTO(ctx, make([]byte, 2048))
			close(done)
		}()

		returnedEarly := false
		select {
		case <-done:
			returnedEarly = true
		case <-time.After(300 * time.Millisecond):
		}
		cancel() // release the goroutine
		<-done

		assert.Falsef(t, returnedEarly,
			"control: readWithPTO returned with no cancel injected, so this fixture "+
				"unblocks on its own and the timed arms above measure nothing")
	})
}

// TestReadWithPTO_CtxCancelNoPTOSpin: with data in flight and the PTO path
// genuinely armed, a context cancel is a terminal exit — it must not fire a probe
// (onPTO would bump ptoCount) or spin.
//
// The premise used to be false. ptoArmed(spaceApp) is gated on handshakeConfirmed,
// so on a Conn that never set it hasInFlight() returned false however many packets
// the fixture recorded as sent: handleExpiry's probe branch was unreachable, and
// the assertion below held for a reason unrelated to the one it names. Confirming
// the handshake is what makes it hold for the stated reason. #836.
func TestReadWithPTO_CtxCancelNoPTOSpin(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	// handshakeConfirmed arms the Application space's PTO timer (RFC 9002 §6.2.1);
	// handshakeComplete removes the anti-deadlock probe, so in-flight data is the
	// only reason a probe could fire here.
	c := &Conn{pc: client, handshakeComplete: true, handshakeConfirmed: true}
	c.sent[spaceApp].onSent(0, time.Now(), true, nil) // an unacked ack-eliciting packet
	require.True(t, c.hasInFlight(),
		"the premise of this test is that the PTO path is armed; without it handleExpiry "+
			"cannot probe and the assertion below proves nothing")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cancels atomic.Int64
	go func() {
		time.Sleep(20 * time.Millisecond)
		// Counted before it is performed, for the reason spelled out on the same
		// pattern in TestReadWithPTO_CancelUnblocksRead above.
		cancels.Add(1)
		cancel()
	}()

	_, err := c.readWithPTO(ctx, make([]byte, 2048))

	require.ErrorIsf(t, err, context.Canceled, "readWithPTO = %v, want context.Canceled", err)
	require.EqualValuesf(t, 1, cancels.Load(),
		"the cancel was never injected (%d): nothing was interrupted", cancels.Load())
	assert.Zerof(t, c.ptoCount,
		"ptoCount = %d after a context cancel. A cancel is the caller giving up, not a "+
			"lost packet: probing on it retransmits data nobody is waiting for and backs "+
			"off a timer the next request inherits", c.ptoCount)
}
