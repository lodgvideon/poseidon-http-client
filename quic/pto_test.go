package quic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn_PTOPeriod(t *testing.T) {
	c := &Conn{}

	preSample := c.ptoPeriod()
	c.rtt.update(40*ms, 0) // smoothed=40ms, rttvar=20ms -> base = 40 + max(80,1) = 120ms
	sampled := c.ptoPeriod()
	c.ptoCount = 2
	backedOff := c.ptoPeriod()

	require.Equalf(t, 2*kInitialRtt, preSample,
		"pre-sample ptoPeriod = %v, want %v", preSample, 2*kInitialRtt)
	require.Equalf(t, 120*ms, sampled, "ptoPeriod = %v, want 120ms", sampled)
	require.Equalf(t, 120*ms<<2, backedOff, "backoff ptoPeriod = %v, want %v", backedOff, 120*ms<<2)
}

// TestConn_OnPTO_QueuesProbe checks a probe timeout re-queues the oldest
// unacknowledged packet's frames and backs off (RFC 9002 §6.2.4).
func TestConn_OnPTO_QueuesProbe(t *testing.T) {
	base := time.Unix(1, 0)
	// handshakeConfirmed: 1-RTT data is only in flight after HANDSHAKE_DONE, and the
	// Application-space probe timer is not armed before that (RFC 9002 §6.2.1).
	c := &Conn{now: func() time.Time { return base }, handshakeConfirmed: true}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "oldest"))
	c.sent[spaceApp].onSent(1, base.Add(ms), true, streamFrame(0, 6, "newer"))

	c.onPTO()

	require.Equalf(t, uint(1), c.ptoCount, "ptoCount = %d, want 1", c.ptoCount)
	require.Lenf(t, c.retransQueue[spaceApp], 1,
		"probe queue = %+v, want the oldest packet's frame", c.retransQueue[spaceApp])
	require.Equalf(t, "oldest", string(c.retransQueue[spaceApp][0].data),
		"probe queue = %+v, want the oldest packet's frame", c.retransQueue[spaceApp])
	_, oldestStillInFlight := c.sent[spaceApp].packets[0]
	assert.False(t, oldestStillInFlight, "the probed (oldest) packet should be removed from flight")
	assert.Contains(t, c.sent[spaceApp].packets, uint64(1),
		"the newer packet should remain in flight")
}

func TestConn_PTOCount_ResetOnAck(t *testing.T) {
	base := time.Unix(2, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.ptoCount = 3
	c.sent[spaceApp].onSent(5, base, true, nil)

	c.onAckRange(spaceApp, 5, 5, 0, 0) // acknowledges an ack-eliciting packet

	require.Zerof(t, c.ptoCount, "ptoCount = %d, want 0 after acknowledgement", c.ptoCount)
}

// TestConformance_RFC9002_Sec622_PTOCountResetOnKeyDiscard checks that discarding
// a packet-number space (Initial or Handshake keys) resets the PTO backoff count,
// since the discard is forward progress (RFC 9002 §6.2.2, App. A.4).
func TestConformance_RFC9002_Sec622_PTOCountResetOnKeyDiscard(t *testing.T) {
	c := &Conn{ptoCount: 3}

	c.discardSpace(spaceHandshake)

	require.Zerof(t, c.ptoCount, "ptoCount = %d after discarding Handshake keys, want 0", c.ptoCount)
}

// deadlinePC times out on the first read (a simulated PTO expiry) then returns a
// canned datagram, recording writes (probes) and that a deadline was set.
type deadlinePC struct {
	reads       int
	writes      int
	deadlineSet bool
	datagram    []byte
}

func (p *deadlinePC) SetReadDeadline(time.Time) error { p.deadlineSet = true; return nil }
func (p *deadlinePC) Read(b []byte) (int, error) {
	p.reads++
	if p.reads == 1 {
		return 0, timeoutError{}
	}
	return copy(b, p.datagram), nil
}
func (p *deadlinePC) Write(b []byte) (int, error) { p.writes++; return len(b), nil }
func (p *deadlinePC) Close() error                { return nil }

type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }
func (timeoutError) Timeout() bool { return true }

// TestConn_ReadWithPTO_ProbesOnTimeout drives the receive loop's probe path:
// with data in flight, a read timeout triggers a probe (written) and a retry
// that then succeeds.
func TestConn_ReadWithPTO_ProbesOnTimeout(t *testing.T) {
	base := time.Unix(3, 0)
	ck, _ := InitialKeys([]byte("ptotest0"))
	sealer, err := NewSealer(ck)
	require.NoError(t, err, "build the 1-RTT sealer the probe is written with")
	pc := &deadlinePC{datagram: []byte("later")}
	c := &Conn{pc: pc, dcid: []byte("ptotest0"), oneRTTSealer: sealer, now: func() time.Time { return base }, handshakeConfirmed: true}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "req")) // in flight, probeable

	buf := make([]byte, 64)
	n, err := c.readWithPTO(context.Background(), buf)

	require.NoError(t, err, "readWithPTO should retry past the PTO expiry and deliver the datagram")
	require.Equalf(t, "later", string(buf[:n]), "read %q, want later", buf[:n])
	assert.True(t, pc.deadlineSet, "a read deadline should have been set")
	assert.NotZero(t, pc.writes, "a probe should have been written on the PTO timeout")
	assert.Equalf(t, uint(1), c.ptoCount, "ptoCount = %d, want 1", c.ptoCount)
}
