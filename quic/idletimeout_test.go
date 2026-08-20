package quic

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec101_EffectiveIdleMin checks the effective idle
// timeout: the smaller of the two advertised values, with zero (disabled) on
// either side deferring to the other (RFC 9000 §10.1).
func TestConformance_RFC9000_Sec101_EffectiveIdleMin(t *testing.T) {
	s := time.Second
	cases := []struct{ local, peer, want time.Duration }{
		{30 * s, 10 * s, 10 * s}, // minimum of the two
		{10 * s, 30 * s, 10 * s},
		{30 * s, 0, 30 * s}, // peer imposes none → ours
		{0, 10 * s, 10 * s}, // we impose none → peer's
		{0, 0, 0},           // neither → disabled
	}

	for _, c := range cases {
		got := effectiveIdle(c.local, c.peer)

		assert.Equalf(t, c.want, got, "effectiveIdle(%v, %v) = %v, want %v",
			c.local, c.peer, got, c.want)
	}
}

// TestConformance_RFC9000_Sec101_AdvertiseAndParse checks that the client
// advertises max_idle_timeout (0x01) in milliseconds and parses the peer's value
// back, and that a zero value omits the parameter entirely (RFC 9000 §10.1, §18.2).
func TestConformance_RFC9000_Sec101_AdvertiseAndParse(t *testing.T) {
	enc := AppendTransportParams(nil, LocalTransportParams{MaxIdleTimeout: 30000, SourceConnectionID: []byte("cid")})
	enc2 := AppendTransportParams(nil, LocalTransportParams{SourceConnectionID: []byte("cid")})

	tp, err := ParseTransportParams(enc)
	tp2, err2 := ParseTransportParams(enc2)

	require.NoError(t, err, "ParseTransportParams with max_idle_timeout advertised")
	require.NoError(t, err2, "ParseTransportParams with max_idle_timeout omitted")
	assert.Equalf(t, 30*time.Second, tp.MaxIdleTimeout,
		"MaxIdleTimeout = %v, want 30s (advertised in milliseconds, §18.2)", tp.MaxIdleTimeout)
	assert.Zerof(t, tp2.MaxIdleTimeout,
		"absent max_idle_timeout must parse to 0, got %v", tp2.MaxIdleTimeout)
}

// TestConformance_RFC9000_Sec101_IdleTimeoutOverflowCapped checks that an absurd
// advertised max_idle_timeout is capped rather than overflowed to a negative
// Duration when scaled to nanoseconds, so the effective timeout stays correct.
func TestConformance_RFC9000_Sec101_IdleTimeoutOverflowCapped(t *testing.T) {
	enc := AppendTransportParams(nil, LocalTransportParams{MaxIdleTimeout: 1 << 55}) // absurdly large ms

	tp, err := ParseTransportParams(enc)

	require.NoError(t, err, "ParseTransportParams with an absurd max_idle_timeout")
	assert.Positivef(t, tp.MaxIdleTimeout,
		"a huge max_idle_timeout must stay positive after scaling, got %v", tp.MaxIdleTimeout)
	// With our 30 s and the capped (effectively unbounded) peer value, the effective
	// timeout is ours — not a wrapped-negative value.
	assert.Equalf(t, 30*time.Second, effectiveIdle(30*time.Second, tp.MaxIdleTimeout),
		"effectiveIdle = %v, want 30s", effectiveIdle(30*time.Second, tp.MaxIdleTimeout))
}

// TestConformance_RFC9000_Sec101_IdleFlooredByPTO checks that the idle timeout is
// floored at three PTOs so a run of lost probes cannot trip it, and that it is not
// in effect when neither endpoint advertises one (RFC 9000 §10.1).
func TestConformance_RFC9000_Sec101_IdleFlooredByPTO(t *testing.T) {
	base := time.Unix(9000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.localMaxIdle = time.Millisecond // far below three PTOs
	c.lastActivity = base

	dl, ok := c.idleDeadline()
	c.localMaxIdle = 0
	_, okDisabled := c.idleDeadline()

	require.True(t, ok, "an advertised idle timeout should be in effect")
	assert.Truef(t, dl.Equal(base.Add(3*c.ptoPeriod())),
		"idleDeadline = %v, want %v (floored at 3×PTO)", dl, base.Add(3*c.ptoPeriod()))
	assert.False(t, okDisabled,
		"no idle timeout should be in effect when neither endpoint advertises one")
}

// TestConformance_RFC9000_Sec101_IdleCloseWithDataInFlight checks that the idle
// timeout fires even while an ack-eliciting packet is outstanding — the
// dead-peer-mid-request case. Sending (including probing) does not reset the idle
// timer, so a silent peer is detected deterministically rather than only after
// the probe backoff is exhausted (RFC 9000 §10.1).
func TestConformance_RFC9000_Sec101_IdleCloseWithDataInFlight(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := &Conn{pc: client}
	c.rtt.update(time.Millisecond, 0)
	c.localMaxIdle = 40 * time.Millisecond
	past := time.Now().Add(-100 * time.Millisecond)
	c.lastActivity = past
	c.sent[spaceApp].onSent(0, past, true, nil) // an ack-eliciting packet in flight

	_, err := c.readWithPTO(context.Background(), make([]byte, 64))

	assert.Equalf(t, ErrIdleTimeout, err,
		"readWithPTO = %v, want ErrIdleTimeout (idle close must fire even with data in flight)", err)
	assert.True(t, c.closed, "the connection should be marked closed after an idle timeout")
}

// TestConformance_RFC9000_Sec101_IdleClose checks that once the connection has
// been idle past the negotiated timeout, the receive path silently closes it and
// returns ErrIdleTimeout (RFC 9000 §10.1) — idleClose writes no CONNECTION_CLOSE.
func TestConformance_RFC9000_Sec101_IdleClose(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := &Conn{pc: client}                 // real clock
	c.rtt.update(time.Millisecond, 0)      // small RTT → ~9ms PTO floor, below the timeout
	c.localMaxIdle = 40 * time.Millisecond // effective idle timeout
	c.lastActivity = time.Now().Add(-100 * time.Millisecond)

	_, err := c.readWithPTO(context.Background(), make([]byte, 64))

	assert.Equalf(t, ErrIdleTimeout, err, "readWithPTO = %v, want ErrIdleTimeout", err)
	assert.True(t, c.closed, "the connection should be marked closed after an idle timeout")
}
