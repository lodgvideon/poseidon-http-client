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

// TestConn_IdleDeadline_RecedesWithPTOBackoff PINS what this package does today
// with max_idle_timeout while something is in flight. It does not endorse it.
//
// Issue #798 reports that once a connection has an unacknowledged ack-eliciting
// packet, max_idle_timeout can no longer close it — not late, but never — and
// asks whether that is right. This test exists so that the answer, whichever it
// turns out to be, has to be given deliberately: today's numbers are written down
// here, and any change to the mechanism turns it red. Nothing here decides the
// design question; that is open on #798, with measurements.
//
// The mechanism, which is what the assertions are on. RFC 9000 §10.1 floors the
// idle period at three PTOs so a run of lost probes cannot idle-close a
// connection that is merely suffering loss. idleDeadline applies that floor
// against the CURRENT, backed-off PTO — ptoBase << ptoCount — so at the k'th
// probe the clock stands ptoBase*(2^k - 1) past lastActivity while the deadline
// has moved out to 3*ptoBase*2^k. The floor recedes faster than the clock
// advances, so lossDetectionDeadline is the nearer of the two at EVERY rung and
// handleExpiry's idle branch is never reached. With a 1s max_idle_timeout and a
// 60ms base PTO the deadline at the last rung is 46.08s.
//
// RFC 9002 §6.2.1 states the intended relationship the other way round: "The
// total length of time over which consecutive PTOs expire is limited by the idle
// timeout." Whether the floor should be taken against ptoBase rather than
// ptoPeriod, or the ladder bounded by the idle timeout, is exactly #798.
func TestConn_IdleDeadline_RecedesWithPTOBackoff(t *testing.T) {
	base := time.Unix(9200, 0)
	now := base
	c := &Conn{now: func() time.Time { return now }}
	c.rtt.update(20*time.Millisecond, 0) // smoothed 20ms, rttvar 10ms -> ptoBase 60ms
	c.localMaxIdle = time.Second
	c.lastActivity = base
	c.handshakeComplete = true  // no anti-deadlock probe: in-flight data is why the PTO runs
	c.handshakeConfirmed = true // §6.2.1: the app space joins the PTO timer only once confirmed
	c.sent[spaceApp].onSent(0, base, true, nil)
	require.True(t, c.hasInFlight(),
		"the fixture must hold an unacknowledged ack-eliciting packet, or no ladder runs")
	ptoBase := c.ptoBase()
	require.Equal(t, 60*time.Millisecond, ptoBase,
		"the arithmetic below is anchored on a 60ms base PTO")

	type rung struct{ idle, loss time.Duration }
	ladder := make([]rung, 0, maxPTOBackoff+1)
	haveIdle := make([]bool, 0, maxPTOBackoff+1)
	for k := 0; k <= maxPTOBackoff; k++ {
		c.ptoCount = uint(k)
		now = base.Add(ptoBase * time.Duration((1<<k)-1)) // the clock at the k'th probe
		idleDL, ok := c.idleDeadline()
		lossDL, _ := c.lossDetectionDeadline()
		ladder = append(ladder, rung{idle: idleDL.Sub(base), loss: lossDL.Sub(base)})
		haveIdle = append(haveIdle, ok)
	}

	for k, r := range ladder {
		assert.Truef(t, haveIdle[k], "rung %d: an advertised max_idle_timeout must be in effect", k)
		assert.Lessf(t, r.loss, r.idle,
			"rung %d: loss deadline +%v, idle deadline +%v. The idle deadline must stay "+
				"the FARTHER of the two at every rung; if it ever became the nearer one "+
				"the connection would idle-close mid-ladder, which is the behaviour "+
				"#798 asks for and this package does not have",
			k, r.loss, r.idle)
	}
	assert.Equalf(t, time.Second, ladder[0].idle,
		"rung 0: idle deadline +%v, want the advertised 1s — three un-backed-off PTOs "+
			"is 180ms, below the advertised value, so the floor does not bind yet",
		ladder[0].idle)
	assert.Equalf(t, 46080*time.Millisecond, ladder[maxPTOBackoff].idle,
		"rung %d: idle deadline +%v, want +46.08s (3 x 60ms x 2^%d). That is 46 times "+
			"the advertised max_idle_timeout of 1s, and it is a constant of the backoff "+
			"ladder rather than a function of the negotiated value — the observation "+
			"#798 is about",
		maxPTOBackoff, ladder[maxPTOBackoff].idle, maxPTOBackoff)
}
