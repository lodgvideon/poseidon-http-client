package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec182_AckDelayParams checks that ack_delay_exponent
// and max_ack_delay are parsed, default correctly when absent, and are rejected
// out of range (RFC 9000 §18.2).
func TestConformance_RFC9000_Sec182_AckDelayParams(t *testing.T) {
	params := concat(tpInt(tpAckDelayExponent, 5), tpInt(tpMaxAckDelay, 100))

	tp, err := ParseTransportParams(params)
	def, _ := ParseTransportParams(nil)
	_, tooLargeExp := ParseTransportParams(tpInt(tpAckDelayExponent, 21))
	_, tooLargeDelay := ParseTransportParams(tpInt(tpMaxAckDelay, 1<<14))

	require.NoError(t, err)
	assert.EqualValuesf(t, 5, tp.AckDelayExponent, "ack_delay_exponent = %d, want 5", tp.AckDelayExponent)
	assert.Equalf(t, 100*time.Millisecond, tp.MaxAckDelay, "max_ack_delay = %v, want 100ms", tp.MaxAckDelay)
	assert.EqualValuesf(t, 3, def.AckDelayExponent,
		"defaults = exp %d / %v, want 3 / 25ms", def.AckDelayExponent, def.MaxAckDelay)
	assert.Equalf(t, 25*time.Millisecond, def.MaxAckDelay,
		"defaults = exp %d / %v, want 3 / 25ms", def.AckDelayExponent, def.MaxAckDelay)
	assert.ErrorIsf(t, tooLargeExp, ErrTransportParameter,
		"ack_delay_exponent 21 = %v, want ErrTransportParameter", tooLargeExp)
	assert.ErrorIsf(t, tooLargeDelay, ErrTransportParameter,
		"max_ack_delay 2^14 = %v, want ErrTransportParameter", tooLargeDelay)
}

// TestConformance_RFC9002_Sec53_AckDelayExponentDecode checks that the ACK Delay
// is decoded with the peer's ack_delay_exponent, not a hardcoded default.
func TestConformance_RFC9002_Sec53_AckDelayExponentDecode(t *testing.T) {
	ms := time.Millisecond
	base := time.Unix(10, 0)
	c := &Conn{now: func() time.Time { return base }, handshakeConfirmed: true,
		peer: TransportParams{AckDelayExponent: 4, MaxAckDelay: 25 * ms}}
	h := &connFrameHandler{c: c, space: spaceApp}
	c.sendPN[spaceApp] = 2 // we sent packets 0 and 1 (§13.1 never-sent gate)
	c.sent[spaceApp].onSent(0, base.Add(-100*ms), true, nil)
	require.NoError(t, h.OnAck(0, 0, 0)) // first sample: RTT 100ms
	c.sent[spaceApp].onSent(1, base.Add(-120*ms), true, nil)

	// ACK Delay 625 decoded with exponent 4 = 625<<4 = 10000µs = 10ms (< 25ms, no
	// clamp): adjusted = 120 - 10 = 110ms.
	err := h.OnAck(1, 625, 0)

	require.NoError(t, err)
	want := (7*100*ms + 110*ms) / 8
	assert.Equalf(t, want, c.rtt.smoothedRTT,
		"smoothed_rtt = %v, want %v (ACK Delay decoded with exponent 4 = 10ms)", c.rtt.smoothedRTT, want)
}

// TestConformance_RFC9002_Sec53_InitialSpaceFixedExponent checks that ACK Delay
// in the Initial/Handshake spaces is decoded with the fixed exponent 3, not the
// peer's negotiated ack_delay_exponent (which does not apply there).
func TestConformance_RFC9002_Sec53_InitialSpaceFixedExponent(t *testing.T) {
	ms := time.Millisecond
	base := time.Unix(10, 0)
	c := &Conn{now: func() time.Time { return base }, peer: TransportParams{AckDelayExponent: 5, MaxAckDelay: 25 * ms}}
	h := &connFrameHandler{c: c, space: spaceHandshake}
	c.sendPN[spaceHandshake] = 2 // we sent packets 0 and 1 (§13.1 never-sent gate)
	c.sent[spaceHandshake].onSent(0, base.Add(-100*ms), true, nil)
	require.NoError(t, h.OnAck(0, 0, 0))
	c.sent[spaceHandshake].onSent(1, base.Add(-120*ms), true, nil)

	// ACK Delay 1250 with the fixed exponent 3 = 1250<<3 = 10000µs = 10ms →
	// adjusted = 110ms (the peer's exponent 5 would give 40ms → 80ms — wrong).
	err := h.OnAck(1, 1250, 0)

	require.NoError(t, err)
	want := (7*100*ms + 110*ms) / 8
	assert.Equalf(t, want, c.rtt.smoothedRTT,
		"smoothed_rtt = %v, want %v (Handshake space uses fixed exponent 3)", c.rtt.smoothedRTT, want)
}

// TestConn_AckDelay_OverflowBounded checks that a peer-controlled ACK Delay large
// enough to overflow int64 when shifted and scaled is saturated, not wrapped to a
// negative duration that would corrupt the RTT estimate.
func TestConn_AckDelay_OverflowBounded(t *testing.T) {
	ms := time.Millisecond
	base := time.Unix(10, 0)
	c := &Conn{now: func() time.Time { return base }, peer: TransportParams{AckDelayExponent: 3}}
	h := &connFrameHandler{c: c, space: spaceHandshake}
	c.sendPN[spaceHandshake] = 2 // we sent packets 0 and 1 (§13.1 never-sent gate)
	c.sent[spaceHandshake].onSent(0, base.Add(-100*ms), true, nil)
	require.NoError(t, h.OnAck(0, 0, 0))
	c.sent[spaceHandshake].onSent(1, base.Add(-120*ms), true, nil)

	// A huge ACK Delay (2^57) would overflow int64 after << 3 and the µs→ns scale.
	err := h.OnAck(1, 1<<57, 0)

	require.NoError(t, err)
	assert.Greaterf(t, c.rtt.smoothedRTT, time.Duration(0),
		"smoothed_rtt = %v, want positive (an overflowing ack delay must not corrupt it)", c.rtt.smoothedRTT)
	// The delay saturates huge, so rtt.update's min_rtt guard leaves the sample
	// unadjusted: adjusted = 120ms.
	want := (7*100*ms + 120*ms) / 8
	assert.Equalf(t, want, c.rtt.smoothedRTT, "smoothed_rtt = %v, want %v", c.rtt.smoothedRTT, want)
}

// TestConformance_RFC9002_Sec53_AckDelayClampedToMax checks that, once the
// handshake is confirmed, the decoded ACK Delay is clamped to the peer's
// max_ack_delay before it is subtracted from the RTT sample.
func TestConformance_RFC9002_Sec53_AckDelayClampedToMax(t *testing.T) {
	ms := time.Millisecond
	base := time.Unix(10, 0)
	c := &Conn{now: func() time.Time { return base }, handshakeConfirmed: true,
		peer: TransportParams{AckDelayExponent: 3, MaxAckDelay: 5 * ms}}
	h := &connFrameHandler{c: c, space: spaceApp}
	c.sendPN[spaceApp] = 2 // we sent packets 0 and 1 (§13.1 never-sent gate)
	c.sent[spaceApp].onSent(0, base.Add(-100*ms), true, nil)
	require.NoError(t, h.OnAck(0, 0, 0))
	c.sent[spaceApp].onSent(1, base.Add(-120*ms), true, nil)

	// ACK Delay 6250 decoded = 6250<<3 = 50000µs = 50ms, clamped to max_ack_delay
	// 5ms: adjusted = 120 - 5 = 115ms.
	err := h.OnAck(1, 6250, 0)

	require.NoError(t, err)
	want := (7*100*ms + 115*ms) / 8
	assert.Equalf(t, want, c.rtt.smoothedRTT,
		"smoothed_rtt = %v, want %v (ACK Delay clamped to max_ack_delay 5ms)", c.rtt.smoothedRTT, want)
}

// TestConformance_RFC9000_Sec182_AckDelayParamBoundaries pins BOTH sides of the
// two RFC 9000 §18.2 ack-delay limits, at the limit rather than comfortably
// inside it.
//
// RFC 9000 §18.2, ack_delay_exponent: "Values above 20 are invalid."
// RFC 9000 §18.2, max_ack_delay: "Values of 2^14 or greater are invalid."
//
// Both sentences name the largest LEGAL value as well as the smallest illegal
// one, and only the illegal side was covered — here and, for the exponent, again
// in fuzz_test.go. That pinned each bound from one direction: moving either limit
// inward, so that a value a conformant peer is entitled to advertise is refused
// with TRANSPORT_PARAMETER_ERROR, was invisible to the whole suite. An
// over-rejecting endpoint fails to interoperate exactly as loudly as an
// under-rejecting one. #827.
//
// The zero cases are their own equivalence class, not a third point on the
// range: ParseTransportParams seeds the §18.2 defaults (3 and 25ms) before
// reading anything, so an explicitly advertised 0 must survive to overwrite them
// — "absent" and "zero" are different inputs with different correct answers.
func TestConformance_RFC9000_Sec182_AckDelayParamBoundaries(t *testing.T) {
	accepted := []struct {
		name      string
		params    []byte
		wantExp   uint64
		wantDelay time.Duration
	}{
		// ack_delay_exponent: 0 (explicit, not the default 3), and 20, the largest
		// value §18.2 leaves legal.
		{"exponent_zero", tpInt(tpAckDelayExponent, 0), 0, 25 * time.Millisecond},
		{"exponent_at_limit_20", tpInt(tpAckDelayExponent, 20), 20, 25 * time.Millisecond},
		// max_ack_delay: 0 (explicit, not the default 25ms), and 16383ms = 2^14-1,
		// the largest value §18.2 leaves legal.
		{"max_ack_delay_zero", tpInt(tpMaxAckDelay, 0), 3, 0},
		{"max_ack_delay_at_limit_16383", tpInt(tpMaxAckDelay, 1<<14-1), 3, 16383 * time.Millisecond},
	}
	rejected := []struct {
		name   string
		params []byte
	}{
		{"exponent_one_past_limit_21", tpInt(tpAckDelayExponent, 21)},
		{"max_ack_delay_one_past_limit_16384", tpInt(tpMaxAckDelay, 1<<14)},
	}

	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			tp, err := ParseTransportParams(tc.params)

			require.NoErrorf(t, err, "ParseTransportParams(%s) = %v; §18.2 leaves this value "+
				"legal, and refusing it makes the handshake fail against a conformant peer", tc.name, err)
			assert.Equalf(t, tc.wantExp, tp.AckDelayExponent,
				"ack_delay_exponent = %d, want %d — a bound that has moved inward silently "+
					"changes how every ACK Delay field is decoded", tp.AckDelayExponent, tc.wantExp)
			assert.Equalf(t, tc.wantDelay, tp.MaxAckDelay,
				"max_ack_delay = %v, want %v — this feeds the PTO period, so a wrong value "+
					"mis-times every probe", tp.MaxAckDelay, tc.wantDelay)
		})
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTransportParams(tc.params)

			assert.ErrorIsf(t, err, ErrTransportParameter,
				"ParseTransportParams(%s) = %v, want ErrTransportParameter — one past the "+
					"§18.2 limit must close the connection, not be silently accepted", tc.name, err)
		})
	}
}
