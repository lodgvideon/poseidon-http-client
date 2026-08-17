package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var bbrEpoch = time.Unix(1000, 0)

// bbrTestConn builds a hand-wired BBR connection whose clock reads *nowp, so a
// test can advance time by reassigning the pointed-to variable. It mirrors
// ccTestConn (cc_test.go) but selects CCBBR and arms the model.
func bbrTestConn(nowp *time.Time) *Conn {
	c := &Conn{
		cwnd:     kInitialWindow,
		ssthresh: ^uint64(0),
		ccAlgo:   CCBBR,
		now:      func() time.Time { return *nowp },
	}
	c.initBBR()
	return c
}

// TestBBR_MaxFilter_WindowedExpiry checks that the windowed-max filter surfaces the
// running maximum and lets an old, larger sample age out of the window so a stale
// bandwidth peak cannot pin btlbw high after capacity drops.
func TestBBR_MaxFilter_WindowedExpiry(t *testing.T) {
	var f maxFilter
	const win = uint64(10)

	first := f.update(win, 0, 100)
	smaller := f.update(win, 3, 40)
	stillInWindow := f.update(win, 6, 60)
	// t=12 is 12 rounds past the 100@t0 sample (> win): it must expire, leaving the
	// 60@t6 sample as the new maximum.
	afterExpiry := f.update(win, 12, 10)

	assert.EqualValuesf(t, 100, first, "first sample: max=%d, want 100", first)
	assert.EqualValuesf(t, 100, smaller, "smaller sample in window: max=%d, want 100", smaller)
	assert.EqualValuesf(t, 100, stillInWindow,
		"still in window: max=%d, want 100 (100@t0 not yet expired)", stillInWindow)
	assert.EqualValuesf(t, 60, afterExpiry, "after expiry: max=%d, want 60 (100@t0 aged out)", afterExpiry)
}

// TestBBR_MinRTT_WindowedMinExpiry checks that min_rtt adopts any lower sample
// immediately, ignores higher samples while the 10 s window is live, and adopts a
// higher sample once the window has expired (draft-cardwell §4.3.1).
func TestBBR_MinRTT_WindowedMinExpiry(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)

	c.bbrUpdateMinRTT(100*time.Millisecond, now)
	first := c.bbr.minRTT
	c.bbrUpdateMinRTT(80*time.Millisecond, now)
	lower := c.bbr.minRTT
	c.bbrUpdateMinRTT(120*time.Millisecond, now)
	higherInWindow := c.bbr.minRTT
	now = bbrEpoch.Add(11 * time.Second) // past the 10 s window
	c.bbrUpdateMinRTT(150*time.Millisecond, now)

	assert.Equalf(t, 100*time.Millisecond, first, "first sample: minRTT=%v, want 100ms", first)
	assert.Equalf(t, 80*time.Millisecond, lower, "lower sample: minRTT=%v, want 80ms", lower)
	assert.Equalf(t, 80*time.Millisecond, higherInWindow,
		"higher sample in window: minRTT=%v, want 80ms (unchanged)", higherInWindow)
	assert.Equalf(t, 150*time.Millisecond, c.bbr.minRTT,
		"after window expiry: minRTT=%v, want 150ms (adopted despite higher)", c.bbr.minRTT)
}

// TestBBR_DeliveryRateSample computes a delivery-rate sample from a known
// send/ack timeline and checks btlbw; an app-limited sample below the estimate is
// ignored while one above it still raises the estimate (draft-cheng §3.3).
func TestBBR_DeliveryRateSample(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	// Two 1000-byte packets sent at t0, both acked 100 ms later: 2000 bytes over
	// 0.1 s = 20000 bytes/s.
	p0 := sentPacket{ackEliciting: true, size: 1000, delivered: 0, deliveredTime: bbrEpoch}
	p1 := sentPacket{ackEliciting: true, size: 1000, delivered: 0, deliveredTime: bbrEpoch}
	c.bytesInFlight = 2000
	now = bbrEpoch.Add(100 * time.Millisecond)
	c.rsPriorValid = false

	c.onPacketAcked(p0, true)
	c.onPacketAcked(p1, true)
	c.bbrOnAckRange()

	require.EqualValuesf(t, 20000, c.bbr.btlbw, "btlbw=%d, want 20000 (2000 bytes / 0.1 s)", c.bbr.btlbw)

	// An app-limited sample of 5000 bytes/s must NOT lower btlbw.
	pLow := sentPacket{ackEliciting: true, size: 500, delivered: c.delivered, deliveredTime: now, appLimited: true}
	base := now
	now = base.Add(100 * time.Millisecond)
	c.rsPriorValid = false

	c.onPacketAcked(pLow, true) // 500 bytes / 0.1 s = 5000 < 20000
	c.bbrOnAckRange()

	assert.EqualValuesf(t, 20000, c.bbr.btlbw,
		"btlbw=%d after app-limited low sample, want 20000 (ignored)", c.bbr.btlbw)

	// An app-limited sample above the estimate still raises it (bandwidth is at
	// least the observed rate): 4000 bytes / 0.1 s = 40000 bytes/s.
	pHigh := sentPacket{ackEliciting: true, size: 4000, delivered: c.delivered, deliveredTime: now, appLimited: true}
	base = now
	now = base.Add(100 * time.Millisecond)
	c.rsPriorValid = false

	c.onPacketAcked(pHigh, true)
	c.bbrOnAckRange()

	assert.EqualValuesf(t, 40000, c.bbr.btlbw,
		"btlbw=%d after app-limited high sample, want 40000 (raised)", c.bbr.btlbw)
}

// TestBBR_Startup_To_Drain checks that Startup ends and Drain begins after btlbw
// fails to grow ≥25% for three consecutive rounds (draft-cardwell §4.3.2).
func TestBBR_Startup_To_Drain(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	b := c.bbr
	grow := func(bw uint64) {
		b.roundStart = true
		b.btlbw = bw
		c.bbrCheckStartupFull()
	}

	grow(1000) // fullBw 0 → 1000
	grow(2000) // ≥25% growth → fullBw 2000, count 0
	afterGrowth := b.state
	grow(2100) // <25% → count 1
	grow(2150) // <25% → count 2
	afterTwoFlat := b.state
	grow(2200) // <25% → count 3 → exit to Drain

	assert.Equalf(t, bbrStartup, afterGrowth, "state=%d after growth, want Startup", afterGrowth)
	assert.Equalf(t, bbrStartup, afterTwoFlat, "state=%d after 2 flat rounds, want Startup", afterTwoFlat)
	assert.Equalf(t, bbrDrain, b.state, "state=%d after 3 flat rounds, want Drain", b.state)
	assert.Equalf(t, bbrDrainGain, b.pacingGain, "pacingGain=%v in Drain, want %v", b.pacingGain, bbrDrainGain)
}

// TestBBR_Drain_To_ProbeBW checks that Drain hands off to ProbeBW once the flight
// has fallen to the BDP (draft-cardwell §4.3.2).
func TestBBR_Drain_To_ProbeBW(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	b := c.bbr
	b.state = bbrDrain
	b.btlbw, b.minRTT, b.minRTTValid = 10000, 100*time.Millisecond, true // BDP = 1000 bytes

	c.bytesInFlight = 2000 // above the BDP: stay in Drain
	c.bbrCheckDrain(now)
	aboveBDP := b.state
	c.bytesInFlight = 500 // at/below the BDP: advance
	c.bbrCheckDrain(now)

	assert.Equalf(t, bbrDrain, aboveBDP, "state=%d with inflight>BDP, want Drain", aboveBDP)
	assert.Equalf(t, bbrProbeBW, b.state, "state=%d with inflight<=BDP, want ProbeBW", b.state)
	assert.Equalf(t, bbrPacingGainCycle[0], b.pacingGain,
		"pacingGain=%v entering ProbeBW, want %v", b.pacingGain, bbrPacingGainCycle[0])
}

// TestBBR_ProbeBW_CycleAdvance checks that the ProbeBW pacing-gain cycle advances
// one phase per min_rtt, and that the 0.75 drain phase exits early when the flight
// has fallen to the BDP (draft-cardwell §4.3.3).
func TestBBR_ProbeBW_CycleAdvance(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	b := c.bbr
	b.state = bbrProbeBW
	b.btlbw, b.minRTT, b.minRTTValid = 10000, 100*time.Millisecond, true // BDP = 1000
	b.cycleIdx, b.pacingGain, b.cycleStamp = 0, bbrPacingGainCycle[0], now
	c.bytesInFlight = 5000 // well above BDP so the <1.0 early-exit never fires here

	c.bbrAdvanceCyclePhase(now) // no time elapsed: no advance
	noElapse := b.cycleIdx
	now = bbrEpoch.Add(101 * time.Millisecond) // one min_rtt elapsed
	c.bbrAdvanceCyclePhase(now)
	oneRTTIdx, oneRTTGain := b.cycleIdx, b.pacingGain
	// Now in the 0.75 phase with no fresh min_rtt elapsed, but inflight has drained
	// to the BDP: it must advance early.
	c.bytesInFlight = 500
	c.bbrAdvanceCyclePhase(now)

	assert.Zerof(t, noElapse, "cycleIdx=%d with no elapsed min_rtt, want 0", noElapse)
	assert.Equalf(t, 1, oneRTTIdx, "cycleIdx=%d gain=%v after one min_rtt, want 1 / %v",
		oneRTTIdx, oneRTTGain, bbrPacingGainCycle[1])
	assert.Equalf(t, bbrPacingGainCycle[1], oneRTTGain,
		"cycleIdx=%d gain=%v after one min_rtt, want 1 / %v", oneRTTIdx, oneRTTGain, bbrPacingGainCycle[1])
	assert.Equalf(t, 2, b.cycleIdx, "cycleIdx=%d, want 2 (0.75 early exit on inflight<=BDP)", b.cycleIdx)
}

// TestBBR_ProbeRTT_EntryAndExit checks that 10 s without a min_rtt refresh enters
// ProbeRTT, and that after the flight drains and one round elapses past the hold
// duration the controller leaves ProbeRTT (draft-cardwell §4.3.4).
func TestBBR_ProbeRTT_EntryAndExit(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	b := c.bbr
	b.state = bbrProbeBW
	b.btlbw, b.minRTT, b.minRTTValid, b.minRTTStamp = 10000, 100*time.Millisecond, true, bbrEpoch
	c.cwnd = 20000
	c.bytesInFlight = 1000 // already below the 4·mds floor so the hold arms on entry

	now = bbrEpoch.Add(11 * time.Second) // > 10 s since the last min_rtt
	c.bbrCheckProbeRTT(now)

	require.Equalf(t, bbrProbeRTT, b.state, "state=%d after 10 s idle-of-min_rtt, want ProbeRTT", b.state)
	assert.EqualValuesf(t, 20000, b.priorCwnd, "priorCwnd=%d, want 20000 saved on entry", b.priorCwnd)
	assert.False(t, b.probeRTTDoneStamp.IsZero(), "probeRTTDoneStamp not armed despite a drained flight")

	// One round of delivery elapses and the hold duration passes: exit ProbeRTT.
	c.delivered++
	now = now.Add(201 * time.Millisecond)
	c.bbrCheckProbeRTT(now)

	assert.Equalf(t, bbrStartup, b.state,
		"state=%d after ProbeRTT hold, want Startup (fullBwReached was false)", b.state)
}

// TestBBR_LossToleranceVsNewReno checks the defining BBR property: a single loss
// does NOT shrink the BBR window, whereas NewReno halves on the same event.
func TestBBR_LossToleranceVsNewReno(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	c.cwnd = 50000
	before := c.cwnd
	nr := &Conn{cwnd: 50000, ssthresh: ^uint64(0), now: func() time.Time { return now }} // CCNewReno (default)

	c.onCongestionEvent(now.Add(-50 * time.Millisecond)) // a loss
	nr.onCongestionEvent(now.Add(-50 * time.Millisecond))

	assert.Equalf(t, before, c.cwnd,
		"BBR cwnd=%d after a loss, want %d (loss-tolerant, no halving)", c.cwnd, before)
	assert.EqualValuesf(t, 25000, nr.cwnd,
		"NewReno cwnd=%d after the same loss, want 25000 (halved)", nr.cwnd)
}

// TestBBR_PersistentCongestionCollapse checks that persistent congestion collapses
// the BBR window to the floor, clears the pacing rate, and re-enters Startup with a
// fresh (empty) bandwidth model.
func TestBBR_PersistentCongestionCollapse(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	c.bbr.state = bbrProbeBW
	c.bbr.btlbw, c.bbr.minRTT, c.bbr.minRTTValid = 10000, 100*time.Millisecond, true
	c.bbr.fullBwReached = true
	c.cwnd, c.pacingRate, c.appLimitedUntil = 50000, 99999, 1234

	c.onPersistentCongestion()

	assert.EqualValuesf(t, kMinimumWindow, c.cwnd,
		"cwnd=%d after persistent congestion, want %d (floor)", c.cwnd, kMinimumWindow)
	assert.Zerof(t, c.pacingRate, "pacingRate=%d, want 0 (fall back to RFC 9002 pacer)", c.pacingRate)
	assert.Equalf(t, bbrStartup, c.bbr.state, "state=%d, want Startup (re-probe the dead path)", c.bbr.state)
	assert.Zerof(t, c.bbr.btlbw, "model not reset: btlbw=%d appLimitedUntil=%d, want 0/0",
		c.bbr.btlbw, c.appLimitedUntil)
	assert.Zerof(t, c.appLimitedUntil, "model not reset: btlbw=%d appLimitedUntil=%d, want 0/0",
		c.bbr.btlbw, c.appLimitedUntil)
}

// TestBBR_DrivesCwndAndPacingThroughSeam drives one real ACK range end-to-end and
// checks that BBR wrote both c.cwnd (from the BDP) and c.pacingRate (from btlbw) —
// the single send gate NewReno also reads — rather than leaving the initial window.
func TestBBR_DrivesCwndAndPacingThroughSeam(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	const size = 1000
	for pn := uint64(0); pn < 2; pn++ {
		c.sent[spaceApp].onSent(pn, now, true, nil)
		c.onPacketSent(spaceApp, pn, true, size)
	}
	c.sendPN[spaceApp] = 2
	now = bbrEpoch.Add(100 * time.Millisecond)

	c.onAckRange(spaceApp, 0, 1, 0, c.bytesInFlight)

	assert.EqualValuesf(t, 20000, c.bbr.btlbw, "btlbw=%d, want 20000", c.bbr.btlbw)
	assert.Equalf(t, 100*time.Millisecond, c.bbr.minRTT, "minRTT=%v, want 100ms", c.bbr.minRTT)
	// BDP = 20000 · 0.1 s = 2000; Startup cwnd_gain·BDP; pacing = pacing_gain·btlbw.
	wantCwnd := uint64(c.bbr.cwndGain * float64(c.bbrBDP()))
	wantPacing := uint64(c.bbr.pacingGain * float64(c.bbr.btlbw))
	assert.Equalf(t, wantCwnd, c.cwnd, "cwnd=%d, want %d (cwnd_gain·BDP)", c.cwnd, wantCwnd)
	assert.Equalf(t, wantPacing, c.pacingRate, "pacingRate=%d, want %d (pacing_gain·btlbw)",
		c.pacingRate, wantPacing)
	assert.NotZerof(t, c.pacingRate, "pacingRate=%d, want %d (pacing_gain·btlbw)", c.pacingRate, wantPacing)
	assert.NotEqualf(t, uint64(kInitialWindow), c.cwnd,
		"cwnd left at the initial window: BBR did not drive the gate")
}

// TestBBR_DefaultInvariance_NewRenoUnchanged is the guard for the non-negotiable
// invariant: a connection with NO option is NewReno, and a scripted ack/loss
// sequence yields byte-identical cwnd/ssthresh to the pre-BBR code. These values
// are pinned; a regression in the default path changes them.
func TestBBR_DefaultInvariance_NewRenoUnchanged(t *testing.T) {
	now := bbrEpoch
	c := &Conn{cwnd: kInitialWindow, ssthresh: ^uint64(0), now: func() time.Time { return now }}
	require.Equalf(t, CCNewReno, c.ccAlgo, "default Conn is not NewReno: ccAlgo=%d bbr=%v pacingRate=%d",
		c.ccAlgo, c.bbr, c.pacingRate)
	require.Truef(t, c.bbr == nil, "default Conn is not NewReno: ccAlgo=%d bbr=%v pacingRate=%d",
		c.ccAlgo, c.bbr, c.pacingRate)
	require.Zerof(t, c.pacingRate, "default Conn is not NewReno: ccAlgo=%d bbr=%v pacingRate=%d",
		c.ccAlgo, c.bbr, c.pacingRate)

	// Three slow-start acks of a full, congestion-limited window grow cwnd by the
	// acked bytes each (12000 → 15600).
	for i := 0; i < 3; i++ {
		c.bytesInFlight = kInitialWindow
		c.onPacketAcked(sentPacket{timeSent: bbrEpoch, ackEliciting: true, size: 1200}, true)
	}
	grown := c.cwnd
	// A loss halves once (15600 → 7800).
	c.onCongestionEvent(bbrEpoch.Add(time.Second))

	assert.EqualValuesf(t, kInitialWindow+3*1200, grown, "NewReno slow-start cwnd=%d, want %d",
		grown, kInitialWindow+3*1200)
	assert.EqualValuesf(t, 7800, c.cwnd, "NewReno post-loss cwnd=%d ssthresh=%d, want 7800/7800",
		c.cwnd, c.ssthresh)
	assert.EqualValuesf(t, 7800, c.ssthresh, "NewReno post-loss cwnd=%d ssthresh=%d, want 7800/7800",
		c.cwnd, c.ssthresh)
	assert.Zerof(t, c.pacingRate, "NewReno pacingRate=%d, want 0 (BBR pacer never engaged)", c.pacingRate)
}
