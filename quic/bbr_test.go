package quic

import (
	"testing"
	"time"
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
	if got := f.update(win, 0, 100); got != 100 {
		t.Fatalf("first sample: max=%d, want 100", got)
	}
	if got := f.update(win, 3, 40); got != 100 {
		t.Fatalf("smaller sample in window: max=%d, want 100", got)
	}
	if got := f.update(win, 6, 60); got != 100 {
		t.Fatalf("still in window: max=%d, want 100 (100@t0 not yet expired)", got)
	}
	// t=12 is 12 rounds past the 100@t0 sample (> win): it must expire, leaving the
	// 60@t6 sample as the new maximum.
	if got := f.update(win, 12, 10); got != 60 {
		t.Fatalf("after expiry: max=%d, want 60 (100@t0 aged out)", got)
	}
}

// TestBBR_MinRTT_WindowedMinExpiry checks that min_rtt adopts any lower sample
// immediately, ignores higher samples while the 10 s window is live, and adopts a
// higher sample once the window has expired (draft-cardwell §4.3.1).
func TestBBR_MinRTT_WindowedMinExpiry(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)

	c.bbrUpdateMinRTT(100*time.Millisecond, now)
	if c.bbr.minRTT != 100*time.Millisecond {
		t.Fatalf("first sample: minRTT=%v, want 100ms", c.bbr.minRTT)
	}
	c.bbrUpdateMinRTT(80*time.Millisecond, now)
	if c.bbr.minRTT != 80*time.Millisecond {
		t.Fatalf("lower sample: minRTT=%v, want 80ms", c.bbr.minRTT)
	}
	c.bbrUpdateMinRTT(120*time.Millisecond, now)
	if c.bbr.minRTT != 80*time.Millisecond {
		t.Fatalf("higher sample in window: minRTT=%v, want 80ms (unchanged)", c.bbr.minRTT)
	}
	now = bbrEpoch.Add(11 * time.Second) // past the 10 s window
	c.bbrUpdateMinRTT(150*time.Millisecond, now)
	if c.bbr.minRTT != 150*time.Millisecond {
		t.Fatalf("after window expiry: minRTT=%v, want 150ms (adopted despite higher)", c.bbr.minRTT)
	}
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
	if c.bbr.btlbw != 20000 {
		t.Fatalf("btlbw=%d, want 20000 (2000 bytes / 0.1 s)", c.bbr.btlbw)
	}

	// An app-limited sample of 5000 bytes/s must NOT lower btlbw.
	pLow := sentPacket{ackEliciting: true, size: 500, delivered: c.delivered, deliveredTime: now, appLimited: true}
	base := now
	now = base.Add(100 * time.Millisecond)
	c.rsPriorValid = false
	c.onPacketAcked(pLow, true) // 500 bytes / 0.1 s = 5000 < 20000
	c.bbrOnAckRange()
	if c.bbr.btlbw != 20000 {
		t.Fatalf("btlbw=%d after app-limited low sample, want 20000 (ignored)", c.bbr.btlbw)
	}

	// An app-limited sample above the estimate still raises it (bandwidth is at
	// least the observed rate): 4000 bytes / 0.1 s = 40000 bytes/s.
	pHigh := sentPacket{ackEliciting: true, size: 4000, delivered: c.delivered, deliveredTime: now, appLimited: true}
	base = now
	now = base.Add(100 * time.Millisecond)
	c.rsPriorValid = false
	c.onPacketAcked(pHigh, true)
	c.bbrOnAckRange()
	if c.bbr.btlbw != 40000 {
		t.Fatalf("btlbw=%d after app-limited high sample, want 40000 (raised)", c.bbr.btlbw)
	}
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
	if b.state != bbrStartup {
		t.Fatalf("state=%d after growth, want Startup", b.state)
	}
	grow(2100) // <25% → count 1
	grow(2150) // <25% → count 2
	if b.state != bbrStartup {
		t.Fatalf("state=%d after 2 flat rounds, want Startup", b.state)
	}
	grow(2200) // <25% → count 3 → exit to Drain
	if b.state != bbrDrain {
		t.Fatalf("state=%d after 3 flat rounds, want Drain", b.state)
	}
	if b.pacingGain != bbrDrainGain {
		t.Fatalf("pacingGain=%v in Drain, want %v", b.pacingGain, bbrDrainGain)
	}
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
	if b.state != bbrDrain {
		t.Fatalf("state=%d with inflight>BDP, want Drain", b.state)
	}
	c.bytesInFlight = 500 // at/below the BDP: advance
	c.bbrCheckDrain(now)
	if b.state != bbrProbeBW {
		t.Fatalf("state=%d with inflight<=BDP, want ProbeBW", b.state)
	}
	if b.pacingGain != bbrPacingGainCycle[0] {
		t.Fatalf("pacingGain=%v entering ProbeBW, want %v", b.pacingGain, bbrPacingGainCycle[0])
	}
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
	if b.cycleIdx != 0 {
		t.Fatalf("cycleIdx=%d with no elapsed min_rtt, want 0", b.cycleIdx)
	}
	now = bbrEpoch.Add(101 * time.Millisecond) // one min_rtt elapsed
	c.bbrAdvanceCyclePhase(now)
	if b.cycleIdx != 1 || b.pacingGain != bbrPacingGainCycle[1] {
		t.Fatalf("cycleIdx=%d gain=%v after one min_rtt, want 1 / %v", b.cycleIdx, b.pacingGain, bbrPacingGainCycle[1])
	}
	// Now in the 0.75 phase with no fresh min_rtt elapsed, but inflight has drained
	// to the BDP: it must advance early.
	c.bytesInFlight = 500
	c.bbrAdvanceCyclePhase(now)
	if b.cycleIdx != 2 {
		t.Fatalf("cycleIdx=%d, want 2 (0.75 early exit on inflight<=BDP)", b.cycleIdx)
	}
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
	if b.state != bbrProbeRTT {
		t.Fatalf("state=%d after 10 s idle-of-min_rtt, want ProbeRTT", b.state)
	}
	if b.priorCwnd != 20000 {
		t.Fatalf("priorCwnd=%d, want 20000 saved on entry", b.priorCwnd)
	}
	if b.probeRTTDoneStamp.IsZero() {
		t.Fatal("probeRTTDoneStamp not armed despite a drained flight")
	}
	// One round of delivery elapses and the hold duration passes: exit ProbeRTT.
	c.delivered += 1
	now = now.Add(201 * time.Millisecond)
	c.bbrCheckProbeRTT(now)
	if b.state != bbrStartup {
		t.Fatalf("state=%d after ProbeRTT hold, want Startup (fullBwReached was false)", b.state)
	}
}

// TestBBR_LossToleranceVsNewReno checks the defining BBR property: a single loss
// does NOT shrink the BBR window, whereas NewReno halves on the same event.
func TestBBR_LossToleranceVsNewReno(t *testing.T) {
	now := bbrEpoch
	c := bbrTestConn(&now)
	c.cwnd = 50000
	before := c.cwnd
	c.onCongestionEvent(now.Add(-50 * time.Millisecond)) // a loss
	if c.cwnd != before {
		t.Fatalf("BBR cwnd=%d after a loss, want %d (loss-tolerant, no halving)", c.cwnd, before)
	}

	nr := &Conn{cwnd: 50000, ssthresh: ^uint64(0), now: func() time.Time { return now }} // CCNewReno (default)
	nr.onCongestionEvent(now.Add(-50 * time.Millisecond))
	if nr.cwnd != 25000 {
		t.Fatalf("NewReno cwnd=%d after the same loss, want 25000 (halved)", nr.cwnd)
	}
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

	if c.cwnd != kMinimumWindow {
		t.Fatalf("cwnd=%d after persistent congestion, want %d (floor)", c.cwnd, kMinimumWindow)
	}
	if c.pacingRate != 0 {
		t.Fatalf("pacingRate=%d, want 0 (fall back to RFC 9002 pacer)", c.pacingRate)
	}
	if c.bbr.state != bbrStartup {
		t.Fatalf("state=%d, want Startup (re-probe the dead path)", c.bbr.state)
	}
	if c.bbr.btlbw != 0 || c.appLimitedUntil != 0 {
		t.Fatalf("model not reset: btlbw=%d appLimitedUntil=%d, want 0/0", c.bbr.btlbw, c.appLimitedUntil)
	}
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

	if c.bbr.btlbw != 20000 {
		t.Fatalf("btlbw=%d, want 20000", c.bbr.btlbw)
	}
	if c.bbr.minRTT != 100*time.Millisecond {
		t.Fatalf("minRTT=%v, want 100ms", c.bbr.minRTT)
	}
	// BDP = 20000 · 0.1 s = 2000; Startup cwnd_gain·BDP; pacing = pacing_gain·btlbw.
	wantCwnd := uint64(c.bbr.cwndGain * float64(c.bbrBDP()))
	wantPacing := uint64(c.bbr.pacingGain * float64(c.bbr.btlbw))
	if c.cwnd != wantCwnd {
		t.Fatalf("cwnd=%d, want %d (cwnd_gain·BDP)", c.cwnd, wantCwnd)
	}
	if c.pacingRate != wantPacing || c.pacingRate == 0 {
		t.Fatalf("pacingRate=%d, want %d (pacing_gain·btlbw)", c.pacingRate, wantPacing)
	}
	if c.cwnd == kInitialWindow {
		t.Fatal("cwnd left at the initial window: BBR did not drive the gate")
	}
}

// TestBBR_DefaultInvariance_NewRenoUnchanged is the guard for the non-negotiable
// invariant: a connection with NO option is NewReno, and a scripted ack/loss
// sequence yields byte-identical cwnd/ssthresh to the pre-BBR code. These values
// are pinned; a regression in the default path changes them.
func TestBBR_DefaultInvariance_NewRenoUnchanged(t *testing.T) {
	now := bbrEpoch
	c := &Conn{cwnd: kInitialWindow, ssthresh: ^uint64(0), now: func() time.Time { return now }}
	if c.ccAlgo != CCNewReno || c.bbr != nil || c.pacingRate != 0 {
		t.Fatalf("default Conn is not NewReno: ccAlgo=%d bbr=%v pacingRate=%d", c.ccAlgo, c.bbr, c.pacingRate)
	}
	// Three slow-start acks of a full, congestion-limited window grow cwnd by the
	// acked bytes each (12000 → 15600).
	for i := 0; i < 3; i++ {
		c.bytesInFlight = kInitialWindow
		c.onPacketAcked(sentPacket{timeSent: bbrEpoch, ackEliciting: true, size: 1200}, true)
	}
	if c.cwnd != kInitialWindow+3*1200 {
		t.Fatalf("NewReno slow-start cwnd=%d, want %d", c.cwnd, kInitialWindow+3*1200)
	}
	// A loss halves once (15600 → 7800).
	c.onCongestionEvent(bbrEpoch.Add(time.Second))
	if c.cwnd != 7800 || c.ssthresh != 7800 {
		t.Fatalf("NewReno post-loss cwnd=%d ssthresh=%d, want 7800/7800", c.cwnd, c.ssthresh)
	}
	if c.pacingRate != 0 {
		t.Fatalf("NewReno pacingRate=%d, want 0 (BBR pacer never engaged)", c.pacingRate)
	}
}
