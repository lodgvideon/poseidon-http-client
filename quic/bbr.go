package quic

import "time"

// This file implements BBR v1 (draft-cardwell-iccrg-bbr-congestion-control) as an
// OPT-IN alternative to the default NewReno controller in cc.go, selected with
// NewConn(..., WithCongestionControl(CCBBR)). BBR is model-based: it estimates the
// bottleneck bandwidth (btlbw, a windowed maximum of delivery-rate samples) and the
// round-trip propagation delay (min_rtt, a windowed minimum), then paces sending at
// pacing_gain·btlbw and caps the flight at cwnd_gain·(btlbw·min_rtt). It drives the
// SAME send gate as NewReno by writing c.cwnd and c.pacingRate; when CCBBR is not
// selected none of this runs and NewReno's arithmetic is byte-for-byte unchanged.
//
// Throughput benefit over NewReno on a bufferbloated or lossy WAN path is not
// quantified in THIS suite, which pins the model's mechanics, not its performance
// — but it is quantified (#362). The shaped lab it needed exists: `tc tbf` on the
// relay in test/integration/http3/docker-compose.cc.yml, driven by
// scripts/cc-matrix.sh, with scripts/cc-ratio-check.sh and cc-scale-check.sh
// gating that the knob binds before any row is read. Result, per path: a tie on a
// clean link, a wide but noisy win on loss, and ~1.5x on a deep queue against a
// TIE on a shallow one — the pair that makes the bufferbloat claim falsifiable.
// The numbers and the caveats are in docs/CLIENT_GUIDE.md; NewReno stays default.

// CongestionControlAlgorithm selects a connection's congestion controller.
type CongestionControlAlgorithm int

const (
	// CCNewReno is the RFC 9002 §7 NewReno controller (cc.go). It is the zero value
	// and the default, so a connection created without WithCongestionControl behaves
	// exactly as before options existed.
	CCNewReno CongestionControlAlgorithm = iota
	// CCBBR is the opt-in BBR v1 controller implemented in this file.
	CCBBR
)

// WithCongestionControl selects the congestion-control algorithm for a connection
// (default CCNewReno). Pass WithCongestionControl(CCBBR) to NewConn to opt into
// BBR.
func WithCongestionControl(a CongestionControlAlgorithm) ConnOption {
	return func(c *Conn) { c.ccAlgo = a }
}

// bbrState is BBR's control-machine phase (draft-cardwell §4.3).
type bbrState int

const (
	bbrStartup  bbrState = iota // exponential ramp to find btlbw
	bbrDrain                    // drain the startup queue back down to the BDP
	bbrProbeBW                  // steady state: cycle pacing_gain to probe for more bw
	bbrProbeRTT                 // periodically drain to re-measure min_rtt
)

// BBR tuning constants (draft-cardwell §4).
const (
	bbrBtlBwWindowRounds   uint64        = 10                     // btlbw max-filter window, in round trips
	bbrMinRTTWindow        time.Duration = 10 * time.Second       // min_rtt min-filter window
	bbrProbeRTTDuration    time.Duration = 200 * time.Millisecond // minimum time held in ProbeRTT
	bbrHighGain            float64       = 2.885                  // ≈ 2/ln2: Startup pacing/cwnd gain
	bbrDrainGain           float64       = 1.0 / 2.885            // inverse: Drain pacing gain
	bbrProbeBWCwndGain     float64       = 2.0                    // ProbeBW cwnd headroom over the BDP
	bbrFullBwGrowthThresh  float64       = 1.25                   // btlbw must grow ≥25% to be "still growing"
	bbrFullBwPlateauRounds int           = 3                      // rounds without growth ⇒ pipe full
	bbrMinPipeCwndSegs     uint64        = 4                      // ProbeRTT cwnd floor, in datagrams
)

// bbrPacingGainCycle is the ProbeBW pacing-gain sequence (draft-cardwell §4.3.3):
// one phase up (1.25) to probe for more bandwidth, one phase down (0.75) to drain
// the queue that probe may have built, then six phases at unity to cruise.
var bbrPacingGainCycle = [8]float64{1.25, 0.75, 1, 1, 1, 1, 1, 1}

// maxSample is one (time, value) point in a windowed-max filter, where time is a
// monotonic counter (here the BBR round-trip count).
type maxSample struct {
	t uint64
	v uint64
}

// maxFilter tracks the running maximum measurement over a sliding window of width
// `win` (Kathleen Nichols' windowed min/max, Linux lib/win_minmax.c, adapted for
// max). Three samples suffice to keep the true max as older, larger samples age
// out. Used for btlbw so a stale bandwidth peak cannot pin the estimate high after
// the path's capacity drops.
type maxFilter struct {
	s    [3]maxSample
	init bool
}

// reset seeds all three slots with a single sample and returns it.
func (m *maxFilter) reset(t, meas uint64) uint64 {
	val := maxSample{t: t, v: meas}
	m.s[0], m.s[1], m.s[2] = val, val, val
	m.init = true
	return m.s[0].v
}

// update folds measurement meas taken at time t into the filter and returns the
// current windowed maximum. t must be non-decreasing across calls.
func (m *maxFilter) update(win, t, meas uint64) uint64 {
	if !m.init || meas >= m.s[0].v || t-m.s[2].t > win {
		return m.reset(t, meas) // new max, or the whole window has aged out
	}
	val := maxSample{t: t, v: meas}
	if meas >= m.s[1].v {
		m.s[1], m.s[2] = val, val
	} else if meas >= m.s[2].v {
		m.s[2] = val
	}
	return m.subwinUpdate(win, val)
}

// subwinUpdate expires stale sub-window maxima, mirroring minmax_subwin_update.
func (m *maxFilter) subwinUpdate(win uint64, val maxSample) uint64 {
	dt := val.t - m.s[0].t
	switch {
	case dt > win: // the primary max aged out: promote the runners-up
		m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], val
		if val.t-m.s[0].t > win {
			m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], val
		}
	case m.s[1].t == m.s[0].t && dt > win/4:
		m.s[1], m.s[2] = val, val
	case m.s[2].t == m.s[1].t && dt > win/2:
		m.s[2] = val
	}
	return m.s[0].v
}

// bbr holds the BBR model and control state for one connection. It is nil unless
// the connection selected CCBBR.
type bbr struct {
	state  bbrState
	filter maxFilter // windowed max of delivery-rate samples (bytes/second)
	btlbw  uint64    // current bottleneck-bandwidth estimate (filter output), bytes/second

	minRTT      time.Duration // windowed-min round-trip propagation delay
	minRTTStamp time.Time     // when minRTT was last (re)set
	minRTTValid bool

	roundCount         uint64 // completed round trips (btlbw filter's time base)
	nextRoundDelivered uint64 // a round completes when a representative P.delivered reaches this
	roundStart         bool   // a new round began on the ACK range just processed

	pacingGain float64
	cwndGain   float64

	fullBw        uint64 // btlbw at the last growth check (Startup plateau detection)
	fullBwCount   int    // consecutive rounds btlbw failed to grow ≥25%
	fullBwReached bool   // Startup has ended (pipe found full)

	cycleIdx   int       // index into bbrPacingGainCycle (ProbeBW)
	cycleStamp time.Time // when the current ProbeBW phase began

	probeRTTDoneStamp           time.Time // earliest time ProbeRTT may end (0 until the flight drains)
	probeRTTRoundStartDelivered uint64    // c.delivered when ProbeRTT began holding
	probeRTTRoundDone           bool      // one round has elapsed inside ProbeRTT
	priorCwnd                   uint64    // cwnd saved on ProbeRTT entry
}

// initBBR arms the BBR model in Startup. Called from NewConn when CCBBR was
// selected; the RFC 9002 initial window (already set on c.cwnd) is retained until
// the model has a bandwidth estimate.
func (c *Conn) initBBR() {
	now := c.clock()
	c.bbr = &bbr{
		state:       bbrStartup,
		pacingGain:  bbrHighGain,
		cwndGain:    bbrHighGain,
		minRTTStamp: now,
		cycleStamp:  now,
	}
}

// bbrOnPacketAcked advances the delivery-rate sampler for one acknowledged packet
// (draft-cheng-iccrg-delivery-rate-estimation). It is called per acked packet from
// onPacketAcked's CCBBR branch, in place of NewReno's window growth. The per-range
// model update and the cwnd/pacing write happen once in bbrOnAckRange.
func (c *Conn) bbrOnPacketAcked(p sentPacket) {
	c.delivered += uint64(p.size)
	c.deliveredTime = c.clock()
	// The rate-sample representative for this range is the most-recently-sent acked
	// packet — the one with the largest P.delivered snapshot — which yields the
	// freshest bandwidth estimate. Choosing it by max is independent of the (random)
	// map iteration order, so the sample is deterministic.
	if !c.rsPriorValid || p.delivered >= c.rsPriorDelivered {
		c.rsPriorDelivered = p.delivered
		c.rsPriorTime = p.deliveredTime
		c.rsPriorAppLimited = p.appLimited
		c.rsPriorValid = true
	}
}

// bbrOnAckRange runs the BBR model once per processed ACK range: it counts round
// trips, folds a delivery-rate sample into btlbw, advances the state machine, and
// writes c.cwnd and c.pacingRate. Called from onAckRange after RTT/min_rtt update.
func (c *Conn) bbrOnAckRange() {
	b := c.bbr
	if b == nil {
		return
	}
	b.roundStart = false
	if c.rsPriorValid {
		if c.rsPriorDelivered >= b.nextRoundDelivered {
			b.nextRoundDelivered = c.delivered
			b.roundCount++
			b.roundStart = true
		}
		c.bbrUpdateBtlBw()
	}
	// An app-limited phase ends once enough has been delivered to have filled a
	// window's worth since it began (draft-cheng §3.3).
	if c.appLimitedUntil != 0 && c.delivered > c.appLimitedUntil {
		c.appLimitedUntil = 0
	}
	now := c.clock()
	c.bbrUpdateState(now)
	c.bbrSetCwndAndPacing()
}

// bbrUpdateBtlBw computes the delivery-rate sample for the current ACK range and
// folds it into the windowed-max btlbw filter.
func (c *Conn) bbrUpdateBtlBw() {
	b := c.bbr
	interval := c.deliveredTime.Sub(c.rsPriorTime)
	if interval <= 0 {
		return
	}
	delivered := c.delivered - c.rsPriorDelivered
	rate := delivered * uint64(time.Second) / uint64(interval) // bytes/second
	// An app-limited sample can only RAISE btlbw (the true bandwidth is at least the
	// observed rate); it must never pull the estimate down (draft-cheng §3.3).
	if !c.rsPriorAppLimited || rate >= b.btlbw {
		b.btlbw = b.filter.update(bbrBtlBwWindowRounds, b.roundCount, rate)
	}
}

// bbrUpdateMinRTT folds an RTT sample into BBR's 10-second windowed-min filter
// (draft-cardwell §4.3.1). Called from onAckRange when a range yields an RTT
// sample. A sample lower than the current estimate, or any sample once the window
// has expired, becomes the new min_rtt.
func (c *Conn) bbrUpdateMinRTT(sample time.Duration, now time.Time) {
	b := c.bbr
	if b == nil || sample <= 0 {
		return
	}
	expired := b.minRTTValid && now.Sub(b.minRTTStamp) > bbrMinRTTWindow
	if !b.minRTTValid || sample <= b.minRTT || expired {
		b.minRTT = sample
		b.minRTTStamp = now
		b.minRTTValid = true
	}
}

// bbrUpdateState drives the control machine: Startup→Drain on a bandwidth plateau,
// Drain→ProbeBW once the flight falls to the BDP, ProbeBW gain cycling, and the
// periodic ProbeRTT excursion.
func (c *Conn) bbrUpdateState(now time.Time) {
	b := c.bbr
	c.bbrCheckStartupFull()
	c.bbrCheckDrain(now)
	if b.state == bbrProbeBW {
		c.bbrAdvanceCyclePhase(now)
	}
	c.bbrCheckProbeRTT(now)
}

// bbrCheckStartupFull ends Startup once btlbw fails to grow by ≥25% for three
// consecutive rounds — the pipe is full — and moves to Drain (draft-cardwell
// §4.3.2). App-limited rounds do not count: a plateau caused by the application
// having nothing to send is not evidence the pipe is full.
func (c *Conn) bbrCheckStartupFull() {
	b := c.bbr
	if b.fullBwReached || b.state != bbrStartup || !b.roundStart || c.appLimitedUntil != 0 {
		return
	}
	if b.btlbw >= uint64(float64(b.fullBw)*bbrFullBwGrowthThresh) {
		b.fullBw = b.btlbw
		b.fullBwCount = 0
		return
	}
	b.fullBwCount++
	if b.fullBwCount >= bbrFullBwPlateauRounds {
		b.fullBwReached = true
		b.state = bbrDrain
		b.pacingGain = bbrDrainGain
		b.cwndGain = bbrHighGain // hold the window high while the queue drains
	}
}

// bbrCheckDrain leaves Drain for ProbeBW once the in-flight bytes have fallen to
// the estimated BDP (draft-cardwell §4.3.2).
func (c *Conn) bbrCheckDrain(now time.Time) {
	if c.bbr.state == bbrDrain && c.bytesInFlight <= c.bbrBDP() {
		c.bbrEnterProbeBW(now)
	}
}

// bbrEnterProbeBW enters steady-state ProbeBW at the first (probe-up) gain phase.
func (c *Conn) bbrEnterProbeBW(now time.Time) {
	b := c.bbr
	b.state = bbrProbeBW
	b.cwndGain = bbrProbeBWCwndGain
	b.cycleIdx = 0
	b.pacingGain = bbrPacingGainCycle[0]
	b.cycleStamp = now
}

// bbrAdvanceCyclePhase advances the ProbeBW pacing-gain cycle one phase per min_rtt
// (draft-cardwell §4.3.3). The 0.75 drain phase also exits early once the flight
// has fallen back to the BDP, so a probe's queue is not held longer than needed.
func (c *Conn) bbrAdvanceCyclePhase(now time.Time) {
	b := c.bbr
	fullLength := b.minRTTValid && now.Sub(b.cycleStamp) > b.minRTT
	advance := fullLength
	if b.pacingGain < 1.0 && c.bytesInFlight <= c.bbrBDP() {
		advance = true
	}
	if !advance {
		return
	}
	b.cycleIdx = (b.cycleIdx + 1) % len(bbrPacingGainCycle)
	b.pacingGain = bbrPacingGainCycle[b.cycleIdx]
	b.cycleStamp = now
}

// bbrCheckProbeRTT enters ProbeRTT when min_rtt has not been refreshed for 10
// seconds, then services the excursion (draft-cardwell §4.3.4).
func (c *Conn) bbrCheckProbeRTT(now time.Time) {
	b := c.bbr
	if b.state != bbrProbeRTT && b.minRTTValid && now.Sub(b.minRTTStamp) > bbrMinRTTWindow {
		b.state = bbrProbeRTT
		b.pacingGain = 1.0
		b.cwndGain = 1.0
		b.priorCwnd = c.cwnd
		b.probeRTTDoneStamp = time.Time{}
		b.probeRTTRoundDone = false
	}
	if b.state == bbrProbeRTT {
		c.bbrHandleProbeRTT(now)
	}
}

// bbrHandleProbeRTT holds the window at the 4-datagram floor until the flight has
// drained, then keeps it there for at least bbrProbeRTTDuration and one round trip
// before restoring the prior mode (draft-cardwell §4.3.4).
func (c *Conn) bbrHandleProbeRTT(now time.Time) {
	b := c.bbr
	minPipe := bbrMinPipeCwndSegs * maxDatagramSize
	if b.probeRTTDoneStamp.IsZero() {
		if c.bytesInFlight <= minPipe {
			b.probeRTTDoneStamp = now.Add(bbrProbeRTTDuration)
			b.probeRTTRoundStartDelivered = c.delivered
			b.probeRTTRoundDone = false
		}
		return
	}
	if c.delivered > b.probeRTTRoundStartDelivered {
		b.probeRTTRoundDone = true
	}
	if b.probeRTTRoundDone && !now.Before(b.probeRTTDoneStamp) {
		b.minRTTStamp = now // the measurement window restarts from this fresh min_rtt
		if b.fullBwReached {
			c.bbrEnterProbeBW(now)
		} else {
			b.state = bbrStartup
			b.pacingGain = bbrHighGain
			b.cwndGain = bbrHighGain
		}
	}
}

// bbrBDP is the bandwidth-delay product btlbw·min_rtt in bytes, or 0 until both
// estimates exist.
func (c *Conn) bbrBDP() uint64 {
	b := c.bbr
	if b.btlbw == 0 || !b.minRTTValid || b.minRTT <= 0 {
		return 0
	}
	return b.btlbw * uint64(b.minRTT) / uint64(time.Second)
}

// bbrSetCwndAndPacing translates the model into the two quantities the send gate
// reads: pacing_rate = pacing_gain·btlbw and cwnd = cwnd_gain·BDP (floored at
// kMinimumWindow, and capped to the 4-datagram floor while in ProbeRTT). Until the
// model has both estimates the RFC 9002 initial window and pacer are left in place.
func (c *Conn) bbrSetCwndAndPacing() {
	b := c.bbr
	if b.btlbw > 0 {
		if rate := uint64(b.pacingGain * float64(b.btlbw)); rate > 0 {
			c.pacingRate = rate
		}
	}
	bdp := c.bbrBDP()
	if bdp == 0 {
		return
	}
	target := uint64(b.cwndGain * float64(bdp))
	if target < kMinimumWindow {
		target = kMinimumWindow
	}
	if b.state == bbrProbeRTT {
		if capWnd := bbrMinPipeCwndSegs * maxDatagramSize; target > capWnd {
			target = capWnd
		}
	}
	c.cwnd = target
}

// bbrMarkAppLimited records that the application handed the connection everything
// it had. If that did not fill the congestion window, the resulting under-full
// delivery-rate samples are flagged app-limited so an idle application cannot pull
// btlbw down (draft-cheng §4). Called from the send path on CCBBR connections.
func (c *Conn) bbrMarkAppLimited() {
	if c.bytesInFlight < c.cwnd {
		c.appLimitedUntil = c.delivered + c.bytesInFlight
		if c.appLimitedUntil == 0 {
			c.appLimitedUntil = 1 // nonzero sentinel: app-limited with nothing in flight
		}
	}
}

// bbrOnPersistentCongestion collapses the window to the floor and re-enters Startup
// after a persistent-congestion episode (every packet across a PTO span lost). BBR
// is loss-tolerant, but a path that lost an entire span is dead, so the bandwidth
// model is discarded and the controller re-probes the path from scratch.
func (c *Conn) bbrOnPersistentCongestion() {
	c.cwnd = kMinimumWindow
	c.pacingRate = 0 // fall back to the RFC 9002 pacer until a new estimate forms
	c.appLimitedUntil = 0
	c.initBBR()
}
