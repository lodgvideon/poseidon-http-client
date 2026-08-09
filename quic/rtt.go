package quic

import "time"

// Loss-detection constants (RFC 9002 §6.1).
const (
	kPacketThreshold uint64        = 3                // §6.1.1
	kGranularity     time.Duration = time.Millisecond // §6.1.2 timer granularity floor
)

// lossDelay is the time-threshold window for declaring a packet lost (RFC 9002
// §6.1.2): 9/8 of the larger of the smoothed and latest RTT, floored at the
// timer granularity. Before the first RTT sample it is the 1 ms floor.
func (r rttStats) lossDelay() time.Duration {
	rtt := r.smoothedRTT
	if r.latestRTT > rtt {
		rtt = r.latestRTT
	}
	if d := rtt * 9 / 8; d > kGranularity {
		return d
	}
	return kGranularity
}

// rttStats maintains the round-trip-time estimates derived from acknowledgements
// (RFC 9002 §5). They feed loss detection and the probe timeout.
type rttStats struct {
	latestRTT   time.Duration
	minRTT      time.Duration
	smoothedRTT time.Duration
	rttvar      time.Duration
	haveSample  bool
	resetMin    bool // §5.2: reset min_rtt to the next sample after persistent congestion
}

// update folds a new RTT sample and the peer's reported ack delay into the
// estimates (RFC 9002 §5.2–5.3).
func (r *rttStats) update(latest, ackDelay time.Duration) {
	r.latestRTT = latest
	if !r.haveSample {
		r.minRTT = latest
		r.smoothedRTT = latest
		r.rttvar = latest / 2
		r.haveSample = true
		return
	}
	if r.resetMin {
		// Persistent congestion reset (§5.2): adopt this sample as the new min_rtt
		// outright, discarding a stale low estimate from before the path changed.
		r.minRTT = latest
		r.resetMin = false
	} else if latest < r.minRTT {
		r.minRTT = latest
	}
	// Subtract the peer's ack delay, but never bring the sample below min_rtt.
	adjusted := latest
	if latest >= r.minRTT+ackDelay {
		adjusted = latest - ackDelay
	}
	r.rttvar = (3*r.rttvar + absDuration(r.smoothedRTT-adjusted)) / 4
	r.smoothedRTT = (7*r.smoothedRTT + adjusted) / 8
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// sentPacket records a packet the client sent, for acknowledgement, RTT
// sampling, and loss detection.
//
// The delivered/deliveredTime/appLimited fields are the delivery-rate sampler's
// per-packet snapshot (draft-cheng-iccrg-delivery-rate-estimation), written by
// onPacketSent only when BBR is the active controller (bbr.go). Under NewReno
// they stay zero and are never read, so NewReno accounting is unchanged.
type sentPacket struct {
	timeSent     time.Time
	ackEliciting bool
	size         int // on-wire packet length in bytes, for congestion control (RFC 9002 §7)

	// frame is the retransmittable frame this packet carries; hasFrame says
	// whether there is one.
	//
	// A value rather than a slice because every send site in this package builds
	// a packet around at most one such frame — the loss path re-seals each queued
	// frame into a packet of its own — so the one-element slice this used to be
	// was allocated per datagram and never held more than one thing. Turn it back
	// into a slice if a change ever coalesces several lost frames into one packet.
	frame    retransFrame
	hasFrame bool

	delivered     uint64    // C.delivered snapshot at send (BBR delivery-rate sampler)
	deliveredTime time.Time // C.delivered_time snapshot at send (BBR delivery-rate sampler)
	appLimited    bool      // sender was application-limited at send (BBR: sample may be skipped)
}

// sentSpace tracks the unacknowledged packets sent in one packet-number space.
type sentSpace struct {
	packets          map[uint64]sentPacket
	largestAckedPN   uint64
	haveLargestAcked bool
	// ackedElicit holds the send times of acknowledged ack-eliciting packets,
	// pruned to those newer than the oldest packet still in flight. It answers
	// whether an acknowledgement fell inside a lost span, which would break the
	// unbroken loss that persistent congestion requires (RFC 9002 §7.6.1).
	ackedElicit []time.Time
}

// ackedInSpan reports whether any acknowledged ack-eliciting packet was sent
// strictly between lo and hi (RFC 9002 §7.6.1).
func (s *sentSpace) ackedInSpan(lo, hi time.Time) bool {
	for _, t := range s.ackedElicit {
		if t.After(lo) && t.Before(hi) {
			return true
		}
	}
	return false
}

// pruneAcked drops recorded acknowledgement times at or before the oldest packet
// still in flight. A future persistent-congestion span begins at a lost (still
// unacknowledged) packet, so an earlier acknowledgement can never fall inside it;
// this bounds the slice to the current in-flight window.
func (s *sentSpace) pruneAcked() {
	if len(s.ackedElicit) == 0 {
		return
	}
	var oldest time.Time
	found := false
	for _, p := range s.packets {
		if p.ackEliciting && (!found || p.timeSent.Before(oldest)) {
			oldest, found = p.timeSent, true
		}
	}
	if !found {
		s.ackedElicit = s.ackedElicit[:0]
		return
	}
	kept := s.ackedElicit[:0]
	for _, t := range s.ackedElicit {
		if t.After(oldest) {
			kept = append(kept, t)
		}
	}
	s.ackedElicit = kept
}

// onSent records that packet pn was sent at t carrying the given
// retransmittable frames.
// rf is nil when the packet carries nothing worth resending; it is copied, not
// retained, so callers may pass the address of a local.
func (s *sentSpace) onSent(pn uint64, t time.Time, ackEliciting bool, rf *retransFrame) {
	if s.packets == nil {
		s.packets = map[uint64]sentPacket{}
	}
	p := sentPacket{timeSent: t, ackEliciting: ackEliciting}
	if rf != nil {
		p.frame, p.hasFrame = *rf, true
	}
	s.packets[pn] = p
}

// ack processes an acknowledgement of the packet-number range [low, high],
// removing those packets from the in-flight set. It returns the largest
// acknowledged packet's send time and a flag: an RTT sample is generated when the
// largest is newly acknowledged AND at least one of the newly acknowledged packets
// was ack-eliciting — even if the largest itself was not (RFC 9002 §5.1). Only the
// range containing the largest acknowledged (high == largest) can yield a sample.
func (s *sentSpace) ack(low, high uint64) (sendTime time.Time, hasRTT bool) {
	newLargest := !s.haveLargestAcked || high > s.largestAckedPN
	haveLargest := false
	if p, ok := s.packets[high]; ok && newLargest {
		sendTime = p.timeSent // the sample uses the largest acked packet's send time
		haveLargest = true
	}
	if newLargest {
		s.largestAckedPN = high
		s.haveLargestAcked = true
	}
	// Iterate our own sent packets (bounded) rather than the ACK range (which a
	// malformed frame could make enormous) to find the ones to remove, noting
	// whether any newly acknowledged packet was ack-eliciting.
	anyAckEliciting := false
	for pn, p := range s.packets {
		if pn >= low && pn <= high {
			if p.ackEliciting {
				anyAckEliciting = true
			}
			delete(s.packets, pn)
		}
	}
	hasRTT = newLargest && haveLargest && anyAckEliciting
	return sendTime, hasRTT
}
