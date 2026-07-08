package quic

import "time"

// ackDelayExponent is the default peer ack_delay_exponent (RFC 9000 §18.2): the
// ACK Delay field is decoded by shifting left this many bits, in microseconds.
const ackDelayExponent = 3

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
	if latest < r.minRTT {
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
type sentPacket struct {
	timeSent     time.Time
	ackEliciting bool
	frames       []retransFrame // retransmittable frames carried (nil = nothing to resend)
}

// sentSpace tracks the unacknowledged packets sent in one packet-number space.
type sentSpace struct {
	packets          map[uint64]sentPacket
	largestAckedPN   uint64
	haveLargestAcked bool
}

// onSent records that packet pn was sent at t carrying the given
// retransmittable frames.
func (s *sentSpace) onSent(pn uint64, t time.Time, ackEliciting bool, frames []retransFrame) {
	if s.packets == nil {
		s.packets = map[uint64]sentPacket{}
	}
	s.packets[pn] = sentPacket{timeSent: t, ackEliciting: ackEliciting, frames: frames}
}

// ack processes an acknowledgement of the packet-number range [low, high],
// removing those packets from the in-flight set. When high is a newly
// acknowledged, ack-eliciting packet it returns its send time so the caller can
// generate an RTT sample (RFC 9002 §5.1). Only the range containing the largest
// acknowledged (high == largest) can yield a sample.
func (s *sentSpace) ack(low, high uint64) (sendTime time.Time, hasRTT bool) {
	if !s.haveLargestAcked || high > s.largestAckedPN {
		if p, ok := s.packets[high]; ok && p.ackEliciting {
			sendTime = p.timeSent
			hasRTT = true
		}
		s.largestAckedPN = high
		s.haveLargestAcked = true
	}
	// Iterate our own sent packets (bounded) rather than the ACK range (which a
	// malformed frame could make enormous) to find the ones to remove.
	for pn := range s.packets {
		if pn >= low && pn <= high {
			delete(s.packets, pn)
		}
	}
	return sendTime, hasRTT
}
