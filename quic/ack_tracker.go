package quic

import "sort"

// pnRange is an inclusive range of received packet numbers.
type pnRange struct{ lo, hi uint64 }

// maxAckRanges bounds how many packet-number ranges a tracker retains. A peer
// that sends packets with permanent gaps (every other packet number, say) would
// otherwise grow ranges without limit — unbounded memory, an O(n log n) re-sort
// per packet, and eventually an ACK frame too large for one datagram, which the
// connection cannot send. RFC 9000 §13.2.4 permits acknowledging only a subset
// of received packets, so dropping the oldest ranges is conformant; the cost is
// at most a spurious retransmission of a long-past packet.
const maxAckRanges = 32

// ackTracker records the packet numbers received in one packet-number space and
// builds ACK frames from them (RFC 9000 §13.2, §19.3). Ranges are kept sorted
// descending by upper bound, non-overlapping and non-adjacent. One tracker per
// space (Initial, Handshake, Application).
type ackTracker struct {
	ranges  []pnRange
	pending bool // an ack-eliciting packet has been received but not yet acked
	// Deferred-ACK state (RFC 9000 §13.2.1), consumed only for the Application
	// (1-RTT) space, where an ACK may be delayed up to max_ack_delay. immediate is
	// set when an owed ACK must be sent without delay — the 2nd ack-eliciting packet
	// since the last ACK, or an out-of-order arrival. elicitCount counts ack-eliciting
	// packets received since the last ACK was built; both reset in acked().
	immediate   bool
	elicitCount int
}

// receive records that packet number pn was received. ackEliciting marks
// whether the packet contained an ack-eliciting frame (RFC 9000 §13.2.1), which
// obliges an ACK to be sent — and, for a new ack-eliciting packet, decides whether
// that ACK must go out immediately (the §13.2.1 immediate triggers) or may be
// deferred up to max_ack_delay.
func (a *ackTracker) receive(pn uint64, ackEliciting bool) {
	isNew := true
	for _, r := range a.ranges {
		if pn >= r.lo && pn <= r.hi {
			isNew = false
			break
		}
	}
	if ackEliciting {
		a.pending = true
		if isNew {
			a.elicitCount++
			// §13.2.1: send the ACK immediately on the 2nd ack-eliciting packet since
			// the last ACK, or when an ack-eliciting packet arrives out of order — a
			// reorder below the top of the received range, or a gap above it. Both
			// assist the peer's loss detection, so they override the max_ack_delay
			// deferral. hi is the previous largest (pn is not yet inserted).
			if a.elicitCount >= 2 {
				a.immediate = true
			}
			if hi, had := a.largest(); had && pn != hi+1 {
				a.immediate = true
			}
		}
	}
	if !isNew {
		return // already recorded; range set unchanged
	}
	a.ranges = append(a.ranges, pnRange{pn, pn})
	sort.Slice(a.ranges, func(i, j int) bool { return a.ranges[i].hi > a.ranges[j].hi })
	merged := a.ranges[:1]
	for _, r := range a.ranges[1:] {
		last := &merged[len(merged)-1]
		if r.hi+1 >= last.lo {
			if r.lo < last.lo {
				last.lo = r.lo
			}
		} else {
			merged = append(merged, r)
		}
	}
	// Drop the oldest ranges past the cap. merged is sorted descending by upper
	// bound, so the newest — including the largest received, which the peer needs
	// for loss detection and RTT — are kept.
	if len(merged) > maxAckRanges {
		merged = merged[:maxAckRanges]
	}
	a.ranges = merged
}

// ackPending reports whether an ACK is owed (an ack-eliciting packet has been
// received since the last ACK was built).
func (a *ackTracker) ackPending() bool { return a.pending }

// largest returns the largest packet number received, and whether any have been.
func (a *ackTracker) largest() (uint64, bool) {
	if len(a.ranges) == 0 {
		return 0, false
	}
	return a.ranges[0].hi, true
}

// buildACK appends an ACK frame covering every received range to dst WITHOUT
// clearing the pending/deferral state, for a caller that may not commit the frame
// to the wire (a piggyback that might not fit the datagram). It returns dst
// unchanged when nothing has been received. ackDelay is the scaled acknowledgement
// delay (RFC 9000 §19.3). Callers clear the owed state via acked() once the packet
// carrying the ACK is committed.
func (a *ackTracker) buildACK(dst []byte, ackDelay uint64) []byte {
	if len(a.ranges) == 0 {
		return dst
	}
	largest := a.ranges[0].hi
	firstRange := a.ranges[0].hi - a.ranges[0].lo
	var extra []AckRange
	for i := 1; i < len(a.ranges); i++ {
		// Gap = unacknowledged packets between this range and the previous one
		// (RFC 9000 §19.3.1): (prev.lo - 1) - (cur.hi + 1).
		gap := a.ranges[i-1].lo - a.ranges[i].hi - 2
		extra = append(extra, AckRange{Gap: gap, Length: a.ranges[i].hi - a.ranges[i].lo})
	}
	return AppendAck(dst, largest, ackDelay, firstRange, extra)
}

// appendACK builds the ACK frame and marks it sent (acked()), the common path when
// the caller writes the ACK unconditionally (the reader's flush).
func (a *ackTracker) appendACK(dst []byte, ackDelay uint64) []byte {
	out := a.buildACK(dst, ackDelay)
	a.acked()
	return out
}

// acked records that the owed ACK has been (or is being) sent: it clears the
// pending flag and resets the §13.2.1 deferral counters so the next ack-eliciting
// packet starts a fresh delay window.
func (a *ackTracker) acked() {
	a.pending = false
	a.immediate = false
	a.elicitCount = 0
}
