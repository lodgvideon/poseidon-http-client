package quic

import "sort"

// pnRange is an inclusive range of received packet numbers.
type pnRange struct{ lo, hi uint64 }

// ackTracker records the packet numbers received in one packet-number space and
// builds ACK frames from them (RFC 9000 §13.2, §19.3). Ranges are kept sorted
// descending by upper bound, non-overlapping and non-adjacent. One tracker per
// space (Initial, Handshake, Application).
type ackTracker struct {
	ranges  []pnRange
	pending bool // an ack-eliciting packet has been received but not yet acked
}

// receive records that packet number pn was received. ackEliciting marks
// whether the packet contained an ack-eliciting frame (RFC 9000 §13.2.1), which
// obliges an ACK to be sent.
func (a *ackTracker) receive(pn uint64, ackEliciting bool) {
	if ackEliciting {
		a.pending = true
	}
	for _, r := range a.ranges {
		if pn >= r.lo && pn <= r.hi {
			return // already recorded
		}
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

// appendACK appends an ACK frame covering every received range to dst and clears
// the pending flag. It returns dst unchanged when nothing has been received.
// ackDelay is the scaled acknowledgement delay (RFC 9000 §19.3).
func (a *ackTracker) appendACK(dst []byte, ackDelay uint64) []byte {
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
	a.pending = false
	return AppendAck(dst, largest, ackDelay, firstRange, extra)
}
