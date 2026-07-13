package quic

import "time"

// retransKind identifies the type of a retransmittable frame.
type retransKind uint8

const (
	retransCrypto retransKind = iota
	retransStream
	retransReset       // RESET_STREAM
	retransStopSending // STOP_SENDING
	retransRetire      // RETIRE_CONNECTION_ID (offset carries the sequence number)
)

// retransFrame is the information needed to re-send a lost frame's data as a new
// frame in a new packet (RFC 9000 §13.3): the payload bytes and where they
// belong on their stream. data is a private copy, so mutation or reuse of the
// original send buffer cannot corrupt a later retransmission.
type retransFrame struct {
	kind     retransKind
	streamID uint64 // stream frames; offset doubles as the final size for retransReset
	offset   uint64
	fin      bool   // retransStream only
	errCode  uint64 // retransReset / retransStopSending only
	data     []byte
}

// encode appends the frame to dst (STREAM/CRYPTO at their original offset).
func (rf retransFrame) encode(dst []byte) []byte {
	switch rf.kind {
	case retransStream:
		return AppendStream(dst, rf.streamID, rf.offset, rf.fin, rf.data)
	case retransReset:
		return AppendResetStream(dst, rf.streamID, rf.errCode, rf.offset)
	case retransStopSending:
		return AppendStopSending(dst, rf.streamID, rf.errCode)
	case retransRetire:
		return AppendRetireConnectionID(dst, rf.offset)
	default:
		return AppendCrypto(dst, rf.offset, rf.data)
	}
}

// earliestLossTime returns the time the earliest still-unacknowledged
// ack-eliciting packet that has a later acknowledged packet will be declared lost
// by the time threshold (RFC 9002 §6.1.2), and the space it is in. These are the
// packets a loss-detection timer must cover: a later packet was acknowledged, so
// they are eligible for time-threshold loss even if no further acknowledgement
// arrives. ok is false when no space has such a packet, in which case only the
// probe timeout applies (a fully lost tail has no later acknowledgement).
func (c *Conn) earliestLossTime() (t time.Time, space int, ok bool) {
	space = -1
	delay := c.rtt.lossDelay()
	for sp := 0; sp < numSpaces; sp++ {
		s := &c.sent[sp]
		if !s.haveLargestAcked {
			continue
		}
		for pn, p := range s.packets {
			if pn > s.largestAckedPN || !p.ackEliciting {
				continue // no later acknowledged packet, or nothing to retransmit
			}
			if lt := p.timeSent.Add(delay); space == -1 || lt.Before(t) {
				t, space = lt, sp
			}
		}
	}
	return t, space, space != -1
}

// detectLost declares packets in space sp lost and queues their retransmittable
// frames for resend (RFC 9002 §6.1). It runs on receipt of an ACK, after the
// acknowledged packets have been removed and the RTT updated. A packet is lost
// if a later packet in the space is acknowledged and the packet is either
// kPacketThreshold packet numbers earlier (§6.1.1) or was sent longer than the
// loss delay ago (§6.1.2).
//
// This is ACK-driven detection only: with no ACK there is no trigger, so a
// fully lost tail or sole flight is not recovered here — that needs the probe
// timeout (RFC 9002 §6.2), a following phase. Until then the caller's read
// deadline bounds the wait.
//
// Assumes c.mu is held (runs from the receive path, mutating sent/cwnd state).
func (c *Conn) detectLost(sp int) {
	s := &c.sent[sp]
	if !s.haveLargestAcked {
		return
	}
	lostSendTime := c.clock().Add(-c.rtt.lossDelay())
	var newestLost, earliestPC, latestPC time.Time
	anyLost, anyPC := false, false
	for pn, p := range s.packets {
		if pn > s.largestAckedPN {
			continue // only packets before an acknowledged one are eligible
		}
		lost := s.largestAckedPN >= pn+kPacketThreshold || // §6.1.1 packet threshold
			!p.timeSent.After(lostSendTime) // §6.1.2 time threshold
		if !lost {
			continue
		}
		c.retransQueue[sp] = append(c.retransQueue[sp], p.frames...)
		if p.ackEliciting { // congestion accounting (RFC 9002 §7.3.1)
			c.removeInFlight(p.size)
			if !anyLost || p.timeSent.After(newestLost) {
				newestLost, anyLost = p.timeSent, true
			}
			// Persistent congestion (§7.6) counts only ack-eliciting packets sent
			// after the first RTT sample: track the span they cover.
			if !c.firstRTTSample.IsZero() && p.timeSent.After(c.firstRTTSample) {
				if !anyPC || p.timeSent.Before(earliestPC) {
					earliestPC = p.timeSent
				}
				if !anyPC || p.timeSent.After(latestPC) {
					latestPC = p.timeSent
				}
				anyPC = true
			}
		}
		delete(s.packets, pn) // safe to delete during range in Go
	}
	if anyLost {
		c.onCongestionEvent(newestLost) // halve cwnd once for this loss episode
		// If the lost ack-eliciting packets span longer than the persistent congestion
		// duration and no packet inside that span was acknowledged, every packet across
		// the period was lost: collapse the window (RFC 9002 §7.6.1).
		if anyPC && latestPC.Sub(earliestPC) > c.persistentCongestionDuration() &&
			!s.ackedInSpan(earliestPC, latestPC) {
			c.onPersistentCongestion()
		}
	}
	// Prune on every ACK, not only when a loss fired: a lossless connection would
	// otherwise never reach this call and ackedElicit would grow without bound. The
	// prune is safe regardless — a future persistent-congestion span begins at a
	// still-unacknowledged packet, so no earlier acknowledgement can fall inside it.
	s.pruneAcked()
}
