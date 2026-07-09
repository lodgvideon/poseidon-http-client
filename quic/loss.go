package quic

import "time"

// retransKind identifies the type of a retransmittable frame.
type retransKind uint8

const (
	retransCrypto retransKind = iota
	retransStream
)

// retransFrame is the information needed to re-send a lost frame's data as a new
// frame in a new packet (RFC 9000 §13.3): the payload bytes and where they
// belong on their stream. data is a private copy, so mutation or reuse of the
// original send buffer cannot corrupt a later retransmission.
type retransFrame struct {
	kind     retransKind
	streamID uint64 // retransStream only
	offset   uint64
	fin      bool // retransStream only
	data     []byte
}

// encode appends the frame to dst at its original offset.
func (rf retransFrame) encode(dst []byte) []byte {
	if rf.kind == retransStream {
		return AppendStream(dst, rf.streamID, rf.offset, rf.fin, rf.data)
	}
	return AppendCrypto(dst, rf.offset, rf.data)
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
func (c *Conn) detectLost(sp int) {
	s := &c.sent[sp]
	if !s.haveLargestAcked {
		return
	}
	lostSendTime := c.clock().Add(-c.rtt.lossDelay())
	var newestLost time.Time
	anyLost := false
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
		}
		delete(s.packets, pn) // safe to delete during range in Go
	}
	if anyLost {
		c.onCongestionEvent(newestLost) // halve cwnd once for this loss episode
	}
}
