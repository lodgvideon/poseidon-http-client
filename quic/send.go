package quic

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// maxDatagramSize is the conservative outbound datagram cap used before path MTU
// discovery: RFC 9000 §14 guarantees any usable path supports at least 1200
// bytes.
const maxDatagramSize = 1200

// blockKind identifies which flow-control limit stalled a send.
type blockKind int

const (
	blockNone blockKind = iota
	blockStream
	blockConn
	blockCong // congestion window full (RFC 9002 §7); not signalled with a frame
)

// Send writes data on the stream as one or more STREAM frames over the 1-RTT
// packet space, honoring the peer's per-stream and connection send limits
// (RFC 9000 §4.1). It consumes a PREFIX of data: on a flow-control block it
// returns the number of bytes admitted with a nil error, having emitted a
// STREAM_DATA_BLOCKED or DATA_BLOCKED frame; the caller retries with data
// advanced by the returned count once a MAX_STREAM_DATA / MAX_DATA frame raises
// the limit. fin marks the end of the stream; the FIN rides on the frame with
// the last byte (or a zero-length frame for empty data) and consumes no
// flow-control credit, so it is never blocked. Send after the FIN returns
// ErrStreamFinished.
func (s *Stream) Send(data []byte, fin bool) (int, error) {
	if s.sendReset {
		return 0, ErrStreamReset
	}
	if s.finSent {
		return 0, ErrStreamFinished
	}
	if s.conn.oneRTTSealer == nil {
		return 0, ErrNotEstablished
	}
	sent := 0
	for {
		if remaining := len(data) - sent; remaining > 0 {
			n, blocked := s.grantable(remaining)
			if n == 0 {
				s.emitBlocked(blocked)
				return sent, nil
			}
			last := fin && sent+n == len(data)
			if err := s.writeStreamFrame(data[sent:sent+n], last); err != nil {
				return sent, err
			}
			sent += n
			if last {
				return sent, nil
			}
			continue
		}
		// All data admitted. A FIN owed with no unsent bytes consumes no
		// credit, so emit the zero-length end-of-stream frame unconditionally.
		if fin && !s.finSent {
			if err := s.writeStreamFrame(nil, true); err != nil {
				return sent, err
			}
		}
		return sent, nil
	}
}

// writeStreamFrame emits one STREAM frame carrying chunk at the current send
// offset, setting FIN when fin is true, then advances the send accounting. The
// chunk is retained (as a copy) so the frame can be retransmitted at this same
// offset if the packet is lost; a resend does not re-advance the accounting.
func (s *Stream) writeStreamFrame(chunk []byte, fin bool) error {
	rf := retransFrame{
		kind: retransStream, streamID: s.id, offset: s.sendOffset, fin: fin,
		data: append([]byte(nil), chunk...),
	}
	if err := s.conn.writeAppFrames(AppendStream(nil, s.id, s.sendOffset, fin, chunk), []retransFrame{rf}); err != nil {
		return err
	}
	s.sendOffset += uint64(len(chunk))
	s.conn.connSent += uint64(len(chunk))
	if fin {
		s.finSent = true
		s.conn.maybeRetire(s) // if the response already arrived, both sides are now done
	}
	return nil
}

// grantable computes how many of the next remaining bytes may be sent now,
// clamped by the per-stream credit, the connection credit, the u62 offset
// ceiling (§19.8), and the per-datagram budget. A zero result also reports which
// limit is responsible; a zero caused solely by the offset ceiling reports
// blockNone (no BLOCKED frame is warranted).
func (s *Stream) grantable(remaining int) (int, blockKind) {
	streamCredit := s.sendMax - s.sendOffset
	connCredit := s.conn.connMax - s.conn.connSent
	credit := min(streamCredit, connCredit, bytesx.MaxVarint-s.sendOffset)
	if credit == 0 {
		switch {
		case streamCredit == 0:
			return 0, blockStream
		case connCredit == 0:
			return 0, blockConn
		default:
			return 0, blockNone
		}
	}
	// Congestion control (RFC 9002 §7): clamp to the space left in the congestion
	// window; a full window stalls the send with no frame (unlike flow control),
	// and the caller's send loop resumes as acknowledgements free the window.
	if s.conn.cwnd > 0 {
		if s.conn.bytesInFlight >= s.conn.cwnd {
			return 0, blockCong
		}
		credit = min(credit, s.conn.cwnd-s.conn.bytesInFlight)
	}
	return int(min(credit, uint64(remaining), uint64(s.maxChunk()))), blockNone
}

// maxChunk is the largest STREAM data length that fits one 1-RTT datagram
// (RFC 9000 §14; QUIC has no per-frame size limit). It subtracts the short
// header, the STREAM frame header, and the AEAD tag from the datagram budget.
func (s *Stream) maxChunk() int {
	hdr := 1 + len(s.conn.dcid) + 4  // first byte + DCID + 4-byte packet number
	fh := 1 + bytesx.VarintLen(s.id) // Type + Stream ID
	if s.sendOffset > 0 {
		fh += bytesx.VarintLen(s.sendOffset) // Offset
	}
	fh += 2                                   // Length varint (< 16384 for a sub-1200 datagram)
	budget := maxDatagramSize - hdr - fh - 16 // 16-byte AEAD tag
	if budget < 1 {
		return 1
	}
	return budget
}

// emitBlocked sends a STREAM_DATA_BLOCKED or DATA_BLOCKED frame once per distinct
// limit value (RFC 9000 §4.1, §19.12-§19.13). These are informational and free
// no credit; the send resumes when a MAX_* frame raises the limit.
func (s *Stream) emitBlocked(kind blockKind) {
	switch kind {
	case blockStream:
		if !s.sdBlockedSet || s.sdBlockedLimit != s.sendMax {
			s.sdBlockedSet = true
			s.sdBlockedLimit = s.sendMax
			_ = s.conn.writeAppFrames(AppendStreamDataBlocked(nil, s.id, s.sendMax), nil)
		}
	case blockConn:
		if !s.conn.dataBlockedSet || s.conn.dataBlockedLimit != s.conn.connMax {
			s.conn.dataBlockedSet = true
			s.conn.dataBlockedLimit = s.conn.connMax
			_ = s.conn.writeAppFrames(AppendDataBlocked(nil, s.conn.connMax), nil)
		}
	}
}

// writeAppFrames seals frames into a 1-RTT packet and writes the datagram. The
// STREAM and *_BLOCKED frames it carries are ack-eliciting; retrans names the
// frames to resend if the packet is lost (nil for the informational *_BLOCKED
// frames, which are not retransmitted this phase).
func (c *Conn) writeAppFrames(frames []byte, retrans []retransFrame) error {
	pkt, err := c.sealPacket(spaceApp, frames, true, retrans)
	if err != nil {
		return err
	}
	_, err = c.pc.Write(pkt)
	return err
}
