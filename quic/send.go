package quic

import (
	"time"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

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
	blockPace // pacing bucket empty (RFC 9002 §7.7); refills with elapsed time, no frame
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
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.sendLocked(data, fin)
}

// sendLocked is Send's body. Assumes s.conn.mu is held.
//
// A large body splits into many equal-size (~1200-byte) 1-RTT datagrams; instead
// of one sendto per datagram, they are accumulated into a GSO batch and handed to
// the transport in a single WriteGSO on Linux (else a per-datagram loop) — see
// quic/gso.go. Each datagram is still sealed and recorded individually inside the
// loop, so packet numbers, retransmit records, and congestion accounting are
// unchanged; only the syscall count drops. The batch is flushed on every exit, and
// early before the informational *_BLOCKED frame, which is a different-size packet
// that must not ride the GSO segment.
func (s *Stream) sendLocked(data []byte, fin bool) (int, error) {
	if s.sendReset {
		return 0, ErrStreamReset
	}
	if s.finSent {
		return 0, ErrStreamFinished
	}
	if s.conn.oneRTTSealer == nil {
		return 0, ErrNotEstablished
	}
	batch := s.conn.newBatch()
	sent := 0
	for {
		if remaining := len(data) - sent; remaining > 0 {
			n, blocked := s.grantable(remaining)
			if n == 0 {
				// Flush the accumulated STREAM datagrams before the BLOCKED frame, then
				// record which limit stalled this send so WaitSendable can pick its wake
				// source (docs/HTTP3_DESIGN.md §3.3): only blockPace arms a timer.
				if err := s.conn.flushBatch(&batch); err != nil {
					return sent, err
				}
				s.sendBlock = blocked
				s.emitBlocked(blocked)
				return sent, nil
			}
			last := fin && sent+n == len(data)
			if err := s.writeStreamFrame(&batch, data[sent:sent+n], last); err != nil {
				_ = s.conn.flushBatch(&batch) // best-effort: send what was already sealed
				return sent, err
			}
			sent += n
			if last {
				if s.conn.ccAlgo == CCBBR {
					s.conn.bbrMarkAppLimited() // sent all the caller had; flag app-limited if the pipe isn't full
				}
				return sent, s.conn.flushBatch(&batch)
			}
			continue
		}
		// All data admitted. A FIN owed with no unsent bytes consumes no
		// credit, so emit the zero-length end-of-stream frame unconditionally.
		if fin && !s.finSent {
			if err := s.writeStreamFrame(&batch, nil, true); err != nil {
				_ = s.conn.flushBatch(&batch)
				return sent, err
			}
		}
		if s.conn.ccAlgo == CCBBR {
			s.conn.bbrMarkAppLimited() // sent all the caller had; flag app-limited if the pipe isn't full
		}
		return sent, s.conn.flushBatch(&batch)
	}
}

// writeStreamFrame seals one STREAM frame carrying chunk at the current send
// offset (FIN set when fin is true) into the GSO batch, then advances the send
// accounting. The chunk is retained (as a copy) so the frame can be retransmitted
// at this same offset if the packet is lost; a resend does not re-advance the
// accounting. The sealed datagram is copied into the batch, so the packet reaches
// the wire when the batch is flushed, not here.
func (s *Stream) writeStreamFrame(b *packetBatch, chunk []byte, fin bool) error {
	c := s.conn
	rf := retransFrame{
		kind: retransStream, streamID: s.id, offset: s.sendOffset, fin: fin,
		data: c.retransCopy(chunk), // private copy retained for retransmit (§13.3); NOT the reused scratch
	}
	// Build the frame payload into the reused frameScratch: it is consumed by the
	// seal below (as the AEAD payload) before any next writeStreamFrame overwrites
	// it, and c.mu is held throughout, so reuse is safe. rf.data above is a
	// separate private copy, so a resend never reads this scratch.
	c.frameScratch = c.frameScratch[:0]
	// Piggyback an owed Application-space ACK on this STREAM packet when one fits
	// (RFC 9000 §13.2.1): the deferred ACK rides the request instead of leaving as
	// its own datagram, halving send syscalls on the GET request/response path. The
	// ACK is not ack-eliciting and is never retransmitted, so it is left out of rf.
	// It is prepended so the peer sees the acknowledgement first. A full-size STREAM
	// chunk leaves no room, so the ACK stays deferred and fires on the timer.
	piggyback := c.handshakeComplete && c.acks[spaceApp].pending
	if piggyback {
		c.frameScratch = c.acks[spaceApp].buildACK(c.frameScratch, 0)
		overhead := 1 + len(c.dcid) + 4 + 16 // short header + 4-byte PN + AEAD tag
		if overhead+len(c.frameScratch)+streamFrameLen(s.id, s.sendOffset, chunk) > maxDatagramSize {
			c.frameScratch = c.frameScratch[:0] // would overflow the datagram; keep deferring
			piggyback = false
		}
	}
	c.frameScratch = AppendStream(c.frameScratch, s.id, s.sendOffset, fin, chunk)
	if err := c.sealAppToBatch(b, c.frameScratch, &rf); err != nil {
		return err
	}
	if piggyback {
		c.acks[spaceApp].acked() // the owed ACK rode this packet; clear it + the deferral
		c.ackDeadline = time.Time{}
	}
	s.sendOffset += uint64(len(chunk))
	c.connSent += uint64(len(chunk))
	if fin {
		s.finSent = true
		c.maybeRetire(s) // if the response already arrived, both sides are now done
	}
	return nil
}

// streamFrameLen is the encoded byte length of the STREAM frame AppendStream would
// produce for these arguments (type + Stream ID + optional Offset + Length + data),
// used to decide whether a piggybacked ACK still fits the 1200-byte datagram.
func streamFrameLen(streamID, offset uint64, data []byte) int {
	n := 1 + bytesx.VarintLen(streamID)
	if offset != 0 {
		n += bytesx.VarintLen(offset)
	}
	return n + bytesx.VarintLen(uint64(len(data))) + len(data)
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
		// Pacing (RFC 9002 §7.7): clamp to the token bucket so a full window is not
		// dumped back-to-back. An empty bucket stalls the send like a congestion
		// block; it refills as wall-clock time passes, so the caller retries later.
		paced := s.conn.pacingCredit()
		if paced == 0 {
			return 0, blockPace
		}
		credit = min(credit, paced)
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
	//exhaustive:ignore // Only the two flow-control blocks have a frame to
	// announce. blockNone is not blocked; blockCong and blockPace are local
	// sender state (congestion window, pacer) that the peer must not be told
	// about — there is no MAX_* frame it could send to relieve them.
	switch kind {
	case blockStream:
		if !s.sdBlockedSet || s.sdBlockedLimit != s.sendMax {
			s.sdBlockedSet = true
			s.sdBlockedLimit = s.sendMax
			_ = s.conn.writeAppFrames(appendStreamDataBlocked(nil, s.id, s.sendMax), nil)
		}
	case blockConn:
		if !s.conn.dataBlockedSet || s.conn.dataBlockedLimit != s.conn.connMax {
			s.conn.dataBlockedSet = true
			s.conn.dataBlockedLimit = s.conn.connMax
			_ = s.conn.writeAppFrames(appendDataBlocked(nil, s.conn.connMax), nil)
		}
	}
}

// emitStreamsBlocked tells the peer that its cumulative stream limit is refusing a
// new stream (RFC 9000 §4.6, §19.14): "A sender SHOULD send a STREAMS_BLOCKED
// frame (type=0x16 or 0x17) when it wishes to open a stream but is unable to do so
// due to the maximum stream limit set by its peer". Without it a server
// that under-grants gets no signal that we are stalled, so it has no trigger to
// send MAX_STREAMS. Informational, latched per distinct
// limit like emitBlocked. Best-effort: before 1-RTT keys exist the seal fails and
// the frame is dropped, which is fine — the limit is re-signalled on the next
// refusal at a new limit value. Assumes c.mu is held.
// streamsBlockedNow reports the peer's current cumulative stream limit for the
// bidirectional or unidirectional scope, and whether we are still blocked on it —
// the two things RFC 9000 §13.3 requires a re-sent STREAMS_BLOCKED to reflect at
// transmit time rather than at the time the lost frame was first built. Assumes
// c.mu is held.
func (c *Conn) streamsBlockedNow(uni bool) (limit uint64, blocked bool) {
	if uni {
		return c.peer.InitialMaxStreamsUni, c.openedUni >= c.peer.InitialMaxStreamsUni
	}
	return c.peer.InitialMaxStreamsBidi, c.openedBidi >= c.peer.InitialMaxStreamsBidi
}

func (c *Conn) emitStreamsBlocked(uni bool, limit uint64) {
	if c.oneRTTSealer == nil || c.closed {
		// No 1-RTT keys yet (or shutting down): there is nothing to send the frame
		// in. Leave the latch clear so the next refusal at this limit signals.
		return
	}
	i := 0
	if uni {
		i = 1
	}
	if c.streamsBlockedSet[i] && c.streamsBlockedLimit[i] == limit {
		return
	}
	// Latch only on a successful write. A stream-limit stall has no other liveness
	// trigger — OpenStreamContext parks on c.streamCredit, whose only wake source is
	// the peer's MAX_STREAMS, which the peer only sends because it saw this frame —
	// so a dropped datagram plus an eager latch turns a transient stall into a wait
	// until the caller's context expires. Re-emitting on the next refusal cannot
	// spin: the opener re-parks after each one. The frame is also retransmitted on
	// loss (RFC 9000 §13.3) — a dropped datagram is not a write error, so the latch
	// alone would not cover it.
	retrans := &retransFrame{kind: retransStreamsBlocked, offset: limit, fin: uni}
	if err := c.writeAppFrames(appendStreamsBlocked(nil, uni, limit), retrans); err != nil {
		return
	}
	c.streamsBlockedSet[i] = true
	c.streamsBlockedLimit[i] = limit
}

// writeAppFrames seals frames into a 1-RTT packet and writes the datagram. The
// STREAM and *_BLOCKED frames it carries are ack-eliciting; retrans names the
// frames to resend if the packet is lost — nil for DATA_BLOCKED and
// STREAM_DATA_BLOCKED, but not for STREAMS_BLOCKED, which RFC 9000 §13.3 has us
// re-send because nothing else can unstick a stream-limit stall. Assumes c.mu is held (it
// seals a packet and writes the wire; callers are sendLocked/resetLocked/
// stopSendingLocked and the receive-path emitBlocked).
func (c *Conn) writeAppFrames(frames []byte, retrans *retransFrame) error {
	if c.closed {
		// Draining or closing (RFC 9000 §10.2.2): no application frame may be sent
		// once the connection is closed, including by a received CONNECTION_CLOSE.
		return ErrConnClosed
	}
	pkt, err := c.sealPacket(spaceApp, frames, true, retrans, false)
	if err != nil {
		return err
	}
	if _, err := c.pc.Write(pkt); err != nil {
		return err
	}
	// A Do-side send put an ack-eliciting packet in flight, shortening the
	// loss-detection deadline below what the parked reader armed; shorten the read
	// deadline so the reader wakes to run PTO/loss detection (docs/HTTP3_DESIGN.md
	// §4 INV-4). A no-op when no reader has parked (armedReadDeadline zero).
	c.rearmReadDeadline()
	return nil
}

// sealAppToBatch is writeAppFrames' batched counterpart (quic/gso.go): it seals an
// ack-eliciting 1-RTT packet exactly as writeAppFrames does — same congestion,
// pacing, AEAD, and sentPacket accounting inside sealPacket — but appends the
// sealed datagram to the GSO batch instead of writing it, so a multi-datagram burst
// leaves in one syscall. The rearmReadDeadline epilogue happens once, when the
// batch is flushed. Assumes c.mu is held.
func (c *Conn) sealAppToBatch(b *packetBatch, frames []byte, retrans *retransFrame) error {
	if c.closed {
		// Draining or closing (RFC 9000 §10.2.2): no application frame may be sent
		// once the connection is closed, including by a received CONNECTION_CLOSE.
		return ErrConnClosed
	}
	pkt, err := c.sealPacket(spaceApp, frames, true, retrans, false)
	if err != nil {
		return err
	}
	return c.addToBatch(b, pkt)
}
