package quic

import "io"

// UDP generic segmentation offload (GSO) lets one sendmsg hand the kernel a buffer
// of several equal-size QUIC datagrams that the NIC (or the kernel) slices back
// into individual UDP packets — turning N sendto syscalls into one. The syscall
// itself is Linux-only and lives in gso_linux.go / gso_other.go; this file holds
// the platform-independent batching that accumulates same-size sealed packets and
// decides when to hand them off.
//
// GSO's wire result is byte-identical to N separate sends: the batch buffer is a
// contiguous concatenation of already-sealed datagrams, and every datagram still
// carries its own packet number and sentPacket record (sealPacket ran per packet,
// under c.mu — see conn_seal.go). Batching only changes how many syscalls carry
// them, never their contents, so the #197 seal-scratch reuse and the retransmit
// accounting are untouched: each packet is copied out of c.sealScratch into the
// batch immediately after sealing, before the next seal overwrites the scratch.

const (
	// maxGSOSegments caps the datagrams in one UDP_SEGMENT super-buffer. The Linux
	// kernel accepts at most 64 segments per GSO sendmsg (UDP_MAX_SEGMENTS).
	maxGSOSegments = 64
	// maxGSOBytes caps the super-buffer size. A GSO buffer must fit the 16-bit
	// lengths the kernel path uses, so 64 KiB - 1 is the conservative ceiling.
	maxGSOBytes = 65535
)

// gsoWriter is the OPTIONAL batched-write capability a PacketConn may implement on
// top of the required Write/Read/Close. When c.pc satisfies it, the send path
// hands a whole burst of equal-size datagrams to WriteGSO for one syscall; a
// PacketConn without it falls back to a per-datagram Write loop (writeSegmentsTo),
// so the interface assertion is the only coupling — the transport stays a plain
// PacketConn. WriteGSO sends buf as consecutive datagrams of segSize bytes, the
// last of which may be shorter, and MUST degrade gracefully (per-datagram) if the
// platform or path cannot offload.
type gsoWriter interface {
	WriteGSO(buf []byte, segSize int) (int, error)
}

// packetBatch accumulates freshly-sealed, equal-size app-space datagrams destined
// for one GSO write. buf is the contiguous concatenation (borrowed from and
// returned to c.gsoBatch so bursts do not re-allocate); segSize is the exact byte
// length every full segment must share; n counts the segments. A zero segSize/n is
// the empty batch. GSO requires all-but-the-last segment to equal segSize, so a
// shorter datagram may join only as the final segment (then forces a flush) and a
// differently-sized datagram starts a fresh batch.
type packetBatch struct {
	buf     []byte
	segSize int
	n       int
}

// newBatch starts an empty batch reusing the Conn's retained backing array, so a
// steady stream of sends does not allocate a fresh batch buffer each time. Assumes
// c.mu is held (the send path's discipline).
func (c *Conn) newBatch() packetBatch {
	return packetBatch{buf: c.gsoBatch[:0]}
}

// addToBatch appends one sealed datagram pkt to b, flushing first when pkt cannot
// share b's GSO buffer: a datagram equal to the segment size joins (until the
// segment/byte caps are hit); a shorter one joins as the final segment and forces
// a flush; a larger one, or one that would exceed a cap, flushes the batch and
// starts a new one sized to pkt. pkt is COPIED (append), so the caller's seal
// scratch is free to be reused immediately. Assumes c.mu is held.
func (c *Conn) addToBatch(b *packetBatch, pkt []byte) error {
	if b.n == 0 {
		b.segSize = len(pkt)
		b.buf = append(b.buf, pkt...)
		b.n++
		return nil
	}
	fits := b.n < maxGSOSegments && len(b.buf)+len(pkt) <= maxGSOBytes
	switch {
	case len(pkt) == b.segSize && fits:
		b.buf = append(b.buf, pkt...)
		b.n++
		return nil
	case len(pkt) < b.segSize && fits:
		// A datagram shorter than the segment size is only valid as the LAST GSO
		// segment (all others must equal segSize), so append it and flush now.
		b.buf = append(b.buf, pkt...)
		b.n++
		return c.flushBatch(b)
	default:
		// Larger than the segment size, or a cap reached: flush what we have and open
		// a fresh batch whose segment size is this datagram's.
		if err := c.flushBatch(b); err != nil {
			return err
		}
		b.segSize = len(pkt)
		b.buf = append(b.buf, pkt...)
		b.n++
		return nil
	}
}

// flushBatch writes the accumulated datagrams and resets b for reuse, retaining
// the (possibly grown) backing array on the Conn. A batch of one is a single
// Write; two or more go out as one WriteGSO when the PacketConn supports it, else
// a per-datagram Write loop — so a non-GSO transport behaves exactly as before.
// On a successful write it shortens the parked reader's deadline (the burst put
// ack-eliciting packets in flight; docs/HTTP3_DESIGN.md §4). Assumes c.mu is held.
func (c *Conn) flushBatch(b *packetBatch) error {
	if b.n == 0 {
		return nil
	}
	buf, seg, n := b.buf, b.segSize, b.n
	c.gsoBatch = buf[:0] // retain the grown backing array for the next burst
	b.buf, b.segSize, b.n = c.gsoBatch, 0, 0

	var err error
	if n == 1 {
		// A lone datagram gains nothing from GSO — a plain Write avoids the cmsg.
		_, err = c.pc.Write(buf)
	} else if gw, ok := c.pc.(gsoWriter); ok {
		_, err = gw.WriteGSO(buf, seg)
	} else {
		_, err = writeSegmentsTo(c.pc, buf, seg)
	}
	if err != nil {
		return err
	}
	c.rearmReadDeadline()
	return nil
}

// writeSegmentsTo writes buf to w as consecutive datagrams of segSize bytes, the
// last of which may be shorter — the per-datagram fallback used when the transport
// has no batched-write support or when a platform / path cannot offload GSO. It
// reproduces the pre-GSO wire behavior exactly (one Write per datagram). Returns
// the total bytes written.
func writeSegmentsTo(w io.Writer, buf []byte, segSize int) (int, error) {
	if segSize <= 0 { // defensive: a zero stride must not spin
		return w.Write(buf)
	}
	total := 0
	for off := 0; off < len(buf); off += segSize {
		end := off + segSize
		if end > len(buf) {
			end = len(buf)
		}
		nn, err := w.Write(buf[off:end])
		total += nn
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
