package quic

// retransFreeMax bounds the recycled-buffer list. A connection needs about a
// congestion window's worth of live retransmit copies at once; past that the
// extra buffers are memory held for nothing, so the surplus goes to the garbage
// collector instead. 64 x maxDatagramSize is ~77 KiB per connection.
const retransFreeMax = 64

// Recycling the RFC 9000 §13.3 retransmit copies.
//
// Every STREAM chunk is copied out of the reusable send scratch and retained so
// the frame can be re-sent at its original offset if the packet is lost. That
// copy was the last per-datagram heap allocation on the send path.
//
// Recycling it is sound only because ownership of a retransFrame's payload is
// LINEAR: it is reachable from exactly one of a sentPacket or the retransmit
// queue, never both. Every exit from the sent-packet map either hands the frame
// to the queue and deletes the packet in the same step — detectLost,
// queueOldestProbe, requeueInitialForRetry — or is an acknowledgement, which
// hands it to nobody. The queue drain then seals the same slice into a fresh
// sentPacket, so the payload cycles between the two owners until an ACK ends it.
//
// That makes sentSpace.ack the one and only moment a payload is dead, and the
// one and only place retransPut is called. Releasing anywhere else — at
// detectLost, say, where the packet also leaves the map — would hand back a
// buffer the retransmit queue still points at, and the next send would overwrite
// it. The lost packet would then be retransmitted at its original offset
// carrying another frame's bytes, which the peer accepts as valid data. Silent
// wire corruption, no error anywhere. TestRetransBuf_* pins the rule directly.
//
// Both ends run under c.mu — "mu guards ALL Conn mutable state and the wire" —
// so the free list needs no synchronisation of its own. A sync.Pool would also
// box the slice header on every Put, which is the allocation this is removing.

// retransCopy returns a copy of src to retain for retransmission, drawn from the
// connection's free list when one fits. The result always has its own backing
// array: src is the reused frame scratch and is overwritten by the next frame.
//
// The fallback allocates with make at an EXACT capacity rather than letting
// append pick one, and that is the whole of #475's item 2.
//
// append([]byte(nil), src...) for a full 1200-byte datagram returns a slice whose
// cap is 1280 — the allocator's size class, not the length asked for. retransPut
// then refuses it, because its guard is cap(b) > maxDatagramSize and 1280 > 1200.
// So a full-datagram copy was NEVER recycled: the free list could only ever hold
// buffers from frames short enough that their rounded capacity stayed under a
// datagram, while the send path of a bulk transfer produces almost nothing else.
// Measured: after one full-datagram copy and put, the free list held 0 entries.
// That is the 67,796 objects (10.21% of the HTTP/3 arm) the issue attributes to
// this line.
//
// make gives back exactly the capacity requested, so the buffer passes the guard
// and is recycled. It also makes every entry the same size, which keeps the
// top-of-stack check enough — no entry can be too short for a later request.
//
// The underlying heap object is the same 1280-byte size class either way; only
// the reported cap differs, and it is the reported cap the guard reads.
func (c *Conn) retransCopy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if n := len(c.retransFree) - 1; n >= 0 && cap(c.retransFree[n]) >= len(src) {
		b := c.retransFree[n]
		c.retransFree[n] = nil // drop the list's reference so the buffer has one owner
		c.retransFree = c.retransFree[:n]
		return append(b[:0], src...)
	}
	if len(src) <= maxDatagramSize {
		return append(make([]byte, 0, maxDatagramSize), src...)
	}
	// A CRYPTO flight. retransPut drops these whatever their capacity, so there
	// is nothing to be gained by rounding one up — and plenty to lose: keeping a
	// multi-KiB buffer alive is what that guard exists to prevent.
	return append([]byte(nil), src...)
}

// retransPut returns a payload whose packet has been acknowledged to the free
// list. Buffers larger than a datagram are dropped rather than recycled: those
// come from CRYPTO flights, which happen a handful of times per connection, and
// keeping one alive would pin far more memory than it ever saves.
//
// MUST be called only from sentSpace.ack. See the ownership note above for what
// calling it anywhere else costs.
func (c *Conn) retransPut(b []byte) {
	if cap(b) == 0 || cap(b) > maxDatagramSize || len(c.retransFree) >= retransFreeMax {
		return
	}
	c.retransFree = append(c.retransFree, b[:0])
}
