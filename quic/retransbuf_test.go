package quic

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetransBuf_AckRecycles pins the release rule's happy path: a payload whose
// packet is acknowledged goes back to the free list, and the next copy comes out
// of it instead of the heap.
func TestRetransBuf_AckRecycles(t *testing.T) {
	base := time.Unix(900, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "payload"))
	require.Emptyf(t, c.retransFree,
		"setup: free list holds %d buffers, want 0", len(c.retransFree))

	c.sent[spaceApp].ack(c, 0, 0)
	freeAfterAck := len(c.retransFree)
	got := c.retransCopy([]byte("next")) // the next copy must reuse it rather than allocate

	require.Equalf(t, 1, freeAfterAck,
		"after ACK the free list holds %d buffers, want 1", freeAfterAck)
	assert.Emptyf(t, c.retransFree,
		"retransCopy left %d buffers on the free list, want 0 — it did not reuse", len(c.retransFree))
	assert.Equalf(t, "next", string(got), "recycled buffer holds %q, want %q", got, "next")
}

// TestRetransBuf_LossDoesNotRecycle is the rule that matters. A lost packet also
// leaves the sent map, but it hands its payload to the retransmit queue on the
// way out — releasing there would give back a buffer the queue still points at.
func TestRetransBuf_LossDoesNotRecycle(t *testing.T) {
	base := time.Unix(910, 0)
	c := &Conn{now: func() time.Time { return base }}
	for pn := uint64(0); pn <= 3; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, streamFrame(0, pn, "lost-payload"))
	}

	// Acknowledging pn 3 releases exactly one buffer: pn 3's own.
	c.sent[spaceApp].ack(c, 3, 3)
	freeAfterAck := len(c.retransFree)
	c.detectLost(spaceApp) // pn 0 crosses the packet threshold and is queued

	require.Equalf(t, 1, freeAfterAck, "ACK of one packet freed %d buffers, want 1", freeAfterAck)
	require.Lenf(t, c.retransQueue[spaceApp], 1,
		"expected pn 0 queued for retransmit, queue = %d", len(c.retransQueue[spaceApp]))
	assert.Equalf(t, freeAfterAck, len(c.retransFree),
		"loss detection changed the free list from %d to %d buffers: a queued "+
			"payload was released while the retransmit queue still owns it",
		freeAfterAck, len(c.retransFree))
}

// TestRetransBuf_QueuedPayloadSurvivesRecycling is the corruption test, and the
// reason the release rule is written down rather than assumed.
//
// It arranges the exact interleaving that a wrong rule breaks: one packet is
// lost and its payload queued, a *different* packet is acknowledged and its
// payload freed, and then fresh sends draw from the free list. If the queued
// payload had been released too, those sends would overwrite it, and the
// retransmission would carry another frame's bytes at the lost frame's offset —
// which the peer accepts as valid data, with no error anywhere.
func TestRetransBuf_QueuedPayloadSurvivesRecycling(t *testing.T) {
	base := time.Unix(920, 0)
	c := &Conn{now: func() time.Time { return base }}
	const lostPayload = "the-bytes-that-must-survive"
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, lostPayload))
	for pn := uint64(1); pn <= 3; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, streamFrame(0, pn, "other-packet-payload"))
	}

	c.sent[spaceApp].ack(c, 1, 3) // frees three payloads
	c.detectLost(spaceApp)        // queues pn 0's payload
	queued := c.retransQueue[spaceApp]
	// Now churn the free list hard: every recycled buffer gets overwritten.
	for i := 0; i < 16; i++ {
		c.retransCopy(bytes.Repeat([]byte{'Z'}, len(lostPayload)))
	}

	require.Lenf(t, queued, 1, "expected 1 queued frame, got %d", len(queued))
	assert.Equalf(t, lostPayload, string(queued[0].data),
		"queued retransmit payload is now %q, want %q — a buffer the "+
			"retransmit queue owns was recycled and overwritten", queued[0].data, lostPayload)
}

// TestRetransBuf_OversizeNotRecycled pins that a CRYPTO-sized payload is dropped
// rather than parked on the free list. Those happen a handful of times per
// connection and keeping one alive pins far more than it saves.
func TestRetransBuf_OversizeNotRecycled(t *testing.T) {
	base := time.Unix(930, 0)
	c := &Conn{now: func() time.Time { return base }}
	big := bytes.Repeat([]byte{'x'}, maxDatagramSize+1)
	c.sent[spaceApp].onSent(0, base, true, &retransFrame{kind: retransCrypto, data: big})

	c.sent[spaceApp].ack(c, 0, 0)

	assert.Emptyf(t, c.retransFree,
		"an oversize payload was parked on the free list (%d buffers)", len(c.retransFree))
}

// TestRetransBuf_FreeListIsBounded pins that a connection acknowledging a long
// burst does not park an unbounded number of buffers.
func TestRetransBuf_FreeListIsBounded(t *testing.T) {
	base := time.Unix(940, 0)
	c := &Conn{now: func() time.Time { return base }}
	const sent = retransFreeMax * 3
	for pn := uint64(0); pn < sent; pn++ {
		c.sent[spaceApp].onSent(pn, base, true, streamFrame(0, pn, "payload"))
	}

	c.sent[spaceApp].ack(c, 0, sent-1)

	assert.LessOrEqualf(t, len(c.retransFree), retransFreeMax,
		"free list holds %d buffers after %d ACKs, want at most %d",
		len(c.retransFree), sent, retransFreeMax)
}

// TestRetransBuf_NilConnDoesNotPanic pins the hand-built-Conn tolerance: a
// sentSpace driven without a connection simply does not recycle.
func TestRetransBuf_NilConnDoesNotPanic(t *testing.T) {
	base := time.Unix(950, 0)
	var s sentSpace
	s.onSent(0, base, true, streamFrame(0, 0, "payload"))

	s.ack(nil, 0, 0)

	assert.NotContains(t, s.packets, uint64(0), "ack with a nil Conn did not remove the packet")
}
