//go:build !race

package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The retransmit free list has the right mechanism and missed almost every time:
// #475 measured retransCopy at 67,796 objects, 10.21% of the HTTP/3 arm's
// allocations, on a path whose whole purpose is to allocate nothing.
//
// The cause is a capacity the code never asked for. append([]byte(nil), src...)
// on a full 1200-byte datagram returns cap 1280 — the allocator's size class —
// and retransPut refuses anything with cap > maxDatagramSize. So a full-datagram
// copy was never recycled at all, and a bulk send path produces almost nothing
// else. The list could only ever hold buffers from frames short enough that
// their rounded capacity stayed under a datagram.
//
// Behind !race for the reason the other allocation gates are: the detector
// allocates as it instruments. For the same reason no testify call may appear
// inside an AllocsPerRun closure — it reflects and allocates, and the gate counts
// the whole process. Every assertion below sits outside the measured body.

// TestRetransPut_KeepsAFullDatagramCopy is the gate on the defect itself, and the
// one that fails loudest: a full-size copy must reach the free list.
//
// It asserts on the list rather than on an allocation count, because the count
// only moves once a later request can be served — this is the step before that,
// and stating it separately is what makes the cause legible rather than a
// statistic.
func TestRetransPut_KeepsAFullDatagramCopy(t *testing.T) {
	c := &Conn{}

	c.retransPut(c.retransCopy(make([]byte, maxDatagramSize)))

	assert.Lenf(t, c.retransFree, 1,
		"free list has %d entries after one full-datagram copy, want 1 — the "+
			"copy's capacity was rounded up to a size class above maxDatagramSize, so "+
			"retransPut dropped exactly the buffer a bulk transfer produces most",
		len(c.retransFree))
}

// TestRetransCopy_SmallBufferOnTopDoesNotBlockReuse is the gate. It alternates a
// short frame with a datagram-sized one, which is what a real send path does, and
// requires the free list to serve both.
//
// The alternation is the whole test: a loop of equal-sized copies hits the top
// entry every time and passes even with the bug, which is why this is not covered
// by the existing TestRetransBuf_* tests.
func TestRetransCopy_SmallBufferOnTopDoesNotBlockReuse(t *testing.T) {
	c := &Conn{}
	small := make([]byte, 40)
	large := make([]byte, maxDatagramSize)
	// Warm the list so steady state is measured, not the first fill.
	for i := 0; i < 8; i++ {
		c.retransPut(c.retransCopy(large))
		c.retransPut(c.retransCopy(small))
	}

	n := testing.AllocsPerRun(200, func() {
		// A short frame is acknowledged and lands on top of the list...
		c.retransPut(c.retransCopy(small))
		// ...and the next full-sized chunk must still be served from it.
		c.retransPut(c.retransCopy(large))
	})

	assert.Zerof(t, n,
		"retransCopy allocates %.1f per small+large pair, want 0 — a buffer "+
			"too short for the request is on top of the free list, so the large copy "+
			"takes the allocating fallback while datagram-sized buffers sit underneath",
		n)
}

// TestRetransCopy_ReusesTheBufferItWasGiven is the control: serving from the free
// list must actually recycle storage, not merely avoid the counter. It checks
// that the returned slice shares the backing array of the buffer just released.
//
// Without this, satisfying the gate by allocating a fresh datagram-sized buffer
// every time and never consulting the list would look like a fix.
func TestRetransCopy_ReusesTheBufferItWasGiven(t *testing.T) {
	c := &Conn{}
	first := c.retransCopy(make([]byte, 100))
	want := &first[:cap(first)][0]
	c.retransPut(first)

	second := c.retransCopy(make([]byte, 100))

	assert.Same(t, want, &second[:cap(second)][0],
		"retransCopy did not reuse the buffer just returned to the free list")
}

// TestRetransCopy_OversizeStillGetsItsOwnBuffer guards the other direction: a
// CRYPTO flight larger than a datagram must still be copied correctly, and
// retransPut must still refuse to keep it (the free list is bounded in bytes by
// assuming datagram-sized entries).
func TestRetransCopy_OversizeStillGetsItsOwnBuffer(t *testing.T) {
	c := &Conn{}
	src := make([]byte, maxDatagramSize*3)
	for i := range src {
		src[i] = byte(i)
	}

	got := c.retransCopy(src)
	c.retransPut(got)

	require.Lenf(t, got, len(src), "len = %d, want %d", len(got), len(src))
	assert.Equal(t, src, got, "an oversize payload must be copied byte for byte")
	assert.Emptyf(t, c.retransFree,
		"an oversize buffer was kept: free list has %d entries, want 0 — "+
			"the list's memory bound assumes every entry is at most a datagram",
		len(c.retransFree))
}
