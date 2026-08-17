//go:build !race

package quic

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildACK's ACK Range section started from a nil slice on every ACK, so a
// connection allocated once per ACK sent — 65,153 objects, 9.81% of the HTTP/3
// arm (#475). The section's length is bounded by the tracker's own ranges, which
// already live on the connection, so the scratch can too.
//
// Reusing scratch buys a hazard in exchange: the second call must not be able to
// emit what the first one left behind. Both tests below exist for that, not for
// the allocation.
//
// Behind !race for the reason the other allocation gates are: the detector
// allocates as it instruments. For the same reason no testify assertion may
// appear INSIDE the measured closure: require/assert reflect over their
// arguments and box them into an interface slice, and AllocsPerRun counts the
// whole process, so one call in there would be charged to every iteration and
// the gate would be measuring the assertion library instead of buildACK. Every
// assertion below sits outside the measurement.

// ackTrackerWithGaps returns a tracker holding n disjoint ranges, so buildACK has
// n-1 extra ACK ranges to encode. Descending because ranges[0] is the largest.
func ackTrackerWithGaps(n int) *ackTracker {
	a := &ackTracker{}
	pn := uint64(n) * 10
	for i := 0; i < n; i++ {
		a.ranges = append(a.ranges, pnRange{lo: pn, hi: pn + 1})
		pn -= 10
	}
	return a
}

// TestBuildACK_NoAllocPerACK is the gate.
func TestBuildACK_NoAllocPerACK(t *testing.T) {
	a := ackTrackerWithGaps(8)
	dst := make([]byte, 0, 256)

	n := testing.AllocsPerRun(200, func() {
		_ = a.buildACK(dst[:0], 0)
	})

	assert.Zerof(t, n, "buildACK allocates %.1f per ACK, want 0 — the ACK Range scratch is "+
		"being rebuilt from nil on every call", n)
}

// TestBuildACK_ShorterACKDoesNotCarryStaleRanges is the hazard the scratch
// introduces, and the reason the gate above is not the only test: a reused slice
// truncated with [:0] must not let a later, shorter ACK emit the ranges of an
// earlier, longer one.
//
// It compares against a tracker that has only ever built the short ACK, so the
// expected bytes come from the same code rather than from a hand-encoded
// constant that could be wrong in the same way.
func TestBuildACK_ShorterACKDoesNotCarryStaleRanges(t *testing.T) {
	warm := ackTrackerWithGaps(8)
	_ = warm.buildACK(nil, 0) // fill the scratch with 7 extra ranges
	fresh := ackTrackerWithGaps(8)

	// Now the same tracker reports far fewer ranges.
	warm.ranges = warm.ranges[:2]
	got := warm.buildACK(nil, 0)
	fresh.ranges = fresh.ranges[:2]
	want := fresh.buildACK(nil, 0)

	assert.Truef(t, bytes.Equal(got, want),
		"an ACK built after a longer one differs from the same ACK built fresh:\n"+
			" got %x\nwant %x\nthe reused scratch leaked the previous ACK's ranges", got, want)
}

// TestBuildACK_ScratchIsPerTracker pins that the scratch belongs to the tracker
// and not to the package.
//
// It compares the backing arrays rather than the encoded bytes, which is the
// correction that matters: an earlier version of this test built an ACK on one
// space, then another, then re-built the first and compared output — and a
// mutation that moved the scratch to a package-level variable **passed it**.
// Truncating to [:0] on entry makes a shared slice produce identical output in
// any single-threaded sequence, so output can never witness the sharing. The
// aliasing itself can.
//
// Sharing would still be a live hazard: buildACK runs under c.mu today, and a
// package-level scratch would make two connections — not just two spaces —
// scribble over each other the moment that stopped being true.
func TestBuildACK_ScratchIsPerTracker(t *testing.T) {
	a := ackTrackerWithGaps(6)
	b := ackTrackerWithGaps(3)

	_ = a.buildACK(nil, 0)
	_ = b.buildACK(nil, 0)

	require.Falsef(t, len(a.extra) == 0 || len(b.extra) == 0,
		"scratch not populated (a=%d b=%d); the test cannot observe sharing",
		len(a.extra), len(b.extra))
	assert.NotSame(t, &a.extra[0], &b.extra[0],
		"two trackers' ACK Range scratches share a backing array — the scratch "+
			"is package-level, so every connection and every packet-number space writes "+
			"into the same slice")
}
