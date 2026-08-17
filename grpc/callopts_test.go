//go:build !race

package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// callOptions is resolved on every RPC, and handing its address to an interface
// method puts it on the heap. Keeping it off costs one allocation per call, and
// the arrangement that achieves it is fragile in a way a reader would not guess:
// the helper must not be inlined, and the caller must skip it entirely when
// there is nothing to apply. Either detail lost and the allocation returns
// silently.
//
// Behind !race for the same reason as the other allocation gates: the detector
// allocates as it instruments, and this turns on a single allocation. The same
// rule applies as in allocgates_test.go — no testify call inside a measured
// closure, since AllocsPerRun counts the whole process and the assertion library
// reflects and allocates.

// TestCallOptions_NoOptionsDoesNotAllocate is the gate, and it measures the REAL
// call path.
//
// The first version of this test rebuilt the caller's shape inside itself —
// declare a callOptions, guard the helper, assign — and measured that. It passed
// while both mutations that break the real thing (removing the noinline
// directive, dropping the caller's len(opts) guard) went undetected, because it
// was measuring its own replica and nothing else. A gate on a copy of the code
// is not a gate.
//
// An absolute ceiling on the real path is what catches the caller's guard:
// removing it puts the allocation back and the count rises past the ceiling
// (verified by mutation under #728 — the gate fails 2/2). The noinline directive
// is the other half of the arrangement and this gate does NOT observe it on its
// own: removing it alone leaves the count unchanged at 2.0, so it survives the
// mutation. Kept anyway, because the two are cheap belt-and-braces and the pair
// is what conn.go's comment records; do not read this test as evidence that
// either edit alone is caught.
func TestCallOptions_NoOptionsDoesNotAllocate(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()
	req := []byte("q")
	var sink []byte
	// Warm the connection so the count reflects steady state.
	_, warmErr := cc.Invoke(ctx, "/bench.Svc/Echo", req, nil)
	require.NoError(t, warmErr, "warm-up")

	n := testing.AllocsPerRun(50, func() {
		var err error
		if sink, err = cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
			t.Fatalf("Invoke: %v", err)
		}
	})
	_ = sink

	// Same constant as the other absolute gate: one number for one measurement.
	t.Logf("Invoke with no call options: %.1f allocs", n)
	assert.LessOrEqualf(t, n, float64(unaryAllocCeiling),
		"Invoke with NO options allocates %.1f per call, ceiling %d — the "+
			"callOptions struct is on the heap again: either applyCallOptions was "+
			"inlined back into its caller, or the caller stopped skipping the call",
		n, unaryAllocCeiling)
	assert.GreaterOrEqualf(t, n, float64(unaryAllocCeiling),
		"Invoke allocates only %.1f, below the recorded %d — the path improved, "+
			"lower unaryAllocCeiling to lock it in", n, unaryAllocCeiling)
}

// TestCallOptions_AreStillApplied is the correctness half: the shape exists to
// remove an allocation, and an option that stopped taking effect would remove it
// far more thoroughly.
func TestCallOptions_AreStillApplied(t *testing.T) {
	md := []conn.HeaderField{{Name: []byte("x-a"), Value: []byte("1")}}
	base := callOptions{maxRecvMessageSize: 111}

	co := applyCallOptions(base, []CallOption{MaxRecvMessageSize(222), WithMetadata(md)})
	_ = applyCallOptions(base, []CallOption{MaxRecvMessageSize(999)})

	assert.Equalf(t, 222, co.maxRecvMessageSize, "maxRecvMessageSize = %d, want 222",
		co.maxRecvMessageSize)
	require.Lenf(t, co.md, 1, "md = %+v, want the one field passed", co.md)
	assert.Equalf(t, "x-a", string(co.md[0].Name), "md = %+v, want the one field passed", co.md)
	// The input value is not mutated: it is taken by value, so a caller reusing
	// its own struct is unaffected.
	assert.Equalf(t, 111, base.maxRecvMessageSize,
		"the caller's own value was mutated: %d, want 111", base.maxRecvMessageSize)
}

// TestCallOptions_EndToEndStillHonoured drives a real call, so the gate cannot
// pass on a helper nothing calls any more.
func TestCallOptions_EndToEndStillHonoured(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)

	s, err := cc.NewStream(t.Context(), "/bench.Svc/Echo", nil, MaxRecvMessageSize(4242))

	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	assert.Equalf(t, 4242, s.dec.max,
		"the call option did not reach the stream: dec.max = %d, want 4242", s.dec.max)
}
