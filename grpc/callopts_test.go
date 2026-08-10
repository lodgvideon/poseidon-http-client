//go:build !race

package grpc

import (
	"testing"

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
// allocates as it instruments, and this turns on a single allocation.

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
// An absolute ceiling on the real path is what catches them: either mutation
// puts the allocation back and the count goes to 10.
func TestCallOptions_NoOptionsDoesNotAllocate(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()
	req := []byte("q")
	var sink []byte

	// Warm the connection so the count reflects steady state.
	if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	n := testing.AllocsPerRun(50, func() {
		var err error
		if sink, err = cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
			t.Fatalf("Invoke: %v", err)
		}
	})
	_ = sink

	// Same constant as the other absolute gate: one number for one measurement.
	t.Logf("Invoke with no call options: %.1f allocs", n)
	if n > unaryAllocCeiling {
		t.Errorf("Invoke with NO options allocates %.1f per call, ceiling %d — the "+
			"callOptions struct is on the heap again: either applyCallOptions was "+
			"inlined back into its caller, or the caller stopped skipping the call",
			n, unaryAllocCeiling)
	}
	if n < unaryAllocCeiling {
		t.Errorf("Invoke allocates only %.1f, below the recorded %d — the path improved, "+
			"lower unaryAllocCeiling to lock it in", n, unaryAllocCeiling)
	}
}

// TestCallOptions_AreStillApplied is the correctness half: the shape exists to
// remove an allocation, and an option that stopped taking effect would remove it
// far more thoroughly.
func TestCallOptions_AreStillApplied(t *testing.T) {
	md := []conn.HeaderField{{Name: []byte("x-a"), Value: []byte("1")}}
	co := applyCallOptions(callOptions{maxRecvMessageSize: 111},
		[]CallOption{MaxRecvMessageSize(222), WithMetadata(md)})

	if co.maxRecvMessageSize != 222 {
		t.Errorf("maxRecvMessageSize = %d, want 222", co.maxRecvMessageSize)
	}
	if len(co.md) != 1 || string(co.md[0].Name) != "x-a" {
		t.Errorf("md = %+v, want the one field passed", co.md)
	}
	// The input value is not mutated: it is taken by value, so a caller reusing
	// its own struct is unaffected.
	base := callOptions{maxRecvMessageSize: 111}
	_ = applyCallOptions(base, []CallOption{MaxRecvMessageSize(999)})
	if base.maxRecvMessageSize != 111 {
		t.Errorf("the caller's own value was mutated: %d, want 111", base.maxRecvMessageSize)
	}
}

// TestCallOptions_EndToEndStillHonoured drives a real call, so the gate cannot
// pass on a helper nothing calls any more.
func TestCallOptions_EndToEndStillHonoured(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(t.Context(), "/bench.Svc/Echo", nil, MaxRecvMessageSize(4242))
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if s.dec.max != 4242 {
		t.Errorf("the call option did not reach the stream: dec.max = %d, want 4242", s.dec.max)
	}
}
