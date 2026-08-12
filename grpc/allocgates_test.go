//go:build !race

package grpc

// Allocation gates live behind !race on purpose.
//
// The race detector allocates as it instruments, so testing.AllocsPerRun under
// -race measures the instrumentation as much as the code. Both gates below turn
// on a difference of a single allocation, which that noise swamps outright:
// TestInvokeInto_AllocsPerCall failed four runs out of five under -race while
// passing every run without it. A gate that reports the detector rather than the
// change is worse than no gate, so these run in the ordinary suite — which CI
// also runs — and sit out the instrumented one.

import (
	"bytes"
	"context"
	"testing"
)

// unaryAllocCeiling is what one Invoke costs, and it is shared by every gate
// that asserts on that number rather than restated per test. Two constants for
// one measurement drift: DiscardMetadata and the callOptions escape fix landed
// on separate branches, each lowered its own copy, and the pair reached main
// reading 6 and 9 for a path that by then allocated 5.
//
// Both directions are errors. Above it, a per-RPC allocation came back; below
// it, the path improved and the win is not locked in until this drops.
// Lowered 5 -> 4 when conn stopped copying :authority into a string on every
// request (#578). Nothing in grpc changed: the RPC path sits on conn, so a
// conn-level allocation is a grpc-level one, and this gate is why that showed up
// as a build failure instead of going unnoticed. The lesson is in which
// direction the check has to run — changing a package means running the
// allocation gates of everything BUILT ON it, not only its own.
const unaryAllocCeiling = 4

// TestInvokeInto_AllocsPerCall gates the win against the allocating form on the
// same connection.
func TestInvokeInto_AllocsPerCall(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := bytes.Repeat([]byte{'q'}, 256)

	buf := make([]byte, 0, 256)
	withReuse := testing.AllocsPerRun(50, func() {
		var err error
		if buf, err = cc.InvokeInto(ctx, "/bench.Svc/Echo", req, buf[:0], nil); err != nil {
			t.Fatalf("InvokeInto: %v", err)
		}
	})
	var sink []byte
	withCopy := testing.AllocsPerRun(50, func() {
		var err error
		if sink, err = cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
			t.Fatalf("Invoke: %v", err)
		}
	})
	_ = sink
	t.Logf("per unary call: Invoke %.1f allocs, InvokeInto %.1f allocs", withCopy, withReuse)
	if withReuse >= withCopy {
		t.Errorf("InvokeInto allocates %.1f per call against Invoke's %.1f — dst is not being reused",
			withReuse, withCopy)
	}

	// An absolute ceiling as well as the relative check. Without it the gate
	// passes on any regression that hurts both forms equally: dropping
	// DiscardMetadata from Invoke puts four allocations back per call and the
	// comparison above never notices, because InvokeInto grows by the same four.
	// Same shape as unaryTransportWrites — when the number improves, lower it.
	if withCopy > unaryAllocCeiling {
		t.Errorf("Invoke allocates %.1f per call, ceiling %d — a per-RPC allocation came back",
			withCopy, unaryAllocCeiling)
	}
	if withCopy < unaryAllocCeiling {
		t.Errorf("Invoke allocates only %.1f per call, below the recorded %d: the path improved "+
			"— lower unaryAllocCeiling to lock the win in", withCopy, unaryAllocCeiling)
	}
}

// TestRecvInto_AllocsPerMessage is the gate. AllocsPerRun counts the whole
// process, and the in-process mock peer allocates nothing per echoed frame, so
// what is left is this package's own cost.
func TestRecvInto_AllocsPerMessage(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	const msgs, size = 64, 256

	measure := func(reuse bool) float64 {
		s := streamOf(t, cc, msgs, size)
		ctx := context.Background()
		buf := make([]byte, 0, size)
		i := 0
		return testing.AllocsPerRun(msgs-2, func() {
			i++
			var err error
			if reuse {
				buf, err = s.RecvInto(ctx, buf[:0])
			} else {
				buf, err = s.Recv(ctx)
			}
			if err != nil {
				t.Fatalf("message %d: %v", i, err)
			}
		})
	}
	withCopy := measure(false)
	withReuse := measure(true)
	t.Logf("per message: Recv %.2f allocs, RecvInto %.2f allocs", withCopy, withReuse)
	if withReuse >= withCopy {
		t.Errorf("RecvInto allocates %.2f per message against Recv's %.2f — it is not reusing dst",
			withReuse, withCopy)
	}
}
