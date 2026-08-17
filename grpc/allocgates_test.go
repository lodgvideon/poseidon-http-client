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
//
// For the same reason no testify assertion may appear INSIDE a measured
// closure: require/assert reflect over their arguments and box them into an
// interface slice, and AllocsPerRun counts the whole process, so one call in
// there would be charged to every iteration and the gate would be measuring the
// assertion library. The closures keep plain t.Fatalf; every assertion sits
// outside the measurement.

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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
// request (#578), then 4 -> 2 when conn stopped allocating the field slice for
// each decoded header block (#577). Nothing in grpc changed either time: the RPC
// path sits on conn, so a conn-level allocation is a grpc-level one, and this
// gate is why both showed up as a build failure instead of going unnoticed. The
// lesson is in which direction the check has to run — changing a package means
// running the allocation gates of everything BUILT ON it, not only its own.
//
// Two per RPC rather than one because a gRPC response carries TWO header blocks,
// HEADERS and TRAILERS, and conn decoded a field slice for each.
const unaryAllocCeiling = 2

// TestInvokeInto_AllocsPerCall gates the win against the allocating form on the
// same connection.
func TestInvokeInto_AllocsPerCall(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := bytes.Repeat([]byte{'q'}, 256)
	buf := make([]byte, 0, 256)
	var sink []byte

	withReuse := testing.AllocsPerRun(50, func() {
		var err error
		if buf, err = cc.InvokeInto(ctx, "/bench.Svc/Echo", req, buf[:0], nil); err != nil {
			t.Fatalf("InvokeInto: %v", err)
		}
	})
	withCopy := testing.AllocsPerRun(50, func() {
		var err error
		if sink, err = cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
			t.Fatalf("Invoke: %v", err)
		}
	})
	_ = sink

	t.Logf("per unary call: Invoke %.1f allocs, InvokeInto %.1f allocs", withCopy, withReuse)
	assert.Lessf(t, withReuse, withCopy,
		"InvokeInto allocates %.1f per call against Invoke's %.1f — dst is not being reused",
		withReuse, withCopy)

	// An absolute ceiling as well as the relative check. Without it the gate
	// passes on any regression that hurts both forms equally: dropping
	// DiscardMetadata from Invoke puts four allocations back per call and the
	// comparison above never notices, because InvokeInto grows by the same four.
	// Same shape as unaryTransportWrites — when the number improves, lower it.
	assert.LessOrEqualf(t, withCopy, float64(unaryAllocCeiling),
		"Invoke allocates %.1f per call, ceiling %d — a per-RPC allocation came back",
		withCopy, unaryAllocCeiling)
	assert.GreaterOrEqualf(t, withCopy, float64(unaryAllocCeiling),
		"Invoke allocates only %.1f per call, below the recorded %d: the path improved "+
			"— lower unaryAllocCeiling to lock the win in", withCopy, unaryAllocCeiling)
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
	assert.Lessf(t, withReuse, withCopy,
		"RecvInto allocates %.2f per message against Recv's %.2f — it is not reusing dst",
		withReuse, withCopy)
}
