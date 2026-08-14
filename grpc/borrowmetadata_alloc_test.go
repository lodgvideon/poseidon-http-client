//go:build !race

package grpc

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Behind !race for the reason allocgates_test.go gives at length: the detector
// allocates as it instruments, and these gates turn on a handful of allocations.

// metadataCopyAllocs is what copying the response header and trailer blocks onto
// the heap costs per RPC: two allocations — the bytes and the field headers —
// for each of the two blocks. It is the number DiscardMetadata's doc quotes and
// the number BorrowMetadata removes (#455 item 2).
const metadataCopyAllocs = 4

// streamMetadataAllocCeiling is what one streaming RPC costs against the mock
// peer with metadata borrowed, options included. Distinct from unaryAllocCeiling
// on purpose: that one measures Invoke, this one measures the NewStream shape
// with Header and Trailer read, and folding two different measurements into one
// constant is the drift allocgates_test.go already had to unpick once.
//
// Both directions are errors, same as every other gate here.
const streamMetadataAllocCeiling = 6

// measureStreamRPC runs the full streaming shape — open, send, drain, read both
// metadata blocks, close — and returns its steady-state allocation count.
//
// Every arm passes exactly ONE call option, including the arm that is not
// borrowing. That is what makes the arms comparable: applyCallOptions puts the
// resolved struct on the heap whenever there is anything to apply, so an arm
// with no options is a whole allocation cheaper for a reason that has nothing to
// do with metadata. The first version of this gate compared a no-option arm
// against BorrowMetadata() and read the 4-allocation win as 3.
func measureStreamRPC(t *testing.T, cc *ClientConn, opt CallOption) float64 {
	t.Helper()
	ctx := context.Background()
	do := func() {
		s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil, opt)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
			t.Fatalf("SendLast: %v", err)
		}
		for {
			if _, err := s.Recv(ctx); err != nil {
				break
			}
		}
		if _, err := s.Header(ctx); err != nil {
			t.Fatalf("Header: %v", err)
		}
		_ = s.Trailer()
		_ = s.Close()
	}
	// Warm the connection and every pool so the count is steady state rather
	// than first-request setup — including the metadata arena, which grows to fit
	// both blocks on the first RPC that uses it and never again.
	for i := 0; i < 20; i++ {
		do()
	}
	return testing.AllocsPerRun(200, do)
}

// TestBorrowMetadata_AllocsPerCall is the gate. A caller that reads the metadata
// must pay nothing for it beyond what a caller that throws it away pays.
func TestBorrowMetadata_AllocsPerCall(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)

	// MaxRecvMessageSize is the neutral one-option arm: it changes no metadata
	// behaviour, so the difference against it is the copy and nothing else.
	onHeap := measureStreamRPC(t, cc, MaxRecvMessageSize(1<<20))
	borrowed := measureStreamRPC(t, cc, BorrowMetadata())
	discarded := measureStreamRPC(t, cc, DiscardMetadata())
	t.Logf("streaming RPC: heap copy %.1f allocs, borrowed %.1f, discarded %.1f",
		onHeap, borrowed, discarded)

	if got := onHeap - borrowed; got != metadataCopyAllocs {
		t.Errorf("borrowing saves %.1f allocations per RPC, want exactly %d — the arena "+
			"is not carrying both blocks any more, or the heap copy changed shape",
			got, metadataCopyAllocs)
	}
	// Borrowing must reach parity with discarding. Discarding copies nothing at
	// all, so it is the floor: anything above it is metadata still on the heap.
	if borrowed != discarded {
		t.Errorf("borrowed %.1f allocs against discarded %.1f — borrowing is meant to cost "+
			"exactly what not copying costs, since neither touches the heap", borrowed, discarded)
	}
	// The absolute ceiling catches a regression that hurts every arm equally,
	// which both comparisons above would sail straight past.
	if borrowed > streamMetadataAllocCeiling {
		t.Errorf("a borrowed-metadata RPC allocates %.1f, ceiling %d — a per-RPC allocation "+
			"came back somewhere every arm shares", borrowed, streamMetadataAllocCeiling)
	}
	if borrowed < streamMetadataAllocCeiling {
		t.Errorf("a borrowed-metadata RPC allocates only %.1f, below the recorded %d — the "+
			"path improved; lower streamMetadataAllocCeiling to lock the win in",
			borrowed, streamMetadataAllocCeiling)
	}
}

// TestBorrowMetadata_ArenaIsReused is the mechanism half. The gate above measures
// the outcome, and an implementation that allocated a fresh arena per RPC and
// happened to hide it inside a pooled struct would still have to fail this one.
func TestBorrowMetadata_ArenaIsReused(t *testing.T) {
	first := &Stream{borrowMD: true}
	first.acquireBufs()
	first.header = first.copyFields([]conn.HeaderField{
		{Name: []byte("x-request-id"), Value: []byte("0123456789abcdef")},
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
	})
	grown := cap(first.bufs.md)
	if grown == 0 {
		t.Fatal("the arena has no capacity after a copy went into it")
	}
	first.releaseBufs()

	// sync.Pool hands back what was most recently Put on the same P in the
	// overwhelming majority of cases, and this test asserts only that the
	// capacity is not rebuilt from nothing when it does.
	second := &Stream{borrowMD: true}
	second.acquireBufs()
	defer second.releaseBufs()
	if cap(second.bufs.md) == 0 && cap(second.bufs.mdFields) == 0 {
		t.Skip("the pool handed out a fresh struct; nothing to say about reuse")
	}
	if cap(second.bufs.md) < grown {
		t.Errorf("the reused arena came back with capacity %d, want at least the %d it grew "+
			"to — acquireBufs is nilling it instead of truncating, so every RPC regrows it",
			cap(second.bufs.md), grown)
	}
}
