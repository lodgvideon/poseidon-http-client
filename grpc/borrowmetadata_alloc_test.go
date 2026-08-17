//go:build !race

package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Behind !race for the reason allocgates_test.go gives at length: the detector
// allocates as it instruments, and these gates turn on a handful of allocations.
// The same file explains why no testify assertion may appear inside a measured
// closure; measureStreamRPC's `do` therefore keeps plain t.Fatalf and every
// assertion sits outside the measurement.

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
// Both directions are errors, same as every other gate here. Lowered 6 -> 4 by
// #577, which stopped conn allocating a field slice per decoded header block —
// two blocks per RPC, and this gate sits on top of conn.
const streamMetadataAllocCeiling = 4

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
	assert.Equalf(t, float64(metadataCopyAllocs), onHeap-borrowed,
		"borrowing saves %.1f allocations per RPC, want exactly %d — the arena "+
			"is not carrying both blocks any more, or the heap copy changed shape",
		onHeap-borrowed, metadataCopyAllocs)
	// Borrowing must reach parity with discarding. Discarding copies nothing at
	// all, so it is the floor: anything above it is metadata still on the heap.
	assert.Equalf(t, discarded, borrowed,
		"borrowed %.1f allocs against discarded %.1f — borrowing is meant to cost "+
			"exactly what not copying costs, since neither touches the heap", borrowed, discarded)
	// The absolute ceiling catches a regression that hurts every arm equally,
	// which both comparisons above would sail straight past.
	assert.LessOrEqualf(t, borrowed, float64(streamMetadataAllocCeiling),
		"a borrowed-metadata RPC allocates %.1f, ceiling %d — a per-RPC allocation "+
			"came back somewhere every arm shares", borrowed, streamMetadataAllocCeiling)
	assert.GreaterOrEqualf(t, borrowed, float64(streamMetadataAllocCeiling),
		"a borrowed-metadata RPC allocates only %.1f, below the recorded %d — the "+
			"path improved; lower streamMetadataAllocCeiling to lock the win in",
		borrowed, streamMetadataAllocCeiling)
}

// TestBorrowMetadata_ReleaseParksCapacitySoTheNextRPCDoesNotAllocate covers the
// half of "the arena is reused" that the gate above cannot see on its own.
//
// The gate catches the acquire side: nilling the arena there makes every RPC
// regrow it, and the count goes straight back to the heap copy's. The release
// side is quieter. Nilling a buffer instead of truncating it on the way into the
// pool is still CORRECT — the next RPC allocates and carries on — so no
// behavioural test can fail on it, and the gate above cannot either, because the
// two arms would regrow equally and its comparisons are all relative. Only a
// capacity check catches it, which is the same reason conn's
// roundtripAllocCeiling exists next to its aliasing test.
//
// It reads the streamBufs struct through the pointer this test kept, so it never
// depends on sync.Pool handing the same one back.
//
// The name is long because it has to carry "DoesNotAllocate": ci.yml runs the
// !race gates by name, and a test whose name matches no alternative in that
// pattern compiles, passes locally and never executes in CI. That comment
// records the trap catching the repo three times already; matching the whole
// BorrowMetadata family in the pattern would be the durable fix and wants a
// commit with workflow scope.
func TestBorrowMetadata_ReleaseParksCapacitySoTheNextRPCDoesNotAllocate(t *testing.T) {
	s := &Stream{borrowMD: true}
	s.acquireBufs()
	b := s.bufs
	require.NotNil(t, b, "acquireBufs attached no buffers")
	s.header = s.copyFields([]conn.HeaderField{
		{Name: []byte("x-request-id"), Value: []byte("0123456789abcdef")},
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
	})
	bytesGrown, fieldsGrown := cap(b.md), cap(b.mdFields)
	require.Truef(t, bytesGrown != 0 && fieldsGrown != 0,
		"the arena has capacity %d/%d after a copy went into it, want both non-zero",
		bytesGrown, fieldsGrown)

	s.releaseBufs()

	assert.GreaterOrEqualf(t, cap(b.md), bytesGrown,
		"the arena went into the pool with byte capacity %d, want the %d it grew to "+
			"— releaseBufs is nilling it instead of truncating, so the next RPC regrows it",
		cap(b.md), bytesGrown)
	assert.GreaterOrEqualf(t, cap(b.mdFields), fieldsGrown,
		"the arena went into the pool with field capacity %d, want the %d it grew to "+
			"— releaseBufs is nilling it instead of truncating", cap(b.mdFields), fieldsGrown)
}
