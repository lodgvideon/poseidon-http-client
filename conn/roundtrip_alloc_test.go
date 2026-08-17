//go:build !race

package conn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// roundtripAllocCeiling is what one complete request/response costs on a warm
// connection, and both directions are errors. Above it, a per-request
// allocation came back; below it, the path improved and the win is not locked
// in until this drops.
//
// It is ZERO, and that is the whole number: a warm connection now completes a
// request and a response without reaching the heap at all.
//
// The last one was the field slice in copyFieldsToSlab, a plain
// make([]hpack.HeaderField, len(fields)) per header block — 2001 objects over
// 2000 iterations, exactly one per request. #577 removed it by pooling the
// fields alongside the bytes they view, as HeaderBlock, rather than rebuilding
// the slice from the heap while the bytes came from a pool.
//
// The one before that was the :authority: authorityOf ended in
// string(fields[i].Value), and converting []byte to string copies by definition
// of the language (#578). The value still IS copied — the push accept path
// compares a PUSH_PROMISE's authority against it, so it must not alias the
// caller's header buffer — but into the pooled Stream's own authorityBuf, which
// resetForPoolLocked truncates rather than nils and which therefore keeps its
// capacity across lifetimes.
//
// That truncate is the reason this gate exists rather than only the aliasing
// test next to it. Nilling the buffer on reset is still CORRECT — every request
// would simply allocate again — so no behavioural test can fail on it. Only a
// count can.
const roundtripAllocCeiling = 0

// TestConn_Roundtrip_AllocsPerRequest gates the per-request allocation count of
// a full request/response against the in-process peer.
//
// AllocsPerRun counts the whole process, so the peer is inside the measurement.
// That is safe here and was checked rather than assumed: profiling this exact
// benchmark attributes every per-iteration object to copyFieldsToSlab, and the
// peer contributes none. It is the same trap that made conn's own benchmarks
// report 34 allocs/op for a path that costs 2 (#574) — there by skipping the
// buffer return, so the pools missed on every iteration.
func TestConn_Roundtrip_AllocsPerRequest(t *testing.T) {
	p := newBenchPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, p.addr(), ConnOptions{Dialer: &PlaintextDialer{}})
	require.NoError(t, err, "Dial")
	defer func() { _ = c.Close() }()

	hdrs := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}

	// This closure is what AllocsPerRun measures, so it uses plain t.Fatalf
	// rather than require: testify reflects and allocates, and the gate counts
	// the whole process. The assertion on the resulting count is outside it.
	do := func() {
		s, nerr := c.NewStream(ctx)
		if nerr != nil {
			t.Fatalf("NewStream: %v", nerr)
		}
		if serr := s.SendHeaders(ctx, hdrs, true); serr != nil {
			t.Fatalf("SendHeaders: %v", serr)
		}
		// benchDrain closes the stream and returns the pooled buffers. Reading
		// to EndStream and walking away leaves both pools missing every time,
		// which is measured as this path allocating.
		if derr := benchDrain(ctx, s); derr != nil {
			t.Fatalf("drain: %v", derr)
		}
	}

	// Warm the connection, the header-slab pool, the DATA-slab pool and the
	// Stream pool, so the measurement is steady state and not first-request
	// setup.
	for i := 0; i < 20; i++ {
		do()
	}

	n := testing.AllocsPerRun(200, do)

	t.Logf("conn round trip: %.1f allocs/request", n)
	assert.LessOrEqualf(t, n, float64(roundtripAllocCeiling),
		"a round trip allocates %.1f, ceiling %d — a per-request allocation is "+
			"back. A pooled buffer nilled instead of truncated on reset is the quiet "+
			"way to cause this: still correct, just allocating again every request",
		n, roundtripAllocCeiling)
	// No lower check: the ceiling is zero and a count cannot go below it. The
	// pair is kept everywhere else in the repo because "the path improved and
	// nobody lowered the constant" is a real way to lose a win silently; here
	// there is nothing left to win.
}
