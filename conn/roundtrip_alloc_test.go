//go:build !race

package conn

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// roundtripAllocCeiling is what one complete request/response costs on a warm
// connection, and both directions are errors. Above it, a per-request
// allocation came back; below it, the path improved and the win is not locked
// in until this drops.
//
// What the remaining one is, so nobody hunts for it twice: the field slice in
// copyFieldsToSlab, a plain make([]hpack.HeaderField, len(fields)) per header
// block. Measured with -memprofilerate=1 it is 2001 objects over 2000
// iterations — exactly one per request — and it is #577, which wants the slice
// carved from the slab it already describes.
//
// The other one used to be the :authority: authorityOf ended in
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
const roundtripAllocCeiling = 1

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
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	hdrs := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}

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
	if n > roundtripAllocCeiling {
		t.Errorf("a round trip allocates %.1f, ceiling %d — a per-request allocation is "+
			"back. A pooled buffer nilled instead of truncated on reset is the quiet "+
			"way to cause this: still correct, just allocating again every request",
			n, roundtripAllocCeiling)
	}
	if n < roundtripAllocCeiling {
		t.Errorf("a round trip allocates only %.1f, below the recorded %d — the path "+
			"improved; lower roundtripAllocCeiling to lock it in", n, roundtripAllocCeiling)
	}
}
