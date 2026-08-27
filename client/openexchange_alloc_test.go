//go:build !race

package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openExchange runs on every request, and for a long time two of its four lines
// were the largest poseidon-owned allocation site on the pooled HTTP/2 path —
// 30% of the regime's allocation count — without doing any work.
//
// The larger of the two was `cn.LookupStream`: a method value, which Go
// heap-allocates a closure for on every evaluation. It is now returned as the
// connection itself, through the pushLookuper interface, which costs nothing to
// build because an interface holding one pointer stores the pointer.
//
// Behind !race for the reason the other allocation gates are: the detector
// allocates as it instruments, and this turns on a difference of one.

// TestPoolTransport_OpenExchangeAllocs is the gate. It measures openExchange,
// the stream's Close and the release together — one request's worth of the
// transport's own cost, with no round trip in it, since NewStream is local until
// the first frame is written. Close is in the measured region because without it
// the streams accumulate against the peer's concurrency cap and the loop dies of
// "in-flight stream cap reached" rather than measuring anything.
//
// NOTE ON testify: the AllocsPerRun closure below deliberately keeps its
// hand-rolled t.Fatalf. testify reflects and allocates, AllocsPerRun measures
// the whole process, and this gate is two-sided on an exact count of 6 — one
// assert inside the closure would destroy it. Every testify call in this file is
// outside the measured region.
func TestPoolTransport_OpenExchangeAllocs(t *testing.T) {
	srv := startOneH2Server(t)
	defer srv.Close()

	p := newPool(srv.Listener.Addr().String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1000,
		HealthCheckPeriod: time.Hour, // keep the background tick out of the count
	}, nil, nil)
	defer func() { _ = p.Close() }()
	pt := newPoolTransportFromPool(p)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Warm the pool so the dial is not in the measured region.
	s, _, release, _, err := pt.openExchange(ctx)
	require.NoError(t, err, "warm-up openExchange")
	_ = s.Close()
	release.Release()

	n := testing.AllocsPerRun(200, func() {
		st, _, rel, _, oerr := pt.openExchange(ctx)
		if oerr != nil {
			// Hand-rolled on purpose: see the NOTE above. Inside the measured
			// closure, testify's reflection would be counted as allocation.
			t.Fatalf("openExchange: %v", oerr)
		}
		_ = st.Close()
		rel.Release()
	})

	// Lower this when the number improves, as the other absolute gates in this
	// repo do. The release closure is gone as of the releaser interface (#476):
	// the pool's managedConn IS the releaser now, and an interface holding a
	// pointer that already exists on the heap allocates nothing.
	//
	// What is left is the stream, the exchange's Close, and one allocation on
	// openExchange's return line that is NOT a closure — it is conn.StreamRef
	// being boxed into protoStream. StreamRef is {s *Stream; gen uint64}, two
	// words, and an interface can only store a single pointer inline, so a
	// two-word value has to go to the heap for the interface to point at it.
	// Measured three ways: 1 object per call flat on that line, 16 bytes each
	// (202 calls x 16 = the 3.16 KiB pprof reports), and `-gcflags=-m` naming
	// `stream escapes to heap` at exactly that column.
	//
	// It is not removable at a price worth paying, which is recorded here so the
	// next reader does not re-derive it. The generation word is the whole point
	// of StreamRef — it is what refuses a handle to a recycled pooled Stream —
	// so the value cannot be made one word. Handing out a *StreamRef instead
	// only moves the allocation, unless the pointee is reused across lifetimes,
	// which is precisely the use-after-recycle bug StreamRef exists to prevent.
	// Sixteen bytes per request is what that guard costs.
	const openExchangeAllocCeiling = 6
	t.Logf("openExchange + release: %.1f allocs", n)
	assert.LessOrEqualf(t, n, float64(openExchangeAllocCeiling),
		"openExchange allocates %.1f per request, ceiling %d — the most likely "+
			"cause is a method value or a closure back on the path: `cn.LookupStream` as "+
			"a value allocates on every evaluation, which is what pushLookuper removed",
		n, openExchangeAllocCeiling)
	assert.GreaterOrEqualf(t, n, float64(openExchangeAllocCeiling),
		"openExchange allocates only %.1f, below the recorded %d — the path "+
			"improved, lower openExchangeAllocCeiling to lock it in", n, openExchangeAllocCeiling)
}
