//go:build !race

package client

import (
	"context"
	"testing"
	"time"
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
	s, _, release, err := pt.openExchange(ctx)
	if err != nil {
		t.Fatalf("warm-up openExchange: %v", err)
	}
	_ = s.Close()
	release.release()

	n := testing.AllocsPerRun(200, func() {
		st, _, rel, oerr := pt.openExchange(ctx)
		if oerr != nil {
			t.Fatalf("openExchange: %v", oerr)
		}
		_ = st.Close()
		rel.release()
	})

	// Lower this when the number improves, as the other absolute gates in this
	// repo do. The release closure is gone as of the releaser interface (#476):
	// the pool's managedConn IS the releaser now, and an interface holding a
	// pointer that already exists on the heap allocates nothing.
	//
	// What is left is the stream, the exchange's Close, and one allocation on
	// openExchange's return line, which is NOT the closure — it was measured
	// separately and survives the closure's removal.
	const openExchangeAllocCeiling = 6
	t.Logf("openExchange + release: %.1f allocs", n)
	if n > openExchangeAllocCeiling {
		t.Errorf("openExchange allocates %.1f per request, ceiling %d — the most likely "+
			"cause is a method value or a closure back on the path: `cn.LookupStream` as "+
			"a value allocates on every evaluation, which is what pushLookuper removed",
			n, openExchangeAllocCeiling)
	}
	if n < openExchangeAllocCeiling {
		t.Errorf("openExchange allocates only %.1f, below the recorded %d — the path "+
			"improved, lower openExchangeAllocCeiling to lock it in", n, openExchangeAllocCeiling)
	}
}
