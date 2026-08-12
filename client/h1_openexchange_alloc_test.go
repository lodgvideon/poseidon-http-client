//go:build !race

package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// The HTTP/2 pooled path lost its per-request release closure in #476. That
// ticket predicted the same shape in the H1 and H3 pooled transports and asked
// for all three to be checked rather than only the one that was profiled. H3's
// was already clean; this is H1's gate, so the sibling cannot drift back.
//
// Behind !race for the reason the other allocation gates are: the detector
// allocates as it instruments, and this turns on a difference of one.

// TestH1PoolTransport_OpenExchangeAllocs measures one exchange's worth of the
// pooled HTTP/1.1 transport's own cost: the pool acquire, the http1.Exchange,
// the h1Exchange wrapper and the release.
//
// The release is driven with keepAlive=true on purpose. h1Exchange.Close
// releases with false, which is the "discard this connection" path — the pool
// would then redial on every iteration and the number would be a dial's, not an
// exchange's. Releasing with keep-alive is what a completed request does
// (Recv passes http1.Exchange.KeepAlive() when the body ends), so it is the
// pooled steady state this gate exists to hold.
//
// The peer is h1RawServer over real TCP, NOT this package's h1FakeDialer. The
// fake hands out net.Pipe halves, and net.Pipe implements deadlines in Go: every
// SetReadDeadline the pool's idle probe performs allocates a timer. Measured on
// the fake, that fixture cost was 5 of 10 allocations — half the gate would have
// been the test's own plumbing, and a transport change would have moved a number
// that is mostly not the transport.
func TestH1PoolTransport_OpenExchangeAllocs(t *testing.T) {
	addr := h1RawServer(t, func(nc net.Conn) error {
		_, werr := nc.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))
		return werr
	}, true)

	p := newH1Pool(addr, &conn.PlaintextDialer{}, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: time.Hour, // keep the background tick out of the count
	}, nil, nil)
	defer func() { _ = p.Close() }()
	pt := &h1PoolTransport{p: p}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Warm the pool so the dial is not in the measured region.
	s, _, _, err := pt.openExchange(ctx)
	if err != nil {
		t.Fatalf("warm-up openExchange: %v", err)
	}
	s.(*h1Exchange).release(true)

	n := testing.AllocsPerRun(200, func() {
		ex, _, _, oerr := pt.openExchange(ctx)
		if oerr != nil {
			t.Fatalf("openExchange: %v", oerr)
		}
		ex.(*h1Exchange).release(true)
	})

	// What is left is the http1.Exchange, the h1Exchange, and two in the pool
	// acquire. It was 6 before the releaser change: a sync.Once, the closure
	// wrapping it, and the closure once.Do ran.
	//
	// Two-sided, as the H2 gate is: an improvement that nobody lowers the
	// ceiling for is an improvement the next regression is free to spend.
	const h1OpenExchangeAllocCeiling = 4
	t.Logf("h1 openExchange + release: %.1f allocs", n)
	if n > h1OpenExchangeAllocCeiling {
		t.Errorf("h1 openExchange allocates %.1f per exchange, ceiling %d — the most "+
			"likely cause is a closure back on the path; the release used to be one, "+
			"and h1ManagedConn is the releaser precisely so none is built",
			n, h1OpenExchangeAllocCeiling)
	}
	if n < h1OpenExchangeAllocCeiling {
		t.Errorf("h1 openExchange allocates only %.1f, below the recorded %d — the path "+
			"improved, lower h1OpenExchangeAllocCeiling to lock it in", n, h1OpenExchangeAllocCeiling)
	}
}
