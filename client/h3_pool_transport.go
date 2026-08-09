package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// h3PoolTransport adapts *h3Pool to the internal transport interface consumed by
// Client. It is the HTTP/3 analogue of poolTransport.
type h3PoolTransport struct {
	p *h3Pool
}

// openExchange implements transport.openExchange for a pooled HTTP/3 client. It
// acquires a QUIC connection from the pool and returns a fresh h3Exchange; the
// pushLookup is nil (HTTP/3 server push is disabled) and release returns the conn's
// stream slot to the pool.
//
// Dead-conn eviction here is driven by the background health-check tick and by
// h3RetireEvict on release, which retires a conn the peer has GOAWAY'd once its
// last exchange finishes (RFC 9114 §5.2).
//
// This is NOT the same as the HTTP/2 pool, whose handleRelease additionally
// re-checks IsAlive() and evicts with CloseDead when the conn is both dead and
// idle. The comment here used to claim the two behaved "exactly" alike; they do
// not, and a reader unifying the two release paths needs to know which one they
// are looking at.
func (pt *h3PoolTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (conn.StreamRef, bool), func(), error) {
	mc, err := pt.p.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	release := func() { pt.p.release(mc) }
	return getH3Exchange(mc.cl), nil, release, nil
}

// close implements transport.close. Idempotent.
func (pt *h3PoolTransport) close() error {
	return pt.p.Close()
}

// shutdown implements transport.shutdown. http3.Client has no graceful drain
// separate from Close, so shutdown closes the whole pool.
func (pt *h3PoolTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return pt.p.Close()
}

// warmup implements transport.warmup. Pre-dials up to n QUIC conns into the pool.
func (pt *h3PoolTransport) warmup(n int) {
	pt.p.warmup(n)
}
