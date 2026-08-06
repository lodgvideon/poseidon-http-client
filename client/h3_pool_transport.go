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
// release reports nil reqErr because the transport interface does not surface
// per-request errors — dead-conn eviction is driven by the actor re-checking
// h3Client.Alive() on release and by the background health-check tick, exactly like
// the HTTP/2 pool.
func (pt *h3PoolTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
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
