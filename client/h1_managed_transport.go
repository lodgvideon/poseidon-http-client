package client

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// h1ManagedTransport adapts *h1ManagedPool to the internal transport interface.
// It is the HTTP/1.1 analogue of managedTransport and h3ManagedTransport.
type h1ManagedTransport struct {
	mp *h1ManagedPool
}

// openExchange implements transport.openExchange. Delegates to
// h1ManagedPool.acquire, which fans across per-address HTTP/1.1 sub-pools via the
// Selector and checks a connection out exclusively for this exchange, then wraps
// it in an h1Exchange. pushLookup is nil (HTTP/1.1 has no server push).
//
// As in h1PoolTransport, the transport-level release is a no-op and the keep-alive
// verdict is threaded through h1Exchange.release instead — it is only known once
// the exchange completes.
func (mt *h1ManagedTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	c, release, err := mt.mp.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	// See h1PoolTransport.openExchange: the Once turns "release is called exactly
	// once" from a contract into an invariant, protecting the sub-pool's
	// active-count and hence its MaxConnsPerHost bound.
	var once sync.Once
	rel := func(keepAlive bool) {
		once.Do(func() { release(keepAlive) })
	}
	return &h1Exchange{ex: c.NewExchange(), nc: c, release: rel}, nil, func() {}, nil
}

// close implements transport.close. Idempotent.
func (mt *h1ManagedTransport) close() error {
	return mt.mp.close()
}

// shutdown implements transport.shutdown. HTTP/1.1 has no in-band graceful drain,
// so shutdown closes the underlying managed pool.
func (mt *h1ManagedTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return mt.mp.close()
}

// warmup implements transport.warmup. Fans out pre-dial across the current set of
// resolved addresses.
func (mt *h1ManagedTransport) warmup(n int) {
	mt.mp.warmup(n)
}
