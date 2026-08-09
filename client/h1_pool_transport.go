package client

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// h1PoolTransport adapts *h1Pool to the internal transport interface consumed by
// Client. It is the HTTP/1.1 analogue of poolTransport and h3PoolTransport.
type h1PoolTransport struct {
	p *h1Pool
}

// openExchange implements transport.openExchange for a pooled HTTP/1.1 client. It
// checks a connection out of the pool for the exclusive duration of one exchange
// and wraps it in an h1Exchange. pushLookup is nil (HTTP/1.1 has no server push).
//
// The transport-level release is a no-op: HTTP/1.1's pool release must carry the
// keep-alive decision, which is only known once the exchange finishes, so it is
// threaded through h1Exchange.release instead — Recv passes
// http1.Exchange.KeepAlive() when the body ends, and Close passes false for every
// error/abort path so a poisoned conn is discarded rather than pooled. This
// mirrors how h1singleConn drives its in-flight slot.
func (pt *h1PoolTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (conn.StreamRef, bool), func(), error) {
	mc, err := pt.p.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	// h1Exchange.release is contractually called exactly once. The sync.Once makes
	// that structural rather than contractual: a double release would decrement
	// the conn's active count twice and break the MaxConnsPerHost invariant that
	// bounds this pool's concurrency.
	var once sync.Once
	release := func(keepAlive bool) {
		once.Do(func() { pt.p.release(mc, keepAlive) })
	}
	return &h1Exchange{ex: mc.c.NewExchange(), nc: mc.c, release: release}, nil, func() {}, nil
}

// close implements transport.close. Idempotent.
func (pt *h1PoolTransport) close() error {
	return pt.p.Close()
}

// shutdown implements transport.shutdown. HTTP/1.1 has no in-band graceful drain
// (no GOAWAY), so shutdown closes the whole pool, as the HTTP/3 pool does.
func (pt *h1PoolTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return pt.p.Close()
}

// warmup implements transport.warmup. Pre-dials up to n conns into the pool.
func (pt *h1PoolTransport) warmup(n int) {
	pt.p.warmup(n)
}
