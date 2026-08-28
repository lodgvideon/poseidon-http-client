package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/trace"
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
func (pt *h1PoolTransport) openExchange(ctx context.Context) (protoStream, pushLookuper, releaser, exchangeStats, error) {
	st := exchangeStats{Proto: trace.ProtoH1, RemoteAddr: pt.p.addr}
	mc, err := pt.p.Acquire(ctx)
	if err != nil {
		return nil, nil, nil, st, err
	}
	// mc IS the releaser: it already exists on the heap and knows its pool, so no
	// closure is built per exchange (#476). Exactly-once — which protects the
	// conn's active count and hence this pool's MaxConnsPerHost bound — is
	// enforced by h1Exchange.release rather than by a per-exchange sync.Once.
	return &h1Exchange{ex: mc.c.NewExchange(), rel: mc}, nil, noRelease, st, nil
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
	pt.p.Warmup(n)
}
