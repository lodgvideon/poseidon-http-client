package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/trace"
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
func (mt *h1ManagedTransport) openExchange(ctx context.Context) (protoStream, pushLookuper, releaser, exchangeStats, error) {
	st := exchangeStats{Proto: trace.ProtoH1}
	c, release, addr, err := mt.mp.Acquire(ctx)
	// Before the error check; see managedTransport.openExchange.
	st.RemoteAddr = addrString(addr)
	if err != nil {
		return nil, nil, nil, st, err
	}
	// acquire already hands back a release func, and a func value boxes into
	// h1Releaser for free, so nothing extra is built per exchange. Exactly-once —
	// which protects the sub-pool's active count and hence its MaxConnsPerHost
	// bound — is enforced by h1Exchange.release rather than by a per-exchange
	// sync.Once.
	return &h1Exchange{ex: c.NewExchange(), rel: h1ReleaseFunc(release)}, nil, noRelease, st, nil
}

// close implements transport.close. Idempotent.
func (mt *h1ManagedTransport) close() error {
	return mt.mp.Close()
}

// shutdown implements transport.shutdown. HTTP/1.1 has no in-band graceful drain,
// so shutdown closes the underlying managed pool.
func (mt *h1ManagedTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return mt.mp.Close()
}

// warmup implements transport.warmup. Fans out pre-dial across the current set of
// resolved addresses.
func (mt *h1ManagedTransport) warmup(n int) {
	mt.mp.Warmup(n)
}
