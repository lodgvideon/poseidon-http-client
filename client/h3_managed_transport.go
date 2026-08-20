package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

// h3ManagedTransport adapts *h3ManagedPool to the internal transport interface.
// It is the HTTP/3 analogue of managedTransport.
type h3ManagedTransport struct {
	mp *h3ManagedPool
}

// openExchange implements transport.openExchange. Delegates to h3ManagedPool.acquire
// which fans across per-address HTTP/3 sub-pools via Selector, then wraps the
// acquired client in a fresh h3Exchange. pushLookup is nil (HTTP/3 server push is
// disabled).
func (mt *h3ManagedTransport) openExchange(ctx context.Context) (protoStream, pushLookuper, releaser, exchangeStats, error) {
	st := exchangeStats{Proto: trace.ProtoH3}
	cl, release, addr, err := mt.mp.acquire(ctx)
	// Before the error check; see managedTransport.openExchange.
	st.RemoteAddr = addrString(addr)
	if err != nil {
		return nil, nil, nil, st, err
	}
	return getH3Exchange(cl), nil, funcReleaser(release), st, nil
}

// close implements transport.close. Idempotent.
func (mt *h3ManagedTransport) close() error {
	return mt.mp.close()
}

// shutdown implements transport.shutdown. Closes the underlying managed pool.
func (mt *h3ManagedTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return mt.mp.close()
}

// warmup implements transport.warmup. Fans out pre-dial across the current set of
// resolved addresses.
func (mt *h3ManagedTransport) warmup(n int) {
	mt.mp.warmup(n)
}
