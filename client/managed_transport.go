package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// managedTransport adapts *managedPool to the internal transport interface.
type managedTransport struct {
	mp *managedPool
}

// acquire implements transport.acquire. Delegates to managedPool.acquire
// which fans across per-address sub-pools via Selector.
func (mt *managedTransport) acquire(ctx context.Context) (*conn.Conn, func(), error) {
	return mt.mp.acquire(ctx)
}

// close implements transport.close. Idempotent.
func (mt *managedTransport) close() error {
	return mt.mp.close()
}

// shutdown implements transport.shutdown. Calls close on the
// underlying managed pool which closes all sub-pools.
func (mt *managedTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return mt.mp.close()
}

// warmup implements transport.warmup. Fans out pre-dial across
// the current set of resolved addresses.
func (mt *managedTransport) warmup(n int) {
	mt.mp.warmup(n)
}
