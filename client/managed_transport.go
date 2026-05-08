package client

import (
	"context"

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
