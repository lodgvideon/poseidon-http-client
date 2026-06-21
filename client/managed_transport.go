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

// openExchange implements transport.openExchange. Delegates to managedPool.acquire
// which fans across per-address sub-pools via Selector, then opens an H2 stream.
func (mt *managedTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	cn, release, err := mt.mp.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	stream, serr := cn.NewStream(ctx)
	if serr != nil {
		release()
		return nil, nil, nil, serr
	}
	return stream, cn.LookupStream, release, nil
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
