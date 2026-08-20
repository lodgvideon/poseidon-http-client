package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

// managedTransport adapts *managedPool to the internal transport interface.
type managedTransport struct {
	mp *managedPool
}

// openExchange implements transport.openExchange. Delegates to managedPool.acquire
// which fans across per-address sub-pools via Selector, then opens an H2 stream.
func (mt *managedTransport) openExchange(ctx context.Context) (protoStream, pushLookuper, releaser, exchangeStats, error) {
	st := exchangeStats{Proto: trace.ProtoH2}
	cn, release, addr, err := mt.mp.acquire(ctx)
	if err != nil {
		return nil, nil, nil, st, err
	}
	st.RemoteAddr = addr.String()
	stream, serr := cn.NewStream(ctx)
	if serr != nil {
		release()
		return nil, nil, nil, st, serr
	}
	return stream, cn, funcReleaser(release), st, nil
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
