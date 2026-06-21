package client

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// poolTransport adapts *Pool to the internal transport interface
// consumed by Client.
type poolTransport struct {
	p *Pool
}

// newPoolTransport constructs a poolTransport. Internal: callers go
// through NewClient with Transport=TransportPool.
func newPoolTransport(addr string, connOpts conn.ConnOptions, opts PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *poolTransport {
	return &poolTransport{p: newPool(addr, connOpts, opts, hooksRef, metrics)}
}

// newPoolTransportFromPool wraps an existing *Pool. Used by tests.
func newPoolTransportFromPool(p *Pool) *poolTransport {
	return &poolTransport{p: p}
}

// openExchange implements transport.openExchange.
//
// release reports nil reqErr because the transport interface does not
// surface per-request errors. Dead-conn eviction is driven by the
// actor's background health-check tick, which runs every
// PoolOptions.HealthCheckPeriod (default 30s). Newly arriving acquires
// also skip dead conns via pickLeastLoaded's IsAlive() guard, so a
// transient dead conn won't be picked between ticks.
func (pt *poolTransport) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	mc, err := pt.p.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	cn := mc.c
	stream, serr := cn.NewStream(ctx)
	if serr != nil {
		pt.p.release(mc, serr)
		return nil, nil, nil, serr
	}
	release := func() { pt.p.release(mc, nil) }
	return stream, cn.LookupStream, release, nil
}

// close implements transport.close. Idempotent.
func (pt *poolTransport) close() error {
	return pt.p.Close()
}

// shutdown implements transport.shutdown. Calls Close on the
// underlying pool which closes all conns. Use *Pool directly when
// you need per-conn Shutdown.
func (pt *poolTransport) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return pt.p.Close()
}

// warmup implements transport.warmup. Pre-dials up to n conns into
// the pool. Errors are recorded via the pool's OnDial hook.
func (pt *poolTransport) warmup(n int) {
	pt.p.warmup(n)
}
