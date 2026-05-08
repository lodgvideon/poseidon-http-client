package client

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// poolTransport adapts *Pool to the internal transport interface
// consumed by Client.
type poolTransport struct {
	p *Pool
}

// newPoolTransport constructs a poolTransport. Internal: callers go
// through NewClient with Transport=TransportPool.
func newPoolTransport(addr string, connOpts conn.ConnOptions, opts PoolOptions) *poolTransport {
	return &poolTransport{p: newPool(addr, connOpts, opts)}
}

// newPoolTransportFromPool wraps an existing *Pool. Used by tests.
func newPoolTransportFromPool(p *Pool) *poolTransport {
	return &poolTransport{p: p}
}

// acquire implements transport.acquire.
//
// release reports nil reqErr because the transport interface does not
// surface per-request errors. Dead-conn eviction is driven by the
// actor's background health-check tick, which runs every
// PoolOptions.HealthCheckPeriod (default 30s). Newly arriving acquires
// also skip dead conns via pickLeastLoaded's IsAlive() guard, so a
// transient dead conn won't be picked between ticks.
func (pt *poolTransport) acquire(ctx context.Context) (*conn.Conn, func(), error) {
	mc, err := pt.p.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	release := func() { pt.p.release(mc, nil) }
	return mc.c, release, nil
}

// close implements transport.close. Idempotent.
func (pt *poolTransport) close() error {
	return pt.p.close()
}
