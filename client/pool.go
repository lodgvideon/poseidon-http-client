// Package client — pool transport (Phase C.2).
package client

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// PoolOptions configures the per-host connection pool. Zero values
// are replaced with sensible defaults at NewClient.
type PoolOptions struct {
	// MaxConnsPerHost caps live connections in this pool.
	// 0 → 1 (effectively single-conn).
	MaxConnsPerHost int

	// MaxStreamsPerConn is the soft cap on concurrent streams the
	// pool will assign to one connection. Effective cap is
	// min(this, peer SETTINGS_MAX_CONCURRENT_STREAMS) where the
	// peer value is observed via (*conn.Conn).PeerMaxConcurrentStreams.
	// 0 → use peer value (or local default 100 if peer unbounded).
	MaxStreamsPerConn int

	// IdleTimeout closes a conn that has been idle (active==0)
	// longer than this duration. 0 → never close on idle.
	IdleTimeout time.Duration

	// HealthCheckPeriod is the actor's tick interval for idle and
	// health-check sweeps. 0 → 30 * time.Second.
	HealthCheckPeriod time.Duration

	// DialBackoff refuses new dials within this window after a
	// dial failure on this pool. 0 → 1 * time.Second.
	DialBackoff time.Duration

	// AcquireTimeout bounds how long Acquire waits for capacity.
	// 0 → governed by ctx only.
	AcquireTimeout time.Duration
}

// Stats is a snapshot of pool state.
type Stats struct {
	ActiveConns     int
	InFlightStreams int
	Waiters         int
	InFlightDials   int
}

// managedConn is the actor's per-conn record. NEVER touched outside
// the actor goroutine.
type managedConn struct {
	c        *conn.Conn
	active   int
	lastUsed time.Time
	dialedAt time.Time
}

// acquireReq is sent on Pool.acquireCh. The actor replies on reply.
type acquireReq struct {
	ctx      context.Context
	reply    chan acquireResp
	deadline time.Time // zero = no AcquireTimeout
}

// acquireResp carries the reply from the actor for an acquireReq.
type acquireResp struct {
	mc  *managedConn
	err error
}

// releaseMsg is sent on Pool.releaseCh after a request completes.
type releaseMsg struct {
	mc  *managedConn
	err error // non-nil → request failed; actor re-checks IsAlive
}

// dialResult is sent by a dial helper goroutine on Pool.dialDoneCh.
type dialResult struct {
	mc  *managedConn
	err error
}

// Pool is a per-host connection pool. Construct via NewClient with
// Transport=TransportPool.
type Pool struct {
	opts     PoolOptions
	connOpts conn.ConnOptions
	addr     string

	// channels
	acquireCh  chan acquireReq
	releaseCh  chan releaseMsg
	dialDoneCh chan dialResult
	statsCh    chan chan Stats
	closeCh    chan struct{}
	closedCh   chan struct{}

	// closeOnce guards closeCh from double-close.
	closeOnce sync.Once
}

// newPool constructs a Pool and starts its actor goroutine. Internal:
// callers go through NewClient.
func newPool(addr string, connOpts conn.ConnOptions, opts PoolOptions) *Pool {
	if opts.MaxConnsPerHost <= 0 {
		opts.MaxConnsPerHost = 1
	}
	if opts.HealthCheckPeriod <= 0 {
		opts.HealthCheckPeriod = 30 * time.Second
	}
	if opts.DialBackoff <= 0 {
		opts.DialBackoff = 1 * time.Second
	}
	p := &Pool{
		opts:       opts,
		connOpts:   connOpts,
		addr:       addr,
		acquireCh:  make(chan acquireReq),
		releaseCh:  make(chan releaseMsg, 16),
		dialDoneCh: make(chan dialResult, 4),
		statsCh:    make(chan chan Stats),
		closeCh:    make(chan struct{}),
		closedCh:   make(chan struct{}),
	}
	go p.run()
	return p
}

// Close stops the actor and closes all conns. Idempotent.
func (p *Pool) Close() error {
	p.closeOnce.Do(func() { close(p.closeCh) })
	<-p.closedCh
	return nil
}

// Stats returns a coherent snapshot of pool state. Safe to call
// concurrently. Returns the zero Stats if the pool is closed.
func (p *Pool) Stats() Stats {
	reply := make(chan Stats, 1)
	select {
	case p.statsCh <- reply:
		return <-reply
	case <-p.closedCh:
		return Stats{}
	}
}

// run is the actor loop. Owns conns, waiters, in-flight dial state,
// dial-error backoff. Replaced with the full select loop in Task 4.
func (p *Pool) run() {
	defer close(p.closedCh)
	for {
		select {
		case respCh := <-p.statsCh:
			respCh <- Stats{}
		case <-p.closeCh:
			return
		}
	}
}

// effectiveStreamCap computes min(opts.MaxStreamsPerConn, peer cap).
// Returns 100 if both are unbounded.
func effectiveStreamCap(opts PoolOptions, c *conn.Conn) int {
	peerCap := c.PeerMaxConcurrentStreams()
	local := opts.MaxStreamsPerConn
	if local <= 0 && peerCap <= 0 {
		return 100
	}
	if local <= 0 {
		return peerCap
	}
	if peerCap <= 0 {
		return local
	}
	if peerCap < local {
		return peerCap
	}
	return local
}
