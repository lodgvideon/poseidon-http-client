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

// run is the actor loop. Owns p.conns, p.waiters, p.inFlightDials,
// p.lastDialErrAt. Never touched from outside.
func (p *Pool) run() {
	defer close(p.closedCh)

	var (
		conns         []*managedConn
		waiters       []acquireReq
		inFlightDials int
		lastDialErrAt time.Time
	)
	tick := time.NewTicker(p.opts.HealthCheckPeriod)
	defer tick.Stop()

	for {
		select {
		case req := <-p.acquireCh:
			mc := p.pickLeastLoaded(conns)
			if mc != nil {
				mc.active++
				mc.lastUsed = time.Now()
				p.replyAcquire(req, mc, nil)
				continue
			}
			// No live capacity. Maybe dial.
			if p.canDial(len(conns), inFlightDials, lastDialErrAt) {
				inFlightDials++
				go p.dialOne()
				waiters = append(waiters, req)
				continue
			}
			// Backoff window: refuse with ErrDialBackoff if no live conn and within window.
			if !lastDialErrAt.IsZero() && time.Since(lastDialErrAt) < p.opts.DialBackoff && len(conns) == 0 && inFlightDials == 0 {
				p.replyAcquire(req, nil, ErrDialBackoff)
				continue
			}
			// At cap and saturated; queue waiter.
			waiters = append(waiters, req)

		case msg := <-p.releaseCh:
			msg.mc.active--
			msg.mc.lastUsed = time.Now()
			if msg.err != nil && !msg.mc.c.IsAlive() {
				conns = p.evict(conns, msg.mc)
			}
			waiters = p.serveWaiters(conns, waiters)

		case dr := <-p.dialDoneCh:
			inFlightDials--
			if dr.err != nil {
				lastDialErrAt = time.Now()
				if len(waiters) > 0 {
					req := waiters[0]
					waiters = waiters[1:]
					p.replyAcquire(req, nil, dr.err)
				}
				continue
			}
			conns = append(conns, dr.mc)
			waiters = p.serveWaiters(conns, waiters)

		case respCh := <-p.statsCh:
			respCh <- Stats{
				ActiveConns:     len(conns),
				InFlightStreams: sumActive(conns),
				Waiters:         len(waiters),
				InFlightDials:   inFlightDials,
			}

		case <-tick.C:
			conns = p.evictIdle(conns)
			conns = p.evictDead(conns)

		case <-p.closeCh:
			for _, w := range waiters {
				p.replyAcquire(w, nil, ErrPoolClosed)
			}
			waiters = nil
			for _, mc := range conns {
				_ = mc.c.Close()
			}
			return
		}
	}
}

// replyAcquire delivers an acquire reply, or returns the assigned mc
// to the pool if the caller's ctx already cancelled.
func (p *Pool) replyAcquire(req acquireReq, mc *managedConn, err error) {
	select {
	case req.reply <- acquireResp{mc: mc, err: err}:
	case <-req.ctx.Done():
		if mc != nil {
			mc.active--
			mc.lastUsed = time.Now()
		}
	}
}

// pickLeastLoaded returns the live, under-cap mc with smallest active
// count, or nil if none qualifies.
func (p *Pool) pickLeastLoaded(conns []*managedConn) *managedConn {
	var best *managedConn
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			continue
		}
		streamCap := effectiveStreamCap(p.opts, mc.c)
		if mc.active >= streamCap {
			continue
		}
		if best == nil || mc.active < best.active {
			best = mc
		}
	}
	return best
}

// canDial reports whether the actor may start a new dial right now.
func (p *Pool) canDial(connCount, inFlight int, lastErrAt time.Time) bool {
	if connCount+inFlight >= p.opts.MaxConnsPerHost {
		return false
	}
	if !lastErrAt.IsZero() && time.Since(lastErrAt) < p.opts.DialBackoff {
		return false
	}
	return true
}

// dialOne is the dial helper goroutine. It dials with a fresh
// background context so a cancelled waiter doesn't tear down a useful
// in-flight dial.
func (p *Pool) dialOne() {
	c, err := conn.Dial(context.Background(), p.addr, p.connOpts)
	if err != nil {
		select {
		case p.dialDoneCh <- dialResult{err: err}:
		case <-p.closedCh:
		}
		return
	}
	mc := &managedConn{c: c, dialedAt: time.Now(), lastUsed: time.Now()}
	select {
	case p.dialDoneCh <- dialResult{mc: mc}:
	case <-p.closedCh:
		_ = c.Close()
	}
}

// serveWaiters hands as many waiters as possible a live mc.
func (p *Pool) serveWaiters(conns []*managedConn, waiters []acquireReq) []acquireReq {
	for len(waiters) > 0 {
		mc := p.pickLeastLoaded(conns)
		if mc == nil {
			return waiters
		}
		mc.active++
		mc.lastUsed = time.Now()
		req := waiters[0]
		waiters = waiters[1:]
		p.replyAcquire(req, mc, nil)
	}
	return waiters
}

// evict removes target from conns and closes its underlying conn.
func (p *Pool) evict(conns []*managedConn, target *managedConn) []*managedConn {
	out := conns[:0]
	for _, mc := range conns {
		if mc == target {
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictIdle removes conns idle past PoolOptions.IdleTimeout.
func (p *Pool) evictIdle(conns []*managedConn) []*managedConn {
	if p.opts.IdleTimeout <= 0 {
		return conns
	}
	now := time.Now()
	out := conns[:0]
	for _, mc := range conns {
		if mc.active == 0 && now.Sub(mc.lastUsed) > p.opts.IdleTimeout {
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDead removes conns whose IsAlive returns false.
func (p *Pool) evictDead(conns []*managedConn) []*managedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			_ = mc.c.Close()
			continue
		}
		out = append(out, mc)
	}
	return out
}

// sumActive sums active stream counts across conns.
func sumActive(conns []*managedConn) int {
	n := 0
	for _, mc := range conns {
		n += mc.active
	}
	return n
}

// acquire requests a managedConn from the actor. The returned mc's
// active count has already been incremented by the actor. Caller MUST
// eventually call p.release(mc, requestErr).
func (p *Pool) acquire(ctx context.Context) (*managedConn, error) {
	reply := make(chan acquireResp, 1)
	req := acquireReq{ctx: ctx, reply: reply}
	if p.opts.AcquireTimeout > 0 {
		req.deadline = time.Now().Add(p.opts.AcquireTimeout)
	}

	// Send the request.
	select {
	case p.acquireCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closedCh:
		return nil, ErrPoolClosed
	}

	// Wait for the reply or a timeout.
	var timeoutCh <-chan time.Time
	if p.opts.AcquireTimeout > 0 {
		t := time.NewTimer(p.opts.AcquireTimeout)
		defer t.Stop()
		timeoutCh = t.C
	}
	select {
	case resp := <-reply:
		return resp.mc, resp.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timeoutCh:
		return nil, ErrAcquireTimeout
	case <-p.closedCh:
		return nil, ErrPoolClosed
	}
}

// release returns mc to the actor with an optional request error.
// Non-nil reqErr causes the actor to re-check IsAlive and evict on
// failure.
func (p *Pool) release(mc *managedConn, reqErr error) {
	if mc == nil {
		return
	}
	select {
	case p.releaseCh <- releaseMsg{mc: mc, err: reqErr}:
	case <-p.closedCh:
	}
}

// close is the transport-interface form of Close.
func (p *Pool) close() error { return p.Close() }

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
