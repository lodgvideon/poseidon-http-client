// Package client — pool transport (Phase C.2).
package client

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// replyPool recycles buffered reply channels to avoid a heap allocation
// on every acquire call. Channels are drained before being returned so
// the next caller always starts with an empty channel.
var replyPool = sync.Pool{
	New: func() any { return make(chan acquireResp, 1) },
}

// statsReplyPool recycles buffered Stats reply channels for the same
// reason as replyPool. Stats() is on the observability path; not
// hot-hot, but a sync.Pool here keeps the alloc count flat under load
// scrapes from metrics endpoints.
var statsReplyPool = sync.Pool{
	New: func() any { return make(chan Stats, 1) },
}

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

	// DialTimeout bounds how long a single dial attempt may block in
	// conn.Dial. Without this bound a dial against a black-hole host
	// hangs the dialOne goroutine indefinitely, leaking it across
	// pool.Close. 0 → 30 * time.Second default.
	DialTimeout time.Duration
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
	// streamCap caches effectiveStreamCap(local, peer). Computed when the
	// dial completes and refreshed on every health-check tick so peer
	// SETTINGS_MAX_CONCURRENT_STREAMS changes are picked up. Without this
	// cache, pickLeastLoaded would take c.psMu.RLock() for every conn on
	// every acquire.
	streamCap int
}

// acquireReq is sent on Pool.acquireCh. The actor replies on reply.
type acquireReq struct {
	ctx   context.Context
	reply chan acquireResp
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
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 30 * time.Second
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
//
// Reply channel is sourced from statsReplyPool to keep this allocation-
// free. Recycling is safe because: (a) on the happy path we read the
// reply before returning, leaving the channel empty; (b) on closedCh
// the actor never received our reply chan so it cannot send on it.
func (p *Pool) Stats() Stats {
	reply := statsReplyPool.Get().(chan Stats)
	var stats Stats
	select {
	case p.statsCh <- reply:
		stats = <-reply
	case <-p.closedCh:
	}
	// Defensive drain in case something landed in the buffer between
	// recv and Put. Cheap insurance; expected to be a no-op.
	select {
	case <-reply:
	default:
	}
	statsReplyPool.Put(reply)
	return stats
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
			// No live capacity. Decide between dial / waiter / immediate ErrDialBackoff.
			// Count only LIVE conns so a dead conn that hasn't been evicted yet
			// doesn't block a replacement dial (BUG-1 regression guard).
			liveConns := countLive(conns)
			atCap := liveConns+inFlightDials >= p.opts.MaxConnsPerHost
			inBackoff := inDialBackoff(lastDialErrAt, p.opts.DialBackoff)

			if !atCap && !inBackoff {
				inFlightDials++
				go p.dialOne()
				waiters = append(waiters, req)
				continue
			}
			// At cap or in backoff: if backoff is the sole reason and there is
			// nothing already in flight, refuse fast so callers don't block on
			// a stalled pool. Otherwise queue and wait for capacity to free.
			if inBackoff && liveConns == 0 && inFlightDials == 0 {
				p.replyAcquire(req, nil, ErrDialBackoff)
				continue
			}
			waiters = append(waiters, req)

		case msg := <-p.releaseCh:
			msg.mc.active--
			msg.mc.lastUsed = time.Now()
			// Always evict dead conns regardless of msg.err. The transport
			// adapter passes nil err today, so a conn that died mid-request
			// (e.g. peer GOAWAY) would otherwise linger in the pool, blocking
			// canDial via len(conns) and forcing a wait until the next
			// HealthCheckPeriod tick (default 30s).
			if !msg.mc.c.IsAlive() {
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
			p.refreshStreamCap(dr.mc)
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
			for _, mc := range conns {
				p.refreshStreamCap(mc)
			}
			waiters = pruneExpiredWaiters(waiters)

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
//
// Reads mc.streamCap (cached) instead of taking c.psMu.RLock() per call.
// The cache is refreshed in the dialDoneCh handler and on every tick.
func (p *Pool) pickLeastLoaded(conns []*managedConn) *managedConn {
	var best *managedConn
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			continue
		}
		if mc.active >= mc.streamCap {
			continue
		}
		if best == nil || mc.active < best.active {
			best = mc
		}
	}
	return best
}

// refreshStreamCap recomputes mc.streamCap from the conn's current peer
// SETTINGS_MAX_CONCURRENT_STREAMS. Called by the actor on dial completion
// and on each tick.
func (p *Pool) refreshStreamCap(mc *managedConn) {
	mc.streamCap = effectiveStreamCap(p.opts.MaxStreamsPerConn, mc.c.PeerMaxConcurrentStreams())
}

// dialOne is the dial helper goroutine. It dials with a fresh
// background context so a cancelled waiter doesn't tear down a useful
// in-flight dial. A DialTimeout bound prevents the goroutine from
// leaking on a hung TCP connect, and a watchdog cancels the dial early
// if the pool is closed mid-dial.
func (p *Pool) dialOne() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if p.opts.DialTimeout > 0 {
		var dlCancel context.CancelFunc
		ctx, dlCancel = context.WithTimeout(ctx, p.opts.DialTimeout)
		defer dlCancel()
	}
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-p.closedCh:
			cancel()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	c, err := conn.Dial(ctx, p.addr, p.connOpts)
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

// countLive returns the number of conns whose underlying *conn.Conn
// reports IsAlive(). Used by the actor to gate canDial on live capacity
// only, not on stale dead-but-not-yet-evicted entries.
func countLive(conns []*managedConn) int {
	n := 0
	for _, mc := range conns {
		if mc.c.IsAlive() {
			n++
		}
	}
	return n
}

// pruneExpiredWaiters drops waiters whose ctx is already done. Reuses
// the slice's backing array to avoid allocation churn.
func pruneExpiredWaiters(ws []acquireReq) []acquireReq {
	out := ws[:0]
	for _, w := range ws {
		select {
		case <-w.ctx.Done():
			// Caller abandoned. Drop the waiter; no reply needed since
			// the caller has already returned ctx.Err() from acquire().
		default:
			out = append(out, w)
		}
	}
	return out
}

// acquire requests a managedConn from the actor. The returned mc's
// active count has already been incremented by the actor. Caller MUST
// eventually call p.release(mc, requestErr).
func (p *Pool) acquire(ctx context.Context) (*managedConn, error) {
	// Merge AcquireTimeout into ctx so that req.ctx.Done() fires on ALL
	// abandonment paths, including AcquireTimeout. This is required for
	// the sync.Pool channel optimisation: replyAcquire checks req.ctx.Done
	// before sending, so once ctx is cancelled the actor will not send on
	// reply. The drain in the defer is then guaranteed to see an empty
	// channel (or one already-buffered message from the rare race window),
	// making it safe to return the channel to replyPool.
	acquireTimeoutActive := false
	if p.opts.AcquireTimeout > 0 {
		deadline := time.Now().Add(p.opts.AcquireTimeout)
		// context.WithDeadline picks the earlier of parent deadline and ours.
		parentDl, hasParent := ctx.Deadline()
		if !hasParent || deadline.Before(parentDl) {
			acquireTimeoutActive = true
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	reply := replyPool.Get().(chan acquireResp)
	defer func() {
		// Drain any message left by the actor before returning to pool.
		// After acquire() exits, one of three states is guaranteed:
		//   (a) We already read the response (happy path) — channel empty.
		//   (b) Actor sent, caller abandoned — channel has one message.
		//   (c) Actor took ctx.Done arm in replyAcquire — channel empty.
		// In all cases a non-blocking drain leaves the channel clean.
		select {
		case <-reply:
		default:
		}
		replyPool.Put(reply)
	}()

	req := acquireReq{ctx: ctx, reply: reply}

	// Send the request to the actor.
	select {
	case p.acquireCh <- req:
	case <-ctx.Done():
		return nil, mapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		return nil, ErrPoolClosed
	}

	// Wait for the actor's reply.
	select {
	case resp := <-reply:
		return resp.mc, resp.err
	case <-ctx.Done():
		return nil, mapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		return nil, ErrPoolClosed
	}
}

// mapAcquireErr converts ctx.Err() to the right sentinel. If the
// deadline was introduced by AcquireTimeout (not the caller's own ctx),
// we return ErrAcquireTimeout to distinguish it from context.Canceled or
// a caller-supplied context.DeadlineExceeded.
func mapAcquireErr(ctx context.Context, acquireTimeoutActive bool) error {
	if acquireTimeoutActive && ctx.Err() == context.DeadlineExceeded {
		return ErrAcquireTimeout
	}
	return ctx.Err()
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


// inDialBackoff reports whether a previous dial error is still within
// the configured DialBackoff window. Returns false if no previous error
// or if window <= 0.
func inDialBackoff(lastErrAt time.Time, window time.Duration) bool {
	if lastErrAt.IsZero() || window <= 0 {
		return false
	}
	return time.Since(lastErrAt) < window
}

// effectiveStreamCap computes min(localCap, peerCap). Either may be
// zero meaning "unbounded". Returns 100 if both are unbounded.
func effectiveStreamCap(localCap, peerCap int) int {
	if localCap <= 0 && peerCap <= 0 {
		return 100
	}
	if localCap <= 0 {
		return peerCap
	}
	if peerCap <= 0 {
		return localCap
	}
	if peerCap < localCap {
		return peerCap
	}
	return localCap
}
