// Package client — pool transport (Phase C.2).
package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// defaultMaxConcurrentStreams is the effective concurrent-stream cap when
// neither the local config nor the peer advertises a limit. RFC 7540
// §6.5.2 recommends servers advertise this value; the default mirrors
// common server behaviour so the pool can make progress without an
// explicit SETTINGS frame.
const defaultMaxConcurrentStreams = 100

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
	// 0 → use peer value (or local defaultMaxConcurrentStreams if peer unbounded).
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
	// Populated by managedPool.Stats(); zero for single-address pools.
	Addresses        int // number of addresses in the current resolved set
	DrainingSubpools int // sub-pools currently draining (removed from resolver set)
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

	// hooksRef points at Client.hooks; nil-safe via Load. metrics is
	// shared with Client and other pools (managed sub-pools).
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
}

// newPool constructs a Pool and starts its actor goroutine. Internal:
// callers go through NewClient.
func newPool(addr string, connOpts conn.ConnOptions, opts PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *Pool {
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
	if metrics == nil {
		metrics = &Metrics{}
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
		hooksRef:   hooksRef,
		metrics:    metrics,
	}
	go p.run()
	return p
}

// Close stops the actor and closes all pooled conns. Idempotent. Returns once
// the actor has exited; a dial still in flight at Close is drained and its conn
// closed by a short-lived background goroutine, so that conn (and any
// OnConnClose hook for it) may complete shortly after Close returns. This keeps
// Close prompt even against a hung dial, whose ctx is cancelled on close.
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
// runState holds the mutable loop-local state of Pool.run. Kept in a
// struct so extracted handlers can receive it without the caller
// unpacking/packing individual variables on every iteration.
type runState struct {
	conns         []*managedConn
	waiters       []acquireReq
	inFlightDials int
	lastDialErrAt time.Time
}

func (p *Pool) run() {
	defer close(p.closedCh)
	rs := &runState{}
	tick := time.NewTicker(p.opts.HealthCheckPeriod)
	defer tick.Stop()

	for {
		select {
		case req := <-p.acquireCh:
			p.handleAcquire(rs, req)
		case msg := <-p.releaseCh:
			p.handleRelease(rs, msg)
		case dr := <-p.dialDoneCh:
			p.handleDialDone(rs, dr)
		case respCh := <-p.statsCh:
			p.handleStats(rs, respCh)
		case <-tick.C:
			p.handleTick(rs)
		case <-p.closeCh:
			p.handleClose(rs)
			return
		}
	}
}

// handleAcquire tries to serve the request from an existing conn.
// If no live capacity exists it decides between dial / queue / fast-refuse.
func (p *Pool) handleAcquire(rs *runState, req acquireReq) {
	mc := p.pickLeastLoaded(rs.conns)
	if mc != nil {
		mc.active++
		mc.lastUsed = time.Now()
		p.replyAcquire(req, mc, nil)
		return
	}
	liveConns := countLive(rs.conns)
	atCap := liveConns+rs.inFlightDials >= p.opts.MaxConnsPerHost
	inBackoff := inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff)

	if !atCap && !inBackoff {
		rs.inFlightDials++
		go p.dialOne()
		rs.waiters = append(rs.waiters, req)
		return
	}
	if inBackoff && liveConns == 0 && rs.inFlightDials == 0 {
		p.replyAcquire(req, nil, ErrDialBackoff)
		return
	}
	rs.waiters = append(rs.waiters, req)
}

// handleRelease decrements the conn's active count and evicts it
// if the underlying connection is no longer alive.
func (p *Pool) handleRelease(rs *runState, msg releaseMsg) {
	msg.mc.active--
	msg.mc.lastUsed = time.Now()
	if !msg.mc.c.IsAlive() {
		reason := CloseDead
		if msg.mc.c.GoAwayReceived() {
			reason = CloseGoAway
			p.metrics.Counters.GoAwaysReceived.Add(1)
		}
		rs.conns = p.evict(rs.conns, msg.mc, reason)
	}
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
}

// handleDialDone processes a completed dial: on success the conn
// enters the pool; on failure the first waiter receives the error.
func (p *Pool) handleDialDone(rs *runState, dr dialResult) {
	rs.inFlightDials--
	if dr.err != nil {
		rs.lastDialErrAt = time.Now()
		if len(rs.waiters) > 0 {
			req := rs.waiters[0]
			rs.waiters = rs.waiters[1:]
			p.replyAcquire(req, nil, dr.err)
		}
		return
	}
	p.refreshStreamCap(dr.mc)
	rs.conns = append(rs.conns, dr.mc)
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
}

// handleStats evicts dead conns silently and reports a snapshot.
func (p *Pool) handleStats(rs *runState, respCh chan<- Stats) {
	rs.conns = p.evictDeadSilent(rs.conns)
	respCh <- Stats{
		ActiveConns:     len(rs.conns),
		InFlightStreams: sumActive(rs.conns),
		Waiters:         len(rs.waiters),
		InFlightDials:   rs.inFlightDials,
	}
}

// handleTick runs periodic maintenance: idle eviction, dead
// eviction, stream-cap refresh, and waiter expiry.
func (p *Pool) handleTick(rs *runState) {
	rs.conns = p.evictIdle(rs.conns)
	rs.conns = p.evictDead(rs.conns)
	for _, mc := range rs.conns {
		p.refreshStreamCap(mc)
	}
	rs.waiters = pruneExpiredWaiters(rs.waiters)
}

// handleClose drains waiters and shuts down all connections.
func (p *Pool) handleClose(rs *runState) {
	for _, w := range rs.waiters {
		p.replyAcquire(w, nil, ErrPoolClosed)
	}
	rs.waiters = nil
	// Drain every in-flight dial asynchronously so Close returns promptly even
	// with a hung dial (the watchdog cancels it once closedCh closes, right
	// after this returns). Each outstanding dialOne delivers exactly one
	// result; Closing any completed conn here keeps it from being orphaned in
	// the buffered dialDoneCh (a conn + reader-goroutine + fd leak).
	if n := rs.inFlightDials; n > 0 {
		rs.inFlightDials = 0
		go func() {
			for i := 0; i < n; i++ {
				if dr := <-p.dialDoneCh; dr.mc != nil {
					_ = dr.mc.c.Close()
					p.notifyClose(CloseManual)
				}
			}
		}()
	}
	for _, mc := range rs.conns {
		reason := CloseManual
		if mc.c.GoAwayReceived() {
			reason = CloseGoAway
			p.metrics.Counters.GoAwaysReceived.Add(1)
		}
		_ = mc.c.Close()
		p.notifyClose(reason)
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

	dialStart := time.Now()
	p.metrics.Counters.DialsAttempted.Add(1)
	c, err := conn.Dial(ctx, p.addr, p.connOpts)
	dur := time.Since(dialStart)
	p.metrics.Latency.Dial.Observe(dur)
	if err != nil {
		p.metrics.Counters.DialsFailed.Add(1)
	}
	if hr := p.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnDial != nil {
			h.OnDial(DialEvent{Addr: p.addr, Err: err, Duration: dur})
		}
	}
	if err != nil {
		// Always deliver the result. Pool.Close drains every in-flight dial,
		// so this send never blocks forever, and the watchdog above already
		// cancels a hung dial's context when the pool closes.
		p.dialDoneCh <- dialResult{err: err}
		return
	}
	mc := &managedConn{c: c, dialedAt: time.Now(), lastUsed: time.Now()}
	// Always deliver; handleClose's drainer receives this and Closes the conn
	// if the pool shut down before the conn could be pooled, so it is never
	// orphaned in the buffered dialDoneCh.
	p.dialDoneCh <- dialResult{mc: mc}
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

// notifyClose increments ConnsClosed and fires OnConnClose.
func (p *Pool) notifyClose(reason CloseReason) {
	p.metrics.Counters.ConnsClosed.Add(1)
	if hr := p.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: p.addr, Reason: reason})
		}
	}
}

// evict removes target from conns, notifies close, and closes the conn.
func (p *Pool) evict(conns []*managedConn, target *managedConn, reason CloseReason) []*managedConn {
	out := conns[:0]
	for _, mc := range conns {
		if mc == target {
			_ = mc.c.Close()
			p.notifyClose(reason)
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
			p.notifyClose(CloseIdle)
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
			reason := CloseDead
			if mc.c.GoAwayReceived() {
				reason = CloseGoAway
				p.metrics.Counters.GoAwaysReceived.Add(1)
			}
			_ = mc.c.Close()
			p.notifyClose(reason)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDeadSilent removes conns whose IsAlive returns false without firing
// hooks or updating counters. Used from the Stats path where eviction is
// purely a bookkeeping cleanup, not a lifecycle event.
func (p *Pool) evictDeadSilent(conns []*managedConn) []*managedConn {
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
	start := time.Now()
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
	// recycle returns the reply channel to replyPool. It is ONLY safe to
	// call when the actor can no longer send on reply — otherwise a late
	// send from the actor would poison the channel for its next user
	// (a different Pool), surfacing as a spurious ErrPoolClosed or a
	// cross-pool conn ("stream reset by peer"). Two states are safe:
	//   (a) the actor never received req (first-select abandonment), or
	//   (b) we consumed the actor's single reply (happy path).
	// On second-select abandonment (ctx.Done / closedCh after the actor
	// took req) the actor may still call replyAcquire; we drop the channel
	// to GC rather than recycle. replyAcquire's send is buffered (cap 1)
	// so the late send never blocks and no goroutine leaks.
	recycle := func() {
		select {
		case <-reply:
		default:
		}
		replyPool.Put(reply)
	}

	req := acquireReq{ctx: ctx, reply: reply}

	// Send the request to the actor.
	select {
	case p.acquireCh <- req:
		// Actor owns req now; fall through to await its reply.
	case <-ctx.Done():
		recycle() // actor never received req — safe to recycle
		return nil, mapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		recycle() // actor never received req — safe to recycle
		return nil, ErrPoolClosed
	}

	// Wait for the actor's reply.
	select {
	case resp := <-reply:
		recycle() // consumed the actor's single send — safe to recycle
		if resp.err == nil {
			p.metrics.Latency.Acquire.Observe(time.Since(start))
		}
		return resp.mc, resp.err
	case <-ctx.Done():
		// Actor may still send on reply later — do NOT recycle.
		return nil, mapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		// Actor may still send on reply later — do NOT recycle.
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
		// Pool already closed: the actor is gone and won't process this
		// release, so close the conn directly rather than dropping it (a leak).
		// conn.Close is idempotent if handleClose already closed it.
		if mc.c != nil {
			_ = mc.c.Close()
		}
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
// zero meaning "unbounded". Returns defaultMaxConcurrentStreams if both
// are unbounded.
func effectiveStreamCap(localCap, peerCap int) int {
	if localCap <= 0 && peerCap <= 0 {
		return defaultMaxConcurrentStreams
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

// warmup pre-dials up to n conns in the background. Idempotent.
// n is capped at MaxConnsPerHost. Returns immediately; dial errors
// are surfaced via the OnDial hook.
func (p *Pool) warmup(n int) {
	if n <= 0 {
		return
	}
	stats := p.Stats()
	target := n
	if target > p.opts.MaxConnsPerHost {
		target = p.opts.MaxConnsPerHost
	}
	need := target - stats.ActiveConns - stats.InFlightDials
	if need <= 0 {
		return
	}
	for i := 0; i < need; i++ {
		// Submit a short-lived acquire that triggers a dial. If it resolves to
		// a conn within the window, release it immediately — acquire increments
		// the conn's active-stream count on success and the caller MUST release
		// it, or warmup leaks a phantom in-flight stream that blocks idle
		// eviction and graceful drain. If it times out, the dial continues in
		// dialOne's goroutine and the conn joins the pool later.
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		mc, err := p.acquire(ctx)
		cancel()
		if err == nil {
			p.release(mc, nil)
		}
	}
}
