// Package client — HTTP/3 connection pool (parallel to the HTTP/2 Pool).
//
// h3Pool is a per-host pool of QUIC connections (each an h3Client). It is a
// deliberate sibling of the HTTP/2 *Pool rather than a generalisation of it: the
// H2 pool actor is the highest-blast-radius component in the client and its tests
// construct managedConn{c: *conn.Conn} directly, so widening managedConn.c to an
// interface would churn the exact regression-gate suite. Keeping a separate actor
// leaves the H2 pool byte-for-byte unchanged.
//
// The two actors share what is genuinely protocol-agnostic — PoolOptions, Stats,
// effectiveStreamCap, inDialBackoff, mapAcquireErr, CloseReason, Hooks, Metrics —
// and diverge only where H3 differs:
//   - liveness is h3Client.Alive() (a QUIC reader-goroutine latch) rather than
//     conn.Conn.IsAlive();
//   - per-conn stream capacity is governed by PoolOptions.MaxStreamsPerConn (the
//     QUIC peer limit is enforced underneath by OpenStreamContext's stream-credit
//     backpressure, RFC 9000 §4.6), so effectiveStreamCap is called with a peer
//     value of 0 ("unbounded from the pool's view");
//   - a conn is retired either as CloseDead (the QUIC reader goroutine is gone)
//     or as CloseGoAway once a GOAWAY'd conn has drained; h3RetireReason is the
//     single spelling of that rule, and pickLeastLoaded / h3CountLive apply the
//     two earlier-firing variants (stop selecting it, stop counting it toward
//     MaxConnsPerHost so a replacement can be dialled while it finishes);
//   - the acquire reply channel is a fresh cap-1 channel per call rather than a
//     recycled sync.Pool channel: the H3 request rate through one QUIC conn is far
//     lower than an H2 stream open, the surrounding h3Exchange allocates anyway,
//     and a fresh channel sidesteps the reply-poisoning reasoning entirely.
package client

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"
)

// h3ManagedConn is the actor's per-conn record. Its MUTABLE fields — active,
// lastUsed, streamCap and the rest — are owned by the actor goroutine and
// must not be read or written anywhere else.
//
// The conn handle itself is the exception and is deliberately readable: it is
// set once when the dial completes and never reassigned, and the transports
// read it straight off the value acquire returns. This comment used to say
// the whole record was NEVER touched outside the actor, which is not true of
// that field and would send anyone unifying these pools looking for a lock
// that is not needed — or hiding a field the transport requires.
type h3ManagedConn struct {
	cl       h3Client
	active   int
	lastUsed time.Time
	dialedAt time.Time
	// streamCap caches effectiveStreamCap(local, 0). Computed on dial completion
	// and refreshed on each health-check tick. HTTP/3's per-conn cap is a pool
	// policy (MaxStreamsPerConn); the peer's QUIC stream limit is enforced
	// underneath by OpenStreamContext, so there is no per-conn peer value to read.
	streamCap int
}

// h3AcquireReq is sent on h3Pool.acquireCh. The actor replies on reply.
type h3AcquireReq struct {
	ctx   context.Context
	reply chan h3AcquireResp
}

// h3AcquireResp carries the reply from the actor for an h3AcquireReq.
type h3AcquireResp struct {
	mc  *h3ManagedConn
	err error
}

// h3ReleaseMsg is sent on h3Pool.releaseCh after a request completes.
type h3ReleaseMsg struct {
	mc *h3ManagedConn
}

// h3DialResult is sent by a dial helper goroutine on h3Pool.dialDoneCh.
type h3DialResult struct {
	mc  *h3ManagedConn
	err error
}

// h3Pool is a per-host pool of QUIC connections. Construct via NewClient with
// Transport=TransportH3Pool, or NewH3PoolClient.
type h3Pool struct {
	opts      PoolOptions
	addr      string
	tlsConfig *tls.Config

	// dialFn dials a new h3Client. Production is h3DialFn (http3.Dial); tests
	// substitute a fake so no live QUIC connection is required.
	dialFn func(ctx context.Context, addr string, tlsConfig *tls.Config) (h3Client, error)

	// channels
	acquireCh  chan h3AcquireReq
	releaseCh  chan h3ReleaseMsg
	warmupCh   chan int
	dialDoneCh chan h3DialResult
	statsCh    chan chan Stats
	closeCh    chan struct{}
	closedCh   chan struct{}

	closeOnce sync.Once

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
}

// newH3Pool constructs an h3Pool and starts its actor goroutine. Internal:
// callers go through NewClient / NewH3PoolClient.
func newH3Pool(addr string, tlsConfig *tls.Config, opts PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error), hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *h3Pool {
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
	if dialFn == nil {
		dialFn = h3DialFn
	}
	if metrics == nil {
		metrics = &Metrics{}
	}
	p := &h3Pool{
		opts:       opts,
		addr:       addr,
		tlsConfig:  tlsConfig,
		dialFn:     dialFn,
		acquireCh:  make(chan h3AcquireReq),
		releaseCh:  make(chan h3ReleaseMsg, 16),
		warmupCh:   make(chan int),
		dialDoneCh: make(chan h3DialResult, 4),
		statsCh:    make(chan chan Stats),
		closeCh:    make(chan struct{}),
		closedCh:   make(chan struct{}),
		hooksRef:   hooksRef,
		metrics:    metrics,
	}
	go p.run()
	return p
}

// Close stops the actor and closes all pooled conns. Idempotent. Returns once the
// actor has exited; a dial still in flight at Close is drained and its conn closed
// by a short-lived background goroutine, whose ctx is cancelled on close.
func (p *h3Pool) Close() error {
	p.closeOnce.Do(func() { close(p.closeCh) })
	<-p.closedCh
	return nil
}

// Stats returns a coherent snapshot of pool state. Safe to call concurrently.
// Returns the zero Stats if the pool is closed.
func (p *h3Pool) Stats() Stats {
	reply := make(chan Stats, 1)
	select {
	case p.statsCh <- reply:
		return <-reply
	case <-p.closedCh:
		return Stats{}
	}
}

// h3RunState holds the mutable loop-local state of h3Pool.run.
type h3RunState struct {
	conns         []*h3ManagedConn
	waiters       []h3AcquireReq
	inFlightDials int
	lastDialErrAt time.Time
}

func (p *h3Pool) run() {
	defer close(p.closedCh)
	rs := &h3RunState{}
	tick := time.NewTicker(p.opts.HealthCheckPeriod)
	defer tick.Stop()

	for {
		select {
		case req := <-p.acquireCh:
			p.handleAcquire(rs, req)
		case msg := <-p.releaseCh:
			p.handleRelease(rs, msg)
		case n := <-p.warmupCh:
			p.handleWarmup(rs, n)
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

// handleAcquire tries to serve the request from an existing conn, else decides
// between dial / queue / fast-refuse.
func (p *h3Pool) handleAcquire(rs *h3RunState, req h3AcquireReq) {
	mc := p.pickLeastLoaded(rs.conns)
	if mc != nil {
		mc.active++
		mc.lastUsed = time.Now()
		p.replyAcquire(req, mc, nil)
		return
	}
	liveConns := h3CountLive(rs.conns)
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

// handleRelease decrements the conn's active count and evicts it if the underlying
// QUIC connection is no longer alive.
func (p *h3Pool) handleRelease(rs *h3RunState, msg h3ReleaseMsg) {
	msg.mc.active--
	msg.mc.lastUsed = time.Now()
	// A GOAWAY'd conn is drained, not killed: pickLeastLoaded already stopped
	// handing it out, so once its last in-flight exchange releases there is nothing
	// left for it to finish (RFC 9114 §5.2).
	rs.conns, _ = p.h3RetireEvict(rs.conns, msg.mc)
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
	p.dialForWaiters(rs)
}

// handleDialDone processes a completed dial: on success the conn enters the pool;
// on failure the first waiter receives the error.
func (p *h3Pool) handleDialDone(rs *h3RunState, dr h3DialResult) {
	rs.inFlightDials--
	if dr.err != nil {
		rs.lastDialErrAt = time.Now()
		if len(rs.waiters) > 0 {
			req := rs.waiters[0]
			rs.waiters = rs.waiters[1:]
			p.replyAcquire(req, nil, dr.err)
		}
		// The waiters behind that one must not be left to a health-check tick.
		// Either start another dial, or — when the pool is in exactly the state that
		// makes handleAcquire fast-refuse a NEW request (dial backoff, nothing live,
		// nothing in flight) — refuse them for the same reason. Leaving them queued is
		// a priority inversion: a fresh acquire gets an immediate ErrDialBackoff while
		// an already-queued one waits a full HealthCheckPeriod. Mirrors h1Pool.
		p.dialForWaiters(rs)
		if rs.inFlightDials == 0 && h3CountLive(rs.conns) == 0 {
			for _, w := range rs.waiters {
				p.replyAcquire(w, nil, dr.err)
			}
			rs.waiters = nil
		}
		return
	}
	p.refreshStreamCap(dr.mc)
	rs.conns = append(rs.conns, dr.mc)
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
	p.dialForWaiters(rs)
}

// handleStats evicts dead conns silently and reports a snapshot.
func (p *h3Pool) handleStats(rs *h3RunState, respCh chan<- Stats) {
	// Deliberately no dialForWaiters here, unlike the four actor paths that can
	// change the picture: h1Pool does not dial from its stats path either, no
	// reachable strand goes through this one (retirement needs !Alive, whose
	// in-flight exchanges all release through handleRelease, or GoingAway with
	// active==0, which handleRelease reaches in the same call), and a read-only
	// scrape that opens a QUIC connection — possibly for a caller that has already
	// given up — is a side effect no metrics reader should pay for.
	rs.conns = p.evictDeadSilent(rs.conns)
	respCh <- Stats{
		ActiveConns:     len(rs.conns),
		InFlightStreams: h3SumActive(rs.conns),
		Waiters:         len(rs.waiters),
		InFlightDials:   rs.inFlightDials,
	}
}

// handleTick runs periodic maintenance: idle eviction, dead eviction, stream-cap
// refresh, and waiter expiry.
func (p *h3Pool) handleTick(rs *h3RunState) {
	// evictDead first: a conn that drained, then received GOAWAY, then idled out is
	// a GOAWAY close, and evictIdle would report it as CloseIdle and skip the
	// GoAwaysReceived counter.
	rs.conns = p.evictDead(rs.conns)
	rs.conns = p.evictIdle(rs.conns)
	for _, mc := range rs.conns {
		p.refreshStreamCap(mc)
	}
	rs.waiters = h3PruneExpiredWaiters(rs.waiters)
	p.dialForWaiters(rs)
}

// handleClose drains waiters and shuts down all connections.
func (p *h3Pool) handleClose(rs *h3RunState) {
	for _, w := range rs.waiters {
		p.replyAcquire(w, nil, ErrPoolClosed)
	}
	rs.waiters = nil
	// Drain every in-flight dial asynchronously so Close returns promptly even
	// with a hung dial (the watchdog cancels it once closedCh closes). Each
	// outstanding dialOne delivers exactly one result; Closing any completed conn
	// here keeps it from being orphaned in the buffered dialDoneCh.
	if n := rs.inFlightDials; n > 0 {
		rs.inFlightDials = 0
		go func() {
			for i := 0; i < n; i++ {
				if dr := <-p.dialDoneCh; dr.mc != nil {
					_ = dr.mc.cl.Close()
					p.notifyClose(CloseManual)
				}
			}
		}()
	}
	for _, mc := range rs.conns {
		_ = mc.cl.Close()
		p.notifyClose(CloseManual)
	}
}

// replyAcquire delivers the single reply owed to req. The send never blocks:
// reply is a cap-1 channel used by exactly one request, and the actor sends
// exactly one reply per request it accepts (happy path, serveWaiters,
// handleDialDone, h3PruneExpiredWaiters, or handleClose).
//
// This must NOT race the send against req.ctx.Done(). When a caller has given up
// AND its buffered reply channel is still writable, both cases are ready and Go
// picks at random; picking the send strands an mc whose active count the actor
// has already incremented in a channel nobody reads, so mc.active-- never runs
// and the stream slot is leaked for the life of the conn. Abandoning callers
// reclaim through acquire's reclaim goroutine instead — the only handoff that
// cannot drop a committed conn.
func (p *h3Pool) replyAcquire(req h3AcquireReq, mc *h3ManagedConn, err error) {
	req.reply <- h3AcquireResp{mc: mc, err: err}
}

// reclaim consumes the reply owed to an abandoned acquire and returns any conn
// the actor committed to it. Spawned only once the actor has accepted the
// request, at which point exactly one reply is guaranteed, so this receive
// always completes and the goroutine always exits.
func (p *h3Pool) reclaim(reply chan h3AcquireResp) {
	if resp := <-reply; resp.mc != nil {
		p.release(resp.mc)
	}
}

// pickLeastLoaded returns the live, under-cap mc with smallest active count, or
// nil if none qualifies. Reads mc.streamCap (cached) so it never blocks.
func (p *h3Pool) pickLeastLoaded(conns []*h3ManagedConn) *h3ManagedConn {
	var best *h3ManagedConn
	for _, mc := range conns {
		if !mc.cl.Alive() || mc.cl.GoingAway() {
			continue // dead, or GOAWAY'd and refusing every new request (§5.2)
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

// refreshStreamCap recomputes mc.streamCap from PoolOptions.MaxStreamsPerConn. The
// peer value is 0 because HTTP/3's per-conn stream limit is enforced by QUIC
// stream credit underneath, not surfaced as a pool-readable setting.
func (p *h3Pool) refreshStreamCap(mc *h3ManagedConn) {
	mc.streamCap = effectiveStreamCap(p.opts.MaxStreamsPerConn, 0)
}

// dialOne is the dial helper goroutine. It dials with a fresh background context
// so a cancelled waiter doesn't tear down a useful in-flight dial. A DialTimeout
// bound prevents the goroutine from leaking on a hung handshake, and a watchdog
// cancels the dial early if the pool is closed mid-dial.
func (p *h3Pool) dialOne() {
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
	cl, err := p.dialFn(ctx, p.addr, p.tlsConfig)
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
		// Wrapped for the same reason as the H2 pool: managedPool's failover
		// and the retry classifier both key on *DialError, and a raw error
		// disables both silently.
		p.dialDoneCh <- h3DialResult{err: &DialError{Addr: p.addr, Err: err}}
		return
	}
	mc := &h3ManagedConn{cl: cl, dialedAt: time.Now(), lastUsed: time.Now()}
	p.dialDoneCh <- h3DialResult{mc: mc}
}

// serveWaiters hands as many waiters as possible a live mc.
func (p *h3Pool) serveWaiters(conns []*h3ManagedConn, waiters []h3AcquireReq) []h3AcquireReq {
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
func (p *h3Pool) notifyClose(reason CloseReason) {
	p.metrics.Counters.ConnsClosed.Add(1)
	if hr := p.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: p.addr, Reason: reason})
		}
	}
}

// evict removes target from conns, notifies close, and closes the conn.
func (p *h3Pool) evict(conns []*h3ManagedConn, target *h3ManagedConn, reason CloseReason) []*h3ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if mc == target {
			_ = mc.cl.Close()
			p.notifyClose(reason)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictIdle removes conns idle past PoolOptions.IdleTimeout.
func (p *h3Pool) evictIdle(conns []*h3ManagedConn) []*h3ManagedConn {
	if p.opts.IdleTimeout <= 0 {
		return conns
	}
	now := time.Now()
	out := conns[:0]
	for _, mc := range conns {
		if mc.active == 0 && now.Sub(mc.lastUsed) > p.opts.IdleTimeout {
			_ = mc.cl.Close()
			p.notifyClose(CloseIdle)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDead removes retired conns: dead, or GOAWAY'd and drained.
func (p *h3Pool) evictDead(conns []*h3ManagedConn) []*h3ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if reason, retire := h3RetireReason(mc); retire {
			_ = mc.cl.Close()
			if reason == CloseGoAway {
				p.metrics.Counters.GoAwaysReceived.Add(1)
			}
			p.notifyClose(reason)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDeadSilent removes retired conns from the Stats path without firing hooks —
// a metrics scrape must not re-enter caller code. It DOES move the counters: since
// GOAWAY made eviction routine, a drained conn is often first observed here rather
// than on the health tick, and a close whose visibility depends on who looked first
// is worse than no counter at all.
func (p *h3Pool) evictDeadSilent(conns []*h3ManagedConn) []*h3ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if reason, retire := h3RetireReason(mc); retire {
			_ = mc.cl.Close()
			p.metrics.Counters.ConnsClosed.Add(1)
			if reason == CloseGoAway {
				p.metrics.Counters.GoAwaysReceived.Add(1)
			}
			continue
		}
		out = append(out, mc)
	}
	return out
}

// dialForWaiters starts a dial whenever a waiter is parked and the pool has room.
// Without it a waiter parked BEFORE a conn was retired is never rescued:
// handleAcquire is the only other dialer, and it does not run again for an
// already-parked request, so with no AcquireTimeout (which newH3Pool does not
// default) the caller waits forever.
//
// Call it UNCONDITIONALLY from every actor path that can change the picture —
// release, both arms of dial-done, and tick — the same four sites
// h1Pool.ensureDialForWaiters is called from.
// Gating it on "this call evicted something" is what turned a recoverable stall
// into a permanent one twice: a dial that failed inside the backoff window, and a
// tick that shrinks nothing because the pool is already empty, both leave waiters
// with no dialer. Respects MaxConnsPerHost and the dial backoff exactly as
// handleAcquire does. Actor goroutine only.
func (p *h3Pool) dialForWaiters(rs *h3RunState) {
	if len(rs.waiters) == 0 || inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {
		return
	}
	if h3CountLive(rs.conns)+rs.inFlightDials >= p.opts.MaxConnsPerHost {
		return
	}
	rs.inFlightDials++
	go p.dialOne()
}

// h3SumActive sums active stream counts across conns.
func h3SumActive(conns []*h3ManagedConn) int {
	n := 0
	for _, mc := range conns {
		n += mc.active
	}
	return n
}

// h3CountLive returns the number of conns whose h3Client reports Alive().
func h3CountLive(conns []*h3ManagedConn) int {
	n := 0
	for _, mc := range conns {
		// A GOAWAY'd conn is draining, not live: it can serve no new request, so
		// counting it toward MaxConnsPerHost would keep the pool at cap and park
		// every acquire as a waiter with no dial started (RFC 9114 §5.2). The trade
		// is deliberate and matches the HTTP/2 pool: MaxConnsPerHost bounds
		// SELECTABLE conns, not open sockets, so a long-lived DoStream on a drained
		// conn is not counted while its replacement is dialled.
		if mc.cl.Alive() && !mc.cl.GoingAway() {
			n++
		}
	}
	return n
}

// h3RetireReason reports whether a conn should leave the pool and why: the QUIC
// connection is gone (CloseDead), or the peer sent GOAWAY and the exchanges it
// undertook to finish have all drained (CloseGoAway). Every eviction site shares
// it — the HTTP/2 pool carries its equivalent at five sites, and an H3 site that
// forgets it strands a GOAWAY'd conn in the pool forever.
//
// Two neighbouring rules are deliberately NOT this one, because they fire earlier:
// pickLeastLoaded skips a GOAWAY'd conn even while it still has work, and
// h3CountLive stops counting it toward MaxConnsPerHost so a replacement can be
// dialled while it drains.
//
// The CloseDead arm has no active==0 guard, unlike the HTTP/2 pool's: Alive is
// false only once the QUIC reader goroutine is gone, at which point the in-flight
// exchanges are already doomed and holding the conn open buys nothing.
func h3RetireReason(mc *h3ManagedConn) (CloseReason, bool) {
	switch {
	case !mc.cl.Alive():
		return CloseDead, true
	case mc.cl.GoingAway() && mc.active == 0:
		return CloseGoAway, true
	}
	return 0, false
}

// h3RetireEvict evicts mc if it is retired, counting a drained GOAWAY as such.
// Returns the (possibly shrunk) slice and whether an eviction happened; callers
// dial for waiters unconditionally, so the flag is informational.
func (p *h3Pool) h3RetireEvict(conns []*h3ManagedConn, mc *h3ManagedConn) ([]*h3ManagedConn, bool) {
	reason, retire := h3RetireReason(mc)
	if !retire {
		return conns, false
	}
	if reason == CloseGoAway {
		p.metrics.Counters.GoAwaysReceived.Add(1)
	}
	return p.evict(conns, mc, reason), true
}

// h3PruneExpiredWaiters drops waiters whose ctx is already done, reusing the
// slice's backing array. Each dropped waiter is still sent the one reply it is
// owed: its caller's reclaim goroutine is blocked waiting for exactly one value,
// and the send cannot block (cap-1, single use).
func h3PruneExpiredWaiters(ws []h3AcquireReq) []h3AcquireReq {
	out := ws[:0]
	for _, w := range ws {
		select {
		case <-w.ctx.Done():
			w.reply <- h3AcquireResp{err: w.ctx.Err()}
		default:
			out = append(out, w)
		}
	}
	return out
}

// acquire requests an h3ManagedConn from the actor. The returned mc's active count
// has already been incremented by the actor. Caller MUST eventually call
// p.release(mc).
func (p *h3Pool) acquire(ctx context.Context) (*h3ManagedConn, error) {
	start := time.Now()
	acquireTimeoutActive := false
	if p.opts.AcquireTimeout > 0 {
		deadline := time.Now().Add(p.opts.AcquireTimeout)
		parentDl, hasParent := ctx.Deadline()
		if !hasParent || deadline.Before(parentDl) {
			acquireTimeoutActive = true
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	// Fresh cap-1 reply channel per acquire (see package doc): replyAcquire's send
	// never blocks, and no channel is shared across acquires, so there is no
	// cross-pool poisoning to guard against.
	reply := make(chan h3AcquireResp, 1)
	req := h3AcquireReq{ctx: ctx, reply: reply}

	select {
	case p.acquireCh <- req:
		// The actor now owns req and owes it exactly one reply.
	case <-ctx.Done():
		return nil, mapAcquireErr(ctx, acquireTimeoutActive) // never accepted; nothing to reclaim
	case <-p.closedCh:
		return nil, ErrPoolClosed
	}

	select {
	case resp := <-reply:
		if resp.err == nil {
			p.metrics.Latency.Acquire.Observe(time.Since(start))
		}
		return resp.mc, resp.err
	case <-ctx.Done():
		// Both this case and the reply case can be ready at once (the actor may
		// have just committed a conn to us); Go would pick between them at random.
		// Hand the pending reply to reclaim so a conn committed to an acquire we
		// are abandoning goes back to the pool instead of being stranded.
		go p.reclaim(reply)
		return nil, mapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		go p.reclaim(reply)
		return nil, ErrPoolClosed
	}
}

// release returns mc to the actor. It carries no request error: handleRelease
// re-checks Alive unconditionally, which is the safer rule — see Pool.release.
func (p *h3Pool) release(mc *h3ManagedConn) {
	if mc == nil {
		return
	}
	select {
	case p.releaseCh <- h3ReleaseMsg{mc: mc}:
	case <-p.closedCh:
		// Pool already closed: the actor is gone, so close the conn directly
		// rather than leaking it. h3Client.Close is idempotent.
		if mc.cl != nil {
			_ = mc.cl.Close()
		}
	}
}

// warmup pre-dials up to n conns in the background. Idempotent. n is capped at
// MaxConnsPerHost. Returns immediately; dial errors surface via the OnDial hook.
func (p *h3Pool) warmup(n int) {
	if n <= 0 {
		return
	}
	select {
	case p.warmupCh <- n:
	case <-p.closedCh:
	}
}

// handleWarmup starts the dials that bring the pool up to n connections. Runs on
// the actor goroutine and makes the same capacity and backoff decision
// handleAcquire makes, minus the waiter — see Pool.handleWarmup for why an
// acquire+release loop cannot express this on a multiplexed pool.
func (p *h3Pool) handleWarmup(rs *h3RunState, n int) {
	target := n
	if target > p.opts.MaxConnsPerHost {
		target = p.opts.MaxConnsPerHost
	}
	need := target - h3CountLive(rs.conns) - rs.inFlightDials
	if need <= 0 {
		return
	}
	if inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {
		return // a warmup must not defeat the backoff a failing peer earned
	}
	for i := 0; i < need; i++ {
		rs.inFlightDials++
		go p.dialOne()
	}
}
