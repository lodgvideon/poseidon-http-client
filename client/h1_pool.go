// Package client — HTTP/1.1 connection pool (parallel to the HTTP/2 Pool and the
// HTTP/3 h3Pool).
//
// h1Pool is a per-host pool of HTTP/1.1 connections. It shares the actor shape of
// its H2/H3 siblings — one goroutine owns the conn list, waiters, and dial
// bookkeeping; callers talk to it over channels — but it is a fundamentally
// different *kind* of pool, because HTTP/1.1 has no multiplexing.
//
// Exclusive checkout, not least-loaded sharing:
//   - The H2/H3 pools hand the same connection to many concurrent streams and pick
//     the least-loaded conn. HTTP/1.1 carries exactly ONE request/response exchange
//     per connection at a time (no pipelining — see internal/http1/conn.go), so this
//     pool takes a conn OUT of the idle set for the exclusive duration of an
//     exchange and puts it back on release.
//   - Consequently PoolOptions.MaxConnsPerHost IS the concurrency limit: it is the
//     only knob that matters here. PoolOptions.MaxStreamsPerConn is MEANINGLESS for
//     HTTP/1.1 and is ignored — the per-conn cap is always 1. (PoolOptions is shared
//     with the H2/H3 pools, hence the dead field rather than a separate struct.)
//   - At the cap with every conn busy, an acquire BLOCKS in the waiter queue until a
//     conn frees or ctx is done. It never serializes a second exchange onto a busy
//     conn and never dials past MaxConnsPerHost.
//
// Keep-alive is the other H1-specific axis: release carries a keepAlive flag rather
// than the H2/H3 "actor re-checks liveness" contract. The flag comes from
// http1.Exchange.KeepAlive() (false for "Connection: close", HTTP/1.0 without
// keep-alive, or a peer-closed socket) and from the transport on any exchange
// error. A conn released with keepAlive=false is closed and evicted instead of
// being handed to the next caller — a poisoned or half-read connection must never
// be reused. The actor additionally re-checks IsAlive on release, so a socket that
// died underneath a nominally keep-alive exchange is still evicted.
//
// Idle eviction and health-check sweeps match the other pools' behaviour.
package client

import (
	"sync"
	"sync/atomic"
	"time"

	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// h1ConnStreamCap is the per-connection exchange cap for HTTP/1.1: exactly one.
// HTTP/1.1 has no multiplexing and this client deliberately does not implement
// pipelining, so a checked-out conn is busy until released.
const h1ConnStreamCap = 1

// h1ProbeIdleAfter is how long a pooled connection must have sat idle before a
// checkout probes it.
//
// Under load connections are reused in microseconds and never reach it, so the
// probe costs a loaded pool nothing; it fires only on a connection that genuinely
// sat idle, which is the only window in which a peer can close it or push an
// unsolicited response at it. The probe itself runs off the actor goroutine (see
// acquire), so even then it does not serialise the pool.
const h1ProbeIdleAfter = 250 * time.Millisecond

// h1ManagedConn is the actor's per-conn record. NEVER touched outside the actor
// goroutine.
type h1ManagedConn struct {
	c        *http1.Conn
	active   int // 0 or 1 — see h1ConnStreamCap
	lastUsed time.Time
	dialedAt time.Time
}

// h1AcquireReq is sent on h1Pool.acquireCh. The actor replies on reply.
type h1AcquireReq struct {
	ctx   context.Context
	reply chan h1AcquireResp
}

// h1AcquireResp carries the reply from the actor for an h1AcquireReq.
type h1AcquireResp struct {
	mc  *h1ManagedConn
	err error
}

// h1ReleaseMsg is sent on h1Pool.releaseCh when an exchange completes.
// keepAlive=false means the conn must be discarded rather than pooled.
type h1ReleaseMsg struct {
	mc        *h1ManagedConn
	keepAlive bool
}

// h1DialResult is sent by a dial helper goroutine on h1Pool.dialDoneCh.
type h1DialResult struct {
	mc  *h1ManagedConn
	err error
}

// h1Pool is a per-host pool of HTTP/1.1 connections with exclusive checkout.
// Construct via NewClient with Transport=TransportH1Pool, or NewH1PoolClient.
type h1Pool struct {
	opts PoolOptions
	addr string

	// dialer establishes the underlying transport (TCP, or TLS whose ALPN does
	// not assert "h2"). It is the test seam: tests pass a fake conn.Dialer, so no
	// live server is required.
	dialer conn.Dialer

	// channels
	acquireCh  chan h1AcquireReq
	releaseCh  chan h1ReleaseMsg
	dialDoneCh chan h1DialResult
	statsCh    chan chan Stats
	closeCh    chan struct{}
	closedCh   chan struct{}

	closeOnce sync.Once

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics
}

// newH1Pool constructs an h1Pool and starts its actor goroutine. Internal:
// callers go through NewClient / NewH1PoolClient.
func newH1Pool(addr string, dialer conn.Dialer, opts PoolOptions, hooksRef *atomic.Pointer[Hooks], metrics *Metrics) *h1Pool {
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
	p := &h1Pool{
		opts:       opts,
		addr:       addr,
		dialer:     dialer,
		acquireCh:  make(chan h1AcquireReq),
		releaseCh:  make(chan h1ReleaseMsg, 16),
		dialDoneCh: make(chan h1DialResult, 4),
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
func (p *h1Pool) Close() error {
	p.closeOnce.Do(func() { close(p.closeCh) })
	<-p.closedCh
	return nil
}

// Stats returns a coherent snapshot of pool state. Safe to call concurrently.
// Returns the zero Stats if the pool is closed. InFlightStreams counts checked-out
// conns — for HTTP/1.1 an in-flight "stream" is one exclusive exchange.
func (p *h1Pool) Stats() Stats {
	reply := make(chan Stats, 1)
	select {
	case p.statsCh <- reply:
		return <-reply
	case <-p.closedCh:
		return Stats{}
	}
}

// h1RunState holds the mutable loop-local state of h1Pool.run.
type h1RunState struct {
	conns         []*h1ManagedConn
	waiters       []h1AcquireReq
	inFlightDials int
	lastDialErrAt time.Time
}

func (p *h1Pool) run() {
	defer close(p.closedCh)
	rs := &h1RunState{}
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

// handleAcquire checks out an idle conn if one exists, else decides between
// dial / queue / fast-refuse. Queuing is what makes a caller at the cap wait for a
// conn to free rather than doubling up on a busy one.
func (p *h1Pool) handleAcquire(rs *h1RunState, req h1AcquireReq) {
	mc := p.pickIdle(rs.conns)
	if mc != nil {
		mc.active++
		// lastUsed is NOT stamped here: it has to mean "idle since", which is what
		// the checkout probe in acquire keys on. Release stamps it.
		p.replyAcquire(req, mc, nil)
		return
	}
	liveConns := h1CountLive(rs.conns)
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
	// At cap (or dialing) with live conns: queue. The waiter is served by
	// handleRelease as soon as an exchange completes.
	rs.waiters = append(rs.waiters, req)
}

// handleRelease returns a conn to the idle set, or discards it. A conn is
// discarded when the exchange said the connection will not persist
// (keepAlive=false: "Connection: close", HTTP/1.0, a peer-closed socket, or any
// exchange error) or when the socket is no longer alive. Anything else would hand
// the next caller a poisoned or closing connection.
func (p *h1Pool) handleRelease(rs *h1RunState, msg h1ReleaseMsg) {
	msg.mc.active--
	msg.mc.lastUsed = time.Now()
	if !msg.keepAlive {
		rs.conns = p.evict(rs.conns, msg.mc, CloseManual)
	} else if !msg.mc.c.IsAlive() {
		rs.conns = p.evict(rs.conns, msg.mc, CloseDead)
	}
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
	p.ensureDialForWaiters(rs)
}

// ensureDialForWaiters starts a dial when queued waiters have nothing left to
// wait for.
//
// serveWaiters can only hand out connections that already exist, and eviction
// routinely removes the last one — a server "Connection: close" is the ordinary
// trigger, not an error path. Without this the state {no live conns, no
// in-flight dials, queued waiters} is TERMINAL: nothing in the pool ever wakes
// those waiters again and each sits until its own timeout, which is a liveness
// bug rather than a slow path. handleAcquire has always made this decision for a
// new request; the release and tick paths have to make it too.
func (p *h1Pool) ensureDialForWaiters(rs *h1RunState) {
	if len(rs.waiters) == 0 {
		return
	}
	if h1CountLive(rs.conns)+rs.inFlightDials >= p.opts.MaxConnsPerHost {
		return
	}
	if inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {
		return
	}
	rs.inFlightDials++
	go p.dialOne()
}

// handleDialDone processes a completed dial: on success the conn enters the pool;
// on failure the first waiter receives the error.
func (p *h1Pool) handleDialDone(rs *h1RunState, dr h1DialResult) {
	rs.inFlightDials--
	if dr.err != nil {
		rs.lastDialErrAt = time.Now()
		if len(rs.waiters) > 0 {
			req := rs.waiters[0]
			rs.waiters = rs.waiters[1:]
			p.replyAcquire(req, nil, dr.err)
		}
		// The waiters behind that one must not be left to a health-check tick.
		// Either start another dial, or — when the pool is in exactly the state
		// that makes handleAcquire fast-refuse a NEW request (dial backoff, nothing
		// live, nothing in flight) — refuse them for the same reason. Leaving them
		// queued was a priority inversion: a fresh acquire got an immediate
		// ErrDialBackoff while an already-queued one waited a full
		// HealthCheckPeriod, so a burst against a downed host drained one request
		// per tick.
		p.ensureDialForWaiters(rs)
		if rs.inFlightDials == 0 && h1CountLive(rs.conns) == 0 {
			for _, w := range rs.waiters {
				p.replyAcquire(w, nil, dr.err)
			}
			rs.waiters = nil
		}
		return
	}
	rs.conns = append(rs.conns, dr.mc)
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
	p.ensureDialForWaiters(rs)
}

// handleStats evicts dead conns silently and reports a snapshot.
func (p *h1Pool) handleStats(rs *h1RunState, respCh chan<- Stats) {
	rs.conns = p.evictDeadSilent(rs.conns)
	respCh <- Stats{
		ActiveConns:     len(rs.conns),
		InFlightStreams: h1SumActive(rs.conns),
		Waiters:         len(rs.waiters),
		InFlightDials:   rs.inFlightDials,
	}
}

// handleTick runs periodic maintenance: idle eviction, dead eviction, and waiter
// expiry. There is no stream-cap refresh: the HTTP/1.1 per-conn cap is the
// constant h1ConnStreamCap.
func (p *h1Pool) handleTick(rs *h1RunState) {
	rs.conns = p.evictIdle(rs.conns)
	rs.conns = p.evictDead(rs.conns)
	rs.waiters = h1PruneExpiredWaiters(rs.waiters)
	p.ensureDialForWaiters(rs)
}

// handleClose drains waiters and shuts down all connections.
func (p *h1Pool) handleClose(rs *h1RunState) {
	for _, w := range rs.waiters {
		p.replyAcquire(w, nil, ErrPoolClosed)
	}
	rs.waiters = nil
	// Drain every in-flight dial asynchronously so Close returns promptly even
	// with a hung dial (the watchdog cancels it once closedCh closes). Each
	// outstanding dialOne delivers exactly one result; closing any completed conn
	// here keeps it from being orphaned in the buffered dialDoneCh.
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
		_ = mc.c.Close()
		p.notifyClose(CloseManual)
	}
}

// replyAcquire delivers the single reply owed to req. The send never blocks: reply
// is a cap-1 channel used by exactly one request, and the actor sends exactly one
// reply per request it accepts.
//
// Unlike its H2/H3 siblings this does NOT race the send against req.ctx.Done().
// Doing so is unsafe for an exclusive-checkout pool: when a caller has given up
// AND its buffered reply channel is still writable, both select cases are ready
// and Go picks at random. Picking the send strands mc in a channel nobody reads —
// its active count is never decremented, so the conn is leaked and, because
// MaxConnsPerHost is this pool's whole concurrency budget, every later caller
// starves. Abandoning callers instead reclaim through acquire's reclaim goroutine,
// which is the only handoff that cannot drop a committed conn.
func (p *h1Pool) replyAcquire(req h1AcquireReq, mc *h1ManagedConn, err error) {
	req.reply <- h1AcquireResp{mc: mc, err: err}
}

// reclaim consumes the reply owed to an abandoned acquire and returns any conn the
// actor committed to it. Spawned only once the actor has accepted the request, at
// which point exactly one reply is guaranteed — from serveWaiters, handleDialDone,
// h1PruneExpiredWaiters, or handleClose — so this receive always completes and the
// goroutine always exits.
func (p *h1Pool) reclaim(reply chan h1AcquireResp) {
	if resp := <-reply; resp.mc != nil {
		p.release(resp.mc, true)
	}
}

// pickIdle returns a live conn that is not currently carrying an exchange, or nil
// if every conn is busy or dead. This is the exclusive checkout: with a per-conn
// cap of one, "least loaded" degenerates to "idle", and a busy conn is never a
// candidate.
func (p *h1Pool) pickIdle(conns []*h1ManagedConn) *h1ManagedConn {
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			continue
		}
		if mc.active >= h1ConnStreamCap {
			continue
		}
		return mc
	}
	return nil
}

// dialOne is the dial helper goroutine. It dials with a fresh background context
// so a cancelled waiter doesn't tear down a useful in-flight dial. A DialTimeout
// bound prevents the goroutine from leaking on a hung TCP connect, and a watchdog
// cancels the dial early if the pool is closed mid-dial.
func (p *h1Pool) dialOne() {
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
	nc, err := p.dialer.Dial(ctx, p.addr)
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
		p.dialDoneCh <- h1DialResult{err: &DialError{Addr: p.addr, Err: err}}
		return
	}
	mc := &h1ManagedConn{c: http1.NewConn(nc), dialedAt: time.Now(), lastUsed: time.Now()}
	p.dialDoneCh <- h1DialResult{mc: mc}
}

// serveWaiters hands as many queued waiters as possible an idle conn.
func (p *h1Pool) serveWaiters(conns []*h1ManagedConn, waiters []h1AcquireReq) []h1AcquireReq {
	for len(waiters) > 0 {
		mc := p.pickIdle(conns)
		if mc == nil {
			return waiters
		}
		mc.active++
		req := waiters[0]
		waiters = waiters[1:]
		p.replyAcquire(req, mc, nil)
	}
	return waiters
}

// notifyClose increments ConnsClosed and fires OnConnClose.
func (p *h1Pool) notifyClose(reason CloseReason) {
	p.metrics.Counters.ConnsClosed.Add(1)
	if hr := p.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: p.addr, Reason: reason})
		}
	}
}

// evict removes target from conns, notifies close, and closes the conn.
func (p *h1Pool) evict(conns []*h1ManagedConn, target *h1ManagedConn, reason CloseReason) []*h1ManagedConn {
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
func (p *h1Pool) evictIdle(conns []*h1ManagedConn) []*h1ManagedConn {
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

// evictDead removes conns whose IsAlive returns false, and — in this periodic
// sweep only — idle conns the peer has closed or written unsolicited data on.
// The socket probe runs here rather than at checkout so the per-request acquire
// path stays syscall-free; it touches only checked-in conns (active == 0), where
// no exchange reader is running, so reading through the bufio reader is safe.
// RFC 9112 §9.6 (monitor idle connections) / RFC 9110 (idle-arriving data is not
// a valid response, so evict rather than let the next request consume it).
func (p *h1Pool) evictDead(conns []*h1ManagedConn) []*h1ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		dead := !mc.c.IsAlive()
		if !dead && mc.active == 0 {
			dead = !mc.c.ProbeIdle()
		}
		if dead {
			_ = mc.c.Close()
			p.notifyClose(CloseDead)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDeadSilent removes conns whose IsAlive returns false without firing hooks
// or updating counters. Used from the Stats path where eviction is bookkeeping.
func (p *h1Pool) evictDeadSilent(conns []*h1ManagedConn) []*h1ManagedConn {
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

// h1SumActive sums checked-out conns (one exchange each).
func h1SumActive(conns []*h1ManagedConn) int {
	n := 0
	for _, mc := range conns {
		n += mc.active
	}
	return n
}

// h1CountLive returns the number of conns whose socket is still alive.
func h1CountLive(conns []*h1ManagedConn) int {
	n := 0
	for _, mc := range conns {
		if mc.c.IsAlive() {
			n++
		}
	}
	return n
}

// h1PruneExpiredWaiters drops waiters whose ctx is already done, reusing the
// slice's backing array. Each dropped waiter is still sent the one reply it is
// owed: its caller's reclaim goroutine is blocked waiting for exactly one value,
// and the send cannot block (cap-1, single use).
func h1PruneExpiredWaiters(ws []h1AcquireReq) []h1AcquireReq {
	out := ws[:0]
	for _, w := range ws {
		select {
		case <-w.ctx.Done():
			w.reply <- h1AcquireResp{err: w.ctx.Err()}
		default:
			out = append(out, w)
		}
	}
	return out
}

// acquire checks out a connection exclusively for one exchange. It blocks until a
// conn is free, the pool dials one, ctx is done, or the pool closes. The returned
// mc is NOT shared with any other caller until released. Caller MUST eventually
// call p.release(mc, keepAlive).
// acquire checks out a connection, probing one that has been idle long enough to
// have gone bad before handing it to the caller.
//
// The probe runs HERE, not inside pickIdle, because it blocks for up to its read
// deadline and the actor is a single goroutine: probing there would serialise
// every acquire in the pool behind one syscall. Out here the cost is per-request
// latency on a connection that was idle anyway, and concurrent acquires pay it in
// parallel.
//
// It is what closes the response-queue poisoning that the completion-time check
// structurally cannot see: octets that arrive AFTER a response was fully read
// are not in the reader, so only asking the socket at checkout finds them.
func (p *h1Pool) acquire(ctx context.Context) (*h1ManagedConn, error) {
	// Bounded: each rejected connection is evicted, so the pool cannot hand out
	// the same bad one twice, and one extra attempt per possible conn is enough.
	for attempt := 0; attempt <= p.opts.MaxConnsPerHost; attempt++ {
		mc, err := p.acquireOnce(ctx)
		if err != nil || mc == nil {
			return mc, err
		}
		// HasResidue is unconditional; ProbeIdle stays gated on idle age.
		//
		// The gate used to cover both, and that was the bug: a connection reused
		// inside h1ProbeIdleAfter was handed back with NO check at all, so a peer
		// that appended an unsolicited response just had to land it inside the
		// reuse gap. Under the load this client is built for that gap is every
		// reuse, which made the threshold an attacker-chosen window rather than a
		// race. Removing the gate outright was not an option — ProbeIdle costs a
		// bounded ~1ms, i.e. more than the request it guards.
		//
		// HasResidue answers the poisoning question directly and costs ~0.5µs with
		// no allocation, so it runs every time. ProbeIdle keeps the threshold and
		// keeps its own job: it is the one that notices a FIN, which FIONREAD
		// cannot report.
		if !mc.c.HasResidue() && (time.Since(mc.lastUsed) <= h1ProbeIdleAfter || mc.c.ProbeIdle()) {
			return mc, nil
		}
		// Peer closed it, or pushed bytes at it, while it sat idle. Give it back
		// as non-reusable so the actor evicts it, and ask again.
		p.release(mc, false)
	}
	return p.acquireOnce(ctx)
}

func (p *h1Pool) acquireOnce(ctx context.Context) (*h1ManagedConn, error) {
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

	// Fresh cap-1 reply channel per acquire (as in h3Pool): replyAcquire's send
	// never blocks and no channel is shared across acquires, so there is no
	// cross-pool reply poisoning to guard against.
	reply := make(chan h1AcquireResp, 1)
	req := h1AcquireReq{ctx: ctx, reply: reply}

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

// release returns mc to the pool. keepAlive=false discards the connection instead
// of pooling it — pass false whenever http1.Exchange.KeepAlive() is false or the
// exchange errored. Must be called exactly once per acquire.
func (p *h1Pool) release(mc *h1ManagedConn, keepAlive bool) {
	if mc == nil {
		return
	}
	select {
	case p.releaseCh <- h1ReleaseMsg{mc: mc, keepAlive: keepAlive}:
	case <-p.closedCh:
		// Pool already closed: the actor is gone, so close the conn directly
		// rather than leaking it. http1.Conn.Close is idempotent.
		if mc.c != nil {
			_ = mc.c.Close()
		}
	}
}

// warmup pre-dials up to n conns in the background. Idempotent. n is capped at
// MaxConnsPerHost. Returns immediately; dial errors surface via the OnDial hook.
func (p *h1Pool) warmup(n int) {
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
	// Hold every conn until all `need` are checked out, then release them
	// together. Releasing as we go would defeat the purpose: under exclusive
	// checkout an idle conn always beats a dial, so the next acquire would just
	// reuse the conn we just freed and the pool would only ever open one.
	// `need` is capped at MaxConnsPerHost, so holding them cannot self-deadlock.
	held := make([]*h1ManagedConn, 0, need)
	for i := 0; i < need; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		mc, err := p.acquire(ctx)
		cancel()
		if err != nil {
			continue // dial still in flight or failed; it surfaces via OnDial
		}
		held = append(held, mc)
	}
	for _, mc := range held {
		p.release(mc, true)
	}
}
