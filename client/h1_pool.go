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
// Idle eviction matches the other pools. The health-check sweep does NOT, and
// the difference is the reason the tick order here is the reverse of theirs:
// this pool's liveness signal is one local bit, so noticing that a peer closed
// an idle connection takes a socket read (http1.Conn.ProbeIdle) rather than a
// flag test. Idle eviction therefore runs first, so a conn about to be dropped
// is never probed, and the probe itself runs off the actor goroutine — see
// startHealthSweep for why that is safe and what it cost before.
package client

import (
	"net"
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

// h1ManagedConn is the actor's per-conn record. Its MUTABLE fields — active,
// lastUsed, streamCap and the rest — are owned by the actor goroutine and
// must not be read or written anywhere else.
//
// The conn handle itself is the exception and is deliberately readable: it is
// set once when the dial completes and never reassigned, and the transports
// read it straight off the value acquire returns. This comment used to say
// the whole record was NEVER touched outside the actor, which is not true of
// that field and would send anyone unifying these pools looking for a lock
// that is not needed — or hiding a field the transport requires.
type h1ManagedConn struct {
	c        *http1.Conn
	active   int // 0 or 1 — see h1ConnStreamCap
	lastUsed time.Time
	dialedAt time.Time
	// probing means the health sweep is reading this conn's socket right now.
	// The actor sets it before handing the conn to the sweep goroutine and
	// clears it when the result comes back; while it is set the conn is not a
	// checkout candidate and not an eviction candidate, which is what keeps the
	// sweep's read the only one touching the bufio reader.
	probing bool
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
	acquireCh   chan h1AcquireReq
	releaseCh   chan h1ReleaseMsg
	dialDoneCh  chan h1DialResult
	sweepDoneCh chan h1SweepResult
	statsCh     chan chan Stats
	closeCh     chan struct{}
	closedCh    chan struct{}

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
		opts:        opts,
		addr:        addr,
		dialer:      dialer,
		acquireCh:   make(chan h1AcquireReq),
		releaseCh:   make(chan h1ReleaseMsg, 16),
		dialDoneCh:  make(chan h1DialResult, 4),
		sweepDoneCh: make(chan h1SweepResult, 1),
		statsCh:     make(chan chan Stats),
		closeCh:     make(chan struct{}),
		closedCh:    make(chan struct{}),
		hooksRef:    hooksRef,
		metrics:     metrics,
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
	// sweeping is true while a health sweep is off the actor. It keeps ticks
	// from piling sweeps on top of each other when a peer is slow to answer.
	sweeping bool
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
		case sr := <-p.sweepDoneCh:
			p.handleSweepDone(rs, sr)
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
	if len(rs.waiters) < h1CountReservedIdle(rs.conns) {
		// pickIdle came back empty only because the health sweep is holding a
		// conn this caller could otherwise have had, and enough of them are held
		// to cover everyone already queued plus this request. Falling through
		// would dial a socket the pool already owns — and since nothing defaults
		// IdleTimeout, evictIdle is disabled and the surplus is never reclaimed,
		// it just ratchets to the cap. Queue instead: handleSweepDone serves
		// waiters the moment the reservation clears, so the wait is one probe
		// deadline. If the probe says the conn is dead it is evicted there and
		// ensureDialForWaiters dials for this waiter anyway.
		rs.waiters = append(rs.waiters, req)
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
	// The eviction above can have been the pool's last conn, and a release with
	// keepAlive=false is the ordinary "Connection: close" path rather than an
	// error one — so this is the likeliest of the four sites to strand a queue.
	p.flushStrandedWaiters(rs, ErrDialBackoff)
}

// ensureDialForWaiters starts a dial for every queued waiter that has nothing
// left to wait for, up to what MaxConnsPerHost still allows.
//
// serveWaiters can only hand out connections that already exist, and eviction
// routinely removes the last one — a server "Connection: close" is the ordinary
// trigger, not an error path. Without this the state {no live conns, no
// in-flight dials, queued waiters} is TERMINAL: nothing in the pool ever wakes
// those waiters again and each sits until its own timeout, which is a liveness
// bug rather than a slow path. handleAcquire has always made this decision for a
// new request; the release and tick paths have to make it too.
//
// It dials for the whole batch rather than one at a time, and that is not the
// same question as the one above. As a pure backstop rescuing a terminal state,
// one dial was right. #411 made this the path that has to re-dial for a batch:
// handleAcquire now queues a caller against a health-sweep reservation with NO
// dial of its own, so when a reservation comes back dead, k waiters are here at
// once. One dial per call serialised them — each dial completed, serveWaiters
// gave the conn to one waiter, and only then did the next dial start, for k
// sequential round trips where k parallel acquires on the pre-#411 pool would
// each have dialled for themselves. The cost scales with MaxConnsPerHost, which
// for HTTP/1.1 IS the concurrency limit.
func (p *h1Pool) ensureDialForWaiters(rs *h1RunState) {
	if inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {
		return
	}
	// Waiters with no capacity already on the way to them. Conns the health sweep
	// is holding are capacity the pool already owns and is about to get back, and
	// a dial in flight is capacity en route, so neither justifies another socket.
	// Without the reserved term the tick's own call here opens a socket for the
	// very waiter its own sweep just displaced; without the in-flight term the
	// batch loop below would re-dial for waiters it has already dialled for.
	//
	// A surplus socket is not self-correcting: nothing in the repo defaults
	// IdleTimeout, so evictIdle is disabled and the count just ratchets to the
	// cap.
	uncovered := len(rs.waiters) - h1CountReservedIdle(rs.conns) - rs.inFlightDials
	room := p.opts.MaxConnsPerHost - h1CountLive(rs.conns) - rs.inFlightDials
	for n := min(uncovered, room); n > 0; n-- {
		rs.inFlightDials++
		go p.dialOne()
	}
}

// flushStrandedWaiters refuses every queued waiter when the pool holds nothing
// that could ever serve them: no live conn, no dial in flight, and an open dial
// backoff window that stops ensureDialForWaiters from starting one.
//
// It is the counterpart to ensureDialForWaiters and belongs immediately after
// every call to it. That pairing is the invariant: ensureDialForWaiters gives a
// queued caller something to wait FOR, and this one says so when there is
// nothing. Leaving the state queued is a priority inversion rather than merely a
// slow path — handleAcquire fast-refuses a FRESH request on these same three
// conditions, so a caller arriving an instant later gets an immediate
// ErrDialBackoff while the already-queued one waits a full HealthCheckPeriod.
//
// The one call to ensureDialForWaiters that needs no flush after it is
// handleDialDone's success arm, which has just appended a live conn.
//
// err is each site's answer to "why": handleDialDone hands over the DialError it
// just saw, and everywhere else the honest answer is that the pool is in
// backoff. That difference is why this takes the error rather than picking one.
//
// This used to be written out at one site only. handleTick, handleRelease and
// handleSweepDone all reach the same state — the tick's evictIdle/evictDead can
// take the last conn, and a release with keepAlive=false does so on the ordinary
// "Connection: close" path.
func (p *h1Pool) flushStrandedWaiters(rs *h1RunState, err error) {
	if len(rs.waiters) == 0 || rs.inFlightDials > 0 {
		return
	}
	// A conn the health sweep has reserved is counted live here, deliberately:
	// it is capacity that exists and is about to come back, so a waiter it will
	// serve is not stranded. handleSweepDone is the site that re-asks once the
	// reservation clears.
	if h1CountLive(rs.conns) > 0 {
		return
	}
	if !inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {
		// Nothing live and nothing in flight with the backoff closed means the
		// preceding ensureDialForWaiters started a dial, so inFlightDials would
		// not be zero and this would be unreachable. Tested anyway, so each call
		// site is correct on its own terms rather than by virtue of what runs
		// before it.
		return
	}
	for _, w := range rs.waiters {
		p.replyAcquire(w, nil, err)
	}
	rs.waiters = nil
}

// handleDialDone processes a completed dial: on success the conn enters the pool;
// on failure one waiter receives the error.
func (p *h1Pool) handleDialDone(rs *h1RunState, dr h1DialResult) {
	rs.inFlightDials--
	if dr.err != nil {
		rs.lastDialErrAt = time.Now()
		// Refused from the BACK of the queue, not the front.
		//
		// A waiter can be queued against a health-sweep reservation rather than
		// against a dial: handleAcquire queues one with no dial of its own while
		// len(waiters) < h1CountReservedIdle, because a reserved conn will serve
		// it within a probe deadline. serveWaiters drains from the FRONT, so
		// those are precisely the waiters nearest the front — and handing the
		// front one this dial's error refuses a caller the pool is about to have
		// capacity for, roughly a millisecond later. Taking from the back refuses
		// one that genuinely has nothing coming, and when the reservations cover
		// everyone queued, nobody is refused at all: the failed dial was started
		// for a caller those reservations can absorb.
		//
		// This perturbs FIFO for ERRORS ONLY. Successful service stays strictly
		// in arrival order through serveWaiters, which is what the rest of this
		// pool assumes of the waiter queue.
		// BACK of the queue, unlike the multiplexing H2/H3 pools, which refuse the
		// FRONT (pool.go, h3_pool.go). The reservation guard is why: under
		// exclusive checkout a front waiter may already be covered by an idle conn
		// on its way to it, so refusing the front would fail a request that was
		// about to succeed. Stated on both sides because the difference is
		// invisible from either one.
		if n := len(rs.waiters); n > h1CountReservedIdle(rs.conns) {
			req := rs.waiters[n-1]
			rs.waiters = rs.waiters[:n-1]
			p.replyAcquire(req, nil, dr.err)
		}
		// The waiters behind that one must not be left to a health-check tick:
		// either start another dial, or refuse them for the same reason
		// handleAcquire fast-refuses a new request. Leaving them queued was a
		// priority inversion — a burst against a downed host drained one request
		// per tick.
		p.ensureDialForWaiters(rs)
		p.flushStrandedWaiters(rs, dr.err)
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

// handleTick runs periodic maintenance: idle eviction, dead eviction, the health
// sweep, and waiter expiry. There is no stream-cap refresh: the HTTP/1.1 per-conn
// cap is the constant h1ConnStreamCap.
//
// Idle eviction still runs first, for the reason it always did: a conn that is
// about to be discarded for idleness should not be probed at all. What changed is
// that the probe itself no longer runs here — startHealthSweep hands it to a
// goroutine. Every step left in this function is a predicate over fields the
// actor already owns, so the tick no longer scales with the number of conns.
func (p *h1Pool) handleTick(rs *h1RunState) {
	rs.conns = p.evictIdle(rs.conns)
	rs.conns = p.evictDead(rs.conns)
	p.startHealthSweep(rs)
	rs.waiters = h1PruneExpiredWaiters(rs.waiters)
	p.ensureDialForWaiters(rs)
	// Either eviction above can take the last conn while waiters queued behind a
	// full pool are still here, and ensureDialForWaiters returns without rescuing
	// them whenever a dial backoff is open. Nothing else looks at the queue until
	// the NEXT tick, a whole HealthCheckPeriod away.
	p.flushStrandedWaiters(rs, ErrDialBackoff)
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
// None of the three pools races the send against req.ctx.Done(), and this
// comment used to claim that as a divergence from the H2/H3 siblings. It is not
// one — the reason below is why NO pool may do it, and it bites hardest here.
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
		if mc.probing {
			// The health sweep is reading this socket. Handing it out would put
			// an exchange reader on the same bufio reader as the probe.
			continue
		}
		return mc
	}
	return nil
}

// dialEnv snapshots what dialAttempt needs from this pool.
func (p *h1Pool) dialEnv() dialEnv {
	return dialEnv{closedCh: p.closedCh, timeout: p.opts.DialTimeout, addr: p.addr, metrics: p.metrics, hooksRef: p.hooksRef}
}

// dialOne dials one conn and delivers it to the actor. The ALPN assertion runs
// inside the timed dial: a peer that answered "h2" has cost a real dial, and
// the OnDial hook should see that attempt fail rather than see it succeed and
// the caller then receive a DialError from nowhere.
func (p *h1Pool) dialOne() {
	nc, err := dialAttempt(p.dialEnv(), func(ctx context.Context) (net.Conn, error) {
		nc, derr := p.dialer.Dial(ctx, p.addr)
		if derr != nil {
			return nil, derr
		}
		if aerr := assertH1Conn(nc); aerr != nil {
			_ = nc.Close()
			return nil, aerr
		}
		return nc, nil
	})
	if err != nil {
		p.dialDoneCh <- h1DialResult{err: &DialError{Addr: p.addr, Err: err}}
		return
	}
	p.dialDoneCh <- h1DialResult{mc: &h1ManagedConn{c: http1.NewConn(nc), dialedAt: time.Now(), lastUsed: time.Now()}}
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
//
// A conn the health sweep has reserved is skipped: the sweep is about to report
// on it, and evicting it here would close it under a probe that is mid-read and
// then evict it a second time when the result arrives.
func (p *h1Pool) evictIdle(conns []*h1ManagedConn) []*h1ManagedConn {
	if p.opts.IdleTimeout <= 0 {
		return conns
	}
	now := time.Now()
	out := conns[:0]
	for _, mc := range conns {
		if !mc.probing && mc.active == 0 && now.Sub(mc.lastUsed) > p.opts.IdleTimeout {
			_ = mc.c.Close()
			p.notifyClose(CloseIdle)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDead removes conns whose IsAlive returns false, and — in this periodic
// sweep only — conns this side has already torn down.
//
// The only question asked here is IsAlive, which reads one atomic that this side
// sets when it tears a conn down. The socket probe that used to run here — the
// part that notices a peer FIN, which no local flag can report — now runs in
// startHealthSweep, off the actor goroutine. Splitting them does not change which
// conns are evicted: the active == 0 guard this function used to carry gated only
// the probe, never the IsAlive test, so a busy conn that had been closed locally
// was evicted before and still is.
//
// Two consequences of the split are timing-only, and are listed so a future
// reader does not mistake either for a bug. A conn the probe finds dead is now
// evicted one channel hop after the tick rather than inside it, so a Stats call
// landing in that window still counts it. And if Close lands mid-sweep the result
// is dropped, so handleClose reports that conn CloseManual where a serialised
// tick would have said CloseDead — the same number of events, one different
// reason, in the shutdown race only.
//
// RFC 9112 §9.6 (monitor idle connections) / RFC 9110 (idle-arriving data is not
// a valid response, so evict rather than let the next request consume it).
func (p *h1Pool) evictDead(conns []*h1ManagedConn) []*h1ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			_ = mc.c.Close()
			p.notifyClose(CloseDead)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// h1CountReservedIdle counts conns the health sweep is holding that would
// otherwise be checkout candidates — capacity that exists and is about to come
// back, as opposed to capacity the pool does not have.
//
// pickIdle returning nil means one or the other, and they call for opposite
// decisions: no capacity justifies a dial, a reservation justifies a short wait.
// Every site that decides whether to dial has to ask, not just the one a new
// request arrives on — handleAcquire AND ensureDialForWaiters, the latter reached
// from the tick, from release, and from both arms of dial-done.
//
// The predicate deliberately mirrors pickIdle's minus the probing test, so the
// two cannot drift into disagreeing about what "available" means.
func h1CountReservedIdle(conns []*h1ManagedConn) int {
	n := 0
	for _, mc := range conns {
		if mc.probing && mc.c.IsAlive() && mc.active < h1ConnStreamCap {
			n++
		}
	}
	return n
}

// h1SweepResult carries a finished health sweep back to the actor. probed is
// every conn the actor reserved; dead is the subset whose probe failed. Both
// slices are handed over wholesale so the actor can clear the reservation even
// for conns it is about to evict.
type h1SweepResult struct {
	probed []*h1ManagedConn
	dead   []*h1ManagedConn
}

// startHealthSweep reserves every checked-in conn and probes them off the actor
// goroutine.
//
// The probe must not run on the actor. http1.Conn.ProbeIdle arms a 1ms FUTURE
// read deadline and blocks in Peek; on a HEALTHY idle socket it blocks for the
// whole deadline by construction, since it returns early only when the peer has
// sent something. Run inline it therefore cost (idle conns) x one deadline of
// actor time per tick, and acquireCh is unbuffered, so nothing could acquire or
// release for that whole stretch. For HTTP/1.1 MaxConnsPerHost IS the concurrency
// limit, so the stall grew with the very knob a caller raises to go faster:
// measured at 4.6ms worst-case acquire latency with 2 conns against 271ms with 64.
//
// What makes moving it safe is the reservation, not the goroutine. ProbeIdle
// reads through the same bufio reader an exchange uses, so it may only run when
// no exchange can start. The actor is the only goroutine that knows active == 0,
// and it stays the one that decides: it marks the candidates probing, and both
// pickIdle and evictIdle skip a probing conn, so it can be neither checked out
// nor evicted while the sweep holds it.
//
// Probes run concurrently, so a sweep costs one probe deadline in wall clock
// rather than one per conn. Callers arriving mid-sweep queue behind a full pool
// for that single deadline instead of blocking for the whole sweep.
//
// KNOWN LIMIT, inherited rather than introduced: the whole design rests on
// ProbeIdle being bounded by the read deadline it sets. A caller-supplied
// conn.Dialer could return a net.Conn whose SetReadDeadline succeeds but does
// nothing, and then Peek blocks until the peer speaks — rs.sweeping stays true,
// the reserved conns stay reserved, and the pool wedges at its cap. A sweep
// cannot defend itself by giving up on a probe, because abandoning one would
// leave a goroutine reading the bufio reader after the reservation cleared,
// which is the one thing the reservation exists to prevent. Note the same conn
// wedges main's actor outright, so this narrows the blast radius from the whole
// pool's actor to its idle set; it does not close it.
func (p *h1Pool) startHealthSweep(rs *h1RunState) {
	if rs.sweeping {
		// A peer that is slow to answer must not make ticks pile sweeps on top
		// of each other, each reserving the conns the last one already holds.
		return
	}
	var cands []*h1ManagedConn
	for _, mc := range rs.conns {
		if mc.active == 0 {
			mc.probing = true
			cands = append(cands, mc)
		}
	}
	if len(cands) == 0 {
		return
	}
	rs.sweeping = true
	go p.runHealthSweep(cands)
}

// runHealthSweep probes every reserved conn concurrently and reports back. It
// runs on its own goroutine and touches only the conn handles, never runState.
func (p *h1Pool) runHealthSweep(cands []*h1ManagedConn) {
	// Indexed rather than appended: each goroutine owns one slot, so the writes
	// need no lock, and the nils are filtered once the wait is over.
	failed := make([]*h1ManagedConn, len(cands))
	var wg sync.WaitGroup
	wg.Add(len(cands))
	for i, mc := range cands {
		go func(i int, mc *h1ManagedConn) {
			defer wg.Done()
			if !mc.c.ProbeIdle() {
				failed[i] = mc
			}
		}(i, mc)
	}
	wg.Wait()

	dead := failed[:0]
	for _, mc := range failed {
		if mc != nil {
			dead = append(dead, mc)
		}
	}

	select {
	case p.sweepDoneCh <- h1SweepResult{probed: cands, dead: dead}:
	case <-p.closedCh:
		// The pool shut down while we were probing. handleClose has already
		// closed every conn, so there is nothing left to evict and no actor to
		// tell — dropping the result is the whole cleanup.
	}
}

// handleSweepDone releases the sweep's reservation and evicts what it found.
//
// A conn cannot have been checked out or idle-evicted while it was reserved, so
// every entry in dead is still in rs.conns and is evicted exactly once. Serving
// waiters afterwards matters: callers that arrived mid-sweep queued because every
// conn was reserved, and this is the moment capacity comes back.
func (p *h1Pool) handleSweepDone(rs *h1RunState, sr h1SweepResult) {
	rs.sweeping = false
	for _, mc := range sr.probed {
		mc.probing = false
	}
	for _, mc := range sr.dead {
		rs.conns = p.evict(rs.conns, mc, CloseDead)
	}
	rs.waiters = p.serveWaiters(rs.conns, rs.waiters)
	p.ensureDialForWaiters(rs)

	// A sweep can evict every conn it reserved, and handleAcquire queues callers
	// against those reservations with no dial of their own — so this path too can
	// reach {nothing live, nothing in flight, waiters queued} and leave them with
	// nothing to wake them.
	//
	// handleDialDone's own flush cannot cover it: when that dial failed these
	// conns were still reserved, and h1CountLive counts a reserved conn as live,
	// so its "nothing live" test was false. By the time they die, that dial is
	// long finished.
	p.flushStrandedWaiters(rs, ErrDialBackoff)
}

// evictDeadSilent removes conns whose IsAlive returns false without firing the
// OnConnClose hook. It DOES count them: the summary used to say "or updating
// counters", which its own body has contradicted since the counter was added —
// an eviction observed only through Stats is still an eviction. Used from the
// Stats path, where firing a hook from a caller's goroutine would be wrong.
func (p *h1Pool) evictDeadSilent(conns []*h1ManagedConn) []*h1ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			_ = mc.c.Close()
			// Counted, not notified — see the H2 sibling's comment. HTTP/1.1
			// has no GOAWAY, so there is no second counter to attribute.
			p.metrics.Counters.ConnsClosed.Add(1)
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
	// The loop is bounded by MaxConnsPerHost, and every rejected connection was
	// evicted, so falling out of it means every connection the pool could offer
	// had residue. Returning one unchecked was the old behaviour and it undid the
	// guard entirely: MaxConnsPerHost defaults to 1, so exactly two checked
	// attempts preceded one unchecked one, and a peer that writes on accept fails
	// the checked attempts by construction — it only had to be persistent to be
	// handed a poisoned connection anyway.
	//
	// Checked here too. A connection that still has residue at this point is not
	// usable by anyone, so the honest answer is the error, not a socket with an
	// attacker's response already on it.
	mc, err := p.acquireOnce(ctx)
	if err != nil || mc == nil {
		return mc, err
	}
	if !mc.c.HasResidue() {
		return mc, nil
	}
	p.release(mc, false)
	return nil, ErrResidueOnAcquire
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

// h1WarmupProbeTimeout bounds one warm-up acquire. It is short on purpose: a
// warm-up dial that has not completed quickly is already in flight and will
// finish on its own — the pool records it — so waiting longer buys nothing and
// only delays the next one. Named rather than a literal because it is a policy
// number, not an arithmetic one.
const h1WarmupProbeTimeout = 50 * time.Millisecond

// warmup pre-dials up to n conns in the background. Idempotent. n is capped at
// MaxConnsPerHost. Returns immediately; dial errors surface via the OnDial hook.
//
// The dialing runs on its own goroutine. It used to run on the caller's: a
// Stats round-trip plus up to MaxConnsPerHost sequential acquires, each bounded
// by h1WarmupProbeTimeout. Against a black-holed host at MaxConnsPerHost 64 that
// is ~3.2 seconds inside a call this contract — and Client.Warmup, and the
// transport interface — all document as returning immediately. The H2 and H3
// pools hand n to their actor and return; this is the same promise kept a
// different way.
//
// Not by adding a warmupCh like the siblings: their handleWarmup starts dials
// directly on actor state, while this pool warms through acquire on purpose, to
// get exclusive checkout. Calling acquire from inside the actor would deadlock
// against the actor that serves it.
func (p *h1Pool) warmup(n int) {
	if n <= 0 {
		return
	}
	go p.warmupDials(n)
}

// warmupDials is warmup's body, off the caller's goroutine.
func (p *h1Pool) warmupDials(n int) {
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
		select {
		case <-p.closedCh:
			// The pool is gone; release what is held and stop dialing into nothing.
			for _, mc := range held {
				p.release(mc, true)
			}
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), h1WarmupProbeTimeout)
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
