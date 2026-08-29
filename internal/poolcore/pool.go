// pool transport (Phase C.2).

package poolcore

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/pool"
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
	New: func() any { return make(chan AcquireResp, 1) },
}

// statsReplyPool recycles buffered Stats reply channels for the same
// reason as replyPool. Stats() is on the observability path; not
// hot-hot, but a sync.Pool here keeps the alloc count flat under load
// scrapes from metrics endpoints.
var statsReplyPool = sync.Pool{
	New: func() any { return make(chan Stats, 1) },
}

// ManagedConn is the actor's per-conn record. Its MUTABLE fields — active,
// lastUsed, streamCap and the rest — are owned by the actor goroutine and
// must not be read or written anywhere else.
//
// The conn handle itself is the exception and is deliberately readable: it is
// set once when the dial completes and never reassigned, and the transports
// read it straight off the value acquire returns. This comment used to say
// the whole record was NEVER touched outside the actor, which is not true of
// that field and would send anyone unifying these pools looking for a lock
// that is not needed — or hiding a field the transport requires.
type ManagedConn struct {
	C *conn.Conn

	// Typed is what New's wrap function built from C, or nil when the pool was
	// constructed without one. It is set once, when the dial completes, and is
	// never recomputed: a wrapper over this connection is fixed for the
	// connection's life, so rebuilding one per Acquire would allocate on the hot
	// path for a value that cannot have changed.
	//
	// This package stores it and hands it back, and does nothing else with it —
	// no type assertion here, ever. The one assertion that recovers the concrete
	// type belongs to the consuming package, in a single helper. That
	// confinement is the whole reason this is `any` rather than a type
	// parameter: making it one would put ManagedConn's EXISTING C field under a
	// type argument that every struct literal in this package's tests and every
	// mc.C call site in pool.go would then have to spell, for machinery that
	// does not differ per wrapped type. See
	// docs/adr/0002-poolcore-stays-non-generic-typed-any-boundary.md.
	//
	// Eviction closes C and NEVER Typed. A wrapper over a connection it does not
	// own has a deliberately no-op Close, so tearing down through it would leave
	// the socket open with nothing left holding it.
	Typed any

	Active   int
	LastUsed time.Time

	// p is the owning pool, so this struct can BE the releaser rather than
	// having one built around it per request (#476).
	p *Pool

	// streamCap caches EffectiveStreamCap(local, peer). Computed when the
	// dial completes and refreshed on every health-check tick so peer
	// SETTINGS_MAX_CONCURRENT_STREAMS changes are picked up. Without this
	// cache, PickLeastLoaded would take c.psMu.RLock() for every conn on
	// every acquire.
	StreamCap int
}

// AcquireReq is sent on Pool.acquireCh. The actor replies on reply.
type AcquireReq struct {
	Ctx   context.Context
	Reply chan AcquireResp
}

// AcquireResp carries the reply from the actor for an AcquireReq.
type AcquireResp struct {
	Mc  *ManagedConn
	Err error
}

// ReleaseMsg is sent on Pool.releaseCh after a request completes.
type ReleaseMsg struct {
	Mc *ManagedConn
}

// DialResult is sent by a dial helper goroutine on Pool.dialDoneCh.
type DialResult struct {
	Mc  *ManagedConn
	Err error
}

// Pool is a per-host connection pool. Construct via NewClient with
// Transport=TransportPool.
type Pool struct {
	opts     PoolOptions
	connOpts conn.ConnOptions
	Addr     string

	// wrap builds the value dialOne stores in ManagedConn.Typed, or is nil when
	// this pool hands out raw connections only — which is every caller in
	// client. Installed once by New and read only by dialOne, so a pool built
	// without one costs a nil check per dial and nothing else.
	wrap func(*conn.Conn) (any, error)

	// pickCursor rotates where PickLeastLoaded starts, so consecutive requests
	// land on different idle connections instead of piling onto the first.
	// Actor-owned: every pick runs on the pool goroutine.
	pickCursor int

	// channels
	acquireCh  chan AcquireReq
	releaseCh  chan ReleaseMsg
	warmupCh   chan int
	dialDoneCh chan DialResult
	statsCh    chan chan Stats
	closeCh    chan struct{}
	closedCh   chan struct{}

	// closeOnce guards closeCh from double-close.
	closeOnce sync.Once

	// obs and rec are the caller's observability, narrowed to the connection
	// events a pool can raise. Both are installed once and are never nil — New
	// substitutes a nop — so the reporting call sites carry no check. An
	// implementation whose underlying callbacks can be swapped at runtime reads
	// them per call; that is the adapter's problem, not this pool's.
	//
	// Both are shared with the owning client and, for a managed pool, with
	// every sibling sub-pool: the counts are the client's, not this pool's.
	obs pool.Observer
	rec pool.Recorder
}

// New constructs a Pool and starts its actor goroutine. Callers reach it
// through client.NewClient or the gRPC channel, never directly.
//
// wrap is optional. When non-nil it runs once per successful dial and what it
// returns is stored on ManagedConn.Typed for the caller to recover; when it
// returns an error the whole dial attempt is treated as failed. Pass nil to
// pool raw connections, which is what every caller in client does.
func New(addr string, connOpts conn.ConnOptions, opts PoolOptions,
	obs pool.Observer, rec pool.Recorder, wrap func(*conn.Conn) (any, error),
) *Pool {
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
	if rec == nil {
		rec = pool.NopRecorder{}
	}
	if obs == nil {
		obs = pool.NopObserver{}
	}
	p := &Pool{
		opts:       opts,
		connOpts:   connOpts,
		Addr:       addr,
		acquireCh:  make(chan AcquireReq),
		releaseCh:  make(chan ReleaseMsg, 16),
		warmupCh:   make(chan int),
		dialDoneCh: make(chan DialResult, 4),
		statsCh:    make(chan chan Stats),
		closeCh:    make(chan struct{}),
		closedCh:   make(chan struct{}),
		obs:        obs,
		rec:        rec,
		wrap:       wrap,
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

// RunState holds the mutable loop-local state of Pool.run. Kept in a
// struct so extracted handlers can receive it without the caller
// unpacking/packing individual variables on every iteration.
type RunState struct {
	Conns         []*ManagedConn
	Waiters       []AcquireReq
	InFlightDials int
	LastDialErrAt time.Time
}

// Run is the actor loop. It owns every field of RunState — conns, waiters,
// inFlightDials, lastDialErrAt — which live there and not on Pool, and which no
// other goroutine reads or writes.
func (p *Pool) run() {
	defer close(p.closedCh)
	rs := &RunState{}
	tick := time.NewTicker(p.opts.HealthCheckPeriod)
	defer tick.Stop()

	for {
		select {
		case req := <-p.acquireCh:
			p.handleAcquire(rs, req)
		case msg := <-p.releaseCh:
			p.HandleRelease(rs, msg)
		case n := <-p.warmupCh:
			p.handleWarmup(rs, n)
		case dr := <-p.dialDoneCh:
			p.HandleDialDone(rs, dr)
		case respCh := <-p.statsCh:
			p.handleStats(rs, respCh)
		case <-tick.C:
			p.HandleTick(rs)
		case <-p.closeCh:
			p.handleClose(rs)
			return
		}
	}
}

// handleAcquire tries to serve the request from an existing conn.
// If no live capacity exists it decides between dial / queue / fast-refuse.
func (p *Pool) handleAcquire(rs *RunState, req AcquireReq) {
	mc := p.PickLeastLoaded(rs.Conns)
	if mc != nil {
		mc.Active++
		mc.LastUsed = time.Now()
		p.replyAcquire(req, mc, nil)
		return
	}
	liveConns := CountLive(rs.Conns)
	atCap := liveConns+rs.InFlightDials >= p.opts.MaxConnsPerHost
	inBackoff := InDialBackoff(rs.LastDialErrAt, p.opts.DialBackoff)

	if !atCap && !inBackoff {
		rs.InFlightDials++
		go p.dialOne()
		rs.Waiters = append(rs.Waiters, req)
		return
	}
	if inBackoff && liveConns == 0 && rs.InFlightDials == 0 {
		p.replyAcquire(req, nil, ErrDialBackoff)
		return
	}
	rs.Waiters = append(rs.Waiters, req)
}

// EnsureDialForWaiters starts a dial when queued waiters have nothing left to
// wait for.
//
// serveWaiters can only hand out capacity that already exists, and eviction
// routinely removes the last conn — a peer GOAWAY followed by its drain is the
// ordinary trigger, not an error path. Without this the state {no live conns,
// no in-flight dials, queued waiters} is TERMINAL: release, dial-done and tick
// all pass through it without acting, so nothing re-enters the dial decision
// and each waiter sits until its own context expires. Measured on the default
// options: that state held across ~30 health-check ticks with the server still
// dialable, and the waiter was served only when an unrelated request happened
// to arrive. It is worse than the H1 case it was ported from, because a pool
// whose every worker is already parked never receives that request.
//
// handleAcquire has always made this decision for a NEW request. The point is
// that the decision belongs to the state, not to the arrival of work.
//
// Deliberately not called from handleStats: that path only removes conns which
// were already not live, so it cannot create the terminal state, and a metrics
// scrape must not open connections.
// It dials for the whole uncovered batch rather than one at a time, which is
// the same correction #411 made on h1Pool — and for the same reason: a mass
// eviction leaves k waiters here at once, and one dial per call serialises them
// into k sequential round trips, each dial starting only after the previous
// conn was handed to a single waiter.
//
// The arithmetic is NOT h1's, and copying it would be a regression rather than
// a port. h1 carries one caller per connection, so "waiters minus coverage" is
// the dial count. Here a connection covers streamCap waiters, so that expression
// opens a socket per waiter for a batch one connection could serve. Nothing in
// this repo defaults IdleTimeout, so EvictIdle is off and a surplus socket
// ratchets to the cap and stays — the serial ramp would be traded for permanent
// over-allocation.
//
// So: subtract the spare stream capacity already live, subtract what the dials
// in flight are expected to bring, and divide the remainder by that same
// expectation. With MaxStreamsPerConn 1 — how a load generator gives each
// request its own connection — this is h1's formula exactly, because there the
// divisor is 1.
//
// The expectation is the LOCAL cap, not any peer's: a connection that does not
// exist yet has no SETTINGS. A peer advertising fewer streams than we asked for
// leaves the batch short, and the next call through this path re-dials for
// whoever is still queued.
func (p *Pool) EnsureDialForWaiters(rs *RunState) {
	if len(rs.Waiters) == 0 {
		return
	}
	room := p.opts.MaxConnsPerHost - CountLive(rs.Conns) - rs.InFlightDials
	if room <= 0 {
		return
	}
	if InDialBackoff(rs.LastDialErrAt, p.opts.DialBackoff) {
		return
	}
	perConn := EffectiveStreamCap(p.opts.MaxStreamsPerConn, 0)
	uncovered := len(rs.Waiters) - SpareStreamCapacity(rs.Conns) - rs.InFlightDials*perConn
	if uncovered <= 0 {
		return
	}
	need := (uncovered + perConn - 1) / perConn // round up: a partial batch still needs a conn
	for n := min(need, room); n > 0; n-- {
		rs.InFlightDials++
		go p.dialOne()
	}
}

// SpareStreamCapacity is how many more callers the live connections can take
// right now — the coverage a queued waiter already has, and so the part of the
// queue that needs no new socket.
//
// Matches PickLeastLoaded's admission test (alive, and active below streamCap)
// so this cannot count capacity serveWaiters would then refuse to use.
func SpareStreamCapacity(conns []*ManagedConn) int {
	n := 0
	for _, mc := range conns {
		if !mc.C.IsAlive() {
			continue
		}
		if spare := mc.StreamCap - mc.Active; spare > 0 {
			n += spare
		}
	}
	return n
}

// FlushStrandedWaiters refuses every queued waiter when the pool holds nothing
// that could ever serve them: no live conn, no dial in flight, and an open dial
// backoff window that stops EnsureDialForWaiters from starting one.
//
// It is the counterpart to EnsureDialForWaiters and belongs immediately after
// every call to it. That pairing is the invariant: EnsureDialForWaiters gives a
// queued caller something to wait FOR, and this one says so when there is
// nothing. handleAcquire fast-refuses a FRESH request on these same three
// conditions, so leaving the state queued is a priority inversion rather than a
// slow path — the caller arriving an instant later is served an immediate
// ErrDialBackoff while the already-queued one waits a full HealthCheckPeriod.
//
// It used to be written out inline in HandleDialDone alone, which made the state
// answerable only when a DIAL produced it. HandleRelease and HandleTick reach it
// by EVICTION and had no copy (#425). HandleRelease is this pool's only eviction
// site for a conn still carrying traffic, so it is where a GOAWAY'd conn is
// finally reaped after RFC 7540 §6.8's drain — the ordinary way the last conn
// goes, not an error path.
//
// CountLive, not len(conns), is the right test and the two differ here: a
// GOAWAY'd conn still draining is deliberately not live, because draining
// streams can never become capacity for a waiter — such a conn is evicted once
// its last stream ends and is never picked again. So this can fire with conns
// still in the slice, and that is correct.
//
// Waiters are per-STREAM in this pool, so one flush can refuse a large queue at
// once. Same semantics as the h1 sibling's, and better than the hang.
//
// Deliberately not called from handleStats, for the reason EnsureDialForWaiters
// is not: that path only removes conns which were already not live, so it cannot
// create the state.
func (p *Pool) FlushStrandedWaiters(rs *RunState, err error) {
	if len(rs.Waiters) == 0 || rs.InFlightDials > 0 {
		return
	}
	if CountLive(rs.Conns) > 0 {
		return
	}
	if !InDialBackoff(rs.LastDialErrAt, p.opts.DialBackoff) {
		// Nothing live and nothing in flight with the backoff closed means the
		// preceding EnsureDialForWaiters started a dial, so inFlightDials would
		// not be zero and this would be unreachable. Tested anyway, so each call
		// site is correct on its own terms rather than by virtue of what runs
		// before it.
		return
	}
	for _, w := range rs.Waiters {
		p.replyAcquire(w, nil, err)
	}
	rs.Waiters = nil
}

// HandleRelease decrements the conn's active count and evicts it once the
// underlying connection is no longer alive AND the stream just released was its
// last. This is the pool's only eviction site for a conn still carrying
// traffic — evictDead and evictDeadSilent both defer to it — so it is where a
// GOAWAY'd conn is finally reaped, after RFC 7540 §6.8's drain.
//
// The active==0 guard is also what makes GoAwaysReceived a count of conns
// rather than of streams: without it every release on a GOAWAY'd conn
// incremented, so one GOAWAY on a conn with 4 draining streams counted 4 —
// while its sibling ConnsClosed, fired from evict, counted 1.
func (p *Pool) HandleRelease(rs *RunState, msg ReleaseMsg) {
	msg.Mc.Active--
	msg.Mc.LastUsed = time.Now()
	if !msg.Mc.C.IsAlive() && msg.Mc.Active == 0 {
		reason := CloseDead
		if msg.Mc.C.GoAwayReceived() {
			reason = CloseGoAway
			p.rec.GoAwayReceived()
		}
		rs.Conns = p.evict(rs.Conns, msg.Mc, reason)
	}
	rs.Waiters = p.serveWaiters(rs.Conns, rs.Waiters)
	p.EnsureDialForWaiters(rs)
	// That eviction can have been the pool's last live conn — this is where a
	// GOAWAY'd conn is reaped after its drain — leaving waiters queued behind
	// capacity that no longer exists.
	p.FlushStrandedWaiters(rs, ErrDialBackoff)
}

// HandleDialDone processes a completed dial: on success the conn
// enters the pool; on failure the first waiter receives the error.
func (p *Pool) HandleDialDone(rs *RunState, dr DialResult) {
	rs.InFlightDials--
	if dr.Err != nil {
		rs.LastDialErrAt = time.Now()
		// FRONT of the queue, unlike the HTTP/1.1 pool, which refuses the BACK
		// behind a reserved-idle guard (h1_pool.go). Not an oversight on either
		// side: under exclusive checkout a front waiter may already be covered by
		// an idle conn about to be handed to it, so refusing the front there would
		// fail a request that was about to succeed. This pool multiplexes, so a
		// waiter is waiting on capacity rather than on one specific conn, and the
		// arrival order the rest of the queue assumes is preserved by taking the
		// head. Stated here because the difference is invisible from either file.
		if len(rs.Waiters) > 0 {
			req := rs.Waiters[0]
			rs.Waiters = rs.Waiters[1:]
			p.replyAcquire(req, nil, dr.Err)
		}
		// The waiters behind that one must not be left to a health-check tick.
		// Either start another dial, or — when the pool is in exactly the state
		// that makes handleAcquire fast-refuse a NEW request (dial backoff,
		// nothing live, nothing in flight) — refuse them for the same reason.
		// Leaving them queued is a priority inversion: measured, a fresh acquire
		// was refused with ErrDialBackoff in 0ms while two earlier waiters stayed
		// parked past the end of the backoff window and left only when the pool
		// was closed.
		//
		// This used to add "H2 has no tick-path dial at all, so it drains none",
		// which stopped being true once HandleTick grew its EnsureDialForWaiters
		// call (pinned by TestHandleTick_DialsForStrandedWaiter).
		//
		// See FlushStrandedWaiters for why CountLive rather than len(conns), and
		// for what per-STREAM waiters mean when a whole queue is refused at once.
		p.EnsureDialForWaiters(rs)
		p.FlushStrandedWaiters(rs, dr.Err)
		return
	}
	p.refreshStreamCap(dr.Mc)
	rs.Conns = append(rs.Conns, dr.Mc)
	rs.Waiters = p.serveWaiters(rs.Conns, rs.Waiters)
	p.EnsureDialForWaiters(rs)
}

// handleStats evicts dead conns silently and reports a snapshot.
func (p *Pool) handleStats(rs *RunState, respCh chan<- Stats) {
	rs.Conns = p.evictDeadSilent(rs.Conns)
	respCh <- Stats{
		ActiveConns:     len(rs.Conns),
		InFlightStreams: sumActive(rs.Conns),
		Waiters:         len(rs.Waiters),
		InFlightDials:   rs.InFlightDials,
	}
}

// HandleTick runs periodic maintenance: idle eviction, dead
// eviction, stream-cap refresh, and waiter expiry.
func (p *Pool) HandleTick(rs *RunState) {
	// Dead before idle. Whichever sweep reaches a conn first decides what
	// killed it, and a conn the peer GOAWAY'd is very often also idle — it
	// stopped taking new streams. Reaping it as CloseIdle attributes a peer's
	// shutdown to local inactivity and never increments GoAwaysReceived, so a
	// rolling restart shows up as ordinary idling. evictDead here is a pair of
	// atomic flag reads, so the order costs nothing.
	//
	// The HTTP/1.1 pool deliberately keeps the opposite order: its evictDead
	// runs a bounded socket probe per conn on the actor goroutine, so probing
	// conns that EvictIdle would have discarded for free would stall every
	// acquire and release for up to MaxConnsPerHost probes per tick — and h1
	// has no GOAWAY, so there is nothing to attribute.
	rs.Conns = p.evictDead(rs.Conns)
	rs.Conns = p.EvictIdle(rs.Conns)
	for _, mc := range rs.Conns {
		p.refreshStreamCap(mc)
	}
	// Serving before dialing is H2-specific. Per-conn capacity is dynamic here:
	// a peer that raised SETTINGS_MAX_CONCURRENT_STREAMS makes existing conns
	// able to serve waiters that had nothing to wait for a moment ago, and no
	// other tick path offers them. Dialing first would open a connection the
	// pool did not need.
	rs.Waiters = p.serveWaiters(rs.Conns, rs.Waiters)
	rs.Waiters = pruneExpiredWaiters(rs.Waiters)
	p.EnsureDialForWaiters(rs)
	// Either eviction above can take the last live conn while waiters queued
	// behind a full pool are still here, and EnsureDialForWaiters returns without
	// rescuing them whenever a dial backoff is open. Nothing else looks at the
	// queue until the NEXT tick, a whole HealthCheckPeriod away.
	p.FlushStrandedWaiters(rs, ErrDialBackoff)
}

// handleClose drains waiters and shuts down all connections.
func (p *Pool) handleClose(rs *RunState) {
	for _, w := range rs.Waiters {
		p.replyAcquire(w, nil, ErrPoolClosed)
	}
	rs.Waiters = nil
	// Drain every in-flight dial asynchronously so Close returns promptly even
	// with a hung dial (the watchdog cancels it once closedCh closes, right
	// after this returns). Each outstanding dialOne delivers exactly one
	// result; Closing any completed conn here keeps it from being orphaned in
	// the buffered dialDoneCh (a conn + reader-goroutine + fd leak).
	if n := rs.InFlightDials; n > 0 {
		rs.InFlightDials = 0
		go func() {
			for i := 0; i < n; i++ {
				if dr := <-p.dialDoneCh; dr.Mc != nil {
					_ = dr.Mc.C.Close()
					p.notifyClose(CloseManual)
				}
			}
		}()
	}
	for _, mc := range rs.Conns {
		reason := CloseManual
		if mc.C.GoAwayReceived() {
			reason = CloseGoAway
			p.rec.GoAwayReceived()
		}
		_ = mc.C.Close()
		p.notifyClose(reason)
	}
}

// replyAcquire delivers the single reply owed to req. The send never blocks:
// reply is a cap-1 channel used by exactly one request, and the actor sends
// exactly one reply per request it accepts (happy path, serveWaiters,
// HandleDialDone, pruneExpiredWaiters, or handleClose).
//
// This must NOT race the send against req.Ctx.Done(). When a caller has given up
// AND its buffered reply channel is still writable, both cases are ready and Go
// picks at random; picking the send strands an mc whose active count the actor
// has already incremented in a channel nobody reads, so mc.Active-- never runs
// and the stream slot is leaked for the life of the conn. Abandoning callers
// reclaim through acquire's reclaim goroutine instead — the only handoff that
// cannot drop a committed conn.
func (p *Pool) replyAcquire(req AcquireReq, mc *ManagedConn, err error) {
	req.Reply <- AcquireResp{Mc: mc, Err: err}
}

// reclaim consumes the reply owed to an abandoned acquire and returns any conn
// the actor committed to it. Spawned only once the actor has accepted the
// request, at which point exactly one reply is guaranteed, so this receive
// always completes and the goroutine always exits.
func (p *Pool) reclaim(reply chan AcquireResp) {
	if resp := <-reply; resp.Mc != nil {
		p.Release(resp.Mc)
	}
}

// PickLeastLoaded returns the live, under-cap mc with smallest active
// count, or nil if none qualifies.
//
// Reads mc.StreamCap (cached) instead of taking c.psMu.RLock() per call.
// The cache is refreshed in the dialDoneCh handler and on every tick.
// It stops at the first idle connection, which is exactly what a full scan would
// return: zero is the smallest possible active count and the comparison is
// strict, so ties already go to the earliest connection in the slice. See the H3
// twin in h3_pool.go — the same loop, the same reasoning, and #448 for the
// profile that motivated it.
func (p *Pool) PickLeastLoaded(conns []*ManagedConn) *ManagedConn {
	n := len(conns)
	if n == 0 {
		return nil
	}
	start := p.pickCursor % n
	var best *ManagedConn
	for k := 0; k < n; k++ {
		mc := conns[(start+k)%n]
		if !mc.C.IsAlive() {
			continue
		}
		if mc.Active >= mc.StreamCap {
			continue
		}
		if mc.Active == 0 {
			p.pickCursor = (start + k + 1) % n
			return mc
		}
		if best == nil || mc.Active < best.Active {
			best = mc
		}
	}
	return best
}

// refreshStreamCap recomputes mc.StreamCap from the conn's current peer
// SETTINGS_MAX_CONCURRENT_STREAMS. Called by the actor on dial completion
// and on each tick.
func (p *Pool) refreshStreamCap(mc *ManagedConn) {
	mc.StreamCap = EffectiveStreamCap(p.opts.MaxStreamsPerConn, mc.C.PeerMaxConcurrentStreams())
}

// DialEnv snapshots what DialAttempt needs from this pool.
func (p *Pool) DialEnv() DialEnv {
	return DialEnv{ClosedCh: p.closedCh, Timeout: p.opts.DialTimeout, Addr: p.Addr, Rec: p.rec, Obs: p.obs}
}

// dialOne dials one conn and delivers it to the actor. Always delivers:
// handleClose's drainer receives this and closes the conn if the pool shut
// down before it could be pooled, so it is never orphaned.
//
// A non-nil wrap runs here — once per connection, on the goroutine that dialled
// it, never on the actor and never per Acquire.
func (p *Pool) dialOne() {
	c, err := DialAttempt(p.DialEnv(), func(ctx context.Context) (*conn.Conn, error) {
		return conn.Dial(ctx, p.Addr, p.connOpts)
	})
	if err != nil {
		p.dialDoneCh <- DialResult{Err: &DialError{Addr: p.Addr, Err: err}}
		return
	}
	var typed any
	if p.wrap != nil {
		typed, err = p.wrap(c)
		if err != nil {
			p.dialDoneCh <- DialResult{Err: &DialError{Addr: p.Addr, Err: err}}
			return
		}
	}
	p.dialDoneCh <- DialResult{Mc: &ManagedConn{C: c, Typed: typed, LastUsed: time.Now(), p: p}}
}

// serveWaiters hands as many waiters as possible a live mc.
func (p *Pool) serveWaiters(conns []*ManagedConn, waiters []AcquireReq) []AcquireReq {
	for len(waiters) > 0 {
		mc := p.PickLeastLoaded(conns)
		if mc == nil {
			return waiters
		}
		mc.Active++
		mc.LastUsed = time.Now()
		req := waiters[0]
		waiters = waiters[1:]
		p.replyAcquire(req, mc, nil)
	}
	return waiters
}

// notifyClose increments ConnsClosed and fires OnConnClose.
func (p *Pool) notifyClose(reason CloseReason) {
	NotifyConnClose(p.Addr, reason, p.obs, p.rec)
}

// evict removes target from conns, notifies close, and closes the conn.
func (p *Pool) evict(conns []*ManagedConn, target *ManagedConn, reason CloseReason) []*ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if mc == target {
			_ = mc.C.Close()
			p.notifyClose(reason)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// EvictIdle removes conns idle past PoolOptions.IdleTimeout.
func (p *Pool) EvictIdle(conns []*ManagedConn) []*ManagedConn {
	if p.opts.IdleTimeout <= 0 {
		return conns
	}
	now := time.Now()
	out := conns[:0]
	for _, mc := range conns {
		if mc.Active == 0 && now.Sub(mc.LastUsed) > p.opts.IdleTimeout {
			_ = mc.C.Close()
			p.notifyClose(CloseIdle)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDead removes conns whose IsAlive returns false AND that have no
// in-flight streams, mirroring EvictIdle's active==0 guard.
//
// The guard is load-bearing, not tidiness. IsAlive() is false the moment a
// GOAWAY lands, but RFC 7540 §6.8 says streams at or below the GOAWAY's
// lastStreamID MUST be allowed to complete — the peer is still processing them
// and holds the conn open for exactly them. Closing here tears the transport
// down under those streams and surfaces them to the caller as
// RST(INTERNAL_ERROR), turning a graceful server drain into failed requests.
//
// A conn that is dead rather than draining loses nothing by waiting: its reader
// is gone and shutdownStreams has already reset every stream on it, so each
// Release arrives promptly and HandleRelease evicts. Either way PickLeastLoaded
// skips it (not IsAlive) and CountLive excludes it, so while it lingers it can
// neither take new streams nor block a redial.
func (p *Pool) evictDead(conns []*ManagedConn) []*ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.C.IsAlive() && mc.Active == 0 {
			reason := CloseDead
			if mc.C.GoAwayReceived() {
				reason = CloseGoAway
				p.rec.GoAwayReceived()
			}
			_ = mc.C.Close()
			p.notifyClose(reason)
			continue
		}
		out = append(out, mc)
	}
	return out
}

// evictDeadSilent removes conns whose IsAlive returns false and that have no
// in-flight streams, without firing hooks. Used from the Stats path.
//
// Silent means no user callback, not no record. A counter is a record of what
// the pool did, and suppressing it made the pool lie about its own behaviour in
// the one place an operator looks: Stats() is reachable from the public
// Client.PoolStats(), so scraping is what causes these evictions to be
// observed — a conn killed out of band and first noticed by a metrics read was
// closed with ConnsClosed staying at zero forever. The hook stays suppressed;
// firing a lifecycle callback from inside a metrics read is a different thing
// and remains wrong.
//
// Carries evictDead's active==0 guard for the same §6.8 reason, and needs it
// more: Stats() is reachable from the public Client.PoolStats(), so without the
// guard a metrics scrape that happened to land during a peer's graceful drain
// would close the conn out from under its own in-flight requests. Observability
// must not be able to fail a request.
func (p *Pool) evictDeadSilent(conns []*ManagedConn) []*ManagedConn {
	out := conns[:0]
	for _, mc := range conns {
		if !mc.C.IsAlive() && mc.Active == 0 {
			_ = mc.C.Close()
			p.rec.ConnClosed()
			if mc.C.GoAwayReceived() {
				p.rec.GoAwayReceived()
			}
			continue
		}
		out = append(out, mc)
	}
	return out
}

// sumActive sums active stream counts across conns.
func sumActive(conns []*ManagedConn) int {
	n := 0
	for _, mc := range conns {
		n += mc.Active
	}
	return n
}

// CountLive returns the number of conns whose underlying *conn.Conn reports
// IsAlive(). The actor uses it wherever a decision turns on capacity that
// still exists -- the at-cap test in handleAcquire, the dial-for-waiters
// guard, and the terminal-state check in HandleDialDone -- so that a stale
// dead-but-not-yet-evicted entry cannot stand in for usable capacity.
//
// It used to name a function called canDial, which does not exist anywhere in
// the repo.
func CountLive(conns []*ManagedConn) int {
	n := 0
	for _, mc := range conns {
		if mc.C.IsAlive() {
			n++
		}
	}
	return n
}

// pruneExpiredWaiters drops waiters whose ctx is already done. Reuses
// the slice's backing array to avoid allocation churn.
//
// Each dropped waiter is still sent the one reply it is owed: its caller's
// reclaim goroutine is blocked waiting for exactly one value, so dropping the
// waiter silently would hang that goroutine forever. The send cannot block
// (cap-1 channel, single use).
func pruneExpiredWaiters(ws []AcquireReq) []AcquireReq {
	out := ws[:0]
	for _, w := range ws {
		select {
		case <-w.Ctx.Done():
			w.Reply <- AcquireResp{Err: w.Ctx.Err()}
		default:
			out = append(out, w)
		}
	}
	return out
}

// Acquire requests a ManagedConn from the actor. The returned mc's
// active count has already been incremented by the actor. Caller MUST
// eventually call p.Release(mc).
func (p *Pool) Acquire(ctx context.Context) (*ManagedConn, error) {
	start := time.Now()
	// Merge AcquireTimeout into ctx so that ctx.Done() fires on ALL abandonment
	// paths, including AcquireTimeout: the reclaim handoff below and the actor's
	// waiter pruning are both keyed on req.Ctx.
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

	reply := replyPool.Get().(chan AcquireResp)
	// recycle returns the reply channel to replyPool. It is ONLY safe to
	// call when the actor can no longer send on reply — otherwise a late
	// send from the actor would poison the channel for its next user
	// (a different Pool), surfacing as a spurious ErrPoolClosed or a
	// cross-pool conn ("stream reset by peer"). Two states are safe:
	//   (a) the actor never received req (first-select abandonment), or
	//   (b) we consumed the actor's single reply (happy path).
	// On second-select abandonment (ctx.Done / closedCh after the actor took
	// req) the actor still owes exactly one reply, which the reclaim goroutine
	// consumes — but reclaim, not us, is then the channel's last reader, so we
	// drop it to GC rather than hand a channel we no longer own back to a pool
	// shared with every other Pool in the process. One allocation on a path that
	// is already returning an error is the cheaper side of that trade.
	recycle := func() {
		select {
		case <-reply:
		default:
		}
		replyPool.Put(reply)
	}

	req := AcquireReq{Ctx: ctx, Reply: reply}

	// Send the request to the actor.
	select {
	case p.acquireCh <- req:
		// The actor now owns req and owes it exactly one reply.
	case <-ctx.Done():
		recycle() // actor never received req — safe to recycle
		return nil, MapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		recycle() // actor never received req — safe to recycle
		return nil, ErrPoolClosed
	}

	// Wait for the actor's reply.
	select {
	case resp := <-reply:
		recycle() // consumed the actor's single send — safe to recycle
		if resp.Err == nil {
			p.rec.ObserveAcquire(time.Since(start))
		}
		return resp.Mc, resp.Err
	case <-ctx.Done():
		// Both this case and the reply case can be ready at once (the actor may
		// have just committed a conn to us); Go would pick between them at random.
		// Hand the pending reply to reclaim so a conn committed to an acquire we
		// are abandoning goes back to the pool instead of being stranded.
		// reclaim owns the channel from here — do NOT recycle.
		go p.reclaim(reply)
		return nil, MapAcquireErr(ctx, acquireTimeoutActive)
	case <-p.closedCh:
		go p.reclaim(reply)
		return nil, ErrPoolClosed
	}
}

// MapAcquireErr converts ctx.Err() to the right sentinel. If the
// deadline was introduced by AcquireTimeout (not the caller's own ctx),
// we return ErrAcquireTimeout to distinguish it from context.Canceled or
// a caller-supplied context.DeadlineExceeded.
func MapAcquireErr(ctx context.Context, acquireTimeoutActive bool) error {
	if acquireTimeoutActive && ctx.Err() == context.DeadlineExceeded {
		return ErrAcquireTimeout
	}
	return ctx.Err()
}

// Release returns mc to the actor.
//
// It carries no request error. HandleRelease re-checks IsAlive
// unconditionally, which is strictly the safer rule: it catches a conn that
// died under a request that SUCCEEDED, and it does not evict a healthy conn
// merely because one request on it failed. The parameter used to be here and
// the actor never read it, while this comment claimed the opposite — so anyone
// reasoning about eviction from the signature was reasoning about behaviour
// that did not exist.
func (p *Pool) Release(mc *ManagedConn) {
	if mc == nil {
		return
	}
	select {
	case p.releaseCh <- ReleaseMsg{Mc: mc}:
	case <-p.closedCh:
		// Pool already closed: the actor is gone and won't process this
		// release, so close the conn directly rather than dropping it (a leak).
		// conn.Close is idempotent if handleClose already closed it.
		if mc.C != nil {
			_ = mc.C.Close()
		}
	}
}

// InDialBackoff reports whether a previous dial error is still within
// the configured DialBackoff window. Returns false if no previous error
// or if window <= 0.
func InDialBackoff(lastErrAt time.Time, window time.Duration) bool {
	if lastErrAt.IsZero() || window <= 0 {
		return false
	}
	return time.Since(lastErrAt) < window
}

// EffectiveStreamCap computes min(localCap, peerCap). Either may be
// zero meaning "unbounded". Returns defaultMaxConcurrentStreams if both
// are unbounded.
func EffectiveStreamCap(localCap, peerCap int) int {
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

// Warmup pre-dials up to n conns in the background. Idempotent.
// n is capped at MaxConnsPerHost. Returns immediately; dial errors
// are surfaced via the OnDial hook.
func (p *Pool) Warmup(n int) {
	if n <= 0 {
		return
	}
	select {
	case p.warmupCh <- n:
	case <-p.closedCh:
	}
}

// handleWarmup starts the dials that bring the pool up to n connections. Runs on
// the actor goroutine, so it reads rs directly and makes the same capacity and
// backoff decision handleAcquire makes — minus the waiter, because warmup has no
// request to serve.
//
// It cannot be expressed as a loop of acquire+release, which is what it used to
// be. This pool multiplexes: PickLeastLoaded returns any conn with a free stream
// slot, so the second acquire reuses the conn the first one just released and
// the pool opens exactly ONE connection no matter what n is. Holding the conns
// instead does not help either — a held conn still has free stream slots — which
// is why the fix h1Pool.warmup uses (hold, then release together) does not port
// here. h1 checks out its connection exclusively; this one does not.
func (p *Pool) handleWarmup(rs *RunState, n int) {
	target := n
	if target > p.opts.MaxConnsPerHost {
		target = p.opts.MaxConnsPerHost
	}
	need := target - CountLive(rs.Conns) - rs.InFlightDials
	if need <= 0 {
		return
	}
	if InDialBackoff(rs.LastDialErrAt, p.opts.DialBackoff) {
		return // a warmup must not defeat the backoff a failing peer earned
	}
	for i := 0; i < need; i++ {
		rs.InFlightDials++
		go p.dialOne()
	}
}

// Release implements releaser: it hands this conn back to its pool.
func (mc *ManagedConn) Release() { mc.p.Release(mc) }
