package client

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http3"
)

// h3FakeDialer hands out a fresh fakeH3Client per dial and records them keyed by
// dial addr, so a pool test can observe how many QUIC connections were opened to
// each host. Safe for concurrent use.
type h3FakeDialer struct {
	mu      sync.Mutex
	byAddr  map[string][]*fakeH3Client
	dialErr error // if set, every dial fails with this error
	dials   atomic.Int32
}

func newH3FakeDialer() *h3FakeDialer {
	return &h3FakeDialer{byAddr: map[string][]*fakeH3Client{}}
}

func (d *h3FakeDialer) dial(_ context.Context, addr string, _ *tls.Config) (h3Client, error) {
	d.dials.Add(1)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	f := &fakeH3Client{resp: &http3.Response{Status: 200}, body: []byte("ok")}
	d.byAddr[addr] = append(d.byAddr[addr], f)
	return f, nil
}

func (d *h3FakeDialer) count(addr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.byAddr[addr])
}

func (d *h3FakeDialer) all() []*fakeH3Client {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []*fakeH3Client
	for _, v := range d.byAddr {
		out = append(out, v...)
	}
	return out
}

func TestH3Pool_Stats_Empty(t *testing.T) {
	t.Parallel()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 2}, newH3FakeDialer().dial, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	s := p.Stats()

	assert.Equal(t, Stats{}, s, "a pool that has dialed nothing must report zero, not a partial snapshot")
}

func TestH3Pool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)

	first := p.Close()
	second := p.Close()

	assert.NoError(t, first, "first Close")
	assert.NoError(t, second, "second Close must be a no-op, not an error a caller has to special-case")
}

func TestH3Pool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)
	_ = p.Close()

	s := p.Stats()
	_, err := p.acquire(context.Background())

	assert.Equal(t, Stats{}, s, "Stats after Close must be zero, not the last live snapshot")
	assert.Truef(t, errors.Is(err, ErrPoolClosed),
		"acquire after Close = %v; a caller cannot tell a closed pool from a dial failure", err)
}

// TestH3Pool_DistributesStreamsAcrossConns is the core pooling behaviour: with
// MaxStreamsPerConn=1 and MaxConnsPerHost=3, holding three concurrent streams
// forces the pool to open three distinct QUIC connections, one stream each.
func TestH3Pool_DistributesStreamsAcrossConns(t *testing.T) {
	t.Parallel()
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   3,
		MaxStreamsPerConn: 1,
		HealthCheckPeriod: time.Second,
	}, d.dial, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	// Hold three streams simultaneously; cap=1 means each must land on its own conn.
	held := make([]*h3ManagedConn, 0, 3)
	seen := map[h3Client]bool{}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		mc, err := p.acquire(ctx)
		cancel()
		require.NoErrorf(t, err, "acquire[%d]", i)
		require.Equalf(t, 1, mc.active, "acquire[%d] conn active count", i)
		require.Falsef(t, seen[mc.cl], "acquire[%d] reused a conn already at its stream cap", i)
		seen[mc.cl] = true
		held = append(held, mc)
	}
	// A fourth acquire is at capacity (3 conns × 1 stream) and must block until a
	// hold is released, proving the pool does not exceed MaxConnsPerHost.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, fourthErr := p.acquire(ctx)
	cancel()

	assert.Equal(t, 3, d.count("h:443"), "one QUIC conn per capped stream")
	s := p.Stats()
	assert.Equal(t, 3, s.ActiveConns, "Stats.ActiveConns with three streams held")
	assert.Equal(t, 3, s.InFlightStreams, "Stats.InFlightStreams with three streams held")
	assert.Truef(t, errors.Is(fourthErr, context.DeadlineExceeded),
		"fourth acquire at capacity = %v, want DeadlineExceeded: the pool must block "+
			"rather than exceed MaxConnsPerHost", fourthErr)

	for _, mc := range held {
		p.release(mc)
	}
}

// TestH3Pool_ReusesConnUnderCap verifies that when a conn is under its stream cap,
// sequential acquires reuse it rather than dialing new QUIC connections.
func TestH3Pool_ReusesConnUnderCap(t *testing.T) {
	t.Parallel()
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 4,
		HealthCheckPeriod: time.Second,
	}, d.dial, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	var first h3Client
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		mc, err := p.acquire(ctx)
		cancel()
		require.NoErrorf(t, err, "acquire[%d]", i)
		if i == 0 {
			first = mc.cl
		} else {
			require.Samef(t, first, mc.cl,
				"acquire[%d] used a new conn; want reuse of the single under-cap conn", i)
		}
		p.release(mc)
	}

	assert.Equal(t, 1, d.count("h:443"),
		"three sequential under-cap acquires must share one conn, not dial per request")
}

// TestH3Pool_EvictsDeadConnOnRelease proves the release path (not the background
// health-check tick) evicts a QUIC connection whose Alive() has flipped false. The
// OnConnClose(CloseDead) hook fires only from an eviction path, and with a 60s
// HealthCheckPeriod the tick cannot be responsible.
func TestH3Pool_EvictsDeadConnOnRelease(t *testing.T) {
	t.Parallel()
	var deadClosed atomic.Int32
	hooks := &Hooks{
		OnConnClose: func(e ConnCloseEvent) {
			if e.Reason == CloseDead {
				deadClosed.Add(1)
			}
		},
	}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(hooks)
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   2,
		MaxStreamsPerConn: 4,
		HealthCheckPeriod: 60 * time.Second, // tick cannot be the cause
		DialBackoff:       10 * time.Millisecond,
	}, d.dial, hp, nil)
	t.Cleanup(func() { _ = p.Close() })
	mc, err := p.acquire(context.Background())
	require.NoError(t, err, "acquire")
	fake, ok := mc.cl.(*fakeH3Client)
	require.Truef(t, ok, "pooled conn is %T, want *fakeH3Client", mc.cl)

	// Simulate the QUIC connection dying, then release.
	fake.kill()
	p.release(mc)

	// The release path must evict (fire CloseDead) promptly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && deadClosed.Load() != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.EqualValues(t, 1, deadClosed.Load(),
		"OnConnClose(CloseDead) did not fire once via the release path; with a 60s "+
			"HealthCheckPeriod the tick cannot have been the evictor")
	assert.GreaterOrEqual(t, atomic.LoadInt32(&fake.closes), int32(1),
		"the dead conn was evicted but never closed")
	assert.Equal(t, 0, p.Stats().ActiveConns, "ActiveConns after a dead-conn release")
}

// TestH3Pool_Close_ClosesAllConns verifies Close closes every pooled QUIC conn and
// fires OnConnClose(CloseManual).
func TestH3Pool_Close_ClosesAllConns(t *testing.T) {
	t.Parallel()
	var closes atomic.Int32
	hooks := &Hooks{OnConnClose: func(ConnCloseEvent) { closes.Add(1) }}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(hooks)
	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 3, MaxStreamsPerConn: 1}, d.dial, hp, nil)
	// Hold three streams so all three conns exist when Close runs.
	for i := 0; i < 3; i++ {
		_, err := p.acquire(context.Background())
		require.NoErrorf(t, err, "acquire[%d]", i)
	}

	_ = p.Close()

	assert.EqualValues(t, 3, closes.Load(), "OnConnClose must fire once per pooled conn on Close")
	for i, f := range d.all() {
		assert.EqualValuesf(t, 1, atomic.LoadInt32(&f.closes),
			"pooled conn %d was closed the wrong number of times", i)
	}
}

// TestH3Pool_DialFailure_SetsBackoff mirrors the H2 pool: a failed dial sets the
// backoff window, and a second acquire inside it returns ErrDialBackoff without a
// new dial.
func TestH3Pool_DialFailure_SetsBackoff(t *testing.T) {
	t.Parallel()
	d := newH3FakeDialer()
	d.dialErr = errors.New("quic: handshake failed")
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost: 1,
		DialBackoff:     500 * time.Millisecond,
	}, d.dial, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, firstErr := p.acquire(ctx)
	_, secondErr := p.acquire(ctx)

	require.Error(t, firstErr, "the first acquire must fail on the dial error")
	assert.Truef(t, errors.Is(secondErr, ErrDialBackoff),
		"second acquire = %v, want ErrDialBackoff", secondErr)
	assert.EqualValues(t, 1, d.dials.Load(),
		"the backoff window did not suppress the second dial")
}

// --- end-to-end through Client ---

// newH3PoolTestClient builds a TransportH3Pool Client whose pool dials fakes. The
// dialFn is swapped before the first request; the acquire→dialOne channel hop
// provides the happens-before so there is no race with the actor goroutine.
func newH3PoolTestClient(t *testing.T, pool PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error)) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		Addr:      "h3.example:443",
		Transport: TransportH3Pool,
		TLSConfig: &tls.Config{ServerName: "h3.example"},
		Pool:      &pool,
	})
	require.NoError(t, err, "NewClient(TransportH3Pool)")
	pt, ok := c.tr.(*h3PoolTransport)
	require.Truef(t, ok, "transport is %T, want *h3PoolTransport", c.tr)
	pt.p.dialFn = dialFn
	return c
}

func TestH3PoolClient_Do_RoundTripAndReuse(t *testing.T) {
	t.Parallel()
	d := newH3FakeDialer()
	c := newH3PoolTestClient(t, PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, d.dial)
	defer func() { _ = c.Close() }()

	resps := make([]Response, 3)
	for i := range resps {
		require.NoErrorf(t,
			c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resps[i]),
			"Do[%d]", i)
	}

	for i, resp := range resps {
		assert.Equalf(t, 200, resp.Status, "Do[%d] status", i)
		assert.Equalf(t, "ok", string(resp.Body), "Do[%d] body", i)
	}
	// Sequential buffered requests release their stream immediately, so all three
	// reuse the one under-cap conn.
	assert.Equal(t, 1, d.count("h3.example:443"),
		"three sequential requests dialed more than one conn: the stream was not "+
			"released back to the under-cap conn")
	assert.Equal(t, 1, c.PoolStats().ActiveConns, "PoolStats.ActiveConns")
}

// TestH3PoolClient_ConcurrentDo_DistributesAcrossConns fires more concurrent
// requests than one conn's stream cap and asserts multiple QUIC conns absorb them.
// A barrier pins every request in-flight simultaneously so the snapshot is race-free.
func TestH3PoolClient_ConcurrentDo_DistributesAcrossConns(t *testing.T) {
	t.Parallel()
	const (
		streamsPerConn = 2
		maxConns       = 3
		n              = streamsPerConn * maxConns // 6 simultaneous requests
	)
	release := make(chan struct{})
	var inflight atomic.Int32
	allInflight := make(chan struct{})
	var once sync.Once
	dial := func(_ context.Context, _ string, _ *tls.Config) (h3Client, error) {
		return &barrierH3Client{
			resp:      &http3.Response{Status: 200},
			release:   release,
			inflight:  &inflight,
			target:    n,
			signalAll: func() { once.Do(func() { close(allInflight) }) },
		}, nil
	}
	c := newH3PoolTestClient(t, PoolOptions{
		MaxConnsPerHost:   maxConns,
		MaxStreamsPerConn: streamsPerConn,
		HealthCheckPeriod: time.Second,
		AcquireTimeout:    5 * time.Second,
	}, dial)
	defer func() { _ = c.Close() }()

	var wg sync.WaitGroup
	wg.Add(n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			var resp Response
			if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp); err != nil {
				errs <- err
			}
		}()
	}

	select {
	case <-allInflight:
	case <-time.After(5 * time.Second):
		close(release)
		require.FailNowf(t, "the pool never got every request in flight",
			"only %d/%d requests reached the server; the pool may be queueing instead "+
				"of opening conns", inflight.Load(), n)
	}
	s := c.PoolStats()
	if s.ActiveConns < 2 {
		close(release)
	}
	require.GreaterOrEqualf(t, s.ActiveConns, 2,
		"ActiveConns = %d during %d-way load with per-conn cap %d; one conn cannot "+
			"carry them all", s.ActiveConns, n, streamsPerConn)
	// Expect exactly ceil(n/streamsPerConn) = maxConns conns to absorb the load.
	if s.ActiveConns != maxConns {
		t.Logf("ActiveConns = %d (want %d); acceptable if >= 2", s.ActiveConns, maxConns)
	}

	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err, "Do under concurrent load")
	}
}

// barrierH3Client blocks each Do until release is closed, after signalling that it
// is in-flight — so a concurrency test can pin all requests simultaneously.
type barrierH3Client struct {
	resp      *http3.Response
	release   <-chan struct{}
	inflight  *atomic.Int32
	target    int
	signalAll func()
	closes    int32
	dead      int32
	goaway    int32
}

func (b *barrierH3Client) Do(ctx context.Context, _ *http3.Request) (*http3.Response, []byte, error) {
	if b.inflight.Add(1) == int32(b.target) {
		b.signalAll()
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	return b.resp, []byte("ok"), nil
}

func (b *barrierH3Client) DoStream(context.Context, *http3.Request) (*http3.Response, http3.ResponseBody, error) {
	return nil, nil, errors.New("barrierH3Client: DoStream not used")
}
func (b *barrierH3Client) Alive() bool     { return atomic.LoadInt32(&b.dead) == 0 }
func (b *barrierH3Client) GoingAway() bool { return atomic.LoadInt32(&b.goaway) != 0 }
func (b *barrierH3Client) Close() error {
	atomic.AddInt32(&b.closes, 1)
	return nil
}

// --- constructor + validation ---

func TestNewH3PoolClient_WiresTransport(t *testing.T) {
	t.Parallel()

	c, err := NewH3PoolClient("h3.example:443", &tls.Config{ServerName: "h3.example"},
		PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 8})

	require.NoError(t, err, "NewH3PoolClient")
	defer func() { _ = c.Close() }()
	pt, ok := c.tr.(*h3PoolTransport)
	require.Truef(t, ok, "transport is %T, want *h3PoolTransport", c.tr)
	assert.Equal(t, "h3.example:443", pt.p.addr, "the pool was not wired with the dial address")
	assert.Equal(t, 4, pt.p.opts.MaxConnsPerHost, "the pool did not receive the caller's PoolOptions")
}

func TestNewClient_H3Pool_RequiresPoolAndTLS(t *testing.T) {
	t.Parallel()

	_, noPool := NewClient(ClientOptions{
		Addr: "h:443", Transport: TransportH3Pool, TLSConfig: &tls.Config{ServerName: "h"},
	})
	_, noTLS := NewClient(ClientOptions{
		Addr: "h:443", Transport: TransportH3Pool, Pool: &PoolOptions{MaxConnsPerHost: 2},
	})

	assert.Truef(t, errors.Is(noPool, ErrInvalidPoolOptions),
		"missing Pool err = %v, want ErrInvalidPoolOptions so a caller can classify it", noPool)
	assert.Error(t, noTLS, "TransportH3Pool without a TLSConfig has nothing to dial with")
}

// TestConformance_RFC9114_Sec52_PoolEvictsGoAwayConn pins the pool half of the §5.2
// rule. Once the H3 client refuses every new request after GOAWAY, a pool that
// still hands that connection out turns a graceful server shutdown into a retry
// loop: the retry classifier treats http3.ErrGoAway as retryable, re-acquires the
// same conn, and fails identically until retries are exhausted. The conn must stop
// being selected immediately and be evicted once its in-flight work drains — never
// closed under a request the server has undertaken to finish.
func TestConformance_RFC9114_Sec52_PoolEvictsGoAwayConn(t *testing.T) {
	p := inertH3Pool(PoolOptions{}, nil)
	gone := &barrierH3Client{}
	fresh := &barrierH3Client{}
	conns := []*h3ManagedConn{
		{cl: gone, active: 0, streamCap: 10},
		{cl: fresh, active: 1, streamCap: 10},
	}
	baseline := p.pickLeastLoaded(conns)
	require.NotNil(t, baseline, "baseline: an idle conn must be selectable at all")
	require.Same(t, gone, baseline.cl, "baseline: the least-loaded pick should be the idle conn")

	atomic.StoreInt32(&gone.goaway, 1)

	afterGoAway := p.pickLeastLoaded(conns)
	require.NotNil(t, afterGoAway, "a healthy conn was still available and must be picked")
	assert.Same(t, fresh, afterGoAway.cl,
		"a GOAWAY'd conn must not be handed out for a new request (RFC 9114 §5.2): "+
			"reusing it turns a graceful shutdown into a retry loop")

	// Two exchanges still in flight on it: the first release must NOT close it —
	// the server has undertaken to finish what it already accepted.
	rs := &h3RunState{conns: conns}
	conns[0].active = 2
	p.handleRelease(rs, h3ReleaseMsg{mc: conns[0]})
	require.Len(t, rs.conns, 2, "a GOAWAY'd conn with work still in flight must not be evicted")
	require.EqualValues(t, 0, atomic.LoadInt32(&gone.closes),
		"a GOAWAY'd conn must not be closed under an in-flight request")

	// The last one drains: now it has nothing left to do and is evicted.
	p.handleRelease(rs, h3ReleaseMsg{mc: conns[0]})

	require.Len(t, rs.conns, 1, "a drained GOAWAY'd conn was not evicted")
	assert.Same(t, fresh, rs.conns[0].cl, "the healthy conn must be the survivor")
	assert.EqualValues(t, 1, atomic.LoadInt32(&gone.closes),
		"the drained GOAWAY'd conn should have been closed exactly once")
}

// TestConformance_RFC9114_Sec52_PoolEvictsIdleGoAwayConn is the other half of the
// §5.2 pool rule, and the one the release-path test cannot reach: a server
// typically starts a graceful shutdown against an IDLE connection, so no release
// ever arrives to trigger eviction. Without the health-tick and live-count halves
// the pool sits at MaxConnsPerHost with a conn that can serve nothing, and every
// acquire is parked as a waiter with no dial started — a permanent wedge.
func TestConformance_RFC9114_Sec52_PoolEvictsIdleGoAwayConn(t *testing.T) {
	p := inertH3Pool(PoolOptions{}, nil)
	gone := &barrierH3Client{}
	rs := &h3RunState{conns: []*h3ManagedConn{{cl: gone, active: 0, streamCap: 10}}}
	atomic.StoreInt32(&gone.goaway, 1)

	live := h3CountLive(rs.conns)
	p.handleTick(rs)

	assert.Equal(t, 0, live,
		"a GOAWAY'd conn must not hold the cap: counting it parks every acquire as a "+
			"waiter with no dial started")
	assert.Empty(t, rs.conns, "an idle GOAWAY'd conn survived the health tick")
	assert.EqualValues(t, 1, atomic.LoadInt32(&gone.closes), "the evicted conn was not closed once")
}

// TestConformance_RFC9114_Sec52_GoAwayEvictionDialsForWaiter covers the case the
// two eviction tests cannot: a request parked as a waiter BEFORE the GOAWAY
// arrived. Skipping the conn in pickLeastLoaded and dropping it from the live count
// only rescues acquires that come afterwards; handleAcquire is the sole other
// dialer, and it does not run again for an already-parked request. With no
// AcquireTimeout set — which newH3Pool does not default — that waiter would hang
// until the pool closed. An eviction that shrinks the pool must start the
// replacement dial itself.
func TestConformance_RFC9114_Sec52_GoAwayEvictionDialsForWaiter(t *testing.T) {
	dialed := make(chan struct{}, 1)
	dialFn := func(context.Context, string, *tls.Config) (h3Client, error) {
		dialed <- struct{}{}
		return &barrierH3Client{}, nil
	}
	p := inertH3Pool(PoolOptions{MaxConnsPerHost: 1}, dialFn)
	gone := &barrierH3Client{}
	mc := &h3ManagedConn{cl: gone, active: 1, streamCap: 1}
	rs := &h3RunState{
		conns:   []*h3ManagedConn{mc},
		waiters: []h3AcquireReq{{ctx: context.Background(), reply: make(chan h3AcquireResp, 1)}},
	}
	atomic.StoreInt32(&gone.goaway, 1)

	p.handleRelease(rs, h3ReleaseMsg{mc: mc})

	assert.Empty(t, rs.conns, "the drained GOAWAY'd conn was not evicted")
	assert.Equalf(t, 1, rs.inFlightDials,
		"inFlightDials = %d, want 1 — the parked waiter has nothing left to be served by",
		rs.inFlightDials)
	select {
	case <-dialed:
	case <-time.After(2 * time.Second):
		assert.Fail(t, "no replacement dial was started for the parked waiter",
			"handleAcquire does not run again for an already-parked request, and "+
				"newH3Pool does not default AcquireTimeout, so this waiter hangs until Close")
	}
}

// inertH3Pool builds an h3Pool WITHOUT starting its actor goroutine, for tests that
// drive handleAcquire / handleRelease / handleDialDone / handleTick directly on
// their own h3RunState. newH3Pool would start a second actor with its own state, so
// a dial these tests provoke would land in that other state instead.
func inertH3Pool(opts PoolOptions, dialFn func(context.Context, string, *tls.Config) (h3Client, error)) *h3Pool {
	// Same defaults newH3Pool applies — DialBackoff in particular, since the
	// dial-done error arm's behaviour turns on whether the backoff is active.
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
		dialFn = func(context.Context, string, *tls.Config) (h3Client, error) {
			return nil, ErrPoolClosed // never reached in these tests; never a real dial
		}
	}
	return &h3Pool{
		opts:       opts,
		addr:       "e",
		dialFn:     dialFn,
		dialDoneCh: make(chan h3DialResult, 4),
		closeCh:    make(chan struct{}),
		closedCh:   make(chan struct{}),
		metrics:    &Metrics{},
	}
}

// TestH3Pool_DialFailureDoesNotStrandQueuedWaiters pins the two ways a parked
// waiter could be abandoned with no dialer. newH3Pool does not default
// AcquireTimeout, so either one is an unbounded hang, not a slow path.
func TestH3Pool_DialFailureDoesNotStrandQueuedWaiters(t *testing.T) {
	newWaiter := func() h3AcquireReq {
		return h3AcquireReq{ctx: context.Background(), reply: make(chan h3AcquireResp, 1)}
	}

	// A failed dial replies to the first waiter. The ones behind it must not be left
	// to a health-check tick: with nothing live and nothing in flight the pool is in
	// exactly the state that fast-refuses a NEW acquire, so they get the same error.
	t.Run("behind_the_first", func(t *testing.T) {
		p := inertH3Pool(PoolOptions{MaxConnsPerHost: 1}, nil)
		a, b := newWaiter(), newWaiter()
		rs := &h3RunState{waiters: []h3AcquireReq{a, b}, inFlightDials: 1}

		p.handleDialDone(rs, h3DialResult{err: ErrPoolClosed})

		for i, w := range []h3AcquireReq{a, b} {
			select {
			case resp := <-w.reply:
				assert.Errorf(t, resp.err, "waiter %d got a conn, want the dial error", i)
			default:
				assert.Failf(t, "a waiter was left queued with no dial in flight",
					"waiter %d waits a whole HealthCheckPeriod while a fresh acquire in the "+
						"same state is refused instantly", i)
			}
		}
		assert.Empty(t, rs.waiters, "waiters remained queued after every one was answered")
	})

	// An eviction that lands inside the dial backoff cannot dial. This used to
	// assert that the waiter waits for the backoff to expire and is then dialled
	// for by a later tick — which #425 changed, because in production that later
	// tick is a whole HealthCheckPeriod away (the manual second handleTick below
	// was the only reason the wait looked short), while handleAcquire refuses a
	// FRESH acquire in the identical state instantly. The waiter is answered now.
	t.Run("eviction_inside_backoff_refuses_rather_than_parks", func(t *testing.T) {
		p := inertH3Pool(PoolOptions{MaxConnsPerHost: 1, DialBackoff: time.Hour}, nil)
		w := newWaiter()
		rs := &h3RunState{waiters: []h3AcquireReq{w}, lastDialErrAt: time.Now()}

		p.handleTick(rs)

		assert.Equal(t, 0, rs.inFlightDials, "the tick dialled inside the backoff window")
		select {
		case resp := <-w.reply:
			assert.Truef(t, errors.Is(resp.err, ErrDialBackoff),
				"waiter got %v, want ErrDialBackoff — the answer handleAcquire "+
					"already gives a new request in this exact state", resp.err)
		default:
			assert.Fail(t, "waiter left parked for the next health tick",
				"that tick is a whole HealthCheckPeriod away, while handleAcquire refuses "+
					"a fresh acquire in the identical state instantly")
		}
		assert.Empty(t, rs.waiters, "waiters remained queued after being answered")
	})

	// The other half of what the subtest above used to cover, split out because it
	// is a different property and the refusal now hides it: the tick's dial is
	// UNCONDITIONAL, not gated on the tick having just shrunk the pool. Were it
	// gated, an already-empty pool would never shrink again and nothing would
	// retry.
	t.Run("tick_dials_for_a_waiter_once_the_backoff_is_closed", func(t *testing.T) {
		dialed := make(chan struct{}, 1)
		dialFn := func(context.Context, string, *tls.Config) (h3Client, error) {
			dialed <- struct{}{}
			return &barrierH3Client{}, nil
		}
		p := inertH3Pool(PoolOptions{MaxConnsPerHost: 1, DialBackoff: 10 * time.Millisecond}, dialFn)
		// Backoff long closed, pool empty, one waiter: nothing shrank in this tick
		// and it must still dial. No sleep — the clock is not what is under test.
		rs := &h3RunState{
			waiters:       []h3AcquireReq{newWaiter()},
			lastDialErrAt: time.Now().Add(-time.Hour),
		}

		p.handleTick(rs)

		assert.Equalf(t, 1, rs.inFlightDials,
			"inFlightDials = %d with the backoff closed, want 1: the tick's dial must be "+
				"unconditional, not gated on the tick having just shrunk the pool",
			rs.inFlightDials)
		select {
		case <-dialed:
		case <-time.After(2 * time.Second):
			assert.Fail(t, "no dial started for the waiter once the backoff was closed")
		}
	})
}
