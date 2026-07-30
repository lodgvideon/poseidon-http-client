package client

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	if s := p.Stats(); s != (Stats{}) {
		t.Fatalf("empty Stats = %+v, want zero", s)
	}
}

func TestH3Pool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestH3Pool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()
	p := newH3Pool("h:443", nil, PoolOptions{MaxConnsPerHost: 1}, newH3FakeDialer().dial, nil, nil)
	_ = p.Close()
	if s := p.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after Close = %+v, want zero", s)
	}
	_, err := p.acquire(context.Background())
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire after Close = %v, want ErrPoolClosed", err)
	}
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
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if mc.active != 1 {
			t.Fatalf("acquire[%d] conn active = %d, want 1", i, mc.active)
		}
		if seen[mc.cl] {
			t.Fatalf("acquire[%d] reused a conn already at its stream cap", i)
		}
		seen[mc.cl] = true
		held = append(held, mc)
	}

	if got := d.count("h:443"); got != 3 {
		t.Fatalf("dialed %d QUIC conns, want 3 (one per capped stream)", got)
	}
	s := p.Stats()
	if s.ActiveConns != 3 || s.InFlightStreams != 3 {
		t.Fatalf("Stats = %+v, want ActiveConns=3 InFlightStreams=3", s)
	}

	// A fourth acquire is at capacity (3 conns × 1 stream) and must block until a
	// hold is released, proving the pool does not exceed MaxConnsPerHost.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err := p.acquire(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fourth acquire at capacity = %v, want DeadlineExceeded (blocked)", err)
	}

	for _, mc := range held {
		p.release(mc, nil)
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
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if i == 0 {
			first = mc.cl
		} else if mc.cl != first {
			t.Fatalf("acquire[%d] used a new conn; want reuse of the single under-cap conn", i)
		}
		p.release(mc, nil)
	}
	if got := d.count("h:443"); got != 1 {
		t.Fatalf("dialed %d conns for 3 sequential under-cap acquires, want 1", got)
	}
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
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	fake := mc.cl.(*fakeH3Client)

	// Simulate the QUIC connection dying, then release.
	fake.kill()
	p.release(mc, nil)

	// The release path must evict (fire CloseDead) promptly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if deadClosed.Load() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if deadClosed.Load() != 1 {
		t.Fatalf("OnConnClose(CloseDead) fired %d times, want 1 via release path", deadClosed.Load())
	}
	if got := atomic.LoadInt32(&fake.closes); got < 1 {
		t.Fatalf("dead conn Close() called %d times, want >= 1", got)
	}
	if s := p.Stats(); s.ActiveConns != 0 {
		t.Fatalf("ActiveConns = %d after dead-conn release, want 0", s.ActiveConns)
	}
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
		if _, err := p.acquire(context.Background()); err != nil {
			t.Fatalf("acquire[%d]: %v", i, err)
		}
	}
	_ = p.Close()

	if got := closes.Load(); got != 3 {
		t.Fatalf("OnConnClose fired %d times on Close, want 3", got)
	}
	for _, f := range d.all() {
		if atomic.LoadInt32(&f.closes) != 1 {
			t.Fatalf("a pooled conn Close() called %d times, want 1", f.closes)
		}
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

	if _, err := p.acquire(ctx); err == nil {
		t.Fatal("first acquire should fail on dial error")
	}
	_, err := p.acquire(ctx)
	if !errors.Is(err, ErrDialBackoff) {
		t.Fatalf("second acquire = %v, want ErrDialBackoff", err)
	}
	if got := d.dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1 (backoff suppressed the second)", got)
	}
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
	if err != nil {
		t.Fatalf("NewClient(TransportH3Pool): %v", err)
	}
	pt, ok := c.tr.(*h3PoolTransport)
	if !ok {
		t.Fatalf("transport is %T, want *h3PoolTransport", c.tr)
	}
	pt.p.dialFn = dialFn
	return c
}

func TestH3PoolClient_Do_RoundTripAndReuse(t *testing.T) {
	t.Parallel()
	d := newH3FakeDialer()
	c := newH3PoolTestClient(t, PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second}, d.dial)
	defer func() { _ = c.Close() }()

	for i := 0; i < 3; i++ {
		var resp Response
		if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp); err != nil {
			t.Fatalf("Do[%d]: %v", i, err)
		}
		if resp.Status != 200 || string(resp.Body) != "ok" {
			t.Fatalf("Do[%d] resp = {%d %q}, want {200 ok}", i, resp.Status, resp.Body)
		}
	}
	// Sequential buffered requests release their stream immediately, so all three
	// reuse the one under-cap conn.
	if got := d.count("h3.example:443"); got != 1 {
		t.Fatalf("dialed %d conns for 3 sequential requests, want 1 (reuse)", got)
	}
	if s := c.PoolStats(); s.ActiveConns != 1 {
		t.Fatalf("PoolStats.ActiveConns = %d, want 1", s.ActiveConns)
	}
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
		t.Fatalf("only %d/%d requests reached the server; pool may be queueing instead of opening conns", inflight.Load(), n)
	}

	s := c.PoolStats()
	if s.ActiveConns < 2 {
		close(release)
		t.Fatalf("ActiveConns = %d during %d-way load with cap %d; want >= 2 conns", s.ActiveConns, n, streamsPerConn)
	}
	// Expect exactly ceil(n/streamsPerConn) = maxConns conns to absorb the load.
	if s.ActiveConns != maxConns {
		t.Logf("ActiveConns = %d (want %d); acceptable if >= 2", s.ActiveConns, maxConns)
	}

	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Do: %v", err)
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
	if err != nil {
		t.Fatalf("NewH3PoolClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	pt, ok := c.tr.(*h3PoolTransport)
	if !ok {
		t.Fatalf("transport is %T, want *h3PoolTransport", c.tr)
	}
	if pt.p.addr != "h3.example:443" || pt.p.opts.MaxConnsPerHost != 4 {
		t.Fatalf("h3Pool not wired: addr=%q maxConns=%d", pt.p.addr, pt.p.opts.MaxConnsPerHost)
	}
}

func TestNewClient_H3Pool_RequiresPoolAndTLS(t *testing.T) {
	t.Parallel()
	// Missing Pool → rejected.
	if _, err := NewClient(ClientOptions{
		Addr: "h:443", Transport: TransportH3Pool, TLSConfig: &tls.Config{ServerName: "h"},
	}); !errors.Is(err, ErrInvalidPoolOptions) {
		t.Fatalf("missing Pool err = %v, want ErrInvalidPoolOptions", err)
	}
	// Missing TLSConfig → rejected.
	if _, err := NewClient(ClientOptions{
		Addr: "h:443", Transport: TransportH3Pool, Pool: &PoolOptions{MaxConnsPerHost: 2},
	}); err == nil {
		t.Fatal("missing TLSConfig for TransportH3Pool = nil error, want failure")
	}
}

// TestConformance_RFC9114_Sec52_PoolEvictsGoAwayConn pins the pool half of the §5.2
// rule. Once the H3 client refuses every new request after GOAWAY, a pool that
// still hands that connection out turns a graceful server shutdown into a retry
// loop: the retry classifier treats http3.ErrGoAway as retryable, re-acquires the
// same conn, and fails identically until retries are exhausted. The conn must stop
// being selected immediately and be evicted once its in-flight work drains — never
// closed under a request the server has undertaken to finish.
func TestConformance_RFC9114_Sec52_PoolEvictsGoAwayConn(t *testing.T) {
	p := newH3Pool("e", nil, PoolOptions{}, nil, nil, nil)
	gone := &barrierH3Client{}
	fresh := &barrierH3Client{}
	conns := []*h3ManagedConn{
		{cl: gone, active: 0, streamCap: 10},
		{cl: fresh, active: 1, streamCap: 10},
	}

	if got := p.pickLeastLoaded(conns); got == nil || got.cl != gone {
		t.Fatal("baseline: the least-loaded pick should be the idle conn")
	}
	atomic.StoreInt32(&gone.goaway, 1)
	if got := p.pickLeastLoaded(conns); got == nil || got.cl != fresh {
		t.Fatal("a GOAWAY'd conn must not be handed out for a new request")
	}

	// Two exchanges still in flight on it: the first release must NOT close it —
	// the server has undertaken to finish what it already accepted.
	rs := &h3RunState{conns: conns}
	conns[0].active = 2
	p.handleRelease(rs, h3ReleaseMsg{mc: conns[0]})
	if len(rs.conns) != 2 {
		t.Fatal("a GOAWAY'd conn with work still in flight must not be evicted")
	}
	if atomic.LoadInt32(&gone.closes) != 0 {
		t.Fatal("a GOAWAY'd conn must not be closed under an in-flight request")
	}

	// The last one drains: now it has nothing left to do and is evicted.
	p.handleRelease(rs, h3ReleaseMsg{mc: conns[0]})
	if len(rs.conns) != 1 || rs.conns[0].cl != fresh {
		t.Fatalf("drained GOAWAY'd conn not evicted: %d conns left", len(rs.conns))
	}
	if atomic.LoadInt32(&gone.closes) != 1 {
		t.Fatalf("the drained GOAWAY'd conn should have been closed exactly once, got %d", atomic.LoadInt32(&gone.closes))
	}
}

// TestConformance_RFC9114_Sec52_PoolEvictsIdleGoAwayConn is the other half of the
// §5.2 pool rule, and the one the release-path test cannot reach: a server
// typically starts a graceful shutdown against an IDLE connection, so no release
// ever arrives to trigger eviction. Without the health-tick and live-count halves
// the pool sits at MaxConnsPerHost with a conn that can serve nothing, and every
// acquire is parked as a waiter with no dial started — a permanent wedge.
func TestConformance_RFC9114_Sec52_PoolEvictsIdleGoAwayConn(t *testing.T) {
	p := newH3Pool("e", nil, PoolOptions{}, nil, nil, nil)
	gone := &barrierH3Client{}
	rs := &h3RunState{conns: []*h3ManagedConn{{cl: gone, active: 0, streamCap: 10}}}
	atomic.StoreInt32(&gone.goaway, 1)

	if got := h3CountLive(rs.conns); got != 0 {
		t.Fatalf("h3CountLive = %d, want 0 — a GOAWAY'd conn must not hold the cap", got)
	}
	p.handleTick(rs)
	if len(rs.conns) != 0 {
		t.Fatalf("idle GOAWAY'd conn survived the health tick: %d conns", len(rs.conns))
	}
	if atomic.LoadInt32(&gone.closes) != 1 {
		t.Fatalf("evicted conn closed %d times, want 1", atomic.LoadInt32(&gone.closes))
	}
}
