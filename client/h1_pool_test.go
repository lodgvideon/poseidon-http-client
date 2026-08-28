package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ————————————————————————————————————————————————————————————————
// Fake HTTP/1.1 dialer — no live server required.
//
// Each dial returns the client half of a net.Pipe whose peer half is driven by
// a goroutine speaking just enough HTTP/1.1 to answer a request: read the head,
// write a canned response. net.Pipe is unbuffered and synchronous, which is
// fine here because HTTP/1.1 is strictly serial (one exchange per conn, no
// pipelining) and the peer goroutine reads concurrently with the client write.
// ————————————————————————————————————————————————————————————————

// h1FakeConn is the client half of one fake connection. It records whether the
// pool closed it, so eviction/discard assertions can observe teardown.
type h1FakeConn struct {
	net.Conn
	srv    net.Conn // peer half, so a test can simulate a server-side close
	idx    int
	reqs   atomic.Int32
	closed atomic.Bool
}

// Close marks the conn closed and tears down the pipe. Idempotent.
func (c *h1FakeConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// closePeer closes the server half, simulating a peer that closes an idle
// connection. The client half then reads EOF, which ProbeIdle turns into
// eviction on the pool's next maintenance sweep.
func (c *h1FakeConn) closePeer() { _ = c.srv.Close() }

// h1FakeDialer hands out a fresh h1FakeConn per dial, recorded by dial addr so a
// pool test can count how many TCP connections were opened to each host. Safe for
// concurrent use; implements conn.Dialer.
type h1FakeDialer struct {
	mu      sync.Mutex
	byAddr  map[string][]*h1FakeConn
	nextIdx int

	dialErr error // if set, every dial fails with this error
	dials   atomic.Int32

	// dialDelay, if set, holds each dial open for that long before it resolves.
	// A zero-latency dial makes any test about WAITERS racy: the pool answers
	// and re-dials faster than the test's own goroutines can queue, so how many
	// waiters are ever simultaneously queued is decided by the scheduler.
	dialDelay time.Duration

	// respFn returns the canned response for the reqIdx'th request served on the
	// connIdx'th conn. nil → 200/"ok" with implicit HTTP/1.1 keep-alive.
	respFn func(connIdx, reqIdx int) string
}

func newH1FakeDialer() *h1FakeDialer {
	return &h1FakeDialer{byAddr: map[string][]*h1FakeConn{}}
}

// h1OKResponse is a minimal keep-alive 200 response.
const h1OKResponse = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

// h1CloseResponse is a 200 response that tells the client not to persist the conn.
const h1CloseResponse = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"

// Dial implements conn.Dialer.
func (d *h1FakeDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.dials.Add(1)
	d.mu.Lock()
	delay := d.dialDelay
	d.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	d.mu.Lock()
	if d.dialErr != nil {
		err := d.dialErr
		d.mu.Unlock()
		return nil, err
	}
	cli, srv := net.Pipe()
	fc := &h1FakeConn{Conn: cli, srv: srv, idx: d.nextIdx}
	d.nextIdx++
	d.byAddr[addr] = append(d.byAddr[addr], fc)
	d.mu.Unlock()

	go d.serve(srv, fc)
	return fc, nil
}

// response returns the canned response body for one exchange.
func (d *h1FakeDialer) response(connIdx, reqIdx int) string {
	d.mu.Lock()
	fn := d.respFn
	d.mu.Unlock()
	if fn == nil {
		return h1OKResponse
	}
	return fn(connIdx, reqIdx)
}

// serve reads request heads off srv and answers each with a canned response until
// the peer goes away or a "Connection: close" response is sent.
func (d *h1FakeDialer) serve(srv net.Conn, fc *h1FakeConn) {
	defer func() { _ = srv.Close() }()
	br := bufio.NewReader(srv)
	for reqIdx := 0; ; reqIdx++ {
		if err := h1ReadRequestHead(br); err != nil {
			return
		}
		// Count on receipt, not on reply: the request read happens-before the
		// client's response read, so a test asserting after Do returns always
		// observes the increment.
		fc.reqs.Add(1)
		resp := d.response(fc.idx, reqIdx)
		if _, err := srv.Write([]byte(resp)); err != nil {
			return
		}
		if strings.Contains(resp, "Connection: close") {
			return
		}
	}
}

// h1ReadRequestHead consumes one request line plus headers up to the blank line.
func h1ReadRequestHead(br *bufio.Reader) error {
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		if strings.TrimRight(line, "\r\n") == "" {
			return nil
		}
	}
}

// count returns how many conns were dialed to addr.
func (d *h1FakeDialer) count(addr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.byAddr[addr])
}

// conns returns every conn dialed to addr, in dial order.
func (d *h1FakeDialer) conns(addr string) []*h1FakeConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*h1FakeConn(nil), d.byAddr[addr]...)
}

// waitForH1 polls cond until it holds or the deadline expires. The pool actor
// applies releases and evictions asynchronously, so state assertions that follow a
// release must wait for the actor rather than read immediately.
func waitForH1(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 3*time.Second, 5*time.Millisecond, msg)
}

// mustAcquireH1 acquires one conn or fails the test.
func mustAcquireH1(t *testing.T, p *h1Pool) *h1ManagedConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mc, err := p.Acquire(ctx)
	require.NoError(t, err, "acquire")
	return mc
}

// ————————————————————————————————————————————————————————————————
// Lifecycle
// ————————————————————————————————————————————————————————————————

func TestH1Pool_Stats_Empty(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 2}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	s := p.Stats()

	require.Equal(t, Stats{}, s, "a pool that has never dialed must report zero, not a phantom conn")
}

func TestH1Pool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)

	err1 := p.Close()
	err2 := p.Close()

	require.NoError(t, err1, "first Close")
	require.NoError(t, err2, "second Close must be a no-op, not an error")
}

func TestH1Pool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	_ = p.Close()

	s := p.Stats()
	_, aerr := p.Acquire(context.Background())

	assert.Equal(t, Stats{}, s, "Stats after Close must be zero, not the pre-close snapshot")
	assert.ErrorIs(t, aerr, ErrPoolClosed,
		"acquire after Close must be classifiable as ErrPoolClosed, not a generic failure")
}

func TestH1Pool_ClosedPool_ClosesPooledConns(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	mc := mustAcquireH1(t, p)
	p.release(mc, true)
	_ = p.Close()

	for _, fc := range d.conns("h:80") {
		assert.Truef(t, fc.closed.Load(),
			"conn %d still open after pool Close — the descriptor leaks", fc.idx)
	}
}

// ————————————————————————————————————————————————————————————————
// Exclusive checkout — the core H1.1 pool property
// ————————————————————————————————————————————————————————————————

// TestH1Pool_ExclusiveCheckout_ConcurrentRequestUsesSecondConn proves the pool is
// an exclusive-checkout pool, not a multiplexing one: while one exchange holds a
// conn, a second concurrent acquire must land on a *different* conn, because
// HTTP/1.1 carries exactly one exchange per connection at a time.
func TestH1Pool_ExclusiveCheckout_ConcurrentRequestUsesSecondConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 2, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc0 := mustAcquireH1(t, p)
	mc1 := mustAcquireH1(t, p) // mc0 is still checked out

	require.NotSame(t, mc0.c, mc1.c,
		"second concurrent acquire reused the busy conn — HTTP/1.1 has no multiplexing")
	assert.Equal(t, 1, mc0.active, "conn 0 must carry exactly one exchange")
	assert.Equal(t, 1, mc1.active, "conn 1 must carry exactly one exchange")
	assert.Equal(t, 2, d.count("h:80"), "want one dial per concurrent exchange")
	s := p.Stats()
	assert.Equal(t, 2, s.ActiveConns, "Stats.ActiveConns must count both checked-out conns")
	assert.Equal(t, 2, s.InFlightStreams, "Stats.InFlightStreams must count both exchanges")
}

// TestH1Pool_AtCap_ThirdAcquireWaitsThenProceeds pins the two rules that make the
// pool usable for load generation: concurrency is bounded by MaxConnsPerHost, and
// at the cap a further request BLOCKS until a conn frees — it must not silently
// serialize onto a busy conn nor exceed the cap.
func TestH1Pool_AtCap_ThirdAcquireWaitsThenProceeds(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 2, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc0 := mustAcquireH1(t, p)
	mc1 := mustAcquireH1(t, p)
	defer p.release(mc1, true)

	// At cap with both conns busy: the third acquire must block.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err := p.Acquire(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the third acquire at the cap must BLOCK until its own deadline, not serialize "+
			"onto a busy conn and not exceed MaxConnsPerHost")

	// Freeing a conn must hand it to a waiter.
	got := make(chan *h1ManagedConn, 1)
	bad := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mc, err := p.Acquire(ctx)
		if err != nil {
			bad <- err
			return
		}
		got <- mc
	}()
	// Let the waiter enqueue with the actor before freeing a slot.
	time.Sleep(30 * time.Millisecond)
	p.release(mc0, true)

	select {
	case mc := <-got:
		assert.Same(t, mc0.c, mc.c, "the waiter must be handed the conn that was just freed")
		p.release(mc, true)
	case werr := <-bad:
		require.NoError(t, werr, "waiter acquire")
	case <-time.After(3 * time.Second):
		require.FailNow(t, "waiter never proceeded after a conn was freed")
	}

	assert.EqualValues(t, 2, d.dials.Load(),
		"the pool must never dial past MaxConnsPerHost")
}

// TestH1Pool_AtCap_CtxCancelWhileWaiting_ReturnsCtxErr proves a waiter blocked at
// the cap observes its own ctx cancellation rather than hanging.
func TestH1Pool_AtCap_CtxCancelWhileWaiting_ReturnsCtxErr(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	defer p.release(mc, true)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(ctx)
		errCh <- err
	}()
	time.Sleep(30 * time.Millisecond) // let the waiter enqueue
	cancel()

	select {
	case werr := <-errCh:
		assert.ErrorIs(t, werr, context.Canceled,
			"a waiter blocked at the cap must observe its own cancellation")
	case <-time.After(3 * time.Second):
		require.FailNow(t, "cancelled waiter hung instead of returning ctx.Err()")
	}
}

// TestH1Pool_MaxStreamsPerConn_Ignored proves MaxStreamsPerConn is meaningless for
// HTTP/1.1: even with a cap of 8, one conn carries one exchange, so a second
// concurrent acquire against MaxConnsPerHost=1 blocks.
func TestH1Pool_MaxStreamsPerConn_Ignored(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 8, // must have no effect on HTTP/1.1
		HealthCheckPeriod: time.Second,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	defer p.release(mc, true)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)

	_, err := p.Acquire(ctx)
	cancel()

	require.ErrorIs(t, err, context.DeadlineExceeded,
		"MaxStreamsPerConn must not multiplex HTTP/1.1: the second acquire has to block "+
			"on MaxConnsPerHost regardless of the stream cap")
}

// TestH1Pool_BoundedConcurrency_UnderLoad drives many concurrent checkouts and
// asserts the pool never hands the same conn to two holders at once and never
// dials past MaxConnsPerHost.
func TestH1Pool_BoundedConcurrency_UnderLoad(t *testing.T) {
	t.Parallel()
	const maxConns = 4
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: maxConns, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	var mu sync.Mutex
	held := map[*h1ManagedConn]bool{}
	var live, peak int

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// assert, never require: this runs off the test goroutine, where
			// require's FailNow is illegal.
			mc, err := p.Acquire(ctx)
			if err != nil {
				assert.NoError(t, err, "acquire under load")
				return
			}
			mu.Lock()
			assert.Falsef(t, held[mc],
				"pool handed one conn to two concurrent holders — exclusive checkout broken")
			held[mc] = true
			live++
			if live > peak {
				peak = live
			}
			mu.Unlock()

			time.Sleep(time.Millisecond)

			mu.Lock()
			delete(held, mc)
			live--
			mu.Unlock()
			p.release(mc, true)
		}()
	}
	wg.Wait()

	assert.LessOrEqualf(t, peak, maxConns,
		"peak concurrent checkouts = %d, want <= %d — the cap did not bound concurrency",
		peak, maxConns)
	assert.LessOrEqualf(t, d.count("h:80"), maxConns,
		"dialed more than %d conns — the pool exceeded MaxConnsPerHost", maxConns)
}

// ————————————————————————————————————————————————————————————————
// Keep-alive / discard
// ————————————————————————————————————————————————————————————————

// TestH1Pool_KeepAlive_ReusesConn proves two sequential exchanges ride the same
// connection when the response says it persists.
func TestH1Pool_KeepAlive_ReusesConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc0 := mustAcquireH1(t, p)
	p.release(mc0, true)
	mc1 := mustAcquireH1(t, p)
	p.release(mc1, true)

	assert.Same(t, mc0.c, mc1.c,
		"sequential keep-alive acquires used different conns; want reuse")
	assert.EqualValues(t, 1, d.dials.Load(),
		"keep-alive must reuse the pooled conn rather than dial again")
}

// TestH1Pool_NotKeepAlive_DiscardsConn proves a conn released with keepAlive=false
// (Connection: close, HTTP/1.0, or a failed exchange) is closed and evicted rather
// than returned to the idle set, and the next acquire dials fresh.
func TestH1Pool_NotKeepAlive_DiscardsConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc0 := mustAcquireH1(t, p)
	p.release(mc0, false)

	waitForH1(t, func() bool { return !mc0.c.IsAlive() },
		"conn released with keepAlive=false was never closed")

	mc1 := mustAcquireH1(t, p)
	p.release(mc1, true)

	require.NotSame(t, mc0.c, mc1.c,
		"discarded conn was handed back out — a poisoned conn returned to the pool")
	assert.EqualValues(t, 2, d.dials.Load(),
		"a discard must force a redial rather than resurrect the dropped conn")
	waitForH1(t, func() bool { return p.Stats().ActiveConns == 1 },
		"discarded conn was never evicted from the pool")
}

// TestH1Pool_DeadConn_EvictedOnRelease proves a conn the peer closed under us is
// evicted on release even when the exchange itself reported keep-alive.
func TestH1Pool_DeadConn_EvictedOnRelease(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	_ = mc.c.Close() // peer went away mid-exchange
	p.release(mc, true)

	waitForH1(t, func() bool { return p.Stats().ActiveConns == 0 },
		"dead conn was not evicted on release despite a keep-alive verdict")
}

// TestH1Pool_IdleEviction closes conns idle past IdleTimeout on the health tick.
func TestH1Pool_IdleEviction(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		IdleTimeout:       20 * time.Millisecond,
		HealthCheckPeriod: 10 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	p.release(mc, true)

	require.Eventually(t, func() bool { return p.Stats().ActiveConns == 0 },
		3*time.Second, 10*time.Millisecond,
		"idle conn never evicted past IdleTimeout on the health tick")
}

// TestH1Pool_DialError_FailsAllQueuedWaiters pins that a dial failure answers
// every queued waiter, not just the one at the head.
//
// The dial-error branch replied to the head waiter and returned, leaving the rest
// queued in exactly the state its own doc comment calls terminal — so a burst
// against a downed host drained at one request per HealthCheckPeriod while NEW
// acquires were refused in microseconds by the dial-backoff fast path. That
// priority inversion is the bug; HealthCheckPeriod is set out of reach here so
// only the dial-done path can answer them.
func TestH1Pool_DialError_FailsAllQueuedWaiters(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.dialErr = fmt.Errorf("connection refused")
	// A dial that resolves instantly makes this test racy in the one dimension it
	// measures. MaxConnsPerHost=2 means only two dials run at once; with a
	// zero-latency dialer the first two can fail, refuse their waiters and let
	// handleAcquire start fresh dials for goroutines 3 and 4 before those ever sit
	// in the queue together — so all four get answered and the missing fan-out is
	// invisible. Measured: 3 catches in 6 mutation runs. Holding each dial open
	// past the time it takes all n goroutines to enqueue makes the queue state the
	// bug needs the state the test actually reaches.
	d.dialDelay = 250 * time.Millisecond
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	// The two timeouts are deliberately an order of magnitude apart, and that is
	// the whole detector. They used to be equal (5s each), which made this test
	// catch the bug only when the scheduler happened to fire the collection
	// deadline first: a stranded waiter eventually returns its OWN
	// context.DeadlineExceeded, which is a non-nil error and satisfies the
	// require.Error below just as well as the pool's refusal does. Measured over
	// six mutation runs against the fix removed, it caught it three times.
	//
	// waiterBudget must therefore be far longer than collectBudget, so a waiter
	// the pool has stranded CANNOT rescue itself inside the window: if the
	// dial-done path does not answer all n, the collection deadline fires first
	// and the test fails every time.
	const (
		n             = 4
		waiterBudget  = 30 * time.Second
		collectBudget = 3 * time.Second
	)
	errs := make(chan error, n)
	var queued sync.WaitGroup
	queued.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), waiterBudget)
			defer cancel()
			queued.Done()
			_, err := p.Acquire(ctx)
			errs <- err
		}()
	}
	// CONTROL: every goroutine has entered acquire before the first dial can
	// resolve, so all n are genuinely contending for the same failed dial.
	queued.Wait()

	deadline := time.After(collectBudget)
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			require.Error(t, err, "acquire against a downed host returned a connection")
			// Name the mechanism: the refusal must come from the failed dial, not
			// from the waiter's own context expiring while it sat queued.
			require.NotErrorIsf(t, err, context.DeadlineExceeded,
				"waiter %d got its own deadline (%v) rather than the pool's dial "+
					"refusal — it was stranded, which is the bug this test exists for", i, err)
		case <-deadline:
			require.FailNowf(t, "queued acquires were stranded",
				"only %d of %d queued acquires were answered; the rest were left for a "+
					"health-check tick", i, n)
		}
	}
}

// TestH1Pool_WaiterServedAfterCloseEviction pins that a queued waiter is still
// served after the last connection is evicted.
//
// serveWaiters can only hand out conns that already exist, and the release path
// never dialled — so evicting the last conn while a waiter was queued left
// {no conns, no in-flight dials, queued waiters}, a TERMINAL state in which the
// waiter sat until its own timeout. The trigger is the ordinary server
// "Connection: close", not an error path. HealthCheckPeriod is set out of reach
// so only the release path can rescue this.
func TestH1Pool_WaiterServedAfterCloseEviction(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p) // the only conn allowed by the cap

	got := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := p.Acquire(ctx)
		got <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the second acquire queue as a waiter

	p.release(mc, false) // "Connection: close": evicted, pool now empty

	select {
	case werr := <-got:
		require.NoError(t, werr, "queued waiter must get a freshly dialled conn")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "queued waiter was never served after the last conn was evicted",
			"the pool had no path back to dialling")
	}
}

// TestH1Pool_CheckoutProbeRejectsPeerClosedConn pins that a connection the peer
// closed while it sat idle is probed at checkout, not handed to the next
// request. The maintenance sweep alone is not enough: at its default period a
// conn is re-acquired long before the sweep would look at it.
func TestH1Pool_CheckoutProbeRejectsPeerClosedConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: time.Hour, // only the checkout probe can catch this
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	p.release(mc, true)

	// Age it past the checkout-probe threshold, then have the peer close it.
	time.Sleep(h1ProbeIdleAfter + 150*time.Millisecond)
	d.mu.Lock()
	fc := d.byAddr["h:80"][0]
	d.mu.Unlock()
	fc.closePeer()

	mc2 := mustAcquireH1(t, p)

	require.NotSame(t, mc, mc2,
		"checkout handed back the connection whose peer had closed it")
	assert.GreaterOrEqual(t, d.dials.Load(), int32(2),
		"want a re-dial after the dead conn was rejected at checkout")
}

// TestH1Pool_ProbeEvictsPeerClosedIdleConn pins that the periodic maintenance
// sweep probes idle connections and evicts one whose peer has closed, so a dead
// connection is never handed to a later request. Recovering only on next use (an
// exchange error plus retry) is weaker than RFC 9112 §9.6's ask to monitor idle
// connections for a closure signal. IdleTimeout is left unset so the eviction
// can only come from the probe, not from age.
func TestH1Pool_ProbeEvictsPeerClosedIdleConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: 10 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	mc := mustAcquireH1(t, p)
	p.release(mc, true) // keep-alive → the conn stays idle in the pool

	// The server closes the idle connection.
	d.mu.Lock()
	fc := d.byAddr["h:80"][0]
	d.mu.Unlock()
	fc.closePeer()

	// The next maintenance tick must probe the idle conn, see EOF, and evict it.
	require.Eventually(t, func() bool { return p.Stats().ActiveConns == 0 },
		3*time.Second, 10*time.Millisecond,
		"peer-closed idle conn never evicted by the probe (RFC 9112 §9.6: idle "+
			"connections must be monitored for a closure signal)")
}

// ————————————————————————————————————————————————————————————————
// Dial errors
// ————————————————————————————————————————————————————————————————

func TestH1Pool_DialError_PropagatesAsDialError(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.dialErr = fmt.Errorf("connection refused")
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Second}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := p.Acquire(ctx)

	var de *DialError
	require.ErrorAsf(t, err, &de,
		"acquire with a failing dial returned %v; callers classify a dial failure by "+
			"*DialError, so an unwrapped error is indistinguishable from a request failure", err)
	assert.Equal(t, "h:80", de.Addr, "*DialError must name the address that failed")
}

// ————————————————————————————————————————————————————————————————
// Constructors + option validation
// ————————————————————————————————————————————————————————————————

func TestNewH1Client_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewH1Client("h:80", newH1FakeDialer())

	require.NoError(t, err, "NewH1Client")
	defer func() { _ = c.Close() }()
	_, ok := c.tr.(*h1singleConn)
	assert.Truef(t, ok, "transport is %T, want *h1singleConn", c.tr)
}

func TestNewH1PoolClient_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewH1PoolClient("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 4})

	require.NoError(t, err, "NewH1PoolClient")
	defer func() { _ = c.Close() }()
	pt, ok := c.tr.(*h1PoolTransport)
	require.Truef(t, ok, "transport is %T, want *h1PoolTransport", c.tr)
	assert.Equal(t, 4, pt.p.opts.MaxConnsPerHost,
		"the caller's MaxConnsPerHost must reach the pool, or the option is ignored")
}

func TestNewClient_H1Pool_Validation(t *testing.T) {
	t.Parallel()
	// Missing Pool → rejected (mirrors TransportPool / TransportH3Pool).
	_, errNoPool := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Pool, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
	})
	// Missing Dialer → rejected (H1 dials via conn.Dialer, not TLSConfig).
	_, errNoDialer := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Pool, Pool: &PoolOptions{MaxConnsPerHost: 1},
	})

	assert.ErrorIs(t, errNoPool, ErrInvalidPoolOptions,
		"a pooled transport with no PoolOptions must be refused at construction")
	assert.Error(t, errNoDialer,
		"an HTTP/1.1 pool with no Dialer must be refused: H1 dials via conn.Dialer, "+
			"never via TLSConfig")
}

func TestNewClient_H1Managed_Validation(t *testing.T) {
	t.Parallel()
	// Missing Resolver → rejected.
	_, errNoResolver := NewClient(ClientOptions{
		Transport: TransportH1Managed, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
	})
	// Addr set on a managed transport → rejected (Resolver owns addressing).
	_, errAddrSet := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Managed, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
		Resolver: StaticResolver(h1Addrs(1)...),
	})

	assert.ErrorIs(t, errNoResolver, ErrInvalidOptions,
		"a managed transport with no Resolver has no addresses and must be refused")
	assert.ErrorIs(t, errAddrSet, ErrInvalidOptions,
		"Addr and Resolver both set is ambiguous addressing and must be refused")
}

// TestTransportKind_ConstantValues pins the wire-order of the TransportKind iota
// block. The H1 pool/managed kinds were appended at the END specifically so the
// existing constants keep their values; this test fails loudly if a future edit
// inserts a kind mid-block and silently renumbers the others.
func TestTransportKind_ConstantValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind TransportKind
		want int
		name string
	}{
		{TransportSingleConn, 0, "TransportSingleConn"},
		{TransportPool, 1, "TransportPool"},
		{TransportManaged, 2, "TransportManaged"},
		{TransportH1SingleConn, 3, "TransportH1SingleConn"},
		{TransportALPN, 4, "TransportALPN"},
		{TransportH3, 5, "TransportH3"},
		{TransportH3Pool, 6, "TransportH3Pool"},
		{TransportH3Managed, 7, "TransportH3Managed"},
		{TransportH1Pool, 8, "TransportH1Pool"},
		{TransportH1Managed, 9, "TransportH1Managed"},
	} {
		assert.Equalf(t, tc.want, int(tc.kind),
			"%s must keep its value — constants must not be renumbered", tc.name)
	}
}

func TestH1Pool_DialBackoff_FastRefuses(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.dialErr = fmt.Errorf("connection refused")
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   1,
		DialBackoff:       2 * time.Second,
		HealthCheckPeriod: time.Second,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, _ = p.Acquire(ctx) // seeds lastDialErrAt
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	_, err := p.Acquire(ctx2)

	require.ErrorIs(t, err, ErrDialBackoff,
		"a second acquire inside DialBackoff must fast-refuse rather than re-dial a "+
			"host that just refused the connection")
}
