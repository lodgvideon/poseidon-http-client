package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
func (d *h1FakeDialer) Dial(_ context.Context, addr string) (net.Conn, error) {
	d.dials.Add(1)
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
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// mustAcquireH1 acquires one conn or fails the test.
func mustAcquireH1(t *testing.T, p *h1Pool) *h1ManagedConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return mc
}

// ————————————————————————————————————————————————————————————————
// Lifecycle
// ————————————————————————————————————————————————————————————————

func TestH1Pool_Stats_Empty(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 2}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	if s := p.Stats(); s != (Stats{}) {
		t.Fatalf("empty Stats = %+v, want zero", s)
	}
}

func TestH1Pool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestH1Pool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()
	p := newH1Pool("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	_ = p.Close()
	if s := p.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after Close = %+v, want zero", s)
	}
	if _, err := p.acquire(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire after Close = %v, want ErrPoolClosed", err)
	}
}

func TestH1Pool_ClosedPool_ClosesPooledConns(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	p := newH1Pool("h:80", d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	mc := mustAcquireH1(t, p)
	p.release(mc, true)
	_ = p.Close()

	for _, fc := range d.conns("h:80") {
		if !fc.closed.Load() {
			t.Fatalf("conn %d still open after pool Close", fc.idx)
		}
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

	if mc0.c == mc1.c {
		t.Fatal("second concurrent acquire reused the busy conn — HTTP/1.1 has no multiplexing")
	}
	if mc0.active != 1 || mc1.active != 1 {
		t.Fatalf("active counts = %d/%d, want 1/1 (one exchange per conn)", mc0.active, mc1.active)
	}
	if got := d.count("h:80"); got != 2 {
		t.Fatalf("dialed %d conns, want 2 (one per concurrent exchange)", got)
	}
	if s := p.Stats(); s.ActiveConns != 2 || s.InFlightStreams != 2 {
		t.Fatalf("Stats = %+v, want ActiveConns=2 InFlightStreams=2", s)
	}
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
	_, err := p.acquire(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third acquire at cap = %v, want DeadlineExceeded (must block, not serialize)", err)
	}

	// Freeing a conn must hand it to a waiter.
	got := make(chan *h1ManagedConn, 1)
	bad := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mc, err := p.acquire(ctx)
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
		if mc.c != mc0.c {
			t.Fatal("waiter did not get the freed conn")
		}
		p.release(mc, true)
	case err := <-bad:
		t.Fatalf("waiter acquire = %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter never proceeded after a conn was freed")
	}

	if n := d.dials.Load(); n != 2 {
		t.Fatalf("dials = %d, want 2 — the pool must never exceed MaxConnsPerHost", n)
	}
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
		_, err := p.acquire(ctx)
		errCh <- err
	}()
	time.Sleep(30 * time.Millisecond) // let the waiter enqueue
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled waiter hung instead of returning ctx.Err()")
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
	_, err := p.acquire(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire = %v, want DeadlineExceeded; MaxStreamsPerConn must not multiplex H1.1", err)
	}
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
			mc, err := p.acquire(ctx)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			if held[mc] {
				t.Error("pool handed one conn to two concurrent holders")
			}
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

	if peak > maxConns {
		t.Fatalf("peak concurrency = %d, want <= %d", peak, maxConns)
	}
	if n := d.count("h:80"); n > maxConns {
		t.Fatalf("dialed %d conns, want <= %d", n, maxConns)
	}
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

	if mc0.c != mc1.c {
		t.Fatal("sequential keep-alive acquires used different conns; want reuse")
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1 (keep-alive must reuse)", n)
	}
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

	if mc0.c == mc1.c {
		t.Fatal("discarded conn was handed back out — poisoned conn returned to the pool")
	}
	if n := d.dials.Load(); n != 2 {
		t.Fatalf("dials = %d, want 2 (discard must force a redial)", n)
	}
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().ActiveConns == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("idle conn never evicted; Stats = %+v", p.Stats())
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
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   2,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	const n = 4
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := p.acquire(ctx)
			errs <- err
		}()
	}

	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("acquire against a downed host returned a connection")
			}
		case <-deadline:
			t.Fatalf("only %d of %d queued acquires were answered; the rest were left "+
				"for a health-check tick", i, n)
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
		_, err := p.acquire(ctx)
		got <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the second acquire queue as a waiter

	p.release(mc, false) // "Connection: close": evicted, pool now empty

	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("queued waiter got %v, want a freshly dialled conn", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued waiter was never served after the last conn was evicted — " +
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
	if mc2 == mc {
		t.Fatal("checkout handed back the connection whose peer had closed it")
	}
	if n := d.dials.Load(); n < 2 {
		t.Errorf("dials = %d, want a re-dial after the dead conn was rejected", n)
	}
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
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().ActiveConns == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer-closed idle conn never evicted by the probe; Stats = %+v", p.Stats())
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
	_, err := p.acquire(ctx)

	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("acquire with failing dial = %v, want *DialError", err)
	}
}

// ————————————————————————————————————————————————————————————————
// Constructors + option validation
// ————————————————————————————————————————————————————————————————

func TestNewH1Client_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewH1Client("h:80", newH1FakeDialer())
	if err != nil {
		t.Fatalf("NewH1Client: %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, ok := c.tr.(*h1singleConn); !ok {
		t.Fatalf("transport is %T, want *h1singleConn", c.tr)
	}
}

func TestNewH1PoolClient_Construction(t *testing.T) {
	t.Parallel()
	c, err := NewH1PoolClient("h:80", newH1FakeDialer(), PoolOptions{MaxConnsPerHost: 4})
	if err != nil {
		t.Fatalf("NewH1PoolClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	pt, ok := c.tr.(*h1PoolTransport)
	if !ok {
		t.Fatalf("transport is %T, want *h1PoolTransport", c.tr)
	}
	if pt.p.opts.MaxConnsPerHost != 4 {
		t.Fatalf("MaxConnsPerHost = %d, want 4", pt.p.opts.MaxConnsPerHost)
	}
}

func TestNewClient_H1Pool_Validation(t *testing.T) {
	t.Parallel()
	// Missing Pool → rejected (mirrors TransportPool / TransportH3Pool).
	if _, err := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Pool, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
	}); !errors.Is(err, ErrInvalidPoolOptions) {
		t.Fatalf("missing Pool = %v, want ErrInvalidPoolOptions", err)
	}
	// Missing Dialer → rejected (H1 dials via conn.Dialer, not TLSConfig).
	if _, err := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Pool, Pool: &PoolOptions{MaxConnsPerHost: 1},
	}); err == nil {
		t.Fatal("missing Dialer = nil error, want failure")
	}
}

func TestNewClient_H1Managed_Validation(t *testing.T) {
	t.Parallel()
	// Missing Resolver → rejected.
	if _, err := NewClient(ClientOptions{
		Transport: TransportH1Managed, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
	}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("missing Resolver = %v, want ErrInvalidOptions", err)
	}
	// Addr set on a managed transport → rejected (Resolver owns addressing).
	if _, err := NewClient(ClientOptions{
		Addr: "h:80", Transport: TransportH1Managed, ConnOpts: conn.ConnOptions{Dialer: newH1FakeDialer()},
		Resolver: StaticResolver(h1Addrs(1)...),
	}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Addr with TransportH1Managed = %v, want ErrInvalidOptions", err)
	}
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
		if int(tc.kind) != tc.want {
			t.Errorf("%s = %d, want %d — constants must not be renumbered", tc.name, int(tc.kind), tc.want)
		}
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
	_, _ = p.acquire(ctx) // seeds lastDialErrAt
	cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if _, err := p.acquire(ctx2); !errors.Is(err, ErrDialBackoff) {
		t.Fatalf("acquire during backoff = %v, want ErrDialBackoff", err)
	}
}
