package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestPool_Stats_Empty(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 2}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	s := p.Stats()

	assert.Equalf(t, Stats{}, s,
		"empty Stats = %+v, want the zero value — a pool that has never dialled must not "+
			"report conns, streams, waiters or dials", s)
}

func TestPool_Close_Idempotent(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)

	first := p.Close()
	second := p.Close()

	assert.NoError(t, first, "first Close")
	assert.NoError(t, second, "second Close — Close must be idempotent, callers double-close on error paths")
}

func TestPool_Stats_Concurrent(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 4}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	const N = 64
	var wg sync.WaitGroup
	wg.Add(N)

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_ = p.Stats()
		}()
	}
	wg.Wait()

	// The property is "no race and no deadlock"; -race is what judges it, and a
	// hung actor fails the test by timeout rather than by assertion.
	assert.Equal(t, Stats{}, p.Stats(),
		"concurrent Stats scrapes must leave the pool's state untouched")
}

func TestPool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	_ = p.Close()

	s := p.Stats()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, _, err := newPoolTransportFromPool(p).openExchange(ctx)

	assert.Equalf(t, Stats{}, s, "Stats after Close = %+v, want zero", s)
	assert.ErrorIsf(t, err, ErrPoolClosed,
		"acquire after Close = %v, want ErrPoolClosed — a caller must be able to tell a "+
			"closed pool from a transport failure", err)
}

// --- effectiveStreamCap ---

func TestEffectiveStreamCap_BothUnbounded(t *testing.T) {
	t.Parallel()

	got := effectiveStreamCap(0, 0)

	assert.Equalf(t, 100, got,
		"effectiveStreamCap(0,0) = %d, want 100 — with neither side declaring a cap the "+
			"pool must fall back to the documented default", got)
}

func TestEffectiveStreamCap_LocalOnly(t *testing.T) {
	t.Parallel()

	got := effectiveStreamCap(50, 0)

	assert.Equalf(t, 50, got, "effectiveStreamCap(50,0) = %d, want 50", got)
}

func TestEffectiveStreamCap_PeerOnly(t *testing.T) {
	t.Parallel()

	got := effectiveStreamCap(0, 30)

	assert.Equalf(t, 30, got, "effectiveStreamCap(0,30) = %d, want 30", got)
}

func TestEffectiveStreamCap_PeerLower(t *testing.T) {
	t.Parallel()

	got := effectiveStreamCap(100, 2)

	assert.Equalf(t, 2, got,
		"effectiveStreamCap(100,2) = %d, want 2 (peer cap) — exceeding the peer's "+
			"MAX_CONCURRENT_STREAMS is a protocol error", got)
}

func TestEffectiveStreamCap_LocalLower(t *testing.T) {
	t.Parallel()

	got := effectiveStreamCap(10, 100)

	assert.Equalf(t, 10, got, "effectiveStreamCap(10,100) = %d, want 10 (local cap)", got)
}

// --- inDialBackoff ---

func TestInDialBackoff_WithinWindow(t *testing.T) {
	t.Parallel()

	got := inDialBackoff(time.Now(), 1*time.Second)

	assert.True(t, got, "inDialBackoff should return true for fresh error within window")
}

func TestInDialBackoff_AfterWindow(t *testing.T) {
	t.Parallel()

	got := inDialBackoff(time.Now().Add(-10*time.Second), 1*time.Millisecond)

	assert.False(t, got, "inDialBackoff should return false after window expired")
}

func TestInDialBackoff_ZeroLastErr(t *testing.T) {
	t.Parallel()

	got := inDialBackoff(time.Time{}, 1*time.Second)

	assert.False(t, got, "inDialBackoff should return false when lastErrAt is zero")
}

func TestInDialBackoff_ZeroWindow(t *testing.T) {
	t.Parallel()

	got := inDialBackoff(time.Now(), 0)

	assert.False(t, got, "inDialBackoff should return false when window is 0")
}

// --- acquire context-cancel paths ---

func TestPool_AcquireCtxCanceledBeforeSend(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before acquire

	_, err := p.acquire(ctx)

	assert.ErrorIsf(t, err, context.Canceled, "acquire = %v, want context.Canceled", err)
}

func TestPool_AcquireClosedChBeforeSend(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	// Close the pool before acquire so closedCh fires.
	_ = p.Close()

	_, err := p.acquire(context.Background())

	assert.ErrorIsf(t, err, ErrPoolClosed, "acquire = %v, want ErrPoolClosed", err)
}

func TestPool_AcquireTimeout(t *testing.T) {
	t.Parallel()

	// failingDialer is already defined in client_test.go (same package).
	// Use a pool with an always-failing dialer so it never provides a conn.
	// AcquireTimeout is short so the wait-for-reply path times out.
	fd := &failingDialer{err: errors.New("no connect")}
	p := newPool("fake:0", conn.ConnOptions{Dialer: fd}, PoolOptions{
		MaxConnsPerHost: 1,
		AcquireTimeout:  20 * time.Millisecond,
		DialBackoff:     10 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First acquire: dial is attempted, actor returns dial error via replyAcquire.
	// The dialer wraps with conn.Dial wrapping → expect non-nil error,
	// not necessarily a known sentinel.
	_, err1 := p.acquire(ctx)
	// Second acquire: within DialBackoff window with no live conns →
	// ErrDialBackoff; if scheduling delays the reply past AcquireTimeout
	// the actor's reply path loses the race and the caller observes
	// ErrAcquireTimeout. Both are valid.
	_, err2 := p.acquire(ctx)

	require.Error(t, err1, "first acquire should fail against an always-failing dialer, got nil")
	assert.Truef(t, errors.Is(err2, ErrDialBackoff) || errors.Is(err2, ErrAcquireTimeout),
		"second acquire = %v, want ErrDialBackoff or ErrAcquireTimeout", err2)
}

// --- BUG-3 regression: pruneExpiredWaiters ---

func TestPruneExpiredWaiters_DropsCancelledKeepsLive(t *testing.T) {
	t.Parallel()

	live := context.Background()
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	// Every waiter carries the cap-1 reply channel acquire gives it: a pruned
	// waiter is still owed exactly one reply (its caller's reclaim goroutine is
	// blocked on it), and a live one must not be replied to.
	in := []acquireReq{
		{ctx: live, reply: make(chan acquireResp, 1)},
		{ctx: dead, reply: make(chan acquireResp, 1)},
		{ctx: live, reply: make(chan acquireResp, 1)},
		{ctx: dead, reply: make(chan acquireResp, 1)},
	}
	dropped := []acquireReq{in[1], in[3]}

	out := pruneExpiredWaiters(in)

	require.Lenf(t, out, 2, "len(out) = %d, want 2", len(out))
	for i, w := range out {
		select {
		case <-w.ctx.Done():
			require.Failf(t, "live waiter dropped", "out[%d] ctx unexpectedly done", i)
		default:
		}
		assert.Emptyf(t, w.reply, "out[%d]: live waiter was replied to by pruning", i)
	}
	for i, w := range dropped {
		select {
		case resp := <-w.reply:
			assert.Errorf(t, resp.err, "dropped[%d] reply err = nil, want ctx error", i)
		default:
			assert.Failf(t, "pruned waiter got no reply",
				"dropped[%d] got no reply: its reclaim goroutine would hang forever", i)
		}
	}
}

func TestPruneExpiredWaiters_EmptyAndAllLive(t *testing.T) {
	t.Parallel()

	live := context.Background()
	in := []acquireReq{{ctx: live}, {ctx: live}}

	fromNil := pruneExpiredWaiters(nil)
	fromLive := pruneExpiredWaiters(in)

	assert.Emptyf(t, fromNil, "nil → len %d, want 0", len(fromNil))
	assert.Lenf(t, fromLive, 2, "all-live len = %d, want 2", len(fromLive))
}

// --- BUG-2 regression: DialTimeout bounds dialOne ---

// hangingDialer blocks Dial until either (a) ctx is cancelled or
// (b) release is closed. Used to verify DialTimeout fires.
// dialStarted is closed on the first Dial call so tests can wait
// until the dial is actually in progress before triggering Close.
type hangingDialer struct {
	release     chan struct{}
	dialStarted chan struct{}
	startOnce   sync.Once
}

func (d *hangingDialer) Dial(ctx context.Context, _ string) (net.Conn, error) {
	if d.dialStarted != nil {
		d.startOnce.Do(func() { close(d.dialStarted) })
	}
	select {
	case <-d.release:
		return nil, errors.New("hangingDialer: released without conn")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPool_DialTimeout_FiresOnHangingDial(t *testing.T) {
	t.Parallel()

	hd := &hangingDialer{release: make(chan struct{})}
	p := newPool("fake:0", conn.ConnOptions{Dialer: hd}, PoolOptions{
		MaxConnsPerHost: 1,
		DialTimeout:     50 * time.Millisecond,
		DialBackoff:     1 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() {
		close(hd.release)
		_ = p.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := p.acquire(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "expected dial-timeout error, got nil")
	// Bound elapsed by an order of magnitude over DialTimeout to detect a
	// regression that bypasses the timeout (e.g. background ctx without
	// deadline).
	assert.Lessf(t, elapsed, 500*time.Millisecond,
		"acquire took %v with DialTimeout=50ms — bound not enforced", elapsed)
}

func TestPool_DialTimeout_DefaultedTo30s(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	got := p.opts.DialTimeout

	assert.Equalf(t, 30*time.Second, got,
		"default DialTimeout = %v, want 30s — an unset DialTimeout must not mean "+
			"'dial forever'", got)
}

// --- BUG-2 regression: dialOne aborts when pool closes mid-dial ---

func TestPool_DialOne_PoolCloseCancelsHangingDial(t *testing.T) {
	t.Parallel()

	hd := &hangingDialer{
		release:     make(chan struct{}),
		dialStarted: make(chan struct{}),
	}
	p := newPool("fake:0", conn.ConnOptions{Dialer: hd}, PoolOptions{
		MaxConnsPerHost: 1,
		// Long DialTimeout so we know cancellation came from Close,
		// not the timeout.
		DialTimeout: 30 * time.Second,
	}, nil, nil)
	// Trigger a dial via acquire in a goroutine so the actor calls dialOne.
	acqErr := make(chan error, 1)
	go func() {
		_, err := p.acquire(context.Background())
		acqErr <- err
	}()
	// Wait until the dial is actually in progress before closing the pool.
	select {
	case <-hd.dialStarted:
	case <-time.After(5 * time.Second):
		close(hd.release)
		require.FailNow(t, "dialOne never started within 5s")
	}

	closeStart := time.Now()
	_ = p.Close()
	closeElapsed := time.Since(closeStart)

	// Close itself returns once actor exits — should be fast even with
	// a dial in flight (watchdog cancels dial ctx → conn.Dial returns).
	assert.Lessf(t, closeElapsed, 1*time.Second,
		"Close took %v with hanging dial — watchdog not wired up", closeElapsed)
	// acquire must complete with an error (pool closed or dial cancelled).
	select {
	case err := <-acqErr:
		assert.Error(t, err, "acquire returned nil error after Close")
	case <-time.After(2 * time.Second):
		assert.Fail(t, "acquire did not return after Close")
	}
	close(hd.release) // unblock any straggler dialer goroutine
}

// --- release nil mc (no-op) ---

func TestPool_Release_NilMC_NoOp(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	// Must not panic or block.
	require.NotPanics(t, func() { p.release(nil) },
		"release(nil) must be a no-op: callers release unconditionally on error paths")

	assert.Equal(t, Stats{}, p.Stats(), "a nil release must not change pool state")
}

// --- release when pool is closed (closedCh branch) ---

func TestPool_Release_PoolClosed_NoDeadlock(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	_ = p.Close()
	// Synthesise a managedConn with a real *conn.Conn placeholder (nil).
	// release(mc, nil) when pool is already closed must take closedCh branch.
	// We can't build a valid *conn.Conn without dialing, but release only reads
	// mc.active/mc.lastUsed inside the actor — and the actor is already gone.
	// Passing a non-nil mc with a nil inner c is fine here since the actor never
	// runs again.
	mc := &managedConn{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.release(mc)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		assert.Fail(t, "release on closed pool deadlocked")
	}
}

// --- dialOne closedCh branch ---

func TestPool_DialOne_PoolClosedBeforeResult(t *testing.T) {
	t.Parallel()

	// Use a fakeDialer (defined in client_test.go) with a blocking srvAfter
	// so we can close the pool while a dial is in progress. Because the
	// pool's actor is already shut down, dialOne will try to send on
	// dialDoneCh but must fall through to the closedCh select arm instead.
	stopSrv := make(chan struct{})
	d := &fakeDialer{srvAfter: func(*frame.Framer) { <-stopSrv }}
	p := newPool("fake:0", conn.ConnOptions{Dialer: d}, PoolOptions{MaxConnsPerHost: 2}, nil, nil)
	// Close the pool immediately so closedCh is closed.
	_ = p.Close()
	close(stopSrv)

	// dialOne must not block after pool is closed.
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.dialOne()
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		assert.Fail(t, "dialOne blocked after pool closed")
	}
}

// --- evict ---

func TestPool_Evict_RemovesTarget(t *testing.T) {
	t.Parallel()

	// Build a tiny pool but exercise evict directly without a live *conn.Conn.
	// We need a real *conn.Conn for evict because it calls mc.c.Close().
	// Use net.Pipe + NewClientConn so we get a real conn object we can close.
	cli, srv := net.Pipe()
	defer srv.Close()
	stopSrv := make(chan struct{})
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		runFakeH2Server(srv, func(*frame.Framer) { <-stopSrv })
	}()
	t.Cleanup(func() { close(stopSrv); <-srvDone })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := conn.NewClientConn(ctx, cli, conn.ConnOptions{})
	require.NoError(t, err, "NewClientConn")
	t.Cleanup(func() { _ = c.Close() })
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 2}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	mc1 := &managedConn{c: c}
	mc2 := &managedConn{c: c} // same underlying conn — just testing slice logic

	result := p.evict([]*managedConn{mc1, mc2}, mc1, CloseDead)

	require.Lenf(t, result, 1, "evict result len = %d, want 1", len(result))
	assert.Same(t, mc2, result[0],
		"evict should keep the other managedConn — removing the wrong entry would close a "+
			"live conn and leave the dead one in the pool")
}

// --- replyAcquire: unconditional single reply ---

// TestPool_ReplyAcquire_DeliversEvenWhenCtxCancelled pins the invariant that
// replaced the old ctx.Done() race: the actor always delivers the one reply it
// owes, and never decrements active itself. A cancelled caller reclaims the
// committed mc via acquire's reclaim goroutine — racing the send against
// ctx.Done() would strand the mc in the buffer and leak the slot instead.
func TestPool_ReplyAcquire_DeliversEvenWhenCtxCancelled(t *testing.T) {
	t.Parallel()

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	reply := make(chan acquireResp, 1)
	req := acquireReq{ctx: ctx, reply: reply}
	mc := &managedConn{active: 1}

	p.replyAcquire(req, mc, nil) // must not block, must not touch mc.active

	select {
	case resp := <-reply:
		assert.Samef(t, mc, resp.mc, "reply mc = %p, want %p", resp.mc, mc)
	default:
		assert.Fail(t, "replyAcquire delivered nothing to an abandoned caller",
			"the committed mc is stranded and its slot leaked")
	}
	assert.Equalf(t, 1, mc.active,
		"mc.active = %d, want 1 (the reclaiming caller releases, not the actor)", mc.active)
}

// --- run: dialDone error with no waiters ---

func TestPool_DialFailure_NoWaiters_SetsBackoff(t *testing.T) {
	t.Parallel()

	fd := &failingDialer{err: errors.New("connection refused")}
	p := newPool("fake:0", conn.ConnOptions{Dialer: fd}, PoolOptions{
		MaxConnsPerHost: 1,
		DialBackoff:     500 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// First acquire: triggers a dial, waiter queued, dial fails → waiter gets error.
	_, err := p.acquire(ctx)
	// Second acquire within backoff window → ErrDialBackoff (no new dial).
	_, err2 := p.acquire(ctx)

	require.Error(t, err, "expected dial error, got nil")
	assert.ErrorIsf(t, err2, ErrDialBackoff, "second acquire = %v, want ErrDialBackoff", err2)
	assert.EqualValuesf(t, 1, fd.dialCount.Load(),
		"dial count = %d, want 1 (backoff suppressed second) — without the backoff a dead "+
			"target is hammered once per acquire", fd.dialCount.Load())
}
