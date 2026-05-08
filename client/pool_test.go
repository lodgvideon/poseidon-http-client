package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestPool_Stats_Empty(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 2})
	t.Cleanup(func() { _ = p.Close() })

	s := p.Stats()
	if s.ActiveConns != 0 || s.InFlightStreams != 0 || s.Waiters != 0 || s.InFlightDials != 0 {
		t.Fatalf("empty Stats = %+v", s)
	}
}

func TestPool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestPool_Stats_Concurrent(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 4})
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
}

func TestPool_StatsAfterClose_ReturnsZero(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	_ = p.Close()
	s := p.Stats()
	if s != (Stats{}) {
		t.Fatalf("Stats after Close = %+v, want zero", s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, err := newPoolTransportFromPool(p).acquire(ctx)
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire after Close = %v, want ErrPoolClosed", err)
	}
}

// --- effectiveStreamCap ---

func TestEffectiveStreamCap_BothUnbounded(t *testing.T) {
	t.Parallel()
	if got := effectiveStreamCap(0, 0); got != 100 {
		t.Fatalf("effectiveStreamCap(0,0) = %d, want 100", got)
	}
}

func TestEffectiveStreamCap_LocalOnly(t *testing.T) {
	t.Parallel()
	if got := effectiveStreamCap(50, 0); got != 50 {
		t.Fatalf("effectiveStreamCap(50,0) = %d, want 50", got)
	}
}

func TestEffectiveStreamCap_PeerOnly(t *testing.T) {
	t.Parallel()
	if got := effectiveStreamCap(0, 30); got != 30 {
		t.Fatalf("effectiveStreamCap(0,30) = %d, want 30", got)
	}
}

func TestEffectiveStreamCap_PeerLower(t *testing.T) {
	t.Parallel()
	if got := effectiveStreamCap(100, 2); got != 2 {
		t.Fatalf("effectiveStreamCap(100,2) = %d, want 2 (peer cap)", got)
	}
}

func TestEffectiveStreamCap_LocalLower(t *testing.T) {
	t.Parallel()
	if got := effectiveStreamCap(10, 100); got != 10 {
		t.Fatalf("effectiveStreamCap(10,100) = %d, want 10 (local cap)", got)
	}
}

// --- canDial ---

func TestCanDial_WithinBackoffWindow(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{
		MaxConnsPerHost: 4,
		DialBackoff:     1 * time.Second,
	})
	t.Cleanup(func() { _ = p.Close() })

	// Simulate a recent dial error: lastErrAt = now.
	recentErr := time.Now()
	got := p.canDial(0, 0, recentErr)
	if got {
		t.Fatal("canDial should return false within backoff window")
	}
}

func TestCanDial_AfterBackoffExpired(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{
		MaxConnsPerHost: 4,
		DialBackoff:     1 * time.Millisecond,
	})
	t.Cleanup(func() { _ = p.Close() })

	// lastErrAt far in the past → backoff expired → can dial.
	pastErr := time.Now().Add(-10 * time.Second)
	got := p.canDial(0, 0, pastErr)
	if !got {
		t.Fatal("canDial should return true after backoff expired")
	}
}

// --- acquire context-cancel paths ---

func TestPool_AcquireCtxCanceledBeforeSend(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before acquire

	_, err := p.acquire(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire = %v, want context.Canceled", err)
	}
}

func TestPool_AcquireClosedChBeforeSend(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	// Close the pool before acquire so closedCh fires.
	_ = p.Close()

	ctx := context.Background()
	_, err := p.acquire(ctx)
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("acquire = %v, want ErrPoolClosed", err)
	}
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
	})
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First acquire: dial is attempted, actor returns dial error via replyAcquire.
	// The dialer wraps with conn.Dial wrapping → expect non-nil error,
	// not necessarily a known sentinel.
	_, err1 := p.acquire(ctx)
	if err1 == nil {
		t.Fatalf("first acquire should fail, got nil")
	}
	// Second acquire: within DialBackoff window with no live conns →
	// ErrDialBackoff; if scheduling delays the reply past AcquireTimeout
	// the actor's reply path loses the race and the caller observes
	// ErrAcquireTimeout. Both are valid.
	_, err2 := p.acquire(ctx)
	if !errors.Is(err2, ErrDialBackoff) && !errors.Is(err2, ErrAcquireTimeout) {
		t.Fatalf("second acquire = %v, want ErrDialBackoff or ErrAcquireTimeout", err2)
	}
}

// --- BUG-3 regression: pruneExpiredWaiters ---

func TestPruneExpiredWaiters_DropsCancelledKeepsLive(t *testing.T) {
	t.Parallel()
	live := context.Background()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	in := []acquireReq{
		{ctx: live},
		{ctx: dead},
		{ctx: live},
		{ctx: dead},
	}
	out := pruneExpiredWaiters(in)
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	for i, w := range out {
		select {
		case <-w.ctx.Done():
			t.Fatalf("out[%d] ctx unexpectedly done", i)
		default:
		}
	}
}

func TestPruneExpiredWaiters_EmptyAndAllLive(t *testing.T) {
	t.Parallel()
	if got := pruneExpiredWaiters(nil); len(got) != 0 {
		t.Fatalf("nil → len %d, want 0", len(got))
	}
	live := context.Background()
	in := []acquireReq{{ctx: live}, {ctx: live}}
	out := pruneExpiredWaiters(in)
	if len(out) != 2 {
		t.Fatalf("all-live len = %d, want 2", len(out))
	}
}

// --- BUG-2 regression: DialTimeout bounds dialOne ---

// hangingDialer blocks Dial until either (a) ctx is cancelled or
// (b) release is closed. Used to verify DialTimeout fires.
type hangingDialer struct {
	release chan struct{}
}

func (d *hangingDialer) Dial(ctx context.Context, _ string) (net.Conn, error) {
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
	})
	t.Cleanup(func() {
		close(hd.release)
		_ = p.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := p.acquire(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected dial-timeout error, got nil")
	}
	// Bound elapsed by an order of magnitude over DialTimeout to detect a
	// regression that bypasses the timeout (e.g. background ctx without
	// deadline).
	if elapsed > 500*time.Millisecond {
		t.Fatalf("acquire took %v with DialTimeout=50ms — bound not enforced", elapsed)
	}
}

func TestPool_DialTimeout_DefaultedTo30s(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	t.Cleanup(func() { _ = p.Close() })
	if p.opts.DialTimeout != 30*time.Second {
		t.Fatalf("default DialTimeout = %v, want 30s", p.opts.DialTimeout)
	}
}

// --- BUG-2 regression: dialOne aborts when pool closes mid-dial ---

func TestPool_DialOne_PoolCloseCancelsHangingDial(t *testing.T) {
	t.Parallel()
	hd := &hangingDialer{release: make(chan struct{})}
	p := newPool("fake:0", conn.ConnOptions{Dialer: hd}, PoolOptions{
		MaxConnsPerHost: 1,
		// Long DialTimeout so we know cancellation came from Close,
		// not the timeout.
		DialTimeout: 30 * time.Second,
	})

	// Trigger a dial via acquire in a goroutine so the actor calls dialOne.
	acqErr := make(chan error, 1)
	go func() {
		ctx := context.Background()
		_, err := p.acquire(ctx)
		acqErr <- err
	}()
	// Give the actor a moment to schedule dialOne.
	time.Sleep(20 * time.Millisecond)

	closeStart := time.Now()
	_ = p.Close()
	closeElapsed := time.Since(closeStart)

	// Close itself returns once actor exits — should be fast even with
	// a dial in flight (watchdog cancels dial ctx → conn.Dial returns).
	if closeElapsed > 1*time.Second {
		t.Fatalf("Close took %v with hanging dial — watchdog not wired up", closeElapsed)
	}
	// acquire must complete with an error (pool closed or dial cancelled).
	select {
	case err := <-acqErr:
		if err == nil {
			t.Fatal("acquire returned nil error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not return after Close")
	}
	close(hd.release) // unblock any straggler dialer goroutine
}

// --- release nil mc (no-op) ---

func TestPool_Release_NilMC_NoOp(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	t.Cleanup(func() { _ = p.Close() })

	// Must not panic or block.
	p.release(nil, nil)
}

// --- release when pool is closed (closedCh branch) ---

func TestPool_Release_PoolClosed_NoDeadlock(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
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
		p.release(mc, nil)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("release on closed pool deadlocked")
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
	p := newPool("fake:0", conn.ConnOptions{Dialer: d}, PoolOptions{MaxConnsPerHost: 2})

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
		t.Fatal("dialOne blocked after pool closed")
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
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 2})
	t.Cleanup(func() { _ = p.Close() })

	mc1 := &managedConn{c: c}
	mc2 := &managedConn{c: c} // same underlying conn — just testing slice logic

	conns := []*managedConn{mc1, mc2}
	result := p.evict(conns, mc1)
	if len(result) != 1 {
		t.Fatalf("evict result len = %d, want 1", len(result))
	}
	if result[0] != mc2 {
		t.Fatal("evict should keep the other managedConn")
	}
}

// --- replyAcquire ctx-cancel branch ---

func TestPool_ReplyAcquire_CtxCancelledReturnsCredit(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	// Use an unbuffered channel so the send arm always blocks, forcing the
	// ctx.Done() arm to be selected.
	reply := make(chan acquireResp) // unbuffered
	req := acquireReq{ctx: ctx, reply: reply}

	// Synthesise a non-nil mc so we can verify active is decremented.
	mc := &managedConn{active: 1}
	p.replyAcquire(req, mc, nil)

	// active must be decremented back.
	if mc.active != 0 {
		t.Fatalf("mc.active = %d, want 0 after ctx-cancel replyAcquire", mc.active)
	}
}

// --- run: dialDone error with no waiters ---

func TestPool_DialFailure_NoWaiters_SetsBackoff(t *testing.T) {
	t.Parallel()
	fd := &failingDialer{err: errors.New("connection refused")}
	p := newPool("fake:0", conn.ConnOptions{Dialer: fd}, PoolOptions{
		MaxConnsPerHost: 1,
		DialBackoff:     500 * time.Millisecond,
	})
	t.Cleanup(func() { _ = p.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// First acquire: triggers a dial, waiter queued, dial fails → waiter gets error.
	_, err := p.acquire(ctx)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}

	// Second acquire within backoff window → ErrDialBackoff (no new dial).
	_, err2 := p.acquire(ctx)
	if !errors.Is(err2, ErrDialBackoff) {
		t.Fatalf("second acquire = %v, want ErrDialBackoff", err2)
	}
	if got := fd.dialCount.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1 (backoff suppressed second)", got)
	}
}
