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
	// Zero-value *conn.Conn has empty peerSettings → PeerMaxConcurrentStreams() == 0.
	// local == 0 too, so both unbounded → fallback 100.
	opts := PoolOptions{}
	got := effectiveStreamCap(opts, &conn.Conn{})
	if got != 100 {
		t.Fatalf("effectiveStreamCap = %d, want 100", got)
	}
}

func TestEffectiveStreamCap_LocalOnly(t *testing.T) {
	t.Parallel()
	// local > 0, peer == 0 → return local.
	opts := PoolOptions{MaxStreamsPerConn: 50}
	got := effectiveStreamCap(opts, &conn.Conn{})
	if got != 50 {
		t.Fatalf("effectiveStreamCap = %d, want 50", got)
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
	_, err1 := p.acquire(ctx)
	// Second acquire: within DialBackoff → ErrDialBackoff; no reply arrives within
	// AcquireTimeout → ErrAcquireTimeout is also acceptable.
	_, err2 := p.acquire(ctx)

	// Both must be non-nil errors.
	if err1 == nil {
		t.Fatalf("first acquire should fail, got nil")
	}
	if err2 == nil {
		t.Fatalf("second acquire should fail, got nil")
	}
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
