package client

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// h2TestServer starts an in-process HTTP/2 server and returns its address.
func h2TestServer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // silence benign mid-handshake abort spam
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func insecureConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

// TestWarmup_NoActiveLeak is a regression test: warmup acquired conns but never
// released them, leaving a phantom active-stream count that never returned to
// zero (blocking idle eviction and graceful drain). After warmup settles with
// no outstanding requests, InFlightStreams must be zero.
func TestWarmup_NoActiveLeak(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	p := newPool(addr, insecureConnOpts(), PoolOptions{
		MaxConnsPerHost:   2,
		MaxStreamsPerConn: 10,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	defer p.Close()
	// Seed one live conn and release it, so warmup's acquire hits this existing
	// conn instantly (returns it, active++) instead of racing a sub-50ms dial.
	// This deterministically exercises the release path the fix added — a
	// timed-out acquire would leave nothing to leak and pass on buggy code too.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	mc, err := p.Acquire(ctx)
	cancel()
	require.NoError(t, err, "seed acquire against a live server")
	p.Release(mc)

	p.Warmup(2)

	// With a conn already established and no outstanding request, the active
	// stream count must settle to 0; the leak (acquire without release) left it
	// permanently > 0.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && p.Stats().InFlightStreams != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Zerof(t, p.Stats().InFlightStreams,
		"InFlightStreams never returned to 0 after warmup: %+v — a phantom active count "+
			"blocks idle eviction and graceful drain forever", p.Stats())
}

// rgTrackedConn counts Close calls on a dialed transport connection, and
// signals once the HTTP/2 handshake has visibly completed.
//
// handshook is closed on the first Read that returns bytes — the server SETTINGS
// frame — at which point NewClientConn is about to succeed. That instant is the
// one the leak needs; see closeDuringDial.
type rgTrackedConn struct {
	net.Conn
	closed    atomic.Bool
	closedCt  *atomic.Int32
	once      sync.Once
	handshook chan struct{}
}

func (c *rgTrackedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err == nil && n > 0 && c.handshook != nil {
		c.once.Do(func() { close(c.handshook) })
	}
	return n, err
}

func (c *rgTrackedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.closedCt.Add(1)
	}
	return c.Conn.Close()
}

// rgTrackingDialer wraps a Dialer and tallies dialed vs Closed conns, threading
// the handshake signal through to the conn it returns.
type rgTrackingDialer struct {
	inner     conn.Dialer
	dialedCt  *atomic.Int32
	closedCt  *atomic.Int32
	handshook chan struct{}
}

func (d *rgTrackingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.dialedCt.Add(1)
	return &rgTrackedConn{Conn: c, closedCt: d.closedCt, handshook: d.handshook}, nil
}

// closeDuringDial runs iters pool lifecycles against addr and reports
// (signals, dialed, closed).
//
// With inject=true each lifecycle waits for the HANDSHAKE signal before calling
// Close. Where that signal sits is the whole design:
//
//   - Signalling when Dial RETURNS — or, as the old form did, sleeping a flat 1ms
//     and hoping — is too early. dialAttempt cancels the dial context the moment
//     closedCh closes, so NewClientConn then fails and conn.Dial closes the
//     transport itself. That is a SECOND mechanism which keeps the tally balanced
//     while the drain never runs at all. Measured with that placement: deleting
//     the drain outright still left 50/50 conns Closed and this test green.
//   - Signalling once the handshake has produced bytes is late enough that
//     NewClientConn succeeds, so dialOne really does hand a live managedConn to
//     the buffered dialDoneCh. That is the state the leak needs.
//
// Which of the two close paths then runs is a genuine select race inside the
// actor and is not controllable from out here; the iteration count exists for
// that residual race alone. Measured with the drain deleted: 300 lifecycles,
// 300 handshakes, 300 dialled, 194 Closed, 106 leaked — the drain path is taken
// on roughly a third of lifecycles, so 100 iterations puts the false-negative
// near 1e-18.
//
// With inject=false the pool is closed having never been asked to dial. That is
// the control arm: it must produce zero dials, which is what makes a non-zero
// count in the injected arm attributable to the injection.
func closeDuringDial(t *testing.T, addr string, iters int, inject bool) (signals int, dialed, closed int32) {
	t.Helper()

	inner := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}
	var dialedCt, closedCt atomic.Int32
	for i := 0; i < iters; i++ {
		td := &rgTrackingDialer{inner: inner, dialedCt: &dialedCt, closedCt: &closedCt}
		if inject {
			td.handshook = make(chan struct{})
		}
		p := newPool(addr, conn.ConnOptions{Dialer: td}, PoolOptions{
			MaxConnsPerHost:   1,
			HealthCheckPeriod: time.Hour,
		}, nil, nil)
		if !inject {
			_ = p.Close()
			continue
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if mc, err := p.Acquire(ctx); err == nil {
				p.Release(mc)
			}
		}()
		select {
		case <-td.handshook:
			signals++
		case <-time.After(10 * time.Second):
			// Fall through — the signal count the caller asserts reports it.
		}
		_ = p.Close()
	}

	// handleClose drains in-flight dials in a background goroutine, so a dialed
	// conn may be Closed shortly after Close returns — allow a brief settle.
	deadline := time.Now().Add(8 * time.Second)
	for closedCt.Load() < dialedCt.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	return signals, dialedCt.Load(), closedCt.Load()
}

// closeDuringDialIters covers the residual select race only. The old form ran
// 1000 lifecycles because its injection also had to win a race against a flat
// 1ms sleep just to dial at all; with the handshake pinned, every lifecycle
// reaches the state and only the actor's choice of path is left to chance.
const closeDuringDialIters = 100

// TestPoolClose_NoDialDoneLeak is a regression test: a dial completing during
// Pool.Close used to be orphaned in the buffered dialDoneCh (never Closed),
// leaking the conn, its reader goroutine, and its fd. handleClose now drains
// every in-flight dial, so every dialed conn is eventually Closed.
//
// The old form approximated the interleaving by sleeping a flat 1ms between
// launching the acquire and calling Close, and hoping the TLS dial landed inside
// it. On CI it never did: run 32336395551 reported "1000 close-during-dial
// races, 0 conns dialled, 0 Closed" — a thousand lifecycles that closed pools
// which had not dialled anything, so the regression path was not executed once.
// Locally the same form lands 1 lifecycle in 50 unloaded and 0 in 50 under
// GOMAXPROCS=1 with four CPU hogs, which is why it looked healthy here and was
// vacuous there. The dialer now signals the completed handshake and this waits
// for it.
func TestPoolClose_NoDialDoneLeak(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	signals, dialed, closed := closeDuringDial(t, addr, closeDuringDialIters, true)

	t.Logf("injections: %d/%d lifecycles closed against a completed handshake; %d conns dialled, %d Closed",
		signals, closeDuringDialIters, dialed, closed)
	require.Equalf(t, closeDuringDialIters, signals,
		"only %d of %d lifecycles saw a completed handshake before Close; the rest closed a "+
			"pool whose dial had not produced a conn, and cannot observe the leak this test "+
			"exists for", signals, closeDuringDialIters)
	require.GreaterOrEqualf(t, dialed, int32(closeDuringDialIters),
		"%d conns dialled across %d injected lifecycles, want at least one each",
		dialed, closeDuringDialIters)
	assert.GreaterOrEqualf(t, closed, dialed,
		"conn leak: %d dialed but only %d Closed (%d leaked) — each leak strands a "+
			"connection, its reader goroutine and its fd", dialed, closed, dialed-closed)
}

// TestPoolClose_NoDialInFlight_ControlArm is the control for the test above: the
// same pools over the same server, closed WITHOUT the injection. It must dial
// nothing at all.
//
// Without it, "dialed >= 100" in the injected arm is only a number; with it the
// number is attributable to the injection, because the identical fixture
// produces zero when the injection is withheld. It is also the arm that would
// catch a fixture that had started dialling on its own, at which point the
// injected arm would be counting dials it did not cause.
func TestPoolClose_NoDialInFlight_ControlArm(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	signals, dialed, closed := closeDuringDial(t, addr, closeDuringDialIters, false)

	t.Logf("injections (control, none performed): %d handshakes, %d conns dialled, %d Closed",
		signals, dialed, closed)
	assert.Zerof(t, signals, "the control arm performed %d injections; it must perform none", signals)
	assert.Zerof(t, dialed,
		"closing a pool that was never asked for a connection dialled %d conns — the "+
			"injected arm's dial count would then not be attributable to its injection", dialed)
	assert.Zerof(t, closed, "%d conns Closed in a run that dialled none", closed)
}

// TestBodyStream_DecompressFail_NoDoubleRelease is a regression test: when the
// BodyStream decompression reader failed to initialize, do() released the conn
// directly while leaving resp.BodyReader set, so the caller's resp.Reset()
// released it a SECOND time, driving the pool's active count negative. Exactly
// one release must occur.
func TestBodyStream_DecompressFail_NoDoubleRelease(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("not-a-valid-gzip-stream"))
	})
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10},
		ConnOpts:  insecureConnOpts(),
	})
	require.NoError(t, err, "NewClient against the bad-gzip server")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var resp Response

	derr := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp)

	require.Error(t, derr, "Do = nil, want a decompression error")
	// Must be the decompression failure, not an unrelated dial/handshake error,
	// so the test truly exercises the fixed newDecompressingReader error path.
	require.Containsf(t, derr.Error(), "gzip",
		"Do error = %q, want a gzip decompression failure — any other error means this "+
			"test is measuring a different path", derr)
	// Translate a nil-deref panic (the unfixed stream-Close-after-recycle bug)
	// into a clean failure so it cannot abort the whole package test binary.
	require.NotPanicsf(t, resp.Reset,
		"resp.Reset() panicked (stream Close-after-recycle)")

	// Exactly one net release. Poll a settle window asserting the active count
	// NEVER goes negative (a double-release drives it to -1), then require it to
	// end at exactly 0 — a transient 0 is not accepted as proof. Note: against
	// the fully-unfixed code resp.Reset() above instead nil-deref-panics (the
	// stream-Close-after-recycle bug, fixed separately); this poll guards the
	// residual pool double-release once that panic is gone.
	settle := time.Now().Add(1 * time.Second)
	for time.Now().Before(settle) {
		n := c.PoolStats().InFlightStreams
		require.GreaterOrEqualf(t, n, 0,
			"double-release: InFlightStreams went negative (%d); the conn was handed back "+
				"twice and the pool's budget is now permanently inflated", n)
		time.Sleep(20 * time.Millisecond)
	}
	assert.Zerof(t, c.PoolStats().InFlightStreams,
		"InFlightStreams settled at %d, want 0", c.PoolStats().InFlightStreams)
}
