package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
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
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec
}

// TestResolve_SuccessfulEmpty_DoesNotServeStale is a regression test for
// stale-masking: a SUCCESSFUL DNS lookup returning zero addresses used to be
// reported as the stale cached set, so a service scaled to zero kept receiving
// traffic to dead backends forever. The authoritative empty result must now
// propagate (cache cleared, ErrNoAddresses), not the stale set.
func TestResolve_SuccessfulEmpty_DoesNotServeStale(t *testing.T) {
	var attempt atomic.Int32
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		if attempt.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		}
		return []net.IPAddr{}, nil // success, but empty
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Nanosecond}, fl)

	tick := 0
	clock := time.Unix(1_700_000_000, 0)
	r.setNow(func() time.Time {
		tick++
		return clock.Add(time.Duration(tick) * 2 * time.Nanosecond)
	})

	if first, err := r.Resolve(context.Background()); err != nil || len(first) != 1 {
		t.Fatalf("first Resolve = (%v, %v), want one address", first, err)
	}

	got, err := r.Resolve(context.Background())
	if len(got) != 0 {
		t.Fatalf("second Resolve returned stale %v; want empty on a successful-but-empty lookup", got)
	}
	if !errors.Is(err, ErrNoAddresses) {
		t.Fatalf("second Resolve err = %v, want ErrNoAddresses", err)
	}
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
	mc, err := p.acquire(ctx)
	cancel()
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	p.release(mc, nil)

	p.warmup(2)

	// With a conn already established and no outstanding request, the active
	// stream count must settle to 0; the leak (acquire without release) left it
	// permanently > 0.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().InFlightStreams == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("InFlightStreams never returned to 0 after warmup: %+v", p.Stats())
}

// rgTrackedConn counts Close calls on a dialed transport connection.
type rgTrackedConn struct {
	net.Conn
	closed   atomic.Bool
	closedCt *atomic.Int32
}

func (c *rgTrackedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.closedCt.Add(1)
	}
	return c.Conn.Close()
}

// rgTrackingDialer wraps a Dialer and tallies dialed vs Closed conns.
type rgTrackingDialer struct {
	inner    conn.Dialer
	dialedCt *atomic.Int32
	closedCt *atomic.Int32
}

func (d *rgTrackingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.dialedCt.Add(1)
	return &rgTrackedConn{Conn: c, closedCt: d.closedCt}, nil
}

// TestPoolClose_NoDialDoneLeak is a regression test: a dial completing during
// Pool.Close used to be orphaned in the buffered dialDoneCh (never Closed),
// leaking the conn, its reader goroutine, and its fd. handleClose now drains
// every in-flight dial, so every dialed conn is eventually Closed.
//
// The leak surfaces only on a probabilistic interleaving (dial result buffered,
// actor selects closeCh before draining it), so this is a stress test — the
// high iteration count drives the per-run false-negative toward zero. The fix's
// correctness does not depend on the race: handleClose drains exactly the
// captured in-flight-dial count regardless of timing.
func TestPoolClose_NoDialDoneLeak(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	inner := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec

	var dialedCt, closedCt atomic.Int32

	for i := 0; i < 1000; i++ {
		td := &rgTrackingDialer{inner: inner, dialedCt: &dialedCt, closedCt: &closedCt}
		p := newPool(addr, conn.ConnOptions{Dialer: td}, PoolOptions{
			MaxConnsPerHost:   1,
			HealthCheckPeriod: time.Hour,
		}, nil, nil)

		// Launch an acquire so a dialOne is in flight, then close while it is
		// completing — exercising the dial-completes-during-close path.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if mc, err := p.acquire(ctx); err == nil {
				p.release(mc, nil)
			}
		}()
		time.Sleep(time.Millisecond)
		_ = p.Close()
	}

	// handleClose drains in-flight dials in a background goroutine, so a dialed
	// conn may be Closed shortly after Close returns — allow a brief settle.
	deadline := time.Now().Add(3 * time.Second)
	for closedCt.Load() < dialedCt.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if d, c := dialedCt.Load(), closedCt.Load(); c < d {
		t.Fatalf("conn leak: %d dialed but only %d Closed (%d leaked)", d, c, d-c)
	}
}

// TestStreamBody_DecompressFail_NoDoubleRelease is a regression test: when the
// StreamBody decompression reader failed to initialize, do() released the conn
// directly while leaving resp.BodyReader set, so the caller's resp.Reset()
// released it a SECOND time, driving the pool's active count negative. Exactly
// one release must occur.
func TestStreamBody_DecompressFail_NoDoubleRelease(t *testing.T) {
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	derr := c.Do(ctx, &Request{Method: "GET", Path: "/", StreamBody: true}, &resp)
	if derr == nil {
		t.Fatalf("Do = nil, want a decompression error")
	}
	// Must be the decompression failure, not an unrelated dial/handshake error,
	// so the test truly exercises the fixed newDecompressingReader error path.
	if !strings.Contains(derr.Error(), "gzip") {
		t.Fatalf("Do error = %q, want a gzip decompression failure", derr)
	}
	// Translate a nil-deref panic (the unfixed stream-Close-after-recycle bug)
	// into a clean failure so it cannot abort the whole package test binary.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("resp.Reset() panicked (stream Close-after-recycle): %v", r)
			}
		}()
		resp.Reset()
	}()

	// Exactly one net release. Poll a settle window asserting the active count
	// NEVER goes negative (a double-release drives it to -1), then require it to
	// end at exactly 0 — a transient 0 is not accepted as proof. Note: against
	// the fully-unfixed code resp.Reset() above instead nil-deref-panics (the
	// stream-Close-after-recycle bug, fixed separately); this poll guards the
	// residual pool double-release once that panic is gone.
	settle := time.Now().Add(1 * time.Second)
	for time.Now().Before(settle) {
		if n := c.PoolStats().InFlightStreams; n < 0 {
			t.Fatalf("double-release: InFlightStreams went negative (%d)", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := c.PoolStats().InFlightStreams; n != 0 {
		t.Fatalf("InFlightStreams settled at %d, want 0", n)
	}
}
