package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// goAwayDrainMode selects which pool code path is allowed to observe the
// GOAWAY'd conn while requests are still draining on it.
type goAwayDrainMode struct {
	name string
	// tickPeriod is PoolOptions.HealthCheckPeriod.
	tickPeriod time.Duration
	// pollStats, when true, calls the public PoolStats() during the drain
	// (Pool.handleStats -> evictDeadSilent).
	pollStats bool
}

// TestConformance_RFC7540_Sec6_8_PoolDrainsInflightOnGoAway pins the pool half
// of RFC 7540 §6.8. The GOAWAY carries a last-stream-id; the sender "will ignore
// frames sent on streams initiated by the receiver if the stream has an
// identifier higher than the included last stream identifier", and "Receivers of
// a GOAWAY frame MUST NOT open additional streams on the connection". By the
// contrapositive of the first clause, streams at or below that id are the ones
// the sender still processes — so the pool must let them complete rather than
// close the connection out from under them.
//
// A real net/http2 server's graceful Shutdown sends GOAWAY(maxClientStreamID)
// and — for a NO_ERROR GOAWAY — does not arm its own shutdown timer until every
// open stream has completed (x/net@v0.56.0/http2/server.go:908-914). So the peer
// is deliberately holding the connection open for exactly these streams. The
// pool must not close it first.
//
// (*conn.Conn).IsAlive() is false the moment a GOAWAY lands, so every pool site
// that closes "not alive" conns is a candidate to tear the transport down under
// in-flight streams. Both such sites are exercised here:
//
//	tick  — Pool.handleTick -> evictDead
//	stats — Pool.handleStats -> evictDeadSilent, reachable from public PoolStats()
//
// Both were unguarded and both killed 4/4 draining requests with
// RST(INTERNAL_ERROR); the fix is the active==0 guard that evictIdle already had.
func TestConformance_RFC7540_Sec6_8_PoolDrainsInflightOnGoAway(t *testing.T) {
	modes := []goAwayDrainMode{
		// Tick lands repeatedly inside the drain window; stats never consulted.
		{name: "tick", tickPeriod: 20 * time.Millisecond, pollStats: false},
		// Tick provably cannot fire (60s >> the sub-second drain); only the
		// PoolStats() scrape can observe the conn.
		{name: "stats", tickPeriod: 60 * time.Second, pollStats: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			runGoAwayDrain(t, mode)
		})
	}
}

func runGoAwayDrain(t *testing.T, mode goAwayDrainMode) {
	t.Helper()
	// N=4 fits under MaxStreamsPerConn=8 and under the net/http2 server's
	// default advertised MAX_CONCURRENT_STREAMS (250), so all four are
	// genuinely concurrent on ONE conn — the shape the bug needs (a single
	// conn carrying several draining streams).
	const N = 4

	var started sync.WaitGroup
	started.Add(N)
	proceed := make(chan struct{})

	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Done()
		<-proceed // hold every stream open across the GOAWAY
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1, // force all N streams onto one conn
			MaxStreamsPerConn: 8,
			HealthCheckPeriod: mode.tickPeriod,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	errs := make(chan error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var res client.Response
			if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res); err != nil {
				errs <- err
				return
			}
			if res.Status != 200 {
				t.Errorf("status = %d, want 200", res.Status)
			}
			errs <- nil
		}()
	}

	// CONTROL: every request must be parked inside a handler before we shut
	// down. Without this the test could pass with requests that completed
	// before the GOAWAY ever landed — i.e. never draining, asserting nothing.
	allStarted := make(chan struct{})
	go func() { started.Wait(); close(allStarted) }()
	select {
	case <-allStarted:
	case <-time.After(10 * time.Second):
		close(proceed)
		t.Fatal("handlers did not all reach the server; requests were never concurrently in-flight")
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		_ = srv.Config.Shutdown(context.Background())
	}()

	// Hold the drain open long enough for the mode's eviction site to run
	// against a conn that is !IsAlive() (GOAWAY received) with active == N.
	const drainWindow = 300 * time.Millisecond
	if mode.pollStats {
		deadline := time.Now().Add(drainWindow)
		for time.Now().Before(deadline) {
			_ = c.PoolStats() // handleStats -> evictDeadSilent
			time.Sleep(20 * time.Millisecond)
		}
	} else {
		// Deliberately does NOT call PoolStats: in "tick" mode the tick must
		// be the only thing that can observe the conn, so a failure here
		// indicts evictDead alone.
		time.Sleep(drainWindow)
	}

	close(proceed) // let the handlers respond
	wg.Wait()
	<-shutdownDone
	close(errs)

	failed := 0
	for err := range errs {
		if err != nil {
			failed++
			t.Errorf("in-flight request died during graceful drain: %v", err)
		}
	}
	if failed > 0 {
		t.Fatalf("%d/%d streams below the peer's GOAWAY lastStreamID were killed by the pool; RFC 7540 §6.8 requires they complete", failed, N)
	}

	// CONTROL: prove a GOAWAY was actually received on this conn. Otherwise
	// "all requests succeeded" is the trivially-true outcome of a test where
	// Shutdown never reached the client, and the §6.8 path was never entered.
	// CONTROL: prove a GOAWAY actually reached the pool on this conn. Without
	// this, "all N requests succeeded" is the trivially-true result of a test
	// where Shutdown never landed and the §6.8 drain path was never entered.
	//
	// Exactly 1, not >=1: one conn received one GOAWAY, and this counter's
	// sibling ConnsClosed counts that conn once. A larger number means the
	// counter is counting draining streams (it counted 4 here) or the same conn
	// at two eviction sites, which makes it useless for alerting on peer
	// restarts and flaky for any test asserting on it.
	if got := c.MetricsSnapshot().Counters.GoAwaysReceived; got != 1 {
		t.Fatalf("GoAwaysReceived = %d, want exactly 1 (one conn, one GOAWAY); 0 = the GOAWAY never reached the pool and this test asserted nothing", got)
	}
}
