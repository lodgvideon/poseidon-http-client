package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// TestPool_StatsEvictionIsCounted pins that "silent" means no user callback,
// not no record.
//
// Stats() is reachable from the public Client.PoolStats(), and scraping is
// exactly what causes a conn killed out of band to be noticed there. Closing it
// without incrementing ConnsClosed made the pool lie about its own behaviour in
// the one place an operator looks — a monitoring-driven eviction that never
// appears in the monitoring.
func TestPool_StatsEvictionIsCounted(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()

	m := &Metrics{}
	p := newPool(addrs[0].String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 2,
		HealthCheckPeriod: time.Hour, // no tick: the Stats path must be the one that reaps
	}, nil, m)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	p.release(mc)
	// release is asynchronous. Without waiting for the actor to process it, the
	// kill below can land first and handleRelease evicts through evict(), which
	// counts — so the test would pass without ever reaching the Stats path it
	// exists to cover. Stats() is a round trip through the same actor, so one
	// call is the barrier.
	if s := waitStats(p, func(s Stats) bool { return s.InFlightStreams == 0 }, 5*time.Second); s.ActiveConns != 1 {
		t.Fatalf("conn was not pooled after release: %+v", s)
	}
	if got := m.Counters.ConnsClosed.Load(); got != 0 {
		t.Fatalf("ConnsClosed = %d before anything closed", got)
	}

	// Kill it out of band, then observe it only through Stats().
	_ = mc.c.Close()
	s := waitStats(p, func(s Stats) bool { return s.ActiveConns == 0 }, 5*time.Second)
	if s.ActiveConns != 0 {
		t.Fatalf("the dead conn was not reaped by the Stats path: %+v", s)
	}
	if got := m.Counters.ConnsClosed.Load(); got != 1 {
		t.Fatalf("ConnsClosed = %d after a Stats-path eviction, want 1", got)
	}
}

// TestPool_TickAttributesGoAwayBeforeIdle pins the eviction order. A conn the
// peer GOAWAY'd is very often also idle — it stopped taking new streams — so
// whichever sweep reaches it first decides what killed it. Reaping it as
// CloseIdle attributes a peer's shutdown to local inactivity and leaves
// GoAwaysReceived at zero, which makes a rolling restart look like ordinary
// idling.
func TestPool_TickAttributesGoAwayBeforeIdle(t *testing.T) {
	srv := startOneH2Server(t)
	defer srv.Close()

	m := &Metrics{}
	p := newPool(srv.Listener.Addr().String(), newConnOpts(), PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 2,
		HealthCheckPeriod: 20 * time.Millisecond,
		// Short enough that the conn is idle-eligible by the time the tick
		// runs, which is the collision this test is about.
		IdleTimeout: time.Millisecond,
	}, nil, m)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	p.release(mc)

	// A real GOAWAY: the server shuts down gracefully.
	shut, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Config.Shutdown(shut)
	shutCancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.Counters.GoAwaysReceived.Load() > 0 {
			return
		}
		if s := waitStats(p, func(s Stats) bool { return s.ActiveConns == 0 }, 100*time.Millisecond); s.ActiveConns == 0 &&
			m.Counters.ConnsClosed.Load() > 0 && m.Counters.GoAwaysReceived.Load() == 0 {
			t.Fatalf("conn reaped as idle: ConnsClosed=%d GoAwaysReceived=0 — the peer's GOAWAY was attributed to local inactivity",
				m.Counters.ConnsClosed.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no eviction observed: ConnsClosed=%d GoAwaysReceived=%d",
		m.Counters.ConnsClosed.Load(), m.Counters.GoAwaysReceived.Load())
}

// startOneH2Server is startH2Servers for a single server, returning the server
// itself so a test can drive a graceful shutdown and get a real GOAWAY.
func startOneH2Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv
}

// TestH1Pool_StatsEvictionIsCounted is the HTTP/1.1 sibling. It exists because
// the mutation run showed nothing covered the h1 counter: deleting it left the
// suite green, so that half of the fix was deletable. HTTP/1.1 has no GOAWAY,
// so there is only ConnsClosed to attribute.
func TestH1Pool_StatsEvictionIsCounted(t *testing.T) {
	d := newH1FakeDialer()
	m := &Metrics{}
	p := newH1Pool("h:80", d, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: time.Hour, // no tick: the Stats path must be the one that reaps
	}, nil, m)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mc, err := p.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	p.release(mc, true)
	// Barrier, for the reason spelled out in the H2 sibling: release is
	// asynchronous, and a kill that beats the actor to it is evicted through
	// evict(), which counts — the test would then pass without exercising the
	// Stats path at all. It did, until this was added.
	if s := p.Stats(); s.ActiveConns != 1 {
		t.Fatalf("conn was not pooled after release: %+v", s)
	}
	if got := m.Counters.ConnsClosed.Load(); got != 0 {
		t.Fatalf("ConnsClosed = %d before anything closed", got)
	}

	// Kill it out of band; Stats() is the first thing to notice.
	_ = mc.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && p.Stats().ActiveConns != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if s := p.Stats(); s.ActiveConns != 0 {
		t.Fatalf("the dead conn was not reaped by the Stats path: %+v", s)
	}
	if got := m.Counters.ConnsClosed.Load(); got != 1 {
		t.Fatalf("ConnsClosed = %d after a Stats-path eviction, want 1", got)
	}
}
