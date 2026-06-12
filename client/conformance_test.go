package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams verifies that the
// connection pool opens additional connections when the peer advertises a small
// MAX_CONCURRENT_STREAMS value, honoring RFC 7540 §5.1.2.
//
// The test forces N concurrent requests to all be in-flight simultaneously
// via a server-side barrier. While they are blocked in the handler, it
// snapshots PoolStats and asserts that ActiveConns >= ceil(N / peerCap),
// proving the pool actually opened additional conns to absorb the load
// rather than queueing. Snapshotting AFTER load (the previous design) was
// TOCTOU-fragile: if conns were evicted between request completion and the
// stats read, the assertion could pass on a buggy pool that never opened
// more than one conn.
func TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams(t *testing.T) {
	const (
		N            = 8
		peerCap      = 2
		expectedMin  = (N + peerCap - 1) / peerCap // ceil(N/peerCap) = 4
		serverMaxStr = peerCap
	)

	// Barrier coordination: each handler increments inflight and signals
	// `allInflight` when N have arrived. Test snapshots stats after that
	// signal, then closes `release` to let handlers respond.
	var inflight atomic.Int32
	allInflight := make(chan struct{})
	release := make(chan struct{})

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if inflight.Add(1) == int32(N) {
			close(allInflight)
		}
		<-release
		w.WriteHeader(200)
	}))
	if err := http2.ConfigureServer(srv.Config, &http2.Server{MaxConcurrentStreams: serverMaxStr}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   expectedMin,
			MaxStreamsPerConn: 0, // unbounded local — peer cap governs
			HealthCheckPeriod: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var _res client.Response
			if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &_res); err != nil {
				errs <- err
			}
		}()
	}

	// Wait until all N requests are simultaneously in-flight at the server.
	// If the pool gates correctly on peer MAX_CONCURRENT_STREAMS, this
	// requires opening >=expectedMin conns; otherwise the barrier never closes.
	select {
	case <-allInflight:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatalf("only %d/%d requests reached server within 5s — pool may be queueing instead of opening more conns", inflight.Load(), N)
	}

	// Snapshot stats while load is still pinned in handlers.
	s := c.PoolStats()
	if s.ActiveConns < expectedMin {
		close(release)
		t.Fatalf("ActiveConns = %d, want >= %d during %d-way load with peer MAX_CONCURRENT_STREAMS=%d", s.ActiveConns, expectedMin, N, peerCap)
	}

	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Do: %v", err)
	}
}

// TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease verifies that
// when the peer sends GOAWAY while a stream is in-flight, the pool evicts
// the dead conn via the release path (BUG-1 fix) — not via the background
// HealthCheckPeriod tick.
//
// RFC 7540 §6.8: the conn-layer guarantee that streams ≤ lastStreamID
// continue normally is separately verified by TestOnGoAway_StreamsAtOrBelowLastID_Survive
// in the conn package. Here we focus on the pool-level eviction contract.
func TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease(t *testing.T) {
	started := make(chan struct{}) // closed when handler goroutine starts
	proceed := make(chan struct{}) // closed when test allows handler to respond

	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started) // stream is in-flight
		<-proceed
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			// Long health-check period so eviction can ONLY happen via the
			// release path (BUG-1 fix), not the background tick.
			HealthCheckPeriod: 60 * time.Second,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Launch request in background; it will be in-flight while we trigger GOAWAY.
	requestDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var _res client.Response
		err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &_res)
		requestDone <- err
	}()

	// Wait for stream to reach the server handler (in-flight).
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(proceed)
		t.Fatal("request did not reach server handler")
	}

	// Trigger graceful shutdown: server sends GOAWAY and waits for handlers
	// to complete, then closes the connection. Whether GOAWAY arrives before
	// or after the response, the conn is dead by the time Shutdown returns.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shCtx, shCancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer shCancel()
		_ = srv.Config.Shutdown(shCtx)
	}()

	// Allow handler to respond and Shutdown to complete.
	close(proceed)
	<-shutdownDone

	// The request may succeed (200) or fail with a connection error if the
	// connection was closed before the response frame arrived — both are
	// acceptable outcomes. What matters at the pool level is eviction.
	select {
	case err := <-requestDone:
		if err != nil {
			t.Logf("request result after GOAWAY: %v (expected, conn may close before response)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request goroutine did not complete")
	}

	// KEY ASSERTION (RFC §6.8 pool contract): the dead conn must be evicted
	// via the release path — not via the background health-check tick (which
	// we set to 60s). Window of 5s is generous enough to absorb scheduler
	// latency between Do() returning and the readerLoop processing EOF, while
	// still proving the tick (60s) cannot be responsible.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pool did not evict GOAWAY'd conn via release path; ActiveConns = %d (HealthCheckPeriod = 60s, so tick cannot be the cause)", c.PoolStats().ActiveConns)
}

func TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: 50 * time.Millisecond,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var _res client.Response
	if err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("first Do = %v", err)
	}

	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv.Config.Shutdown(shCtx)
	shCancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ActiveConns = %d, want 0 after peer shutdown", c.PoolStats().ActiveConns)
}

// TestConformance_RFC7540_Sec8_1_StreamBody_EndStream verifies that
// the final DATA frame in a streaming response carries END_STREAM=1,
// satisfying RFC 7540 §8.1 half-close semantics.
func TestConformance_RFC7540_Sec8_1_StreamBody_EndStream(t *testing.T) {
	payload := []byte("conformance body")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, err := io.ReadAll(res.BodyReader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("body = %q, want %q", got, payload)
	}
	// Close returned without error — stream ended cleanly (END_STREAM
	// received, no RST_STREAM sent). Confirms §8.1 half-close.
}
