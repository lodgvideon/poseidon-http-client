package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newH2TestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, srv.Listener.Addr().String()
}

func TestHooks_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	var startN, completeN atomic.Int32
	var lastStatus atomic.Int32
	var startMethod, startPath atomic.Value
	var badLatency atomic.Bool
	hooks := &client.Hooks{
		OnRequestStart: func(e client.RequestStartEvent) {
			startN.Add(1)
			startMethod.Store(e.Method)
			startPath.Store(e.Path)
		},
		OnRequestComplete: func(e client.RequestCompleteEvent) {
			completeN.Add(1)
			lastStatus.Store(int32(e.Status))
			if e.Latency <= 0 {
				badLatency.Store(true)
			}
		},
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var resp client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"}, &resp)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, resp.Status, "response status")
	assert.EqualValues(t, 1, startN.Load(), "OnRequestStart must fire exactly once per request")
	assert.EqualValues(t, 1, completeN.Load(), "OnRequestComplete must fire exactly once per request")
	assert.Equal(t, "GET", startMethod.Load(), "RequestStartEvent.Method")
	assert.Equal(t, "/x", startPath.Load(), "RequestStartEvent.Path")
	assert.EqualValues(t, 200, lastStatus.Load(), "RequestCompleteEvent.Status")
	assert.False(t, badLatency.Load(),
		"RequestCompleteEvent.Latency was not positive: a zero latency makes the hook "+
			"useless for the timing it exists to report")
}

func TestHooks_NilSafe(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		// Hooks intentionally nil.
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var resp client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err,
		"a client constructed with no Hooks must serve requests: every hook site has to "+
			"nil-check rather than assume a Hooks value exists")
}

func TestHooks_SetHooksAfterNewClient(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var n atomic.Int32
	c.SetHooks(&client.Hooks{
		OnRequestComplete: func(client.RequestCompleteEvent) { n.Add(1) },
	})

	var resp client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do")
	assert.EqualValues(t, 1, n.Load(),
		"hooks installed after construction did not take effect: SetHooks swaps the "+
			"atomic pointer every hook site reads")
}

func TestHooks_DoStream_OnRequestStartAndComplete(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	var startN, completeN atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) { startN.Add(1) },
		OnRequestComplete: func(client.RequestCompleteEvent) { completeN.Add(1) },
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var sr client.StreamResponse
	err = c.DoStream(context.Background(), &client.Request{Method: "GET", Path: "/"}, &sr)
	if err == nil {
		_ = sr.Close()
	}

	require.NoError(t, err, "DoStream")
	assert.EqualValues(t, 1, startN.Load(),
		"OnRequestStart must fire on the streaming path too, not only on buffered Do")
	assert.EqualValues(t, 1, completeN.Load(),
		"OnRequestComplete must fire on the streaming path too, not only on buffered Do")
}

func TestHooks_OnRetry(t *testing.T) {
	t.Parallel()
	// Server: first request 503, subsequent 200.
	var attempts atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	var retryN atomic.Int32
	var lowAttempt atomic.Bool
	hooks := &client.Hooks{
		OnRetry: func(e client.RetryEvent) {
			retryN.Add(1)
			if e.Attempt < 1 {
				lowAttempt.Store(true)
			}
		},
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(_ error, resp *client.Response) bool { return resp != nil && resp.Status == 503 },
	})

	var resp client.Response
	err = r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Retryer.Do")
	assert.Equal(t, 200, resp.Status, "the retried request must return the second answer")
	assert.EqualValues(t, 1, retryN.Load(),
		"OnRetry must fire exactly once for one retried attempt")
	assert.False(t, lowAttempt.Load(),
		"RetryEvent.Attempt was below 1: the field numbers attempts from 1, so 0 tells "+
			"a caller nothing about which try failed")
}

func TestHooks_OnDial(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	var dialN atomic.Int32
	var gotAddr atomic.Value
	var badDuration atomic.Bool
	var dialErr atomic.Value
	hooks := &client.Hooks{
		OnDial: func(e client.DialEvent) {
			dialN.Add(1)
			gotAddr.Store(e.Addr)
			if e.Duration <= 0 {
				badDuration.Store(true)
			}
			if e.Err != nil {
				dialErr.Store(e.Err.Error())
			}
		},
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 2},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var resp client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do")
	assert.EqualValues(t, 1, dialN.Load(), "OnDial must fire once for the one conn opened")
	assert.Equal(t, addr, gotAddr.Load(), "DialEvent.Addr")
	assert.False(t, badDuration.Load(), "DialEvent.Duration was not positive")
	// == nil, not assert.Nil: assert.Nil decides by reflection and would accept a
	// typed nil stored in the interface, which is exactly the shape a hook that
	// reports a (*someError)(nil) would produce.
	assert.Truef(t, dialErr.Load() == nil,
		"DialEvent.err = %v on a dial that succeeded", dialErr.Load())
}

func TestHooks_OnConnClose_Idle(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	var mu sync.Mutex
	var closeEvents []client.ConnCloseEvent
	hooks := &client.Hooks{
		OnConnClose: func(e client.ConnCloseEvent) {
			mu.Lock()
			closeEvents = append(closeEvents, e)
			mu.Unlock()
		},
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			IdleTimeout:       50 * time.Millisecond,
			HealthCheckPeriod: 25 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	require.NoError(t,
		c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp), "Do")

	// The conn now sits idle past IdleTimeout; the health tick must evict it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(closeEvents)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, closeEvents,
		"OnConnClose never fired: the idle eviction an operator watches for is invisible")
	assert.Equal(t, client.CloseIdle, closeEvents[0].Reason,
		"an idle eviction must be attributed to idleness, not to another close reason")
	assert.Equal(t, addr, closeEvents[0].Addr, "ConnCloseEvent.Addr")
}

func TestHooks_AllHooks_EndToEnd(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	var startN, completeN, dialN, closeN atomic.Int32
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) { startN.Add(1) },
		OnRequestComplete: func(client.RequestCompleteEvent) { completeN.Add(1) },
		OnDial:            func(client.DialEvent) { dialN.Add(1) },
		OnConnClose:       func(client.ConnCloseEvent) { closeN.Add(1) },
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	})
	require.NoError(t, err, "NewClient")

	for i := 0; i < 5; i++ {
		var resp client.Response
		require.NoErrorf(t,
			doWithRetry(t, c, context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp),
			"Do[%d]", i)
	}
	_ = c.Close()

	assert.EqualValues(t, 5, startN.Load(), "OnRequestStart across 5 requests")
	assert.EqualValues(t, 5, completeN.Load(), "OnRequestComplete across 5 requests")
	assert.EqualValues(t, 1, dialN.Load(),
		"OnDial fired more than once: five requests over one pooled conn need one dial")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && closeN.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.EqualValues(t, 1, closeN.Load(), "OnConnClose on the manual Close")

	// The counters must agree with the hooks: two independent observers of the
	// same events, so a discrepancy means one of them is wired to the wrong site.
	snap := c.MetricsSnapshot()
	assert.EqualValues(t, 5, snap.Counters.RequestsStarted, "RequestsStarted")
	assert.EqualValues(t, 5, snap.Counters.RequestsSucceeded, "RequestsSucceeded")
	assert.EqualValues(t, 1, snap.Counters.DialsAttempted, "DialsAttempted")
	assert.EqualValues(t, 1, snap.Counters.ConnsClosed, "ConnsClosed")
	assert.EqualValues(t, 5, snap.Latency.Request.Count, "Latency.Request.Count")
}
