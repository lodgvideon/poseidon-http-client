package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// recordComplete returns a Hooks that appends every RequestCompleteEvent to the
// returned slice pointer. The events are appended from Do on the caller's own
// goroutine — the hook contract — so a test that issues its requests
// sequentially needs no synchronisation, and every test in this file does.
func recordComplete() (*client.Hooks, *[]client.RequestCompleteEvent) {
	var events []client.RequestCompleteEvent
	h := &client.Hooks{
		OnRequestComplete: func(e client.RequestCompleteEvent) {
			events = append(events, e)
		},
	}
	return h, &events
}

// TestRequestComplete_TimingsNestWithinEachOther pins the relationship the
// fields are documented under, because that is what a load-test report divides
// and subtracts. A field that is merely "non-zero" can still be measured from
// the wrong instant; only the ordering catches that.
func TestRequestComplete_TimingsNestWithinEachOther(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient against the test server")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	before := time.Now()
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"}, &resp)
	after := time.Now()

	require.NoError(t, err, "Do against the test server")
	require.Len(t, *events, 1, "one attempt must produce exactly one completion event")
	e := (*events)[0]
	assert.Falsef(t, e.Start.Before(before) || e.Start.After(after),
		"Start = %v, outside the [%v, %v] window the call ran in — a record that cannot be ordered against the rest of a run is not a timestamp",
		e.Start, before, after)
	assert.Positivef(t, e.TTFB, "TTFB = %v; a request that got a 200 had a first byte", e.TTFB)
	assert.LessOrEqualf(t, e.TTFB, e.Latency,
		"TTFB %v > Latency %v; the head cannot arrive after the last byte, so one of the two is measured from the wrong instant",
		e.TTFB, e.Latency)
	assert.LessOrEqualf(t, e.Acquire, e.TTFB,
		"Acquire %v > TTFB %v; the connection is acquired before the request is even sent",
		e.Acquire, e.TTFB)
	assert.LessOrEqualf(t, e.Connect, e.Acquire,
		"Connect %v > Acquire %v; the dial happens inside the acquire window",
		e.Connect, e.Acquire)
}

// TestRequestComplete_ConnectChargedOnlyToTheDiallingAttempt is the pair of
// equivalence classes Connect exists to separate: the attempt that established
// the connection, and every attempt afterwards that reused it. Reporting the
// dial on both would inflate every sample after a reconnect.
func TestRequestComplete_ConnectChargedOnlyToTheDiallingAttempt(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient against the test server")
	defer c.Close()

	for i := 0; i < 2; i++ {
		var resp client.Response
		resp.Reset()
		require.NoErrorf(t, c.Do(context.Background(),
			&client.Request{Method: "GET", Path: "/x"}, &resp), "Do #%d", i)
	}

	require.Len(t, *events, 2, "two requests, two completion events")
	first, second := (*events)[0], (*events)[1]
	assert.Positivef(t, first.Connect,
		"Connect = %v on the request that lazy-dialled the connection; the one sample that paid for the handshake is the one that must show it",
		first.Connect)
	assert.Zerof(t, second.Connect,
		"Connect = %v on a request that reused the connection; a reused connection has no connect time to report, and charging it again double-counts one dial",
		second.Connect)
}

// TestRequestComplete_ReportsProtocolAndBackend covers the two routing fields
// over the transport where they are least guessable: managed, whose Addr is
// empty by construction and whose backend is chosen per request by the
// Selector.
func TestRequestComplete_ReportsProtocolAndBackend(t *testing.T) {
	t.Parallel()
	srv, addr, _ := startCountedTLSServer(t)
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  client.StaticResolver(addr),
		ConnOpts:  conn.ConnOptions{Dialer: newTLSDialer(srv)},
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4},
		Hooks:     hooks,
	})
	require.NoError(t, err, "NewClient with a managed transport")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do through the managed transport")
	require.Len(t, *events, 1, "one attempt must produce exactly one completion event")
	e := (*events)[0]
	assert.Equalf(t, trace.ProtoH2, e.Proto,
		"Proto = %v, want h2; the transport reports what it negotiated, which is the only answer TransportALPN can give either", e.Proto)
	assert.Equalf(t, addr.String(), e.RemoteAddr,
		"RemoteAddr = %q, want the backend the Selector picked (%q) — a managed client has no configured Addr to fall back on",
		e.RemoteAddr, addr.String())
}

// TestRequestComplete_ReportsHTTP11 is the sibling check on the other protocol
// a Client can be configured for, so Proto is read from the transport rather
// than hard-coded to the one the H2 tests happen to exercise.
func TestRequestComplete_ReportsHTTP11(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		Hooks:     hooks,
	})
	require.NoError(t, err, "NewClient with an HTTP/1.1 transport")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, &resp)

	require.NoError(t, err, "Do over HTTP/1.1")
	require.Len(t, *events, 1, "one attempt must produce exactly one completion event")
	e := (*events)[0]
	assert.Equalf(t, trace.ProtoH1, e.Proto, "Proto = %v, want h1", e.Proto)
	assert.Equalf(t, srv.Listener.Addr().String(), e.RemoteAddr, "RemoteAddr = %q, want the server's address", e.RemoteAddr)
	assert.Positivef(t, e.TTFB, "TTFB = %v; the HTTP/1.1 path reports a first byte like the others", e.TTFB)
	assert.Positivef(t, e.Connect,
		"Connect = %v on the request that dialled; HTTP/1.1 keeps its own copy of the dial timing, so it can go missing here while the other two protocols still report it",
		e.Connect)
}

// TestRequestComplete_FailedAttemptReportsNoFirstByte is the negative class.
// A request that never got a response head must report no TTFB at all rather
// than the moment it gave up — otherwise a run's TTFB percentiles quietly
// include its timeouts, which is the one number a load test must not blur.
func TestRequestComplete_FailedAttemptReportsNoFirstByte(t *testing.T) {
	t.Parallel()
	// A listener that is closed immediately: the port is real, so the dial
	// fails fast and deterministically rather than hanging on a firewall.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "reserve a port to point the client at")
	deadAddr := l.Addr().String()
	require.NoError(t, l.Close(), "close the listener before the client dials it")
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     deadAddr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient pointed at a dead address")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"}, &resp)

	require.Error(t, err, "Do against a closed port must fail")
	require.Len(t, *events, 1, "a failed attempt is still one attempt, and still reports")
	e := (*events)[0]
	assert.Zerof(t, e.TTFB, "TTFB = %v on a request that never received a response head", e.TTFB)
	assert.Zerof(t, e.Status, "Status = %d on a request that never received a response head", e.Status)
	assert.Positivef(t, e.Acquire,
		"Acquire = %v; the failed dial took time, and dropping it under-reports exactly the attempts worth investigating", e.Acquire)
	assert.Equalf(t, deadAddr, e.RemoteAddr,
		"RemoteAddr = %q on a failed attempt, want the address it failed against (%q)", e.RemoteAddr, deadAddr)
}

// TestRequestComplete_RetriedRequestNumbersItsAttempts proves the replay is
// reported as a replay. Without it, three completion events for one retried
// request are indistinguishable from three separate requests, and a run's
// request count silently disagrees with the server's.
func TestRequestComplete_RetriedRequestNumbersItsAttempts(t *testing.T) {
	t.Parallel()
	// The server refuses the first request at the HTTP/2 layer, which is a
	// retryable transport failure, then answers normally.
	var seen int
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		seen++
		if seen == 1 {
			// Panicking in an h2 handler makes the server reset the stream,
			// which the built-in classifier treats as retryable.
			panic(http.ErrAbortHandler)
		}
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	hooks, events := recordComplete()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	require.NoError(t, err, "NewClient against the flaky test server")
	defer c.Close()
	r := c.Retryer(client.RetryOptions{MaxAttempts: 3})
	var resp client.Response
	resp.Reset()

	err = r.Do(context.Background(), &client.Request{Method: "GET", Path: "/x"}, &resp)

	require.NoError(t, err, "the Retryer must succeed on the second attempt")
	require.Len(t, *events, 2, "one event per attempt: the refused try and the successful replay")
	assert.Equalf(t, 0, (*events)[0].Attempt, "first attempt reported as %d, want 0", (*events)[0].Attempt)
	assert.Equalf(t, 1, (*events)[1].Attempt,
		"replay reported as attempt %d, want 1 — an unnumbered replay is indistinguishable from a separate request",
		(*events)[1].Attempt)
}
