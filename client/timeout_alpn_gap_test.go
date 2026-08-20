package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ————————————————————————————————————————————————————————————————
// Two equivalence classes the deadline and ALPN suites never build (#871,
// #879): a dial that completes the TCP handshake and then stalls in TLS, and a
// deadline that has to bind while the request BODY is going out. Both are the
// black holes an operator actually meets — a load balancer that accepts and
// never speaks, and a peer that stops reading.
// ————————————————————————————————————————————————————————————————

// startSilentTLSListener accepts TCP connections and never speaks TLS. The
// accepted conns are held, not closed, so the client's handshake blocks reading
// a ServerHello that never comes — the phase every hangingDialer fixture in
// dialtimeout_test.go skips over, because hangingDialer blocks BEFORE returning
// a conn and so only ever models the TCP-connect phase.
func startSilentTLSListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	held := make(chan net.Conn, 8)
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			select {
			case held <- c:
			default:
				_ = c.Close()
			}
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		close(held)
		for c := range held {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestSingleConn_DialTimeoutBindsDuringTheTLSHandshake is the equivalence class
// dialtimeout_test.go leaves out (#871).
//
// Every fixture there uses hangingDialer, which blocks before returning a conn —
// the TCP-connect phase. Whether the dial context's deadline reaches
// tls.Client(...).HandshakeContext(dctx) is a separate question, and a peer that
// completes the TCP handshake and then says nothing is the commoner real-world
// black hole.
func TestSingleConn_DialTimeoutBindsDuringTheTLSHandshake(t *testing.T) {
	addr := startSilentTLSListener(t)
	s := &singleConn{
		addr: addr,
		connOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{
			Config: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		}},
		metrics:     &Metrics{},
		dialTimeout: 150 * time.Millisecond,
	}
	// Far longer than the dial timeout: if the bound does not reach the TLS
	// handshake, this is what the dial waits for instead.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, _, _, err := s.acquireConn(ctx)
	elapsed := time.Since(start)

	require.Error(t, err,
		"acquireConn against a peer that accepts TCP and never speaks TLS returned no error")
	assert.NoErrorf(t, ctx.Err(),
		"the caller's context expired after %v, so this measured the caller's deadline "+
			"rather than DialTimeout", elapsed)
	assert.Lessf(t, elapsed, 5*time.Second,
		"the dial ran for %v against a 150ms DialTimeout — the deadline does not reach "+
			"HandshakeContext, so a load balancer that accepts and never speaks holds Do "+
			"open for as long as the caller allows", elapsed)
}

// TestH1SingleConn_DialTimeoutBindsDuringTheTLSHandshake is the HTTP/1.1 twin.
// The three transports dial through different code, and a bound present on one
// and absent on another is this repo's most productive bug shape.
func TestH1SingleConn_DialTimeoutBindsDuringTheTLSHandshake(t *testing.T) {
	addr := startSilentTLSListener(t)
	s := &h1singleConn{
		addr: addr,
		dialer: &conn.H1TLSDialer{
			Config: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
		},
		metrics:     &Metrics{},
		dialTimeout: 150 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, _, _, _, err := s.openExchange(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "openExchange against a silent TLS peer returned no error")
	assert.NoErrorf(t, ctx.Err(), "the caller's context expired after %v", elapsed)
	assert.Lessf(t, elapsed, 5*time.Second,
		"the HTTP/1.1 dial ran for %v against a 150ms DialTimeout; the H2 sibling bounds "+
			"the same phase", elapsed)
}

// TestNewClient_H2ManagedTransportRejectsH1Dialer closes the one cell the
// construction-time ALPN check leaves untested (#871).
//
// TestNewClient_H2TransportRejectsH1Dialer iterates TransportSingleConn and
// TransportPool; its HTTP/1.1 sibling covers all three H1 transports. The
// managed transport builds its pools behind a resolver, so its constructor is a
// different path — and it is the one a load generator against a service name
// actually uses.
func TestNewClient_H2ManagedTransportRejectsH1Dialer(t *testing.T) {
	t.Parallel()
	h1Dialer := &conn.H1TLSDialer{Config: &tls.Config{
		ServerName: "example.com",
		MinVersion: tls.VersionTLS12,
	}}

	c, err := NewClient(ClientOptions{
		Transport: TransportManaged,
		Resolver:  StaticResolver(Address{Host: "example.com", Port: 443}),
		ConnOpts:  conn.ConnOptions{Dialer: h1Dialer},
		Pool:      &PoolOptions{MaxConnsPerHost: 2},
	})

	if err == nil {
		_ = c.Close()
	}
	require.Error(t, err,
		"the managed HTTP/2 transport accepted a dialer that asserts http/1.1; it would "+
			"write an HTTP/2 preface into an HTTP/1.1 connection on the first request")
	assert.ErrorIsf(t, err, ErrALPNProtocolMismatch,
		"refusal = %v, want ErrALPNProtocolMismatch — the two single-conn transports "+
			"classify it that way and a caller cannot branch on an unclassified error", err)
}

// startH11OnlyTLSServer offers ONLY http/1.1 over ALPN.
func startH11OnlyTLSServer(t *testing.T) (addr string, clientCfg *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.TLS = &tls.Config{NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.NextProtos = nil
	return srv.Listener.Addr().String(), cfg
}

// TestSingleConn_UnassertedDialerAgainstAnH11OnlyOrigin pins what the HTTP/2
// transport does today in the fourth cell of #871's table, which is the mirror
// of assertH1Conn and has no assertH2Conn behind it.
//
// This test PINS the present behaviour rather than deciding the design: the
// request must fail rather than silently mis-frame, and it must fail promptly.
// It deliberately does NOT require ErrALPNProtocolMismatch, because there is no
// dial-time assertion on this side — adding one is a source change and a
// maintainer's call, left on #871. If an assertH2Conn is ever added, this test
// keeps passing and gains a sharper sibling.
func TestSingleConn_UnassertedDialerAgainstAnH11OnlyOrigin(t *testing.T) {
	addr, cfg := startH11OnlyTLSServer(t)
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportSingleConn,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.FlexDialer{Config: cfg}},
	})
	require.NoError(t, err,
		"a non-asserting dialer is accepted at construction by design; the protocol is "+
			"only known once ALPN has run")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := &Response{}

	start := time.Now()
	err = c.Do(ctx, GET("/"), resp)
	elapsed := time.Since(start)

	t.Logf("H2 transport against an http/1.1-only origin: err=%v after %v", err, elapsed)
	require.Error(t, err,
		"the HTTP/2 transport reported success against an origin that negotiated "+
			"http/1.1; it wrote an HTTP/2 preface into an HTTP/1.1 connection")
	assert.NoErrorf(t, ctx.Err(),
		"the caller's context expired after %v, so the failure is a hang rather than a "+
			"refusal — an operator sees a stall, not an error", elapsed)
}

// TestRequest_Timeout_BindsDuringTheRequestBodySend is #879's send-phase class.
//
// timeout_test.go covers one shape: a handler that stalls before writing the
// response header, i.e. the RECEIVE phase. The send phase unblocks through a
// different mechanism entirely — writeData blocks in acquireSendCredits on
// fcOutCond and is woken by a short-lived watchdog goroutine on ctx cancel — so
// nothing showed that Request.Timeout reaches it.
//
// The body is oversized well past the peer's upload buffer on purpose: that is
// what makes the send block deterministically rather than racily, so the
// deadline has something to bind against.
func TestRequest_Timeout_BindsDuringTheRequestBodySend(t *testing.T) {
	const timeout = 150 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release // never read the request body
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(func() { close(release); srv.Close() })
	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	// 8 MiB: far past any peer's initial receive window, so acquireSendCredits
	// blocks and the deadline is the only thing that can end the request.
	body := make([]byte, 8<<20)
	outer, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var resp Response

	start := time.Now()
	err = c.Do(outer, &Request{
		Method: "POST", Path: "/", Body: body, Timeout: timeout,
	}, &resp)
	elapsed := time.Since(start)

	t.Logf("send-phase timeout: body=%d bytes, Request.Timeout=%v, Do returned after %v (err=%v)",
		len(body), timeout, elapsed, err)
	select {
	case <-started:
	default:
		require.FailNow(t, "the handler never ran",
			"the request never reached the peer, so the send phase was not where the "+
				"deadline bound and this measured something else")
	}
	require.Error(t, err, "a request whose body the peer never reads must not report success")
	assert.Truef(t, errors.Is(err, context.DeadlineExceeded),
		"err = %v, want context.DeadlineExceeded — the send path is woken by a different "+
			"mechanism from the receive path (fcOutCond plus a watchdog, not a stream "+
			"event), so a caller classifying deadlines needs it to arrive as one here too", err)
	assert.NoErrorf(t, outer.Err(),
		"the outer 20s context expired after %v, so Request.Timeout did not bind during "+
			"the send and the request rode the caller's deadline instead", elapsed)
	assert.Lessf(t, elapsed, 10*time.Second,
		"the send blocked for %v against a %v Request.Timeout", elapsed, timeout)
}

// TestRequest_Timeout_BindsDuringTheDial is #879's dial-phase class: the
// deadline has to bind before there is a stream at all.
func TestRequest_Timeout_BindsDuringTheDial(t *testing.T) {
	const timeout = 150 * time.Millisecond
	addr := startSilentTLSListener(t)
	c, err := NewClient(ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		DialTimeout: 20 * time.Second, // out of reach: Request.Timeout must be the bound
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	outer, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var resp Response

	start := time.Now()
	err = c.Do(outer, &Request{Method: "GET", Path: "/", Timeout: timeout}, &resp)
	elapsed := time.Since(start)

	t.Logf("dial-phase timeout: DialTimeout=20s, Request.Timeout=%v, Do returned after %v (err=%v)",
		timeout, elapsed, err)
	require.Error(t, err, "Do against a peer that never speaks TLS reported success")
	assert.Truef(t, errors.Is(err, context.DeadlineExceeded),
		"err = %v, want context.DeadlineExceeded — a per-request deadline that expires "+
			"while the transport is still dialling must still be reported as a deadline", err)
	assert.Lessf(t, elapsed, 10*time.Second,
		"the dial ran for %v against a %v Request.Timeout, so the request is bounded by "+
			"DialTimeout or the caller's context rather than by its own deadline",
		elapsed, timeout)
}
