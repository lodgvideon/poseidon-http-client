//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/require"
)

// ServerKind identifies which HTTP/2 server implementation is under test.
type ServerKind int

// New kinds go at the END: allReadyServers ranges from ServerGoHTTP to the last
// kind, so the order is load-bearing and a kind inserted in the middle would
// silently drop the ones after it from every matrix test.
const (
	ServerGoHTTP ServerKind = iota
	ServerNginx
	ServerUndertow
	ServerNghttpx

	serverKindCount // must stay last; bounds the matrix range
)

func (k ServerKind) String() string {
	switch k {
	case ServerGoHTTP:
		return "go-http"
	case ServerNginx:
		return "nginx"
	case ServerUndertow:
		return "undertow"
	case ServerNghttpx:
		return "nghttpx"
	default:
		return "unknown"
	}
}

// TestServer holds connection details for one HTTP/2 server under test.
type TestServer struct {
	Kind    ServerKind
	TLSAddr string // host:port for h2 (TLS+ALPN)
	H2CAddr string // host:port for h2c (cleartext prior-knowledge); empty if unsupported
	Ready   bool   // true if healthcheck passed
}

// allServers is populated by TestMain. Key = ServerKind.
var allServers = make(map[ServerKind]*TestServer)

// skipRemote is true when POSEIDON_IT_SKIP_REMOTE=true (e.g. make it-test-fast).
var skipRemote bool

// goRefURL is the in-process Go reference server base URL (set in TestMain).
var goRefURL string

// ── TestMain: discovery + warmup ──────────────────────────────────

var goRefServer *http.Server

func TestMain(m *testing.M) {
	skipRemote = os.Getenv("POSEIDON_IT_SKIP_REMOTE") == "true"

	// Always start the in-process Go reference server.
	startGoReference()

	if !skipRemote {
		discoverRemoteServers()
	}

	code := m.Run()
	shutdownGoReference()
	os.Exit(code)
}

// startGoReference launches an in-process h2c (HTTP/2 cleartext) server.
func startGoReference() {
	mux := http.NewServeMux()
	registerFixtures(mux)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("startGoReference: listen: %v", err))
	}

	// HTTP/1.1 and cleartext HTTP/2 on one listener, as x/net's h2c.NewHandler
	// used to do for us. That package is deprecated in favour of this field.
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	goRefServer = &http.Server{
		Handler:     mux,
		Protocols:   protos,
		IdleTimeout: 60 * time.Second,
	}
	goRefURL = "http://" + ln.Addr().String()

	go func() { _ = goRefServer.Serve(ln) }()

	allServers[ServerGoHTTP] = &TestServer{
		Kind:    ServerGoHTTP,
		H2CAddr: ln.Addr().String(),
		Ready:   true,
	}
}

// discoverRemoteServers pings each Docker service via TCP healthcheck.
func discoverRemoteServers() {
	type srvDef struct {
		kind    ServerKind
		tlsAddr string
		h2cAddr string
	}
	defs := []srvDef{
		{ServerNginx, envOr("POSEIDON_IT_NGINX_TLS", "127.0.0.1:18080"), ""},
		{ServerUndertow, envOr("POSEIDON_IT_UNDERTOW_TLS", "127.0.0.1:18081"), envOr("POSEIDON_IT_UNDERTOW_H2C", "127.0.0.1:18082")},
		{ServerNghttpx, envOr("POSEIDON_IT_NGHTTPX_TLS", "127.0.0.1:18083"), envOr("POSEIDON_IT_NGHTTPX_H2C", "127.0.0.1:18084")},
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, d := range defs {
		wg.Add(1)
		go func(d srvDef) {
			defer wg.Done()
			ready := waitReady(d.h2cAddr, d.tlsAddr, 20*time.Second)
			mu.Lock()
			allServers[d.kind] = &TestServer{
				Kind:    d.kind,
				TLSAddr: d.tlsAddr,
				H2CAddr: d.h2cAddr,
				Ready:   ready,
			}
			mu.Unlock()
		}(d)
	}
	wg.Wait()
}

// waitReady tries a TCP dial on the h2c port (or TLS port) with exponential backoff.
func waitReady(h2cAddr, tlsAddr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := h2cAddr
	if addr == "" {
		addr = tlsAddr
	}
	backoff := 100 * time.Millisecond
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(backoff)
		if backoff < 1*time.Second {
			backoff *= 2
		}
	}
	return false
}

// requireServer skips the test if the named server is not ready.
func requireServer(t *testing.T, kind ServerKind) *TestServer {
	t.Helper()
	srv, ok := allServers[kind]
	if !ok || !srv.Ready {
		t.Skipf("server %s not available (not running or healthcheck failed)", kind)
	}
	return srv
}

// envOr returns os.Getenv(key) or fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// shutdownGoReference gracefully stops the in-process server.
func shutdownGoReference() {
	if goRefServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = goRefServer.Shutdown(ctx)
	}
}

// newTestClient creates a poseidon *client.Client connected to srv.
// Uses PlaintextDialer for h2c, TLSDialer (InsecureSkipVerify) for h2.
// The client is automatically closed via t.Cleanup.
func newTestClient(t *testing.T, srv *TestServer) *client.Client {
	t.Helper()
	addr, scheme, dialer := preferredLeg(srv)
	return newTestClientAt(t, srv, addr, scheme, dialer)
}

// preferredLeg is newTestClient's target selection, split out so the counting
// variant below aims at exactly the same peer address rather than a second
// spelling of the same rule that could drift from it.
func preferredLeg(srv *TestServer) (addr, scheme string, dialer conn.Dialer) {
	addr = srv.H2CAddr
	scheme = "http"
	dialer = &conn.PlaintextDialer{}
	if addr == "" {
		// TLS mode
		addr = srv.TLSAddr
		scheme = "https"
		dialer = &conn.TLSDialer{Config: tlsConfig()}
	}
	return addr, scheme, dialer
}

// newCountingTestClient is newTestClient with a completed-dial counter attached.
//
// It exists because "the connection was reused" is not observable from a
// response: every reuse test in this suite asserted that N sequential requests
// each came back 200, and a transport that dials a brand-new connection for
// every single request satisfies that exactly as well as one that reuses a
// single connection (#893). Counting dials is what tells the two apart, and it
// is the same instrument toxiproxy_test.go already uses for its pooled fixtures.
//
// Only successful dials are counted. A failed dial establishes no connection, so
// including it would make the count answer a different question than the one
// every caller of this helper asks — how many connections did this client end up
// with.
func newCountingTestClient(t *testing.T, srv *TestServer, dials *atomic.Int64) *client.Client {
	t.Helper()
	addr, scheme, dialer := preferredLeg(srv)
	// OnDial fires on the dialling goroutine, so the counter is atomic.
	return newTestClientAt(t, srv, addr, scheme, dialer,
		&client.Hooks{OnDial: func(ev client.DialEvent) {
			if ev.Err == nil {
				dials.Add(1)
			}
		}})
}

// newTestClientTLS is newTestClient pinned to the TLS leg.
//
// newTestClient prefers h2c whenever a peer offers it, so for every dual-mode
// peer — Undertow and nghttpx — the matrix would otherwise never negotiate ALPN
// at all. nginx, having no h2c port, was the only peer whose TLS path was
// exercised. Returns nil when srv has no TLS address.
func newTestClientTLS(t *testing.T, srv *TestServer) *client.Client {
	t.Helper()
	if srv.TLSAddr == "" {
		return nil
	}
	return newTestClientAt(t, srv, srv.TLSAddr, "https", &conn.TLSDialer{Config: tlsConfig()})
}

func newTestClientAt(t *testing.T, srv *TestServer, addr, scheme string, dialer conn.Dialer, hooks ...*client.Hooks) *client.Client {
	t.Helper()

	var h *client.Hooks
	if len(hooks) > 0 {
		h = hooks[0]
	}
	c, err := client.NewClient(client.ClientOptions{
		Addr:          addr,
		DefaultScheme: scheme,
		ConnOpts: conn.ConnOptions{
			Dialer:            dialer,
			StreamEventBuffer: 1024, // avoid event-channel overflow on large bodies
		},
		Hooks: h,
	})
	require.NoErrorf(t, err, "NewClient(%s): %v", srv.Kind, err)

	t.Cleanup(func() {
		_ = c.Close()
	})
	return c
}

// doGET is a convenience wrapper: sends GET, returns status + body.
func doGET(t *testing.T, c *client.Client, path string, wantBody bool) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mode := client.BodyDiscard
	if wantBody {
		mode = client.BodyBuffer
	}
	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     path,
		BodyMode: mode,
	}, &resp)
	require.NoErrorf(t, err, "Do GET %s: %v", path, err)
	return resp.Status, resp.Body
}

// tlsConfig returns a TLS config that skips cert verification (self-signed).
func tlsConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	}
}
