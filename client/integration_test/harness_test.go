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
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ServerKind identifies which HTTP/2 server implementation is under test.
type ServerKind int

const (
	ServerGoHTTP ServerKind = iota
	ServerNginx
	ServerUndertow
	ServerNghttp2
)

func (k ServerKind) String() string {
	switch k {
	case ServerGoHTTP:
		return "go-http"
	case ServerNginx:
		return "nginx"
	case ServerUndertow:
		return "undertow"
	case ServerNghttp2:
		return "nghttp2"
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

	h2s := &http2.Server{}
	h := h2c.NewHandler(mux, h2s)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("startGoReference: listen: %v", err))
	}

	goRefServer = &http.Server{
		Handler:     h,
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
		{ServerNghttp2, envOr("POSEIDON_IT_NGHTTP2_TLS", "127.0.0.1:18083"), envOr("POSEIDON_IT_NGHTTP2_H2C", "127.0.0.1:18084")},
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

	addr := srv.H2CAddr
	scheme := "http"
	var dialer conn.Dialer = &conn.PlaintextDialer{}

	if addr == "" {
		// TLS mode
		addr = srv.TLSAddr
		scheme = "https"
		dialer = &conn.TLSDialer{Config: tlsConfig()}
	}

	c, err := client.NewClient(client.ClientOptions{
		Addr:          addr,
		DefaultScheme: scheme,
		ConnOpts: conn.ConnOptions{
			Dialer: dialer,
		},
	})
	if err != nil {
		t.Fatalf("NewClient(%s): %v", srv.Kind, err)
	}

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

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     path,
		WantBody: wantBody,
	}, &resp)
	if err != nil {
		t.Fatalf("Do GET %s: %v", path, err)
	}
	return resp.Status, resp.Body
}

// tlsConfig returns a TLS config that skips cert verification (self-signed).
func tlsConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only, self-signed certs
		NextProtos:         []string{"h2"},
	}
}
