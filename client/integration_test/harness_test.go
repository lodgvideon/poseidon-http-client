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

// startGoReference launches an in-process HTTP/2 server with all test fixtures.
func startGoReference() {
	mux := http.NewServeMux()
	registerFixtures(mux)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("startGoReference: listen: %v", err))
	}

	goRefServer = &http.Server{
		Handler:     mux,
		IdleTimeout: 60 * time.Second,
	}
	// Enable HTTP/2 (using crypto/tls with self-signed cert is complex here;
	// for h2c we use the plaintext HTTP/2 prior-knowledge path).
	// For now, we register as an h2c reference via the client's PlaintextDialer.
	goRefURL = "http://" + ln.Addr().String()

	// Use golang.org/x/net/http2/h2c for cleartext h2.
	// We avoid the import here and let the client's PlaintextDialer do h2c.
	// The Go server runs plain HTTP/1.1; the h2c layer is added in the
	// full harness (commit 2). For commit 1 (infra), we just need it to boot.
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

// tlsConfig returns a TLS config that skips cert verification (self-signed).
// This is intentional: we test HTTP/2 protocol behavior, not PKI.
func tlsConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only, self-signed certs
		NextProtos:         []string{"h2"},
	}
}
