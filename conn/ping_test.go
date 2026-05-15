package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// startPingServer is identical to startH2TestServer in integration_test.go.
// Duplicated here to keep this file self-contained.
func startPingServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialPingServer(t *testing.T, srv *httptest.Server, cfg *tls.Config, opts ConnOptions) *Conn {
	t.Helper()
	opts.Dialer = &TLSDialer{Config: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestConn_Ping_RTT(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rtt, err := c.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if rtt <= 0 {
		t.Errorf("RTT = %v, want > 0", rtt)
	}
	if rtt >= time.Second {
		t.Errorf("RTT = %v, want < 1s (loopback server)", rtt)
	}
}

func TestConn_Ping_ConcurrentSafe(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 20
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Ping(ctx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Ping = %v", i, err)
		}
	}
}

func TestConn_Ping_CtxCancelledBeforeACK(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	// context.WithTimeout(..., 0) creates an already-expired context.
	// ctx.Done() is closed before Ping enters the select, so only that
	// branch fires. The ACK arrives later (after network round-trip).
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	_, err := c.Ping(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ping with expired ctx = %v, want context.DeadlineExceeded", err)
	}
}

func TestConn_Ping_AfterClose(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})
	_ = c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Ping(ctx)
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("Ping after Close = %v, want ErrConnClosed", err)
	}
}
