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

	// Run many iterations. Each time we pre-cancel the context and call Ping.
	// The Ping method writes the frame first (synchronous), then enters select.
	// ctx.Done() is already closed, so the select returns ctx.Err() unless the
	// ACK arrives in the tiny window between WritePing returning and select
	// executing. By running 50 iterations we verify it *can* return the right
	// error. Accept that on a very fast loopback some iterations may return nil
	// (ACK arrived first) — that's not a bug, just timing.
	// The invariant we test: when err != nil it must be context.DeadlineExceeded.
	gotCtxErr := false
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 0) // already expired
		_, err := c.Ping(ctx)
		cancel()
		if err == nil {
			continue // ACK arrived before select — OK
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("iteration %d: Ping = %v, want nil or context.DeadlineExceeded", i, err)
		}
		gotCtxErr = true
	}
	if !gotCtxErr {
		t.Fatal("never observed context.DeadlineExceeded in 50 iterations with pre-expired ctx")
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

func TestConn_Keepalive_HealthyConn(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// KeepaliveInterval set; server responds to PINGs normally.
	c := dialPingServer(t, srv, cfg, ConnOptions{KeepaliveInterval: 30 * time.Millisecond})

	// Wait 3 keepalive intervals; conn must remain alive.
	time.Sleep(100 * time.Millisecond)
	if !c.IsAlive() {
		t.Fatal("keepalive closed a healthy connection")
	}
}

func TestConn_Keepalive_ClosesDeadConn(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	c := dialPingServer(t, srv, cfg, ConnOptions{KeepaliveInterval: 50 * time.Millisecond})

	// Close server: kills the TCP connection. The TCP FIN causes
	// readerLoop to exit (closing readerDone), which wakes the
	// keepaliveLoop's readerDone case, calling c.Close(). Allow 3× interval.
	srv.Close()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !c.IsAlive() {
			return // test passes
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("conn still alive 200ms after server closed — keepalive did not detect dead conn")
}
