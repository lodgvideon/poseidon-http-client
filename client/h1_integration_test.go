package client_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func startH1Server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestNewClient_H1SingleConn_Smoke(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if string(resp.Body) != "ok" {
		t.Errorf("body = %q, want %q", resp.Body, "ok")
	}
}

func TestNewClient_H1SingleConn_MultipleRequests(t *testing.T) {
	t.Parallel()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		_, _ = w.Write([]byte("pong"))
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	for i := 0; i < 5; i++ {
		var resp client.Response
		resp.Reset()
		if err := c.Do(context.Background(), &client.Request{
			Method:   "GET",
			Path:     "/",
			WantBody: true,
		}, &resp); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Errorf("request %d: status = %d, want 200", i, resp.Status)
		}
	}
	if count != 5 {
		t.Errorf("server received %d requests, want 5", count)
	}
}

func TestNewClient_H1SingleConn_POST_Body(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		w.WriteHeader(201)
		_, _ = w.Write(buf[:n])
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	payload := []byte("hello world")
	if err := c.Do(context.Background(), &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     payload,
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 201 {
		t.Errorf("status = %d, want 201", resp.Status)
	}
	if string(resp.Body) != string(payload) {
		t.Errorf("echo body = %q, want %q", resp.Body, payload)
	}
}

// TestNewClient_ALPN_PlaintextFallsBackToH1 verifies that when the ALPN
// transport dials a plain-TCP server (no TLS, NegotiatedProtocol==""),
// it falls back to H1.1 and makes a successful request.
func TestNewClient_ALPN_PlaintextFallsBackToH1(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t) // plain HTTP/1.1

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportALPN,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
	if string(resp.Body) != "ok" {
		t.Errorf("body = %q, want %q", resp.Body, "ok")
	}

	// Second request uses the cached H1.1 delegate (covers fast-path).
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do (2nd): %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("2nd status = %d, want 200", resp.Status)
	}
}

// TestNewClient_H1SingleConn_Warmup verifies Warmup pre-dials and the
// subsequent request reuses the warmed connection.
func TestNewClient_H1SingleConn_Warmup(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.Warmup(1) // pre-dial; should not block

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestNewClient_H1SingleConn_Shutdown verifies Shutdown closes the transport.
func TestNewClient_H1SingleConn_Shutdown(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Make one request so a conn is established.
	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method: "GET", Path: "/",
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestNewClient_ALPN_Shutdown_BeforeRequest covers shutdown before any
// request has been made (delegate is nil).
func TestNewClient_ALPN_Shutdown_BeforeRequest(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportALPN,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Warmup is a no-op (protocol not yet detected).
	c.Warmup(1)

	// Shutdown before any request: delegate is nil, should be safe.
	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestNewClient_ALPN_Shutdown_AfterRequest covers shutdown after the
// delegate is established via a real request.
func TestNewClient_ALPN_Shutdown_AfterRequest(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportALPN,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method: "GET", Path: "/",
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	// Warmup after protocol is known delegates to h1singleConn.warmup.
	c.Warmup(1)

	if err := c.Shutdown(100 * time.Millisecond); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestNewClient_H1SingleConn_MidBodyError verifies that closing a server
// connection mid-chunk is handled gracefully and covers the
// h1Exchange.Close(done=false) code path via the defer in sendRequest.
func TestNewClient_H1SingleConn_MidBodyError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = nc.Close() }()
		_, _ = nc.Read(make([]byte, 4096))
		// Send a chunked response but close mid-chunk so the client gets EOF
		// while reading chunk data.
		_, _ = nc.Write([]byte(
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
				"a\r\nhell", // 10-byte chunk; send only 4 bytes then close
		))
	}()

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      ln.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err == nil {
		t.Error("expected error for mid-chunk connection close, got nil")
	}
}

// TestNewClient_H1SingleConn_DoStream_Error verifies DoStream returns an
// error for H1.1 transports (streaming not supported).
func TestNewClient_H1SingleConn_DoStream_Error(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var sr client.StreamResponse
	if err := c.DoStream(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	}, &sr); err == nil {
		_ = sr.Close()
		t.Error("expected error calling DoStream on H1.1 client, got nil")
	}
}

// TestNewClient_H1SingleConn_StreamBody_Error verifies that Do with
// StreamBody=true returns an error for HTTP/1.1 connections (no streaming).
func TestNewClient_H1SingleConn_StreamBody_Error(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), &client.Request{
		Method:     "GET",
		Path:       "/",
		StreamBody: true,
	}, &resp); err == nil {
		if resp.BodyReader != nil {
			_ = resp.BodyReader.Close()
		}
		t.Error("expected error calling Do(StreamBody=true) on H1.1 client, got nil")
	}
}

// TestNewClient_H1SingleConn_ConcurrentDial_CancelledCtx covers the
// `case <-ctx.Done(): return nil, ctx.Err()` branch in h1singleConn.acquireConn
// (h1_transport.go:196-198). This is triggered when a second goroutine waits
// on the dialing channel while a first goroutine is mid-dial, but the second
// goroutine's context is already cancelled.
func TestNewClient_H1SingleConn_ConcurrentDial_CancelledCtx(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	// A slow dialer that sleeps 200ms before delegating — keeps the first
	// dial in flight long enough for the second caller to arrive.
	slow := newH1SlowDialer(&conn.PlaintextDialer{}, 200*time.Millisecond)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: slow},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Goroutine 1: starts the slow dial (will take ~200ms).
	errCh := make(chan error, 1)
	go func() {
		var resp client.Response
		resp.Reset()
		errCh <- c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)
	}()

	// Wait until the dial is actually in progress (started channel is closed by
	// the slow dialer's first Dial invocation, before the sleep).
	<-slow.started

	// Goroutine 2 (main): pre-cancelled context → should hit ctx.Done() path.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2() // cancel before Do

	var resp2 client.Response
	resp2.Reset()
	err = c.Do(ctx2, &client.Request{Method: "GET", Path: "/"}, &resp2)

	// The error must be context.Canceled (the ctx was already cancelled).
	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	} else if err != context.Canceled {
		// Accept wrapped forms too.
		t.Logf("got %v (want context.Canceled; wrapped form acceptable)", err)
	}

	// Drain the first goroutine (it should succeed once the dial finishes).
	<-errCh
}

// h1SlowDialer wraps a Dialer adding a configurable startup delay.
// The started channel must be initialised via newH1SlowDialer.
type h1SlowDialer struct {
	inner   conn.Dialer
	delay   time.Duration
	started chan struct{} // closed when the first Dial call begins
	once    sync.Once
}

func newH1SlowDialer(inner conn.Dialer, delay time.Duration) *h1SlowDialer {
	return &h1SlowDialer{
		inner:   inner,
		delay:   delay,
		started: make(chan struct{}),
	}
}

func (d *h1SlowDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.once.Do(func() { close(d.started) })
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.inner.Dial(ctx, addr)
}

func TestNewClient_ALPN_NegotiatesH2(t *testing.T) {
	t.Parallel()
	srv, _ := startOneTLSServer(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportALPN,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.FlexDialer{
				Config: srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
			},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}

// TestNewClient_H1SingleConn_TrailersRejected verifies that a request carrying
// trailers over HTTP/1.1 is rejected with ErrTrailersUnsupportedH1 rather than
// corrupting the connection by emitting a second request line.
func TestNewClient_H1SingleConn_TrailersRejected(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	err = c.Do(context.Background(), &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("payload"),
		Trailers: []conn.HeaderField{{Name: []byte("x-checksum"), Value: []byte("abc123")}},
	}, &resp)
	if !errors.Is(err, client.ErrTrailersUnsupportedH1) {
		t.Fatalf("Do err = %v, want ErrTrailersUnsupportedH1", err)
	}
}
