package client_test

import (
	"context"
	"io"
	"net"
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do over the HTTP/1.1 single-conn transport")
	assert.Equal(t, 200, resp.Status, "the origin answered 200; anything else is a framing failure")
	assert.Equal(t, "ok", string(resp.Body), "the body must round-trip unchanged")
}

func TestNewClient_H1SingleConn_MultipleRequests(t *testing.T) {
	t.Parallel()
	// atomic, not a plain int: the handler runs on the server's goroutine and the
	// assertion reads from the test's, with no happens-before edge between them.
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		_, _ = w.Write([]byte("pong"))
	}))
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	for i := 0; i < 5; i++ {
		var resp client.Response
		resp.Reset()

		derr := c.Do(context.Background(), &client.Request{
			Method:   "GET",
			Path:     "/",
			BodyMode: client.BodyBuffer,
		}, &resp)

		require.NoErrorf(t, derr, "request %d", i)
		assert.Equalf(t, 200, resp.Status, "request %d", i)
	}

	assert.EqualValues(t, 5, count.Load(),
		"the origin must see every request; a smaller count means an exchange never "+
			"reached the wire")
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()
	payload := []byte("hello world")

	err = c.Do(context.Background(), &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     payload,
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do(POST)")
	assert.Equal(t, 201, resp.Status, "the handler answers 201 only when it saw a POST")
	assert.Equal(t, string(payload), string(resp.Body),
		"the echoed body must match what was sent, or the request body was mis-framed")
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do over a plaintext origin through the ALPN transport")
	assert.Equal(t, 200, resp.Status, "plaintext must fall back to HTTP/1.1, not fail")
	assert.Equal(t, "ok", string(resp.Body), "the fallback path must deliver the body")

	// Second request uses the cached H1.1 delegate (covers the fast path).
	resp.Reset()
	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do (2nd) — the cached HTTP/1.1 delegate must be reusable")
	assert.Equal(t, 200, resp.Status, "2nd request through the cached delegate")
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	c.Warmup(1) // pre-dial; should not block
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoError(t, err, "Do after Warmup")
	assert.Equal(t, 200, resp.Status, "a warmed connection must still serve a request")
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
	require.NoError(t, err, "NewClient")
	// Make one request so a conn is established.
	var resp client.Response
	resp.Reset()
	require.NoError(t, c.Do(context.Background(), &client.Request{
		Method: "GET", Path: "/",
	}, &resp), "Do")

	serr := c.Shutdown(100 * time.Millisecond)

	require.NoError(t, serr, "Shutdown must close an established HTTP/1.1 transport cleanly")
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
	require.NoError(t, err, "NewClient")
	// Warmup is a no-op (protocol not yet detected).
	c.Warmup(1)

	// Shutdown before any request: the delegate is nil and must stay safe.
	serr := c.Shutdown(100 * time.Millisecond)

	require.NoError(t, serr,
		"Shutdown before the ALPN delegate exists must be a no-op, not a nil dereference")
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
	require.NoError(t, err, "NewClient")
	var resp client.Response
	resp.Reset()
	require.NoError(t, c.Do(context.Background(), &client.Request{
		Method: "GET", Path: "/",
	}, &resp), "Do")
	// Warmup after the protocol is known delegates to h1singleConn.warmup.
	c.Warmup(1)

	serr := c.Shutdown(100 * time.Millisecond)

	require.NoError(t, serr, "Shutdown after the ALPN delegate is established")
}

// TestNewClient_H1SingleConn_MidBodyError verifies that closing a server
// connection mid-chunk is handled gracefully and covers the
// h1Exchange.Close(done=false) code path via the defer in sendRequest.
func TestNewClient_H1SingleConn_MidBodyError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	derr := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	assert.Error(t, derr,
		"a connection closed mid-chunk leaves the body incomplete; reporting success "+
			"would hand the caller a truncated body as if it were whole")
}

// TestNewClient_H1SingleConn_DoStream verifies DoStream works over H1.1 and
// delivers the response head plus body events. Its former name ended in _Error:
// it asserted the rejection that beginRespStream's dispatch used to produce.
func TestNewClient_H1SingleConn_DoStream(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var sr client.StreamResponse

	err = c.DoStream(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	}, &sr)

	require.NoError(t, err, "DoStream on an H1.1 client")
	defer func() { _ = sr.Close() }()
	require.Equal(t, 200, sr.Status, "the response head must arrive before any body event")
	var sawData bool
	for {
		ev, rerr := sr.Recv(context.Background())
		if rerr != nil {
			break
		}
		if ev.Type == client.EventData && len(ev.Data) > 0 {
			sawData = true
		}
		if ev.EndStream {
			break
		}
	}
	assert.True(t, sawData,
		"DoStream delivered no DATA event; the origin wrote a body, so an empty event "+
			"stream means the body never reached the caller")
}

// TestNewClient_H1SingleConn_BodyStream verifies that Do with BodyStream
// succeeds over HTTP/1.1 and hands back a readable body.
//
// It asserted the opposite until h1Exchange was added to beginRespStream's
// dispatch: the rejection was never a property of the H1 code, which has always
// read one chunk per Recv, only of that switch.
func TestNewClient_H1SingleConn_BodyStream(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response

	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoError(t, err, "Do(BodyStream) on an H1.1 client")
	require.NotNil(t, resp.BodyReader, "Do(BodyStream) returned no BodyReader")
	defer func() { _ = resp.BodyReader.Close() }()
	body, rerr := io.ReadAll(resp.BodyReader)
	require.NoError(t, rerr, "read streamed H1 body")
	assert.Equal(t, "ok", string(body),
		"the streamed body must match what the origin wrote, not an empty read")
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
	require.NoError(t, err, "NewClient")
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

	// The error must be context.Canceled, and that is ASSERTED rather than logged.
	// Until this test was migrated the type check was an else-if branch whose only
	// statement was t.Logf, so the test passed on ANY error — including one from a
	// path that never reached the `case <-ctx.Done()` branch it exists to cover.
	require.Error(t, err, "expected error from cancelled context, got nil")
	assert.ErrorIsf(t, err, context.Canceled,
		"acquireConn returned %v; a second caller waiting on an in-flight dial with an "+
			"already-cancelled context must observe its own cancellation, and any other "+
			"error means the ctx.Done() branch is not what produced this", err)

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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{
		Method: "GET",
		Path:   "/",
	}, &resp)

	require.NoError(t, err, "Do over an h2-negotiated ALPN connection")
	assert.Equal(t, 200, resp.Status, "ALPN must hand the exchange to the HTTP/2 delegate")
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
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var resp client.Response
	resp.Reset()

	err = c.Do(context.Background(), &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("payload"),
		Trailers: []conn.HeaderField{{Name: []byte("x-checksum"), Value: []byte("abc123")}},
	}, &resp)

	require.ErrorIs(t, err, client.ErrTrailersUnsupportedH1,
		"HTTP/1.1 has no trailer frame; a request carrying trailers must be refused "+
			"rather than emitting a second request line into the same connection")
}
