package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

func newTLSH2Server(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.StartTLS()
	t.Cleanup(s.Close)
	addr := strings.TrimPrefix(s.URL, "https://")
	return s, addr
}

// doWithRetry issues c.Do with a bounded retry loop on RST_STREAM
// (INTERNAL_ERROR or REFUSED_STREAM). The bare c.Do path does not
// retry by design — see docs/RFC_COVERAGE.md §7 — but a test that
// merely exercises c.Do does not need to expose transient
// server-side noise (httptest frequently emits RST(2) when a
// sibling t.Parallel() test closes its server). This helper
// surfaces the SAME behavior the production code recommends via
// client.NewRetryer. Tests that explicitly validate non-retry
// semantics should call c.Do directly instead.
func doWithRetry(t *testing.T, c *client.Client, ctx context.Context, req *client.Request, resp *client.Response) error {
	t.Helper()
	const maxAttempts = 5
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = c.Do(ctx, req, resp)
		if err == nil {
			return nil
		}
		// Retry on transient httptest noise: RST_STREAM and connection-closed
		// errors that occur when a sibling test's server shuts down.
		var sre *client.StreamResetError
		if errors.As(err, &sre) {
			if sre.Code != frame.ErrCodeInternalError &&
				sre.Code != frame.ErrCodeRefusedStream {
				return err
			}
		} else if !errors.Is(err, conn.ErrConnClosed) {
			return err
		}
		// Transient. Reset response and back off briefly.
		resp.Reset()
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func clientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestIntegration_Client_GET_Status200(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "response status")
}

func TestIntegration_Client_POST_EchoBody(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	c := clientFor(t, addr)
	want := []byte("hello integration")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method: "POST", Path: "/echo",
		Body:     want,
		BodyMode: client.BodyBuffer,
	}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "response status")
	assert.Truef(t, bytes.Equal(res.Body, want),
		"echoed body = %q, want %q: the request body did not survive the round trip",
		res.Body, want)
}

func TestIntegration_Client_POST_LargeBody_ChunkedUpload(t *testing.T) {
	want := bytes.Repeat([]byte("ab"), 10000) // 20 KiB, multi-frame
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		BodyMode:   client.BodyBuffer,
	}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "response status")
	assert.Lenf(t, res.Body, len(want),
		"echoed body length = %d, want %d: a multi-frame upload lost or duplicated data",
		len(res.Body), len(want))
	assert.True(t, bytes.Equal(res.Body, want), "echoed body content differs from what was sent")
}

func TestIntegration_Client_ConcurrentRequests_OneClient(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)
	const N = 32
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var res client.Response
			if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res); err != nil {
				errCh <- err
				return
			}
			if res.Status != 200 {
				errCh <- fmt.Errorf("status=%d", res.Status)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err,
			"a concurrent request failed: one HTTP/2 connection must multiplex all 32")
	}
}

func TestIntegration_ClientPool_ConcurrentRequests_MultipleConns(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   3,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: time.Second,
			DialBackoff:       50 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	const N = 24

	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var res client.Response
			if derr := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res); derr != nil {
				errs <- derr
				return
			}
			if res.Status != 200 {
				errs <- fmt.Errorf("status = %d", res.Status)
			}
		}()
	}
	wg.Wait()
	close(errs)

	for derr := range errs {
		assert.NoError(t, derr, "request err under 24-way pooled load")
	}
	assert.GreaterOrEqualf(t, c.PoolStats().ActiveConns, 2,
		"ActiveConns = %d, want >= 2: 24 requests at 4 streams per conn cannot fit on one, "+
			"so the load did not spread", c.PoolStats().ActiveConns)
}

func TestIntegration_ClientPool_IdleEviction(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			IdleTimeout:       150 * time.Millisecond,
			HealthCheckPeriod: 50 * time.Millisecond,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var res client.Response
	require.NoError(t,
		c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &res), "first Do")
	require.Equal(t, 1, c.PoolStats().ActiveConns,
		"the first request must leave exactly one conn in the pool for the tick to evict")

	// Nothing further is issued: the conn now ages past IdleTimeout.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && c.PoolStats().ActiveConns != 0 {
		time.Sleep(50 * time.Millisecond)
	}

	assert.Equalf(t, 0, c.PoolStats().ActiveConns,
		"idle eviction did not run; ActiveConns = %d after %v idle at IdleTimeout=150ms",
		c.PoolStats().ActiveConns, 2*time.Second)
}

func TestIntegration_ClientPool_GoAwayMidFlight_Replaces(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: 50 * time.Millisecond,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	var res client.Response
	require.NoError(t,
		c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &res), "first Do")

	// A graceful server shutdown sends GOAWAY on the pooled connection.
	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if serr := srv.Config.Shutdown(shCtx); serr != nil {
		t.Logf("Shutdown returned %v (continuing)", serr)
	}
	shCancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && c.PoolStats().ActiveConns != 0 {
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equalf(t, 0, c.PoolStats().ActiveConns,
		"ActiveConns = %d after the server GOAWAY'd and shut down; a drained conn that "+
			"stays in the pool wedges every later acquire at the cap",
		c.PoolStats().ActiveConns)
}

func TestIntegration_Client_DoStream_LargeResponse(t *testing.T) {
	const total = 1 << 20 // 1 MiB
	chunk := []byte(strings.Repeat("x", 4096))
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		for written := 0; written < total; written += len(chunk) {
			_, _ = w.Write(chunk)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	// Larger StreamEventBuffer so the stream's events channel can absorb
	// up to 256 inbound DATA frames if the test goroutine drains slowly
	// under the race detector or shared-CI scheduling. The default of 8
	// risks a silent RST_STREAM(CANCEL) when the channel fills,
	// after which Recv blocks until the context deadline.
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			StreamEventBuffer: 1024,
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var sr client.StreamResponse
	require.NoError(t,
		c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer sr.Close()

	var got int
	var recvErr error
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			recvErr = err
			break
		}
		if ev.Type == client.EventData {
			got += len(ev.Data)
		}
		if ev.EndStream {
			break
		}
	}

	require.NoError(t, recvErr, "Recv")
	assert.Equal(t, 200, sr.Status, "response status")
	assert.Equalf(t, total, got,
		"read %d bytes of a %d-byte streamed response", got, total)
}

// TestDo_ResponseReuse is the test Response.Reset exists for: Reset() then Do()
// in a loop is the documented zero-alloc path for a load generator.
//
// Its handler used to write no body at all, so the property that matters most —
// Reset truncating r.Body — was unobservable in the one test that should own it,
// and deleting `r.Body = r.Body[:0]` from Reset left 22 integration tests green
// (#888). A Body that is not truncated means every request appends to the
// previous one: the caller reads the concatenation of every response so far, the
// slice grows without bound for the life of the client, and the result looks
// like a server bug.
//
// BodyMode is BodyBuffer for the same reason: BodyDiscard is the zero value, so
// the loop never appended to resp.Body at all and Reset had nothing to truncate.
func TestDo_ResponseReuse(t *testing.T) {
	t.Parallel()
	const body = "abc"
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-test", "value")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	const N = 5
	var resp client.Response
	var prevHdrCap int
	for i := 0; i < N; i++ {
		resp.Reset()
		require.NoErrorf(t,
			c.Do(context.Background(), &client.Request{
				Method: "GET", Path: "/", BodyMode: client.BodyBuffer,
			}, &resp), "Do[%d]", i)
		require.Equalf(t, 200, resp.Status, "Do[%d] status", i)
		require.Equalf(t, body, string(resp.Body),
			"Do[%d] body = %q, want %q — a reused Response that does not truncate its Body "+
				"hands the caller every previous response concatenated onto this one",
			i, resp.Body, body)
		require.EqualValuesf(t, len(body), resp.BytesReceived,
			"Do[%d] BytesReceived = %d, want %d: the counter is per-response, not cumulative",
			i, resp.BytesReceived, len(body))
		if i > 0 {
			assert.GreaterOrEqualf(t, cap(resp.Headers), prevHdrCap,
				"Headers backing array reallocated at iteration %d (cap went %d→%d): the "+
					"whole point of reusing a Response is that steady-state requests stop "+
					"allocating", i, prevHdrCap, cap(resp.Headers))
		}
		prevHdrCap = cap(resp.Headers)
	}
}

func TestDoStream_SRReuse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var sr client.StreamResponse
	for i := 0; i < 5; i++ {
		require.NoErrorf(t,
			c.DoStream(context.Background(), &client.Request{Method: "GET", Path: "/"}, &sr),
			"DoStream[%d]: a StreamResponse must be reusable across requests", i)
		assert.Equalf(t, 200, sr.Status, "DoStream[%d] status", i)
		require.NoErrorf(t, sr.Close(), "Close[%d]", i)
	}
}

func TestIntegration_Client_BodyStream_Small(t *testing.T) {
	want := []byte("hello stream")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	require.NoError(t,
		c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res), "Do")
	require.NotNil(t, res.BodyReader, "BodyReader is nil on the BodyStream path")

	got, err := io.ReadAll(res.BodyReader)
	closeErr := res.BodyReader.Close()

	assert.Equal(t, 200, res.Status, "response status")
	require.NoError(t, err, "ReadAll")
	require.NoError(t, closeErr, "Close")
	assert.Truef(t, bytes.Equal(got, want), "streamed body = %q, want %q", got, want)
}

func TestIntegration_Client_BodyStream_Large(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	// Large StreamEventBuffer prevents RST_STREAM(CANCEL) on slow
	// CI runners where the server sends many DATA frames before the body
	// reader goroutine is scheduled. 1 MiB / 16 KiB frames = ~64 events max.
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			StreamEventBuffer: 128,
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	require.NoError(t,
		c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res), "Do")

	n, cerr := io.Copy(io.Discard, res.BodyReader)
	closeErr := res.BodyReader.Close()

	require.NoError(t, cerr, "Copy")
	require.NoError(t, closeErr, "Close")
	assert.EqualValuesf(t, len(want), n,
		"read %d bytes of a %d-byte body through BodyReader", n, len(want))
}

func TestIntegration_Client_BodyStream_CloseEarly(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(bytes.Repeat([]byte("z"), 64*1024))
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	require.NoError(t,
		c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res), "Do")

	buf := make([]byte, 1)
	_, readErr := res.BodyReader.Read(buf)
	closeErr := res.BodyReader.Close()

	if readErr != nil {
		require.ErrorIs(t, readErr, io.EOF, "Read")
	}
	assert.NoError(t, closeErr,
		"abandoning a 64 KiB body after one byte must close cleanly, not error: this is "+
			"the ordinary early-abort path")
}

func TestIntegration_Client_BodyStream_ResetForgot(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("abc"))
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	require.NoError(t,
		doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res),
		"Do")
	// Hold the reader: Reset nils the field either way, so the field alone cannot
	// say whether Reset CLOSED it.
	reader := res.BodyReader
	require.NotNil(t, reader, "BodyReader is nil on the BodyStream path")

	res.Reset() // must call BodyReader.Close() internally; no panic

	// == nil on the interface, not assert.Nil: the latter reflects and would pass
	// for a non-nil interface holding a nil *responseBodyReader.
	assert.Truef(t, res.BodyReader == nil,
		"Reset left BodyReader = %v; it must clear the field", res.BodyReader)
	// A closed responseBodyReader latches `closed` and answers every later Read
	// with (0, io.EOF). An UNCLOSED one still has "abc" waiting, so this is what
	// tells "Reset closed it" apart from "Reset just dropped the pointer" —
	// which is all the assertion here used to be able to see.
	buf := make([]byte, 8)
	n, err := reader.Read(buf)
	assert.Equalf(t, 0, n,
		"the abandoned body reader returned %d bytes (%q) after Reset: Reset dropped the "+
			"BodyReader without closing it, so the stream and its pooled buffers leak",
		n, buf[:n])
	assert.ErrorIs(t, err, io.EOF, "a closed body reader must answer Read with io.EOF")
}

func TestIntegration_Client_POST_ContentLength_Header(t *testing.T) {
	gotCL := make(chan string, 1)
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCL <- r.Header.Get("Content-Length")
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:        "POST",
		Path:          "/",
		BodyReader:    strings.NewReader("hello"),
		ContentLength: 5,
	}, &res)

	require.NoError(t, err, "Do")
	select {
	case cl := <-gotCL:
		assert.Equal(t, "5", cl,
			"the peer saw the wrong content-length: a streamed body needs the declared "+
				"length emitted verbatim, or the server frames the request wrongly")
	default:
		assert.Fail(t, "the handler never ran, so no content-length was observed")
	}
}

// newH2CServer starts an H2C (cleartext HTTP/2) server on a random
// port and returns the "host:port" address.
func newH2CServer(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	// HTTP/1.1 and cleartext HTTP/2 on one listener, as x/net's h2c.NewHandler
	// used to do for us. That package is deprecated in favour of this field.
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           h,
		Protocols:         protos,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func TestIntegration_Client_H2C_Do(t *testing.T) {
	addr := newH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "h2c ok")
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.PlaintextDialer{},
		},
		DefaultScheme: "http",
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })

	var res client.Response
	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, &res)

	require.NoError(t, err, "Do over cleartext HTTP/2")
	assert.Equal(t, 200, res.Status, "response status")
	assert.Equal(t, "h2c ok", string(res.Body), "response body over h2c")
}
