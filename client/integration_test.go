package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
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

func clientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestIntegration_Client_GET_Status200(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
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
		WantBody: true,
	}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if !bytes.Equal(res.Body, want) {
		t.Fatalf("body = %q, want %q", res.Body, want)
	}
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
		WantBody:   true,
	}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if !bytes.Equal(res.Body, want) {
		t.Fatalf("body length %d, want %d", len(res.Body), len(want))
	}
}

func TestIntegration_Client_ConcurrentRequests_OneClient(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if err != nil {
			t.Fatalf("concurrent Do failed: %v", err)
		}
	}
}

func TestIntegration_ClientPool_ConcurrentRequests_MultipleConns(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if err != nil {
		t.Fatalf("NewClient = %v", err)
	}
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
			if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res); err != nil {
				errs <- err
				return
			}
			if res.Status != 200 {
				errs <- fmt.Errorf("status = %d", res.Status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("request err: %v", err)
	}

	s := c.PoolStats()
	if s.ActiveConns < 2 {
		t.Fatalf("ActiveConns = %d, want >= 2 (load did not spread)", s.ActiveConns)
	}
}

func TestIntegration_ClientPool_IdleEviction(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if err != nil {
		t.Fatalf("NewClient = %v", err)
	}
	defer c.Close()

	var _res1 client.Response
	if err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res1); err != nil {
		t.Fatalf("first Do = %v", err)
	}
	if got := c.PoolStats().ActiveConns; got != 1 {
		t.Fatalf("after first req ActiveConns = %d, want 1", got)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("idle eviction did not run; ActiveConns = %d", c.PoolStats().ActiveConns)
}

func TestIntegration_ClientPool_GoAwayMidFlight_Replaces(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	if err != nil {
		t.Fatalf("NewClient = %v", err)
	}
	defer c.Close()

	var _res2 client.Response
	if err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res2); err != nil {
		t.Fatalf("first Do = %v", err)
	}

	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := srv.Config.Shutdown(shCtx); err != nil {
		t.Logf("Shutdown returned %v (continuing)", err)
	}
	shCancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ActiveConns = %d, want 0 after server shutdown", c.PoolStats().ActiveConns)
}

func TestIntegration_Client_DoStream_LargeResponse(t *testing.T) {
	const total = 1 << 20 // 1 MiB
	chunk := []byte(strings.Repeat("x", 4096))
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	// risks a silent RST_STREAM(REFUSED_STREAM) when the channel fills,
	// after which Recv blocks until the context deadline.
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			StreamEventBuffer: 1024,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	if sr.Status != 200 {
		t.Fatalf("status = %d", sr.Status)
	}
	var got int
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == client.EventData {
			got += len(ev.Data)
		}
		if ev.EndStream {
			break
		}
	}
	if got != total {
		t.Fatalf("read %d, want %d", got, total)
	}
}

func TestDo_ResponseReuse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-test", "value")
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	const N = 5
	var prevHdrCap int
	for i := 0; i < N; i++ {
		resp.Reset()
		if err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp); err != nil {
			t.Fatalf("Do[%d]: %v", i, err)
		}
		if resp.Status != 200 {
			t.Fatalf("Do[%d]: status %d", i, resp.Status)
		}
		if i > 0 && cap(resp.Headers) < prevHdrCap {
			t.Errorf("Headers backing array reallocated at iteration %d (cap went %d→%d)",
				i, prevHdrCap, cap(resp.Headers))
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var sr client.StreamResponse
	for i := 0; i < 5; i++ {
		if err := c.DoStream(context.Background(), &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
			t.Fatalf("DoStream[%d]: %v", i, err)
		}
		if sr.Status != 200 {
			t.Fatalf("DoStream[%d]: status %d", i, sr.Status)
		}
		if err := sr.Close(); err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
}

func TestIntegration_Client_StreamBody_Small(t *testing.T) {
	want := []byte("hello stream")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if res.BodyReader == nil {
		t.Fatal("BodyReader is nil")
	}
	got, err := io.ReadAll(res.BodyReader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestIntegration_Client_StreamBody_Large(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	n, err := io.Copy(io.Discard, res.BodyReader)
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n != int64(len(want)) {
		t.Fatalf("read %d bytes, want %d", n, len(want))
	}
}

func TestIntegration_Client_StreamBody_CloseEarly(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(bytes.Repeat([]byte("z"), 64*1024))
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := res.BodyReader.Read(buf); err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIntegration_Client_StreamBody_ResetForgot(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("abc"))
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", StreamBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	res.Reset() // must call BodyReader.Close() internally; no panic
}

func TestIntegration_Client_POST_ContentLength_Header(t *testing.T) {
	var gotCL string
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCL = r.Header.Get("Content-Length")
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	body := strings.NewReader("hello")
	err := c.Do(ctx, &client.Request{
		Method:        "POST",
		Path:          "/",
		BodyReader:    body,
		ContentLength: 5,
	}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if gotCL != "5" {
		t.Fatalf("content-length = %q, want %q", gotCL, "5")
	}
}
