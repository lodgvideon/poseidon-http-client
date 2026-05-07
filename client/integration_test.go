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
	res, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
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
	res, err := c.Do(ctx, &client.Request{
		Method: "POST", Path: "/echo",
		Body:     want,
		WantBody: true,
	})
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
	res, err := c.Do(ctx, &client.Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		WantBody:   true,
	})
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
			res, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
			if err != nil {
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
	sr, err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"})
	if err != nil {
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
