package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestIntegration_Push_HandlerInvoked verifies that when a server pushes
// a resource, the client's PushHandler is called with the promised
// request headers and the fully drained pushed response.
func TestIntegration_Push_HandlerInvoked(t *testing.T) {
	t.Parallel()

	pushedBody := []byte("body { color: red; }")

	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/style.css" {
			w.Header().Set("content-type", "text/css")
			w.WriteHeader(200)
			_, _ = w.Write(pushedBody)
			return
		}
		// Main request — push /style.css
		if pusher, ok := w.(http.Pusher); ok {
			_ = pusher.Push("/style.css", nil)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("<html>main</html>"))
	}))

	var (
		mu        sync.Mutex
		gotPush   bool
		pushPath  string
		pushBody  []byte
		pushStatus int
		pushErr   error
	)

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: tlsConfig()},
		},
		PushHandler: func(_ context.Context, promisedHeaders []conn.HeaderField, resp *client.Response, err error) {
			mu.Lock()
			defer mu.Unlock()
			gotPush = true
			pushErr = err
			if resp != nil {
				pushStatus = resp.Status
				pushBody = resp.Body
			}
			for _, h := range promisedHeaders {
				if string(h.Name) == ":path" {
					pushPath = string(h.Value)
				}
			}
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/", WantBody: true}, &res)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("main status = %d", res.Status)
	}

	// Wait for push handler goroutine to complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := gotPush
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if !gotPush {
		t.Fatal("PushHandler was not called within timeout")
	}
	if pushErr != nil {
		t.Fatalf("push handler err: %v", pushErr)
	}
	if pushPath != "/style.css" {
		t.Errorf("pushed :path = %q, want /style.css", pushPath)
	}
	if pushStatus != 200 {
		t.Errorf("pushed status = %d, want 200", pushStatus)
	}
	if string(pushBody) != string(pushedBody) {
		t.Errorf("pushed body = %q, want %q", pushBody, pushedBody)
	}
}

// TestIntegration_Push_Disabled verifies that when PushHandler is nil,
// server push is not enabled and PUSH_PROMISE triggers PROTOCOL_ERROR
// at the conn layer. The main response should still succeed (the error
// on the pushed stream does not affect the parent).
func TestIntegration_Push_Disabled(t *testing.T) {
	t.Parallel()

	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/style.css" {
			w.WriteHeader(200)
			return
		}
		if pusher, ok := w.(http.Pusher); ok {
			_ = pusher.Push("/style.css", nil)
		}
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: tlsConfig()},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &res)
	if err != nil {
		t.Fatalf("Do: %v (push disabled should not fail main request)", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

// tlsConfig returns a TLS config that skips certificate verification
// for test servers.
func tlsConfig() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

// TestIntegration_Push_HandlerReceivesNon2xx confirms that when a
// pushed stream returns a non-2xx status (e.g. 404, 500), the
// PushHandler is invoked with a non-nil Response carrying that
// status. The main response is unaffected.
func TestIntegration_Push_HandlerReceivesNon2xx(t *testing.T) {
	t.Parallel()

	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing.css" {
			http.NotFound(w, r)
			return
		}
		if pusher, ok := w.(http.Pusher); ok {
			_ = pusher.Push("/missing.css", nil)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("<html>main</html>"))
	}))

	var (
		mu       sync.Mutex
		gotPush  bool
		pushResp *client.Response
	)
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: tlsConfig()},
		},
		PushHandler: func(_ context.Context, _ []conn.HeaderField, resp *client.Response, _ error) {
			mu.Lock()
			defer mu.Unlock()
			gotPush = true
			pushResp = resp
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", WantBody: true}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("main status = %d, want 200", res.Status)
	}

	// Wait for push handler goroutine.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := gotPush
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if !gotPush {
		t.Fatal("PushHandler was not called within timeout")
	}
	if pushResp == nil {
		t.Fatal("push handler called with nil resp")
	}
	if pushResp.Status != 404 {
		t.Errorf("pushed status = %d, want 404", pushResp.Status)
	}
}

// TestIntegration_Push_MultipleConcurrent confirms that the client
// can handle multiple PUSH_PROMISE frames from the server, each
// delivering a distinct pushed resource. All handlers must be invoked.
func TestIntegration_Push_MultipleConcurrent(t *testing.T) {
	t.Parallel()

	const numPushes = 4
	pushPaths := []string{"/a.css", "/b.css", "/c.css", "/d.css"}

	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range pushPaths {
			if r.URL.Path == p {
				w.Header().Set("content-type", "text/css")
				w.WriteHeader(200)
				_, _ = w.Write([]byte("body-" + p))
				return
			}
		}
		if pusher, ok := w.(http.Pusher); ok {
			for _, p := range pushPaths {
				_ = pusher.Push(p, nil)
			}
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("<html>main</html>"))
	}))

	var (
		mu       sync.Mutex
		gotPaths = make(map[string]bool)
	)
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: tlsConfig()},
		},
		PushHandler: func(_ context.Context, promisedHeaders []conn.HeaderField, _ *client.Response, _ error) {
			for _, h := range promisedHeaders {
				if string(h.Name) == ":path" {
					mu.Lock()
					gotPaths[string(h.Value)] = true
					mu.Unlock()
				}
			}
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var res client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", WantBody: true}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("main status = %d, want 200", res.Status)
	}

	// Wait until all numPushes are received (or timeout).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(gotPaths)
		mu.Unlock()
		if n >= numPushes {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gotPaths) != numPushes {
		t.Fatalf("got %d pushes, want %d: %v", len(gotPaths), numPushes, gotPaths)
	}
	for _, p := range pushPaths {
		if !gotPaths[p] {
			t.Errorf("push for %q was not delivered to handler", p)
		}
	}
}
