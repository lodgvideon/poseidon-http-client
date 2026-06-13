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
