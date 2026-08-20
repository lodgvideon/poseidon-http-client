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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pushRecord is what a PushHandler saw, guarded so the test goroutine can read
// it while the handler goroutine is still writing.
type pushRecord struct {
	mu     sync.Mutex
	n      int
	paths  map[string]bool
	status int
	body   []byte
	resp   *client.Response
	err    error
}

func newPushRecord() *pushRecord { return &pushRecord{paths: map[string]bool{}} }

// handler returns a client.PushHandler that files everything into the record.
func (p *pushRecord) handler() func(context.Context, []conn.HeaderField, *client.Response, error) {
	return func(_ context.Context, promised []conn.HeaderField, resp *client.Response, err error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.n++
		p.err = err
		p.resp = resp
		if resp != nil {
			p.status = resp.Status
			p.body = resp.Body
		}
		for _, h := range promised {
			if string(h.Name) == ":path" {
				p.paths[string(h.Value)] = true
			}
		}
	}
}

// waitFor blocks until at least want pushes have been recorded, or d elapses. It
// returns the count reached, so the caller can assert on the number rather than
// on a bare "did it happen" flag.
func (p *pushRecord) waitFor(want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	for {
		p.mu.Lock()
		n := p.n
		p.mu.Unlock()
		if n >= want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newPushClient builds a client against addr with h installed as its push
// handler (nil for none).
func newPushClient(t *testing.T, addr string, h func(context.Context, []conn.HeaderField, *client.Response, error)) *client.Client {
	t.Helper()

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: tlsConfig()},
		},
		PushHandler: h,
	})
	require.NoError(t, err, "NewClient against the local h2 server")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// doMainGET issues the main request that provokes the server's pushes.
//
// The context is torn down by t.Cleanup rather than by a deferred cancel: the
// push handler runs on its own goroutine after Do returns and inherits this
// context, so cancelling it here would abort every push before it is drained
// and the handler would be invoked with context.Canceled instead of a response.
func doMainGET(t *testing.T, c *client.Client, mode client.BodyMode) client.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	var res client.Response
	require.NoError(t, c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: mode}, &res),
		"main GET / must succeed regardless of what happens on the pushed streams")
	return res
}

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
	rec := newPushRecord()
	c := newPushClient(t, addr, rec.handler())

	res := doMainGET(t, c, client.BodyBuffer)

	require.Equal(t, 200, res.Status, "main status = %d, want 200", res.Status)
	require.Equalf(t, 1, rec.waitFor(1, 5*time.Second),
		"PushHandler was called %d times within the timeout, want 1 — a pushed resource "+
			"the caller never hears about is a wasted round trip", rec.n)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.NoError(t, rec.err, "push handler err: %v", rec.err)
	assert.Truef(t, rec.paths["/style.css"],
		"pushed :path set = %v, want /style.css — the promised headers are how a caller "+
			"decides whether it wanted this resource", rec.paths)
	assert.Equalf(t, 200, rec.status, "pushed status = %d, want 200", rec.status)
	assert.Equalf(t, string(pushedBody), string(rec.body),
		"pushed body = %q, want %q — the handler is invoked with a FULLY drained response",
		rec.body, pushedBody)
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
	c := newPushClient(t, addr, nil)

	res := doMainGET(t, c, 0)

	assert.Equalf(t, 200, res.Status,
		"status = %d, want 200 — a refused push must not take the parent stream down with it",
		res.Status)
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
	rec := newPushRecord()
	c := newPushClient(t, addr, rec.handler())

	res := doMainGET(t, c, client.BodyBuffer)

	require.Equal(t, 200, res.Status, "main status = %d, want 200", res.Status)
	require.Equalf(t, 1, rec.waitFor(1, 5*time.Second),
		"PushHandler was called %d times within the timeout, want 1", rec.n)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.NotNil(t, rec.resp,
		"push handler called with a nil resp for a non-2xx push; the caller cannot tell a "+
			"404 push from a transport failure")
	assert.Equalf(t, 404, rec.resp.Status,
		"pushed status = %d, want 404 — a non-2xx push is delivered, not swallowed",
		rec.resp.Status)
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
	rec := newPushRecord()
	c := newPushClient(t, addr, rec.handler())

	res := doMainGET(t, c, client.BodyBuffer)

	require.Equal(t, 200, res.Status, "main status = %d, want 200", res.Status)
	got := rec.waitFor(numPushes, 5*time.Second)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equalf(t, numPushes, got,
		"got %d push invocations, want %d: %v — concurrent promises on one connection must "+
			"each reach the handler, not just the first", got, numPushes, rec.paths)
	for _, p := range pushPaths {
		assert.Truef(t, rec.paths[p], "push for %q was not delivered to the handler", p)
	}
}
