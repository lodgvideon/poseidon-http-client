package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newShutdownClient starts a 200-answering h2 server and returns a client aimed
// at it. Both are torn down by t.Cleanup.
func newShutdownClient(t *testing.T) *Client {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoError(t, err, "NewClient against the local h2 server")
	return c
}

// TestClient_Shutdown_NewRequestAfterShutdownReturnsError verifies that
// after Shutdown, new requests return an error.
func TestClient_Shutdown_NewRequestAfterShutdownReturnsError(t *testing.T) {
	c := newShutdownClient(t)
	// Trigger the lazy dial and run one request, so Shutdown has a live
	// connection to drain rather than nothing at all.
	var resp Response
	require.NoError(t, c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp),
		"the priming request must succeed, or Shutdown is not draining anything")
	require.Equal(t, 200, resp.Status, "priming status = %d, want 200", resp.Status)
	require.NoError(t, c.Shutdown(100*time.Millisecond), "Shutdown on a live client")

	var after Response
	err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &after)

	assert.Error(t, err,
		"a request after Shutdown succeeded; a drained client must refuse new work rather "+
			"than silently re-dialling the peer it just said goodbye to")
}

// TestClient_Shutdown_Idempotent verifies that calling Shutdown twice is safe.
func TestClient_Shutdown_Idempotent(t *testing.T) {
	c := newShutdownClient(t)

	first := c.Shutdown(100 * time.Millisecond)
	second := c.Shutdown(100 * time.Millisecond)

	assert.NoError(t, first, "Shutdown 1")
	assert.NoError(t, second,
		"Shutdown 2 failed — a second call must be a no-op, or a deferred Shutdown in a "+
			"caller's cleanup turns into a spurious error")
}

// TestClient_Shutdown_NoConnYet verifies Shutdown on a client that never made a
// request is a no-op: the lazy dial means there is nothing to drain.
func TestClient_Shutdown_NoConnYet(t *testing.T) {
	c := newShutdownClient(t)

	err := c.Shutdown(100 * time.Millisecond)

	assert.NoError(t, err,
		"Shutdown on a client that never dialled must not error — a caller that shuts down "+
			"before its first request would otherwise see a failure it cannot act on")
}
