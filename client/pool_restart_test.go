package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// serveH2On starts a real HTTP/2 server on an existing listener, so a test can
// rebind the same address after killing one.
func serveH2On(ln net.Listener) *httptest.Server {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Listener = ln
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv
}

// TestIntegration_Pool_RecoversAfterServerRestart is the scenario this library
// exists for: a load generator pointed at a target that restarts. Deploys,
// crashes and autoscaling make this a Tuesday, not an edge case.
//
// The pool holds one connection. The server is really killed and really comes
// back on the same address. The pool must notice the corpse and redial.
//
// It did not. Measured before this test existed, with a 50ms health check:
// every attempt after the restart failed on the *same socket pair*, forever —
// the pool never evicted, because eviction asks conn.IsAlive(), and IsAlive did
// not consider a dead reader to be death. The health check ran and changed
// nothing: it asks the same question.
//
// A single connection is the point, not a simplification: at MaxConnsPerHost:1
// there is nowhere to hide. A larger cap would let healthy connections mask a
// poisoned one.
//
// The restart is the injection, and it is logged along with the attempt on which
// recovery happened: "a later request succeeded" is also true of a run in which
// the server was never actually killed, and that run must not read as a pass.
func TestIntegration_Pool_RecoversAfterServerRestart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	addr := ln.Addr().String()
	srv := serveH2On(ln)

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			HealthCheckPeriod: 50 * time.Millisecond,
			// Explicit and short. The probe below is a REAL dial failure against
			// a closed port, which opens the dial backoff; on the default window
			// every post-restart retry is refused by the backoff rather than
			// attempted, and the test fails for a reason that is not the pool
			// forgetting how to redial.
			DialBackoff: 20 * time.Millisecond,
		},
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{
			Config: &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"h2"},
			},
		}},
	})
	require.NoError(t, err, "NewClient")
	defer func() { _ = c.Close() }()

	do := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return c.Do(ctx, client.GET("/"), &client.Response{})
	}

	// The control: without a healthy baseline the rest measures a broken setup,
	// not a broken pool.
	require.NoError(t, do(), "request against a healthy server failed")

	// INJECTION: kill the server and prove it is really gone before rebinding.
	// Without this check a run in which the old server somehow kept serving
	// would "recover" on attempt 0 and look identical to a real pass.
	srv.Close()
	time.Sleep(200 * time.Millisecond)
	require.Error(t, do(),
		"a request succeeded after the server was closed — the injection did not take "+
			"effect, so the recovery below would prove nothing")

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("cannot rebind %s after the server closed it: %v", addr, err)
	}
	srv2 := serveH2On(ln2)
	defer srv2.Close()

	// Early attempts are allowed to fail: one discovers the corpse, and others
	// can land inside the dial backoff the probe above opened. What matters is
	// that a later one succeeds — that the pool learned. Bounded by a deadline
	// rather than an attempt count so a backoff window cannot silently consume
	// every retry.
	var lastErr error
	attempts, recoveredOn := 0, -1
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		attempts++
		if lastErr = do(); lastErr == nil {
			recoveredOn = attempts
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("injection fired: server killed and rebound on %s; recovered on attempt %d of %d",
		addr, recoveredOn, attempts)
	require.Positivef(t, recoveredOn,
		"pool never recovered after the server restarted: %d attempts over 8s, last error: %v",
		attempts, lastErr)
}
