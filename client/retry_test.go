package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetryer_Integration_5xxRetriesViaIsRetryable proves the retry loop drives
// the real transport. Server returns 503 on the first two attempts and 200 on
// the third; IsRetryable opts retries in.
//
// The method is GET on purpose. canRetry refuses a non-idempotent method
// outright, so the same test written with POST would be un-replayed for a reason
// that has nothing to do with IsRetryable, and would stay green with the whole
// user-predicate path removed.
func TestRetryer_Integration_5xxRetriesViaIsRetryable(t *testing.T) {
	var attempt atomic.Int32
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempt.Add(1) < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoError(t, err, "NewClient against the local h2 server")
	defer c.Close()
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status >= 500
		},
	})
	var resp client.Response

	err = r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v", err)
	assert.Equalf(t, 200, resp.Status,
		"Status = %d, want 200 after two 503 retries — the caller must see the final "+
			"response, not the first one that triggered a retry", resp.Status)
	assert.Equalf(t, int32(3), attempt.Load(),
		"server saw %d attempts, want 3 — the retries must reach the real transport, not "+
			"be satisfied from a cached response", attempt.Load())
}
