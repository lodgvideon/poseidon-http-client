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
)

// TestRetryer_Integration_5xxRetriesViaIsRetryable proves the retry
// loop drives the real transport. Server returns 503 on the first
// two attempts and 200 on the third; IsRetryable opts retries in.
func TestRetryer_Integration_5xxRetriesViaIsRetryable(t *testing.T) {
	var attempt atomic.Int32
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempt.Add(1)
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	_ = srv

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 10 * time.Millisecond },
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status >= 500
		},
	})

	resp, err := r.Do(context.Background(), &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do err = %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200 after 2× 503 retries", resp.Status)
	}
	if got := attempt.Load(); got != 3 {
		t.Errorf("server attempts = %d, want 3", got)
	}
}
