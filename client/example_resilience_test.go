package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ExampleClient_Retryer wraps a client with bounded automatic retry. The
// Retryer re-issues idempotent requests on transient transport failures
// (REFUSED_STREAM, GOAWAY, dial errors) using exponential backoff with jitter.
func ExampleClient_Retryer() {
	ctx := context.Background()

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Up to 5 attempts; a custom predicate also retries 503 responses.
	r := c.Retryer(client.RetryOptions{
		MaxAttempts: 5,
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status == 503
		},
	})

	var resp client.Response
	if err := r.Do(ctx, client.GET("/v1/health"), &resp); err != nil {
		log.Fatal(err)
	}
	log.Printf("status=%d", resp.Status)
}

// ExampleNewManagedClient builds a managed multi-address client: a resolver
// supplies the backend set, and a round-robin selector spreads requests across
// them. One per-address sub-pool fans out behind the selector.
func ExampleNewManagedClient() {
	ctx := context.Background()

	resolver := client.StaticResolver(
		client.Address{Host: "10.0.0.1", Port: 443},
		client.Address{Host: "10.0.0.2", Port: 443},
		client.Address{Host: "10.0.0.3", Port: 443},
	)

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewManagedClient(resolver, dialer,
		client.WithSelector(client.RoundRobin()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	var resp client.Response
	if err := c.Do(ctx, client.GET("/v1/things"), &resp); err != nil {
		log.Fatal(err)
	}
	log.Printf("status=%d", resp.Status)
}

// ExampleStaticResolver feeds a fixed set of backends to a managed client.
// StaticResolver copies its argument, so later mutation of the caller's slice
// has no effect on the resolved set.
func ExampleStaticResolver() {
	ctx := context.Background()

	resolver := client.StaticResolver(
		client.Address{Host: "backend-a.internal", Port: 8443},
		client.Address{Host: "backend-b.internal", Port: 8443},
	)

	// The resolver can also be queried directly.
	addrs, err := resolver.Resolve(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, a := range addrs {
		log.Printf("backend %s", a.String())
	}

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewManagedClient(resolver, dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()
}

// Example_rateLimit caps outgoing request rate with a token bucket. WithRateLimit
// throttles new requests to perSecond QPS, allowing short bursts up to burst.
func Example_rateLimit() {
	ctx := context.Background()

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	// 100 QPS sustained, bursts up to 20.
	c, err := client.NewSingleConnClient("api.example.com:443", dialer,
		client.WithRateLimit(100, 20))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	var resp client.Response
	for i := 0; i < 5; i++ {
		resp.Reset()
		if err := c.Do(ctx, client.GET("/v1/ping"), &resp); err != nil {
			log.Fatal(err)
		}
	}
}

// Example_requestTimeout sets a per-request deadline. When Request.Timeout fires,
// Do fails with context.DeadlineExceeded and the in-flight stream is reset with
// RST_STREAM(CANCEL). This is independent of any deadline on ctx.
func Example_requestTimeout() {
	ctx := context.Background()

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	req := client.GET("/v1/slow")
	req.Timeout = 500 * time.Millisecond

	var resp client.Response
	if err := c.Do(ctx, req, &resp); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Print("request timed out")
			return
		}
		log.Fatal(err)
	}
	log.Printf("status=%d", resp.Status)
}

// Example_idempotencyOverride retries a POST that would otherwise be considered
// non-idempotent. Setting Request.Idempotency = client.ForceIdempotent (e.g. for
// a request guarded by an idempotency key) opts it into transport-level retry.
func Example_idempotencyOverride() {
	ctx := context.Background()

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	req := client.POST("/v1/charges", []byte(`{"amount":100}`))
	req.WithHeaders(client.H("idempotency-key", "abc-123"))
	// POST is non-idempotent by default; force it so the Retryer will retry.
	req.Idempotency = client.ForceIdempotent

	r := c.Retryer(client.RetryOptions{MaxAttempts: 3})

	var resp client.Response
	if err := r.Do(ctx, req, &resp); err != nil {
		log.Fatal(err)
	}
	log.Printf("status=%d", resp.Status)
}
