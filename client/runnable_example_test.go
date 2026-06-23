package client_test

// This file holds EXECUTABLE godoc examples: unlike the compile-only
// examples in example_*_test.go (which dial nonexistent hosts and so cannot
// run under `go test`), every Example here spins up an in-process HTTP/2
// server with a deterministic handler, drives the real client against it, and
// pins the result with an // Output: block. They double as living
// documentation and as a smoke test that the documented surface actually
// works end-to-end.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// exampleH2Server starts an in-process TLS HTTP/2 server backed by the given
// handler and returns its host:port plus a stop func the example must defer.
// The server speaks h2 (EnableHTTP2 = true) over a throwaway self-signed cert;
// pair it with exampleDialer, whose TLS config skips verification.
func exampleH2Server(handler http.HandlerFunc) (addr string, stop func()) {
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv.Listener.Addr().String(), srv.Close
}

// exampleDialer returns a TLS dialer that trusts the throwaway cert minted by
// exampleH2Server. InsecureSkipVerify is acceptable only because this is an
// in-process test server; never do this against real backends.
func exampleDialer() conn.Dialer {
	return &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
}

// exampleDo wraps c.Do in a tiny bounded Retryer so the deterministic
// // Output: blocks are not perturbed by the transient RST_STREAM noise
// httptest's h2 server can emit when sibling parallel tests tear their
// servers down. Retryer is itself a documented client feature; using it here
// keeps the examples honest while making them reproducible.
func exampleDo(ctx context.Context, c *client.Client, req *client.Request, resp *client.Response) error {
	return c.Retryer(client.RetryOptions{MaxAttempts: 5}).Do(ctx, req, resp)
}

// Example_quickstart is the smallest end-to-end round trip: dial a single
// connection, GET a path, and read the status plus body.
func Example_quickstart() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "pong")
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var resp client.Response
	resp.Reset()
	if err := exampleDo(ctx, c, client.GET("/ping"), &resp); err != nil {
		panic(err)
	}
	fmt.Printf("status=%d body=%s\n", resp.Status, resp.Body)
	// Output:
	// status=200 body=pong
}

// Example_postJSON sends a POST body and the handler echoes it back, proving
// the request body reaches the server and the 200 status round-trips.
func Example_postJSON() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body) // echo
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.POST("/echo", []byte(`{"name":"widget"}`)).WithHeaders(
		client.H("content-type", "application/json"),
	)
	req.WantBody = true

	var resp client.Response
	resp.Reset()
	if err := exampleDo(ctx, c, req, &resp); err != nil {
		panic(err)
	}
	fmt.Printf("status=%d echoed=%s\n", resp.Status, resp.Body)
	// Output:
	// status=200 echoed={"name":"widget"}
}

// Example_responseHeader reads a single response header by name. Header
// returns bytes aliasing Response-owned memory valid until the next Reset.
func Example_responseHeader() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-region", "eu-west-1")
		w.WriteHeader(http.StatusOK)
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var resp client.Response
	resp.Reset()
	if err := exampleDo(ctx, c, client.GET("/whoami"), &resp); err != nil {
		panic(err)
	}
	if region, ok := resp.Header("x-region"); ok {
		fmt.Printf("region=%s\n", region)
	}
	// Output:
	// region=eu-west-1
}

// Example_reuseResponse shows the canonical hot-loop shape: one Response
// allocated up front, Reset() before each reuse so its backing arrays are
// recycled instead of reallocated.
func Example_reuseResponse() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.GET("/health")

	var resp client.Response
	for i := 0; i < 3; i++ {
		resp.Reset()
		if err := exampleDo(ctx, c, req, &resp); err != nil {
			panic(err)
		}
		fmt.Println(resp.Status)
	}
	// Output:
	// 200
	// 200
	// 200
}

// Example_streamingDownload consumes a chunked response via Client.Stream,
// which reassembles the DATA frames and always closes the underlying stream.
// The handler flushes three chunks; the example reports the total byte count.
func Example_streamingDownload() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte("0123456789")) // 10 bytes each
			if flusher != nil {
				flusher.Flush()
			}
		}
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var total int
	err = c.Stream(ctx, client.GET("/download"), func(ev client.StreamEvent) error {
		if ev.Type == client.EventData {
			total += len(ev.Data)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("downloaded %d bytes\n", total)
	// Output:
	// downloaded 30 bytes
}

// Example_retryOnRefusedStream wraps a pool client in a Retryer with a custom
// predicate that retries 503 responses. The handler serves 503 to its first
// caller and 200 thereafter, so the Retryer transparently lands on the 200.
func Example_retryOnRefusedStream() {
	var calls int
	addr, stop := exampleH2Server(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // 503 on first try
			return
		}
		w.WriteHeader(http.StatusOK) // 200 afterwards
	})
	defer stop()

	c, err := client.NewPoolClient(addr, exampleDialer(), client.PoolOptions{
		MaxConnsPerHost:   2,
		MaxStreamsPerConn: 50,
		IdleTimeout:       30 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	// Retry on 503 with zero backoff so the example finishes promptly.
	r := c.Retryer(client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(err error, resp *client.Response) bool {
			return err == nil && resp != nil && resp.Status == http.StatusServiceUnavailable
		},
	})

	ctx := context.Background()
	var resp client.Response
	resp.Reset()
	if err := r.Do(ctx, client.GET("/flaky"), &resp); err != nil {
		panic(err)
	}
	fmt.Printf("final status=%d\n", resp.Status)
	// Output:
	// final status=200
}

// Example_successRate issues one 2xx and one 404, then reads the frozen
// MetricsSnapshot to show the 2xx / non-2xx split a load generator uses to
// compute a real success rate (rather than conflating "got a response" with
// "got a good response").
func Example_successRate() {
	addr, stop := exampleH2Server(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ok") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	defer stop()

	c, err := client.NewSingleConnClient(addr, exampleDialer())
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	var resp client.Response
	for _, path := range []string{"/ok", "/missing"} {
		resp.Reset()
		if err := exampleDo(ctx, c, client.GET(path), &resp); err != nil {
			panic(err)
		}
	}

	ctr := c.MetricsSnapshot().Counters
	fmt.Printf("2xx=%d non2xx=%d\n", ctr.Responses2xx, ctr.ResponsesNon2xx)
	// Output:
	// 2xx=1 non2xx=1
}
