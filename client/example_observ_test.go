package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// ExampleHooks installs a lifecycle callback set via WithHooks. The
// OnRequestComplete hook fires once Do returns, carrying the status,
// latency, and byte counts for the exchange — handy for structured
// request logging without wrapping every call site.
func ExampleHooks() {
	hooks := &client.Hooks{
		OnRequestComplete: func(ev client.RequestCompleteEvent) {
			if ev.Err != nil {
				log.Printf("%s %s failed after %s: %v",
					ev.Method, ev.Path, ev.Latency, ev.Err)
				return
			}
			log.Printf("%s %s -> %d in %s (%d B sent, %d B recv)",
				ev.Method, ev.Path, ev.Status, ev.Latency,
				ev.BytesSent, ev.BytesRecv)
		},
	}

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer, client.WithHooks(hooks))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatal(err)
	}
}

// ExampleClient_MetricsSnapshot reads a frozen, value-safe view of the
// client's counters after a batch of requests and derives a real success
// rate from the 2xx / non-2xx split (rather than conflating "got a
// response" with "got a good response").
func ExampleClient_MetricsSnapshot() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		resp := &client.Response{}
		_ = c.Do(ctx, client.GET("/healthz"), resp)
	}

	snap := c.MetricsSnapshot()
	ctr := snap.Counters
	if ctr.RequestsSucceeded > 0 {
		successRate := float64(ctr.Responses2xx) / float64(ctr.RequestsSucceeded)
		fmt.Printf("2xx=%d non-2xx=%d errored=%d success-rate=%.2f p99=%s\n",
			ctr.Responses2xx, ctr.ResponsesNon2xx, ctr.RequestsErrored,
			successRate, snap.Latency.Request.Quantile(0.99))
	}
}

// Example_serverPush registers a PushHandler via WithPushHandler. Setting
// the handler automatically enables server push, so PUSH_PROMISE frames
// (RFC 7540 §8.2) are drained into a *client.Response and delivered to the
// callback instead of triggering a protocol error.
func Example_serverPush() {
	push := func(ctx context.Context, promised []conn.HeaderField, resp *client.Response, err error) {
		if err != nil {
			log.Printf("push failed: %v", err)
			return
		}
		var path string
		for _, hf := range promised {
			if string(hf.Name) == ":path" {
				path = string(hf.Value)
			}
		}
		log.Printf("server pushed %s -> %d (%d bytes)", path, resp.Status, len(resp.Body))
	}

	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer, client.WithPushHandler(push))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/index.html"), resp); err != nil {
		log.Fatal(err)
	}
}

// Example_requestPriority attaches an RFC 7540 §5.3 priority hint to a
// request. The HEADERS frame then carries the PRIORITY flag so the server
// may weight response delivery — e.g. deliver a stylesheet ahead of images.
func Example_requestPriority() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	req := client.GET("/style.css")
	// Weight is the on-wire byte (RFC weight = Weight + 1); 255 -> top
	// priority. StreamDep=0 roots the stream with no parent dependency.
	req.Priority = &frame.Priority{
		StreamDep: 0,
		Weight:    255,
		Exclusive: false,
	}

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, req, resp); err != nil {
		log.Fatal(err)
	}
}

// Example_requestTrailers sends request trailers computed after the body is
// flushed via Request.TrailerFunc. TrailerFunc is called twice per Do (once
// to announce the trailer key names in the initial HEADERS frame, once to
// send the values), so it must return the same key set both times.
func Example_requestTrailers() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	req := client.POST("/upload", []byte("payload-bytes"))
	// A checksum trailer: the key ("x-checksum") is stable across both
	// calls; the value may differ once computed after the body is sent.
	req.TrailerFunc = func() []conn.HeaderField {
		return []conn.HeaderField{
			client.H("x-checksum", "sha256:deadbeef"),
		}
	}

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, req, resp); err != nil {
		log.Fatal(err)
	}
}

// Example_errorsIs classifies a request failure: errors.Is matches a client
// sentinel (here ErrClosed), while errors.As unwraps a typed
// *client.StreamResetError to inspect the RST_STREAM code the peer sent.
func Example_errorsIs() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	derr := c.Do(ctx, client.GET("/"), resp)

	switch {
	case derr == nil:
		fmt.Printf("ok: status %d\n", resp.Status)
	case errors.Is(derr, client.ErrClosed):
		fmt.Println("client was closed; stop issuing requests")
	default:
		var rst *client.StreamResetError
		if errors.As(derr, &rst) {
			fmt.Printf("peer reset the stream with code %v\n", rst.Code)
			_ = time.Second // retry/backoff would go here
		} else {
			fmt.Printf("request failed: %v\n", derr)
		}
	}
}
