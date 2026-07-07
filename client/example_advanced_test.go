package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ExampleProxyTLSDialer routes traffic through an HTTP CONNECT proxy and then
// performs TLS + ALPN h2 to the target over the tunnel. Put credentials in the
// proxy URL for Basic auth; use an "https://" proxy URL to TLS-wrap the proxy
// hop itself.
func ExampleProxyTLSDialer() {
	proxyURL, err := url.Parse("http://user:pass@proxy.internal:8080")
	if err != nil {
		log.Fatal(err)
	}
	dialer := &conn.ProxyTLSDialer{
		ProxyURL:  proxyURL,
		TLSConfig: &tls.Config{ServerName: "api.example.com"},
	}
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), client.GET("/"), &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// Example_h2c speaks cleartext HTTP/2 with prior knowledge (H2C): no TLS, no
// ALPN. Use a PlaintextDialer and set the default scheme to "http".
func Example_h2c() {
	c, err := client.NewSingleConnClient("localhost:8080",
		&conn.PlaintextDialer{},
		client.WithDefaultScheme("http"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), client.GET("/healthz"), &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// Example_alpnTransport dials with a FlexDialer that offers both h2 and
// http/1.1 in ALPN, then permanently routes every request over whichever
// protocol the server negotiated — so the same client code works against
// HTTP/2 and HTTP/1.1 origins.
func Example_alpnTransport() {
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "api.example.com:443",
		Transport: client.TransportALPN,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.FlexDialer{Config: &tls.Config{ServerName: "api.example.com"}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), client.GET("/"), &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleDNSResolver builds a managed client with DNS-based service discovery:
// the resolver re-resolves A/AAAA records on its TTL and the pool follows the
// resolved set, dialing new backends and draining removed ones.
func ExampleDNSResolver() {
	resolver := client.DNSResolver("api.example.com", 443, client.DNSOptions{
		TTL:        30 * time.Second,
		PreferIPv4: true,
	})
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}}
	c, err := client.NewManagedClient(resolver, dialer,
		client.WithSelector(client.RoundRobin()))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var resp client.Response
	if err := c.Do(context.Background(), client.GET("/"), &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleHash pins each key to a backend for session affinity. The key
// function derives a string from the request (here, the path); Hash returns an
// error only when the key function is nil.
func ExampleHash() {
	sel, err := client.Hash(func(pc client.PickContext) string {
		if pc.Request != nil {
			return pc.Request.Path
		}
		return ""
	})
	if err != nil {
		log.Fatal(err)
	}
	resolver := client.StaticResolver(
		client.Address{Host: "10.0.0.1", Port: 443},
		client.Address{Host: "10.0.0.2", Port: 443},
	)
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}}
	c, err := client.NewManagedClient(resolver, dialer, client.WithSelector(sel))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	fmt.Println("hash selector ready")
}

// ExampleRandom spreads load uniformly across the resolved set. Pass your own
// *rand.Rand (seeded deterministically here) so tests are reproducible.
func ExampleRandom() {
	sel := client.Random(rand.New(rand.NewSource(1))) //nolint:gosec // not security-sensitive
	resolver := client.StaticResolver(
		client.Address{Host: "10.0.0.1", Port: 443},
		client.Address{Host: "10.0.0.2", Port: 443},
	)
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}}
	c, err := client.NewManagedClient(resolver, dialer, client.WithSelector(sel))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	fmt.Println("random selector ready")
}

// Example_poolLifecycle pre-warms a pool, inspects live stats, and drains it
// gracefully. Warmup avoids paying dial latency on the first requests;
// PoolStats feeds a /debug endpoint or metrics scrape; Shutdown stops new
// streams and lets in-flight ones finish.
func Example_poolLifecycle() {
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}}
	c, err := client.NewPoolClient("api.example.com:443", dialer,
		client.PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 100})
	if err != nil {
		log.Fatal(err)
	}

	c.Warmup(4) // pre-dial up to 4 conns in the background

	st := c.PoolStats()
	fmt.Printf("conns=%d inflight=%d waiters=%d\n",
		st.ActiveConns, st.InFlightStreams, st.Waiters)

	// Single-conn honours the timeout; pool/managed behave like Close.
	_ = c.Shutdown(5 * time.Second)
}

// ExampleRequest_disableDecompression fetches the raw compressed body instead
// of transparently decoding it — useful when proxying bytes through untouched.
// It also shows matching the ErrBodyTooLarge guard.
func ExampleRequest_disableDecompression() {
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "api.example.com"}}
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	req := client.GET("/data.json")
	req.DisableDecompression = true // deliver raw gzip/deflate bytes

	var resp client.Response
	err = c.Do(context.Background(), req, &resp)
	if errors.Is(err, client.ErrBodyTooLarge) {
		log.Print("body exceeded MaxResponseBodySize")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	enc, _ := resp.HeaderString("content-encoding")
	fmt.Println(resp.Status, enc)
}

// Example_extendedConnect opens an RFC 8441 extended-CONNECT tunnel (e.g.
// WebSocket) over a single HTTP/2 stream. Method MUST be CONNECT and the
// server MUST have advertised SETTINGS_ENABLE_CONNECT_PROTOCOL=1. Read the
// downstream via the StreamResponse; attach a Request.BodyReader to send
// bytes upstream.
func Example_extendedConnect() {
	dialer := &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}}
	c, err := client.NewSingleConnClient("example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	req := &client.Request{
		Method:   "CONNECT",
		Protocol: "websocket",
		Path:     "/chat",
		BodyMode: client.BodyStream,
	}
	var sr client.StreamResponse
	if err := c.DoStream(context.Background(), req, &sr); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sr.Close() }()
	fmt.Println("tunnel established")
}
