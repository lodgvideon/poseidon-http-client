package client_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ExampleClient_Do shows the canonical unary request loop: allocate one
// Response, call resp.Reset() before each reuse, and issue requests against a
// single reused Request. Reusing both structs keeps the hot path allocation-free.
func ExampleClient_Do() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewClient(client.ClientOptions{
		Addr:     "api.example.com:443",
		ConnOpts: conn.ConnOptions{Dialer: dialer},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := &client.Request{Method: "GET", Path: "/v1/health", WantBody: true}

	// Allocate the Response once and Reset() it before every reuse so the
	// backing Headers/Body arrays are recycled instead of reallocated.
	var resp client.Response
	for i := 0; i < 3; i++ {
		resp.Reset()
		if err := c.Do(ctx, req, &resp); err != nil {
			log.Fatal(err)
		}
		fmt.Println(resp.Status, len(resp.Body))
	}
}

// ExampleNewSingleConnClient shows the focused single-connection constructor:
// one HTTP/2 connection with auto-redial, lazy-dialed on the first request.
func ExampleNewSingleConnClient() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/v1/things"), resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleNewPoolClient shows the pooled constructor: several connections to one
// address, each multiplexing many concurrent streams, with idle eviction.
func ExampleNewPoolClient() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewPoolClient("api.example.com:443", dialer, client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		IdleTimeout:       30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/v1/things"), resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleGET shows the GET sugar plus WithHeaders for a one-off request.
func ExampleGET() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.GET("/v1/things").WithHeaders(
		client.H("accept", "application/json"),
	)
	resp := &client.Response{}
	if err := c.Do(ctx, req, resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExamplePOST shows the POST sugar carrying a request body.
func ExamplePOST() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.POST("/v1/things", []byte(`{"name":"widget"}`)).WithHeaders(
		client.H("content-type", "application/json"),
	)
	resp := &client.Response{}
	if err := c.Do(ctx, req, resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleH shows building a prebuilt header slice with the H helper and
// attaching it to a reused Request literal.
func ExampleH() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Prebuild the header slice once; H lower-cases the name per RFC 7540.
	headers := []client.HeaderField{
		client.H("accept", "application/json"),
		client.H("x-api-key", "secret-token"),
	}
	req := &client.Request{
		Method:   "GET",
		Path:     "/v1/things",
		Headers:  headers,
		WantBody: true,
	}

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, req, resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Status)
}

// ExampleResponse_Header shows reading a single response header. The returned
// bytes alias Response-owned memory valid only until the next Reset; use
// HeaderString (or copy) to retain past then.
func ExampleResponse_Header() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/v1/things"), resp); err != nil {
		log.Fatal(err)
	}

	if ct, ok := resp.Header("content-type"); ok {
		fmt.Printf("content-type: %s\n", ct)
	}
}
