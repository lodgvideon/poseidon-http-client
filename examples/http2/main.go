// Command http2-example issues a single HTTP/2 GET with the poseidon client.
// TransportSingleConn (the default) negotiates h2 over TLS and reuses one
// connection with automatic redial.
//
//	go run ./examples/http2
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewSingleConnClient(
		"www.cloudflare.com:443",
		&conn.TLSDialer{Config: &tls.Config{
			ServerName: "www.cloudflare.com",
			NextProtos: []string{"h2"},
		}},
	)
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Response is caller-owned and reusable; Reset() clears it between requests.
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/2 %d — %d bytes\n", resp.Status, len(resp.Body))
}
