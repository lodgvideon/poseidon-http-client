// Command http1-example issues a single HTTP/1.1 request with the poseidon
// client. HTTP/1.1 is reached by pinning the transport to TransportH1SingleConn
// and offering only the "http/1.1" ALPN token, so the connection never upgrades
// to HTTP/2.
//
//	go run ./examples/http1
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
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "example.com:443",
		Transport: client.TransportH1SingleConn,
		ConnOpts: conn.ConnOptions{
			// H1TLSDialer offers only http/1.1, so an h2-capable server cannot
			// select h2. (conn.TLSDialer asserts h2 and is refused here.)
			Dialer: &conn.H1TLSDialer{Config: &tls.Config{
				ServerName: "example.com",
				MinVersion: tls.VersionTLS12,
			}},
		},
	})
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/1.1 %d — %d bytes\n", resp.Status, len(resp.Body))
}
