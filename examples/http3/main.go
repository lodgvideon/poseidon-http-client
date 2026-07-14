// Command http3-example issues a single HTTP/3 GET over QUIC with the poseidon
// client, and shows how to opt into BBR congestion control.
//
//	go run ./examples/http3
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

func main() {
	// The simple path: one QUIC connection, buffered response.
	//
	//	c, err := client.NewH3Client("www.cloudflare.com:443",
	//	    &tls.Config{ServerName: "www.cloudflare.com"})
	//
	// Below uses NewClient so we can also select BBR congestion control.
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "www.cloudflare.com:443",
		Transport: client.TransportH3,
		TLSConfig: &tls.Config{ServerName: "www.cloudflare.com"},
		H3ConnOptions: []quic.ConnOption{
			quic.WithCongestionControl(quic.CCBBR), // default is NewReno
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
	fmt.Printf("HTTP/3 %d — %d bytes\n", resp.Status, len(resp.Body))
}
