//go:build interop

package http3

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"testing"
	"time"
)

// TestInterop_GET dials a real HTTP/3 server (see interop/README.md) and
// performs a GET, asserting a 200 response. The server address comes from
// H3_INTEROP_ADDR (default localhost:4433). Build-tagged "interop"; excluded
// from the default build because it needs a live server.
func TestInterop_GET(t *testing.T) {
	addr := os.Getenv("H3_INTEROP_ADDR")
	if addr == "" {
		addr = "localhost:4433"
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := Dial(ctx, addr, &tls.Config{ServerName: host, InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}

	resp, body, err := client.Do(&Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Logf("HTTP/3 response: status=%d body=%q", resp.Status, string(body))
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
}
