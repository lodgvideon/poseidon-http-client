//go:build interop

package http3

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// interopAddr is the server the interop tests dial (default localhost:4433,
// overridden to h3caddy:443 by the docker-compose runner).
func interopAddr() string {
	if a := os.Getenv("H3_INTEROP_ADDR"); a != "" {
		return a
	}
	return "localhost:4433"
}

// dialInterop opens an HTTP/3 connection to the interop server. See
// test/integration/http3 for the harness (make h3-interop). Build-tagged
// "interop"; excluded from the default build because it needs a live server.
func dialInterop(t *testing.T) (*Client, string) {
	t.Helper()
	addr := interopAddr()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	client, err := Dial(ctx, addr, &tls.Config{ServerName: host, InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}
	return client, host
}

// TestInterop_GET performs a GET and asserts a 200 response.
func TestInterop_GET(t *testing.T) {
	client, host := dialInterop(t)
	resp, body, err := client.Do(&Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Logf("GET / -> status=%d body=%q", resp.Status, string(body))
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
}

// TestInterop_POST sends a request body in a DATA frame and asserts the server
// accepts it (a malformed body framing would make the server reset the stream).
func TestInterop_POST(t *testing.T) {
	client, host := dialInterop(t)
	resp, _, err := client.Do(&Request{
		Method: "POST", Scheme: "https", Authority: host, Path: "/",
		Body: []byte("hello from a from-scratch http/3 client"),
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Logf("POST / -> status=%d", resp.Status)
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
}

// TestInterop_LargeResponse fetches a 16 KiB body that spans many DATA/STREAM
// frames across multiple datagrams, exercising receive reassembly at scale.
func TestInterop_LargeResponse(t *testing.T) {
	client, host := dialInterop(t)
	resp, body, err := client.Do(&Request{Method: "GET", Scheme: "https", Authority: host, Path: "/big.txt"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	want := []byte(strings.Repeat("0123456789abcdef", 1024))
	if !bytes.Equal(body, want) {
		t.Fatalf("large body mismatch: got %d bytes, want %d", len(body), len(want))
	}
	t.Logf("GET /big.txt -> status=%d, %d bytes reassembled correctly", resp.Status, len(body))
}
