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

// interopServer is one server the interop matrix dials.
type interopServer struct {
	name string
	addr string
}

// interopAddr is the single server the interop tests dial when no matrix is
// configured (default localhost:4433, overridden by H3_INTEROP_ADDR).
func interopAddr() string {
	if a := os.Getenv("H3_INTEROP_ADDR"); a != "" {
		return a
	}
	return "localhost:4433"
}

// interopServers returns the servers under test. H3_INTEROP_ADDRS is a
// comma-separated list of "name=host:port" pairs (e.g.
// "caddy=h3caddy:443,nginx=h3nginx:443"), so the same scenarios run against
// several real HTTP/3 stacks as subtests. It falls back to the single
// H3_INTEROP_ADDR server when the matrix variable is unset (the loss harness),
// so both runners share one test file.
func interopServers() []interopServer {
	list := os.Getenv("H3_INTEROP_ADDRS")
	if list == "" {
		return []interopServer{{name: "default", addr: interopAddr()}}
	}
	var out []interopServer
	for _, e := range strings.Split(list, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		name, addr, ok := strings.Cut(e, "=")
		if !ok {
			addr = name // a bare "host:port" with no name
		}
		out = append(out, interopServer{name: name, addr: addr})
	}
	if len(out) == 0 {
		return []interopServer{{name: "default", addr: interopAddr()}}
	}
	return out
}

// dialServer opens an HTTP/3 connection to addr, retrying until the server is
// serving or a deadline passes — nginx is gated only on container start (its
// image has no curl/wget for a healthcheck), so the first dials can race its
// startup.
func dialServer(t *testing.T, addr string) (*Client, string) {
	t.Helper()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	cfg := &tls.Config{ServerName: host, InsecureSkipVerify: true}

	deadline := time.Now().Add(20 * time.Second)
	var client *Client
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err = Dial(ctx, addr, cfg)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Dial(%s): %v", addr, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Exercise the graceful CONNECTION_CLOSE (RFC 9114 §8.1 H3_NO_ERROR) against
	// the live server at the end of every interop test.
	t.Cleanup(func() { _ = client.Close() })
	return client, host
}

// forEachInteropServer runs body against every configured server as a named
// subtest, so a failure names the server (e.g. TestInterop_GET/nginx).
func forEachInteropServer(t *testing.T, body func(t *testing.T, client *Client, host string)) {
	t.Helper()
	for _, srv := range interopServers() {
		srv := srv
		t.Run(srv.name, func(t *testing.T) {
			client, host := dialServer(t, srv.addr)
			body(t, client, host)
		})
	}
}

// TestInterop_GET performs a GET and asserts a 200 response.
func TestInterop_GET(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		t.Logf("GET / -> status=%d body=%q", resp.Status, string(body))
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
	})
}

// TestInterop_POST sends a request body in a DATA frame and asserts the server
// accepts it (a malformed body framing would make the server reset the stream).
func TestInterop_POST(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, _, err := client.Do(context.Background(), &Request{
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
	})
}

// TestInterop_LargeResponse fetches a 16 KiB body that spans many DATA/STREAM
// frames across multiple datagrams, exercising receive reassembly at scale.
func TestInterop_LargeResponse(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/big.txt"})
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
	})
}

// TestInterop_HugeResponse fetches a 1 MiB body — hundreds of DATA/STREAM frames
// across ~900 datagrams that span the server's assured key update at ~packet 100
// (RFC 9001 §6, Key Phase 0→1). It is the end-to-end proof that key update
// (G.6k) plus the batch drain (G.6j) sustain a large transfer: before key update
// the client held only the phase-0 keys and dropped every packet after the
// update, freezing largestRecv and stalling the transfer. The fixture is
// generated by the compose init service (see docker-compose.yml).
func TestInterop_HugeResponse(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/huge.txt"})
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		want := []byte(strings.Repeat("0123456789abcdef", 65536)) // 1 MiB
		if !bytes.Equal(body, want) {
			t.Fatalf("huge body mismatch: got %d bytes, want %d", len(body), len(want))
		}
		t.Logf("GET /huge.txt -> status=%d, %d bytes reassembled across the key-update boundary", resp.Status, len(body))
	})
}
