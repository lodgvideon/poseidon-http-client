//go:build interop

package http3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
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

	// Default 20s; the fault harness raises it (H3_DIAL_TIMEOUT) because its server
	// compiles quic-go before it listens. Kept low for the matrix so a genuinely
	// down server fails per-subtest rather than blowing the whole binary timeout.
	deadline := time.Now().Add(dialTimeout())
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

// do issues one request with a bounded per-request timeout, so a single stalled
// exchange fails that subtest quickly (with the server named) instead of hanging
// the whole binary until the outer -timeout kills it.
func do(t *testing.T, client *Client, req *Request) (*Response, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Do(ctx, req)
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

// dialTimeout is how long dialServer retries before giving up, overridable via
// H3_DIAL_TIMEOUT (seconds) for a server that is slow to start listening.
func dialTimeout() time.Duration {
	if v := os.Getenv("H3_DIAL_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 20 * time.Second
}

// findHeader returns the value of the named response header (case-insensitive)
// and whether it was present.
func findHeader(headers []hpack.HeaderField, name string) (string, bool) {
	for _, h := range headers {
		if strings.EqualFold(string(h.Name), name) {
			return string(h.Value), true
		}
	}
	return "", false
}

// TestInterop_GET performs a GET and asserts a 200 with a non-empty body.
func TestInterop_GET(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		if len(body) == 0 {
			t.Fatal("GET / body is empty, want the server's canned response")
		}
		t.Logf("GET / -> status=%d body=%q", resp.Status, string(body))
	})
}

// TestInterop_POST sends a request body in a DATA frame and asserts the server
// accepts it (a malformed body framing would make the server reset the stream).
func TestInterop_POST(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, _, err := do(t, client, &Request{
			Method: "POST", Scheme: "https", Authority: host, Path: "/",
			Body: []byte("hello from a from-scratch http/3 client"),
		})
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		t.Logf("POST / -> status=%d", resp.Status)
	})
}

// TestInterop_LargeResponse fetches a 16 KiB body that spans many DATA/STREAM
// frames across multiple datagrams, exercising receive reassembly at scale, and
// checks the declared Content-Length matches the reassembled length (A4).
func TestInterop_LargeResponse(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/big.txt"})
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
		if cl, ok := findHeader(resp.Headers, "content-length"); !ok || cl != strconv.Itoa(len(want)) {
			t.Fatalf("content-length = %q,%v, want %d (must match the reassembled body)", cl, ok, len(want))
		}
		t.Logf("GET /big.txt -> status=%d, %d bytes, content-length matches", resp.Status, len(body))
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
		resp, body, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/huge.txt"})
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

// TestInterop_HEAD checks a HEAD request returns the response head with no body
// (RFC 9114 §4.1): the server sends HEADERS with a Content-Length but no DATA,
// and the client must not treat the absent body as a Content-Length mismatch.
func TestInterop_HEAD(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, body, err := do(t, client, &Request{Method: "HEAD", Scheme: "https", Authority: host, Path: "/"})
		if err != nil {
			t.Fatalf("Do(HEAD): %v", err)
		}
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		if len(body) != 0 {
			t.Fatalf("HEAD body = %d bytes, want 0", len(body))
		}
		// The response must carry a Content-Length (both servers set it on HEAD),
		// so this actually exercises the mismatch-tolerance: a non-zero declared
		// length with an absent body must not be rejected as ErrH3Message. A future
		// matrix server that legally omits CL on HEAD (RFC 9110 §9.3.2) would fail
		// here loudly — never silently pass — and can be gated then.
		cl, ok := findHeader(resp.Headers, "content-length")
		if !ok || cl == "0" {
			t.Fatalf("HEAD response should carry a non-zero Content-Length; headers=%v", resp.Headers)
		}
		t.Logf("HEAD / -> status=%d, content-length=%s, body empty", resp.Status, cl)
	})
}

// TestInterop_StatusCodes checks the client passes non-2xx responses through
// unchanged (RFC 9114 §4.1: HTTP status is application data): bodyless statuses
// (204, 304 — the no-content carve-outs) arrive with an empty body, and 4xx/5xx
// arrive with their body and a matching Content-Length (A4).
func TestInterop_StatusCodes(t *testing.T) {
	cases := []struct {
		code     int
		wantBody bool
	}{
		{204, false}, {304, false}, {404, true}, {500, true},
	}
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		for _, tc := range cases {
			path := "/status/" + strconv.Itoa(tc.code)
			resp, body, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: path})
			if err != nil {
				t.Fatalf("Do(GET %s): %v", path, err)
			}
			if resp.Status != tc.code {
				t.Fatalf("GET %s -> status %d, want %d", path, resp.Status, tc.code)
			}
			if tc.wantBody {
				if len(body) == 0 {
					t.Fatalf("GET %s -> empty body, want non-empty", path)
				}
				if cl, ok := findHeader(resp.Headers, "content-length"); ok && cl != strconv.Itoa(len(body)) {
					t.Fatalf("GET %s -> content-length %s != body %d", path, cl, len(body))
				}
			} else if len(body) != 0 {
				t.Fatalf("GET %s -> %d-byte body, want empty (no-content status)", path, len(body))
			}
			t.Logf("GET %s -> status=%d, %d-byte body (passed through)", path, resp.Status, len(body))
		}
	})
}

// TestInterop_SequentialReuse issues many requests on one connection, so each
// opens and retires a stream: it exercises connection reuse, monotonic stream-id
// allocation, and the stream-map retirement path at scale against a real server.
//
// n must stay above both servers' concurrent-stream credit windows (quic-go
// default 100, nginx http3_max_concurrent_streams default 128): a client that
// ignored the server's MAX_STREAMS frames would fail deterministically at
// ~request 129 with ErrTooManyStreams. It also crosses the 1→2-byte stream-id
// varint boundary (id 64, ~request 17). The retirement *bound* — the map staying
// small — is unit-tested in the quic package (#166); c.streams is not reachable
// from here, so this is a functional-scale guard, not a memory-leak assertion.
func TestInterop_SequentialReuse(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		const n = 300
		for i := 0; i < n; i++ {
			resp, _, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
			if err != nil {
				t.Fatalf("request %d/%d: %v", i+1, n, err)
			}
			if resp.Status != 200 {
				t.Fatalf("request %d/%d: status %d, want 200", i+1, n, resp.Status)
			}
		}
		t.Logf("%d sequential requests on one connection: all 200", n)
	})
}

// TestFault_ServerReset checks that when the server aborts the request stream
// with RESET_STREAM carrying H3_REQUEST_REJECTED (RFC 9114 §8.1), the client
// surfaces a *StreamResetError with that code, classified retryable so the caller
// can safely retry on a new connection (§4.1.1) — rather than returning a
// truncated body as success. It runs only against the fault server (which resets
// every request); its TestFault_ name keeps it out of the `-run TestInterop`
// matrix, where Caddy and nginx would answer normally.
func TestFault_ServerReset(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		_, _, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: "/"})
		var rst *StreamResetError
		if !errors.As(err, &rst) {
			t.Fatalf("Do = %v (%T), want *StreamResetError", err, err)
		}
		if rst.Code != H3RequestRejected {
			t.Fatalf("reset code = %#x, want H3_REQUEST_REJECTED (%#x)", rst.Code, H3RequestRejected)
		}
		if !rst.Retryable() {
			t.Fatal("a H3_REQUEST_REJECTED reset must be Retryable()")
		}
		t.Logf("server reset -> StreamResetError code=%#x retryable=%v", rst.Code, rst.Retryable())
	})
}
