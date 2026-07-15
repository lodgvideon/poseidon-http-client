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
	parts := strings.Split(list, ",")
	out := make([]interopServer, 0, len(parts))
	for _, e := range parts {
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

// interopChaChaAddr is the ChaCha20-only server the ChaCha interop test dials
// (H3_INTEROP_CHACHA_ADDR, e.g. "h3nginxchacha:443"). Empty ⇒ the test is skipped;
// it runs only under docker-compose.chacha.yml, not the default matrix.
func interopChaChaAddr() string { return os.Getenv("H3_INTEROP_CHACHA_ADDR") }

// TestInteropChaCha_GET dials a server pinned to TLS_CHACHA20_POLY1305_SHA256 and
// asserts the from-scratch client completes the handshake with ChaCha20-Poly1305
// packet protection (RFC 9001 §5.3 AEAD, §5.4.4 header protection), gets a 200,
// and that the negotiated cipher suite really is ChaCha20-Poly1305 — proving the
// ChaCha path on the wire, not only in unit round-trips. Skipped unless
// H3_INTEROP_CHACHA_ADDR is set (the ChaCha compose file sets it).
func TestInteropChaCha_GET(t *testing.T) {
	addr := interopChaChaAddr()
	if addr == "" {
		t.Skip("H3_INTEROP_CHACHA_ADDR unset; run via test/integration/http3/docker-compose.chacha.yml")
	}
	client, host := dialServer(t, addr)
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
	if cs := client.ConnectionState().CipherSuite; cs != tls.TLS_CHACHA20_POLY1305_SHA256 {
		t.Fatalf("negotiated cipher suite = %#x, want TLS_CHACHA20_POLY1305_SHA256 (%#x)", cs, tls.TLS_CHACHA20_POLY1305_SHA256)
	}
	t.Logf("ChaCha20 GET / -> status=%d, cipher=TLS_CHACHA20_POLY1305_SHA256, %d bytes", resp.Status, len(body))
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
// across ~900 datagrams that span Caddy/quic-go's assured key update at ~packet
// 100 (RFC 9001 §6, Key Phase 0→1). Against that server it is the end-to-end proof
// that key update (G.6k) plus the batch drain (G.6j) sustain a large transfer:
// before key update the client held only the phase-0 keys and dropped every packet
// after the update, freezing largestRecv and stalling the transfer. Servers that
// do not key-update mid-transfer (nginx, aioquic) exercise it as a plain large
// reassembly. The fixture is generated by the compose init service (see
// docker-compose.yml).
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

// expectConnError drives a request whose response the fault server corrupts at
// the frame level, and asserts the client raises a fatal HTTP/3 connection error
// (ErrH3Control — it sends CONNECTION_CLOSE with the right code) rather than
// hanging or returning a partial response as success.
func expectConnError(t *testing.T, path string) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		_, _, err := do(t, client, &Request{Method: "GET", Scheme: "https", Authority: host, Path: path})
		if !errors.Is(err, ErrH3Control) {
			t.Fatalf("GET %s = %v (%T), want a connection error (ErrH3Control)", path, err, err)
		}
		t.Logf("GET %s -> connection error, as required", path)
	})
}

// TestFault_DataBeforeHeaders: a DATA frame before any HEADERS on the response
// stream is an invalid frame sequence — H3_FRAME_UNEXPECTED (RFC 9114 §4.1).
func TestFault_DataBeforeHeaders(t *testing.T) { expectConnError(t, "/data-before-headers") }

// TestFault_SettingsOnRequestStream: SETTINGS is control-stream-only; on a
// request stream it is H3_FRAME_UNEXPECTED (RFC 9114 §7.2.4).
func TestFault_SettingsOnRequestStream(t *testing.T) { expectConnError(t, "/settings-on-request") }

// TestFault_TruncatedHeaders: a HEADERS frame whose declared length is never
// satisfied before the stream's clean FIN is a truncated frame — H3_FRAME_ERROR
// (RFC 9114 §7.1).
func TestFault_TruncatedHeaders(t *testing.T) { expectConnError(t, "/trunc-headers") }

// TestFault_QpackDynamicRef: the client now advertises a non-zero
// SETTINGS_QPACK_MAX_TABLE_CAPACITY, so a WELL-FORMED dynamic reference decodes
// (that live path is exercised end-to-end by the real interop matrix — Caddy /
// nginx / aioquic all use dynamic QPACK for response headers once capacity > 0 —
// e.g. TestInterop_GET). This fault stays an error because its reference is
// MALFORMED, not merely dynamic: the field section is prefix RIC=0, Base=0, then a
// dynamic indexed field line (0x80), which resolves to Base-relative index 0 of an
// empty acknowledged window — an out-of-range reference the decoder must reject as
// QPACK_DECOMPRESSION_FAILED (a connection error, RFC 9204 §2.2), regardless of
// capacity, not misdecode and not hang.
func TestFault_QpackDynamicRef(t *testing.T) { expectConnError(t, "/qpack-dynamic-ref") }

// TestFault_QpackRequiredInsertCount: a field-section prefix with a non-zero
// Required Insert Count that exceeds the entries the server actually inserted (the
// fault server runs no encoder, so our table's insert count is 0).
//
// Q4 changed this behavior: the client now advertises SETTINGS_QPACK_BLOCKED_STREAMS
// > 0, so a Required Insert Count ahead of the insert count no longer fails fast —
// the decode PARKS until the encoder stream catches up (RFC 9204 §2.1.3), bounded by
// the request ctx. Against this fault server (which never sends the promised insert)
// the request would now fail on the ctx deadline, not with an immediate
// QPACK_DECOMPRESSION_FAILED, so expectConnError no longer applies. The interop-level
// coverage of the RIC-ahead wait — a fault server that sends the section AHEAD of its
// encoder-stream insert, then delivers the insert so the decode unblocks — is a Q4
// follow-up; the unit tests (TestConformance_RFC9204_Sec213_*) fully exercise the
// wait/wake and the ctx-bounded park.
func TestFault_QpackRequiredInsertCount(t *testing.T) {
	t.Skip("Q4: RIC-ahead now parks until the encoder catches up; fault-server RIC-ahead scenario is a follow-up")
}

// TestFault_StopSending checks that when the server aborts reading the request
// with STOP_SENDING while the client is still sending its body, the client stops
// sending but still reads the response on the stream's independent receive side
// (RFC 9114 §4.1) — it must not fail the request. The body (2 MiB) is well past
// the server's initial receive windows (quic-go's are 512 KiB stream, 768 KiB
// conn, and it never grants more here since /stop-sending never reads the body),
// so the client is provably still mid-send when STOP_SENDING arrives — the abort
// is load-bearing (without it the send stalls to the deadline), so the path is
// genuinely exercised, not a body that fit in one go.
func TestFault_StopSending(t *testing.T) {
	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		resp, _, err := do(t, client, &Request{
			Method: "POST", Scheme: "https", Authority: host, Path: "/stop-sending",
			Body: make([]byte, 2<<20),
		})
		if err != nil {
			t.Fatalf("Do = %v, want the response read despite STOP_SENDING", err)
		}
		if resp.Status != 200 {
			t.Fatalf("status = %d, want 200", resp.Status)
		}
		t.Logf("STOP_SENDING mid-body -> response still read, status=%d", resp.Status)
	})
}
