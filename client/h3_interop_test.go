//go:build interop

package client

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// h3InteropServer is one HTTP/3 server the client interop test dials.
type h3InteropServer struct {
	name string
	addr string
}

// h3InteropServers mirrors the http3 package's interop harness: H3_INTEROP_ADDRS
// is a comma-separated list of "name=host:port" pairs so the same scenario runs
// against several real HTTP/3 stacks (Caddy/nginx/aioquic). It falls back to
// H3_INTEROP_ADDR, then localhost:4433.
func h3InteropServers() []h3InteropServer {
	if list := os.Getenv("H3_INTEROP_ADDRS"); list != "" {
		var out []h3InteropServer
		for _, e := range strings.Split(list, ",") {
			e = strings.TrimSpace(e)
			if e == "" {
				continue
			}
			name, addr, ok := strings.Cut(e, "=")
			if !ok {
				addr = name
			}
			out = append(out, h3InteropServer{name: name, addr: addr})
		}
		if len(out) > 0 {
			return out
		}
	}
	addr := os.Getenv("H3_INTEROP_ADDR")
	if addr == "" {
		addr = "localhost:4433"
	}
	return []h3InteropServer{{name: "default", addr: addr}}
}

// h3DialTimeout is how long the interop test retries the first request before
// giving up, overridable via H3_DIAL_TIMEOUT (seconds) for a slow-to-listen
// server.
func h3DialTimeout() time.Duration {
	if v := os.Getenv("H3_DIAL_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 20 * time.Second
}

// h3InteropHost is the SNI/authority for a server address.
func h3InteropHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// h3InteropDoUntilUp issues req against c, retrying until the server is
// listening (nginx is gated only on container start) or the dial budget runs
// out. It returns the final error so the caller can assert on it.
func h3InteropDoUntilUp(t *testing.T, c *Client, req *Request, resp *Response, per time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(h3DialTimeout())
	for {
		resp.Reset()
		ctx, cancel := context.WithTimeout(context.Background(), per)
		err := c.Do(ctx, req, resp)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestInterop_ClientH3_GetStatusBody drives a real HTTP/3 GET through the public
// buffered Client.Do over TransportH3 against each configured live server, and
// asserts the status + body come back and that metrics/hooks fire identically to
// the H2 buffered path. The -run filter in the interop runner (TestInterop)
// picks this up alongside the http3 package's own interop tests.
func TestInterop_ClientH3_GetStatusBody(t *testing.T) {
	for _, srv := range h3InteropServers() {
		srv := srv
		t.Run(srv.name, func(t *testing.T) {
			host := h3InteropHost(srv.addr)
			var completed, gotStatus atomic.Int32
			hooks := &Hooks{
				OnRequestComplete: func(e RequestCompleteEvent) {
					completed.Add(1)
					gotStatus.Store(int32(e.Status))
				},
			}
			c, err := NewH3Client(srv.addr,
				&tls.Config{ServerName: host, InsecureSkipVerify: true},
				WithHooks(hooks))
			require.NoErrorf(t, err, "NewH3Client(%s)", srv.addr)
			t.Cleanup(func() { _ = c.Close() })

			var resp Response
			err = h3InteropDoUntilUp(t, c, &Request{
				Method:    "GET",
				Scheme:    "https",
				Authority: host,
				Path:      "/",
				BodyMode:  BodyBuffer,
			}, &resp, 10*time.Second)

			require.NoError(t, err, "Do(GET /)")
			assert.Equal(t, 200, resp.Status, "status from a live HTTP/3 server")
			assert.NotEmpty(t, resp.Body, "GET / body is empty, want the server's canned response")
			assert.GreaterOrEqual(t, completed.Load(), int32(1),
				"OnRequestComplete never fired: the H3 buffered path must drive the same "+
					"lifecycle hooks as H2")
			assert.EqualValues(t, 200, gotStatus.Load(),
				"OnRequestComplete reported the wrong status")
			assert.GreaterOrEqual(t, c.Metrics().Counters.RequestsSucceeded.Load(), int64(1),
				"RequestsSucceeded counter did not increment on the H3 path")
			t.Logf("H3 GET / via client.Do -> status=%d bodylen=%d", resp.Status, len(resp.Body))
		})
	}
}

// TestInterop_ClientH3_BBR_HugeTransfer drives a 1 MiB HTTP/3 download through the
// public Client.Do with BBR congestion control opted in via
// ClientOptions.H3ConnOptions, proving the opt-in BBR controller completes a real
// multi-RTT transfer over the wire against each live server — the on-the-wire
// evidence that complements BBR's unit tests (#203).
func TestInterop_ClientH3_BBR_HugeTransfer(t *testing.T) {
	for _, srv := range h3InteropServers() {
		srv := srv
		t.Run(srv.name, func(t *testing.T) {
			host := h3InteropHost(srv.addr)
			c, err := NewClient(ClientOptions{
				Addr:      srv.addr,
				Transport: TransportH3,
				TLSConfig: &tls.Config{ServerName: host, InsecureSkipVerify: true},
				H3ConnOptions: []quic.ConnOption{
					quic.WithCongestionControl(quic.CCBBR),
				},
			})
			require.NoError(t, err, "NewClient(H3+BBR)")
			t.Cleanup(func() { _ = c.Close() })

			var resp Response
			err = h3InteropDoUntilUp(t, c, &Request{
				Method:    "GET",
				Scheme:    "https",
				Authority: host,
				Path:      "/huge.txt",
				BodyMode:  BodyBuffer,
			}, &resp, 20*time.Second)

			require.NoError(t, err, "Do(GET /huge.txt) with BBR")
			assert.Equal(t, 200, resp.Status, "status for the 1 MiB throughput fixture")
			// huge.txt is the 1 MiB throughput fixture — a full BBR transfer must
			// deliver all of it (hundreds of packets, enough to leave Startup).
			assert.GreaterOrEqualf(t, len(resp.Body), 1<<20,
				"body = %d bytes, want the full 1 MiB huge.txt (BBR transfer truncated?)",
				len(resp.Body))
			t.Logf("H3+BBR GET /huge.txt -> status=%d bodylen=%d", resp.Status, len(resp.Body))
		})
	}
}
