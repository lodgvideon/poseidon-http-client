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

// TestInterop_ClientH3_GetStatusBody drives a real HTTP/3 GET through the public
// buffered Client.Do over TransportH3 against each configured live server, and
// asserts the status + body come back and that metrics/hooks fire identically to
// the H2 buffered path. The -run filter in the interop runner (TestInterop)
// picks this up alongside the http3 package's own interop tests.
func TestInterop_ClientH3_GetStatusBody(t *testing.T) {
	for _, srv := range h3InteropServers() {
		srv := srv
		t.Run(srv.name, func(t *testing.T) {
			host, _, err := net.SplitHostPort(srv.addr)
			if err != nil {
				host = srv.addr
			}

			var completed, gotStatus atomic.Int32
			hooks := &Hooks{
				OnRequestComplete: func(e RequestCompleteEvent) {
					completed.Add(1)
					gotStatus.Store(int32(e.Status))
				},
			}
			c, err := NewH3Client(srv.addr,
				&tls.Config{ServerName: host, InsecureSkipVerify: true}, //nolint:gosec // interop test dials self-signed servers
				WithHooks(hooks))
			if err != nil {
				t.Fatalf("NewH3Client(%s): %v", srv.addr, err)
			}
			t.Cleanup(func() { _ = c.Close() })

			// Retry the first request until the server is listening (nginx is
			// gated only on container start).
			var resp Response
			deadline := time.Now().Add(h3DialTimeout())
			for {
				resp.Reset()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = c.Do(ctx, &Request{
					Method:    "GET",
					Scheme:    "https",
					Authority: host,
					Path:      "/",
					BodyMode:  BodyBuffer,
				}, &resp)
				cancel()
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("Do(GET /): %v", err)
				}
				time.Sleep(500 * time.Millisecond)
			}

			if resp.Status != 200 {
				t.Fatalf("status = %d, want 200", resp.Status)
			}
			if len(resp.Body) == 0 {
				t.Fatal("GET / body is empty, want the server's canned response")
			}
			if got := gotStatus.Load(); got != 200 || completed.Load() < 1 {
				t.Fatalf("OnRequestComplete fired=%d status=%d, want >=1 with status 200", completed.Load(), got)
			}
			if c.Metrics().Counters.RequestsSucceeded.Load() < 1 {
				t.Fatal("RequestsSucceeded counter did not increment")
			}
			t.Logf("H3 GET / via client.Do -> status=%d bodylen=%d", resp.Status, len(resp.Body))
		})
	}
}
