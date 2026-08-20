package client_test

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
)

// TestConformance_RFC9112_Sec6_3_ResponseQueuePoisonNotPooled is the payoff
// assertion for the leftover-octet rule, at the layer where the damage lands.
//
// Unlike the CL.CL fixture, response 1 here is PERFECTLY well framed — the
// client parses it, succeeds, and is right to. The attack is what follows it in
// the same write: a complete second response the server never generated for any
// request. RFC 9112 §6.3: "A client MUST NOT process, cache, or forward such
// extra data as a separate response, since such behavior would be vulnerable to
// cache poisoning."
//
// These are the same-write shapes, which the completion-time guard in
// http1.ReadBodyChunk catches: the appended response is already in the reader
// when the message ends, so Buffered() sees it. The variant that guard cannot
// see — the response arriving later, while the connection is idle — is
// TestConformance_RFC9112_Sec6_3_LatePoisonNotPooled below.
//
// Four body shapes, because they end through different code paths: a
// Content-Length body, a chunked body, a bodyless 204, and a body larger than
// http1's reader.
func TestConformance_RFC9112_Sec6_3_ResponseQueuePoisonNotPooled(t *testing.T) {
	const poison = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"
	const clean = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

	cases := []struct {
		name  string
		first string
	}{
		{"content-length body", "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHELLO" + poison},
		{"chunked body", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n" + poison},
		{"bodyless 204", "HTTP/1.1 204 No Content\r\n\r\n" + poison},
		// Larger than http1's 16 KiB reader, so the body is read in several
		// gulps and the last one is the size that used to bypass bufio.
		{"large body", "HTTP/1.1 200 OK\r\nContent-Length: 40000\r\n\r\n" +
			strings.Repeat("x", 40000) + poison},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err, "Listen")
			defer ln.Close()
			var accepted atomic.Int64
			go func() {
				for {
					nc, aerr := ln.Accept()
					if aerr != nil {
						return
					}
					n := accepted.Add(1)
					go func(nc net.Conn, n int64) {
						defer nc.Close()
						buf := make([]byte, 4096)
						if _, rerr := nc.Read(buf); rerr != nil {
							return
						}
						if n == 1 {
							_, _ = nc.Write([]byte(tc.first))
						} else {
							_, _ = nc.Write([]byte(clean))
						}
						// Hold the conn open so the pool's own verdict, not a
						// server-side close, decides whether it is reused.
						_, _ = nc.Read(buf)
					}(nc, n)
				}
			}()
			c, err := client.NewH1PoolClient(
				ln.Addr().String(),
				h1clDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				}),
				client.PoolOptions{MaxConnsPerHost: 1},
				client.WithDefaultScheme("http"),
			)
			require.NoError(t, err, "NewH1PoolClient")
			defer c.Close()
			do := func(resp *client.Response) error {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				resp.Reset()
				return c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, resp)
			}

			// Request 1 is legitimately fine: its own framing is valid.
			var resp1, resp2 client.Response
			err1 := do(&resp1)
			err2 := do(&resp2)

			require.NoError(t, err1, "request 1 must succeed — response 1 is well framed")
			require.NoError(t, err2, "request 2 must succeed on a fresh connection")
			assert.Equal(t, "ok", string(resp2.Body),
				"request 2 got a body the server never generated for it: the connection was "+
					"pooled with the appended response still on it")
			assert.EqualValues(t, 2, accepted.Load(),
				"the poisoned connection was returned to the pool and reused instead of "+
					"being discarded")
		})
	}
}

// TestConformance_RFC9112_Sec6_3_LatePoisonNotPooled is the variant the
// completion-time check structurally cannot catch, and the one that actually
// reproduced end to end.
//
// Response 1 is well framed and fully read before the server writes anything
// else; the unsolicited response arrives afterwards, while the connection sits
// idle in the pool. Nothing is in the reader at completion, so only asking the
// SOCKET at checkout finds it — which is what the probe in h1Pool.acquire does.
//
// Mutation-checked: with the checkout probe disabled (h1ProbeIdleAfter raised
// past the idle gap) this returns `body="PWNED"` on one connection.
func TestConformance_RFC9112_Sec6_3_LatePoisonNotPooled(t *testing.T) {
	const poison = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"
	const first = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHELLO"
	const clean = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	defer ln.Close()
	var accepted atomic.Int64
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			n := accepted.Add(1)
			go func(nc net.Conn, n int64) {
				defer nc.Close()
				buf := make([]byte, 4096)
				if _, rerr := nc.Read(buf); rerr != nil {
					return
				}
				if n == 1 {
					_, _ = nc.Write([]byte(first))
					// Let the client finish reading response 1 before the
					// unsolicited one lands, so it cannot be in the reader.
					time.Sleep(150 * time.Millisecond)
					_, _ = nc.Write([]byte(poison))
				} else {
					_, _ = nc.Write([]byte(clean))
				}
				_, _ = nc.Read(buf)
			}(nc, n)
		}
	}()
	c, err := client.NewH1PoolClient(
		ln.Addr().String(),
		h1clDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}),
		client.PoolOptions{MaxConnsPerHost: 1},
		client.WithDefaultScheme("http"),
	)
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()
	do := func(resp *client.Response) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp.Reset()
		return c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, resp)
	}

	var resp1, resp2 client.Response
	err1 := do(&resp1)
	// Long enough for the unsolicited response to land AND for the connection to
	// pass the checkout-probe threshold.
	time.Sleep(300 * time.Millisecond)
	err2 := do(&resp2)

	require.NoError(t, err1, "request 1")
	require.NoError(t, err2, "request 2 must succeed on a fresh connection")
	assert.Equal(t, "ok", string(resp2.Body),
		"the connection was reused with an unsolicited response waiting on it")
	assert.EqualValues(t, 2, accepted.Load(), "the poisoned connection was reused")
}

// TestConformance_RFC9112_Sec6_3_BufioBypassPoisonNotPooled pins the exact shape
// that defeated the completion-time guard: a final body read large enough to
// take bufio's direct-read path.
//
// bufio.Read short-circuits when its buffer is empty AND the caller's slice is at
// least buffer-sized — it reads straight into that slice. The head is written as
// its own segment so the buffer IS empty when the body read starts, and the body
// is exactly readBufSize so the read is exactly buffer-sized. The appended
// response then never enters the reader, Buffered() reports 0 at completion, and
// the connection is pooled with the peer's response still on the socket.
//
// Mutation-checked: with the bypass probe removed, request 2 returns "PWNED" on
// one connection.
func TestConformance_RFC9112_Sec6_3_BufioBypassPoisonNotPooled(t *testing.T) {
	const bodyLen = 16384 // == http1 readBufSize: the size that takes the direct path
	const poison = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"
	const clean = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
	body := strings.Repeat("x", bodyLen)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	defer ln.Close()
	var accepted atomic.Int64
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			n := accepted.Add(1)
			go func(nc net.Conn, n int64) {
				defer nc.Close()
				buf := make([]byte, 4096)
				if _, rerr := nc.Read(buf); rerr != nil {
					return
				}
				if n == 1 {
					// Head alone first, so the client's bufio buffer is empty when
					// it starts reading the body.
					_, _ = nc.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 16384\r\n\r\n"))
					time.Sleep(50 * time.Millisecond)
					_, _ = nc.Write([]byte(body + poison))
				} else {
					_, _ = nc.Write([]byte(clean))
				}
				_, _ = nc.Read(buf)
			}(nc, n)
		}
	}()
	c, err := client.NewH1PoolClient(
		ln.Addr().String(),
		h1clDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}),
		client.PoolOptions{MaxConnsPerHost: 1},
		client.WithDefaultScheme("http"),
	)
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()
	do := func(resp *client.Response) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp.Reset()
		return c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, resp)
	}

	var resp1, resp2 client.Response
	err1 := do(&resp1)
	err2 := do(&resp2)

	require.NoError(t, err1, "request 1")
	require.Len(t, resp1.Body, bodyLen, "request 1 body length")
	require.NoError(t, err2, "request 2 must succeed on a fresh connection")
	assert.Equal(t, "ok", string(resp2.Body),
		"the appended response bypassed the reader and the connection was pooled with "+
			"it still on the socket")
	assert.EqualValues(t, 2, accepted.Load(), "the poisoned connection was reused")
}
