package client_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// smuggledResponse is the response the attacker wants some later request to
// receive. In the poisoned response below it sits inside the message body, past
// the 5 octets the first Content-Length declares — so it is only ever parsed as
// a response if the client both believes the "5" and then reuses the socket.
const smuggledResponse = "HTTP/1.1 200 OK\r\n" +
	"Content-Length: 3\r\n" +
	"\r\n" +
	"pwn"

// TestConformance_RFC9112_Sec6_3_Rule5_PoisonedConnNotPooled is the payoff
// assertion for the CL.CL desync, made at the layer where the damage is done.
//
// http1's own tests pin that ReadResponse rejects the response and reports
// KeepAlive()=false. That is necessary but not sufficient: the security claim is
// that the poisoned socket is never handed to another request. This drives a
// real HTTP/1.1 pool (MaxConnsPerHost=1, so a pooled conn IS what the next
// request gets) over a raw listener and asserts the two facts that matter — the
// second request runs on a new connection, and it receives the server's response
// rather than the one hidden in the first response's body.
//
// The bug: with "Content-Length: 5" and "Content-Length: 999" the client took 5,
// left the rest unread, and reported keepAlive=true. client/h1_pool.go's
// handleRelease pools any conn released with keepAlive=true (`if !msg.keepAlive`
// is the eviction test), so request 2 resumed the socket mid-body and parsed
// smuggledResponse — bytes chosen by whoever wrote response 1 — as its own
// status line, headers, and body.
//
// net/http cannot serve this fixture: it will not emit two conflicting
// Content-Length fields. The listener writes literal bytes instead.
func TestConformance_RFC9112_Sec6_3_Rule5_PoisonedConnNotPooled(t *testing.T) {
	// The two lengths disagree, so RFC 9112 §6.3 rule 5 makes the framing
	// invalid and unrecoverable. 5 delimits "HELLO"; everything after it is the
	// smuggled response.
	poisoned := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5\r\n" +
		"Content-Length: 999\r\n" +
		"\r\n" +
		"HELLO" + smuggledResponse
	// What a fresh connection answers request 2 with.
	const clean = "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 2\r\n" +
		"\r\n" +
		"ok"

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
					_, _ = nc.Write([]byte(poisoned))
				} else {
					_, _ = nc.Write([]byte(clean))
				}
				// Answer one request per connection, then block until the client
				// hangs up. Closing early would evict the conn from the pool on
				// liveness grounds and mask the keep-alive verdict under test;
				// this read returns only when the client closes, so nothing here
				// races the assertions.
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

	// Each Do gets its own fresh deadline rather than one budget spanning both:
	// a per-operation bound catches a hung request, whereas a shared deadline
	// would let a slow -race build fail the second request for the first one's
	// cost.
	do := func(resp *client.Response) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		resp.Reset()
		return c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, resp)
	}

	var resp1 client.Response
	err1 := do(&resp1)
	var resp2 client.Response
	err2 := do(&resp2)

	assert.Error(t, err1, "request 1: a response whose two Content-Length values disagree "+
		"has no trustworthy body boundary and must not be a success")
	require.NoError(t, err2, "request 2: want success on a fresh connection")
	assert.Equal(t, "ok", string(resp2.Body),
		"request 2 body — anything else means it resumed the poisoned socket and read the "+
			"smuggled response out of request 1's body")
	// The assertion this test exists for. 1 == the poisoned conn was pooled and
	// reused; 2 == it was evicted and request 2 dialled fresh.
	assert.EqualValues(t, 2, accepted.Load(),
		"want 2 accepted connections — anything less means the connection poisoned by the "+
			"conflicting Content-Length was returned to the pool and reused")
}

type h1clDialer func(ctx context.Context, addr string) (net.Conn, error)

func (f h1clDialer) Dial(ctx context.Context, addr string) (net.Conn, error) { return f(ctx, addr) }

var _ conn.Dialer = h1clDialer(nil)
