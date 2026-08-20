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
)

// The two checkout paths that reuse an HTTP/1.1 connection, both tested against
// the same attack: a server sends a well-framed response, waits for the client
// to finish reading it, and only THEN appends a second complete response the
// client never asked for. RFC 9112 §6.3: "A client MUST NOT process, cache, or
// forward such extra data as a separate response, since such behavior would be
// vulnerable to cache poisoning."
//
// TestConformance_RFC9112_Sec6_3_LatePoisonNotPooled already covers the pool
// with a 300 ms idle gap. That gap is precisely why it did not catch these: the
// pool's probe was gated on `time.Since(mc.lastUsed) > h1ProbeIdleAfter` (250 ms),
// so the test was measuring the path where the probe fires. Reuse FASTER than
// the threshold skipped the check entirely — and reuse faster than 250 ms is not
// an edge case for a client built for load generation, it is every request.
// The single-connection transport had no probe at all.
//
// Both fixtures therefore reuse ~25 ms after the poison lands: an order of
// magnitude inside the old hole, so passing cannot be an accident of exceeding
// the threshold.

// poisonAfterFirstRead runs a server that answers request 1 well-framed, waits
// to be told the client is done with it, appends an unsolicited response, and
// answers any later connection cleanly. It returns the listener address and a
// func that releases the poison and reports when the write has completed.
func poisonAfterFirstRead(t *testing.T, accepted *atomic.Int64) (string, func()) {
	t.Helper()
	const first = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHELLO"
	const poison = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"
	const clean = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { _ = ln.Close() })

	release := make(chan struct{})
	written := make(chan struct{})

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
				if n != 1 {
					_, _ = nc.Write([]byte(clean))
					_, _ = nc.Read(buf)
					return
				}
				_, _ = nc.Write([]byte(first))
				<-release
				_, _ = nc.Write([]byte(poison))
				close(written)
				// Hold the connection open so the client's own verdict, not a
				// server-side close, decides whether it is reused.
				_, _ = nc.Read(buf)
			}(nc, n)
		}
	}()

	return ln.Addr().String(), func() {
		close(release)
		<-written
		// The write has returned, so the octets are on their way over loopback.
		// This wait is for delivery only — it is bounded, per-operation, and an
		// order of magnitude under the 250 ms threshold whose skip is the defect,
		// so it cannot turn a failing implementation into a passing one.
		time.Sleep(25 * time.Millisecond)
	}
}

func doOnce(t *testing.T, c *client.Client, resp *client.Response) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp.Reset()
	return c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, resp)
}

func dialTCP() h1clDialer {
	return h1clDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	})
}

// TestConformance_RFC9112_Sec6_3_FastReusePoisonNotPooled_Pool pins the pool
// path. Mutation-checked: restoring the
// `time.Since(mc.lastUsed) <= h1ProbeIdleAfter ||` short-circuit in
// h1Pool.acquire makes request 2 return body "PWNED" on one connection.
func TestConformance_RFC9112_Sec6_3_FastReusePoisonNotPooled_Pool(t *testing.T) {
	var accepted atomic.Int64
	addr, poison := poisonAfterFirstRead(t, &accepted)

	c, err := client.NewH1PoolClient(addr, dialTCP(),
		client.PoolOptions{MaxConnsPerHost: 1}, client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()

	var resp1 client.Response
	err1 := doOnce(t, c, &resp1)
	poison()
	var resp2 client.Response
	err2 := doOnce(t, c, &resp2)

	require.NoError(t, err1, "request 1")
	// CONTROL: request 1 must have read the WELL-FRAMED response. If it did not,
	// the poison landed before request 1 completed and nothing below means anything.
	require.Equal(t, "HELLO", string(resp1.Body),
		"request 1 did not read the well-framed response, so the injection did not land "+
			"where this test needs it")
	require.NoError(t, err2, "request 2: want success on a fresh connection")
	assert.Equal(t, "ok", string(resp2.Body),
		"request 2 body — the connection was reused inside the probe threshold with an "+
			"unsolicited response already on it")
	assert.EqualValues(t, 2, accepted.Load(),
		"want 2 accepted connections — anything less means the poisoned connection was reused")
}

// TestConformance_RFC9112_Sec6_3_FastReusePoisonNotPooled_SingleConn pins the
// single-connection transport, which had no checkout check of any kind: it
// returned s.cur on IsAlive() alone, a local flag that says nothing about what
// the peer has put on the socket. Mutation-checked: dropping the HasResidue
// branch in h1singleConn.acquireConn makes request 2 return "PWNED".
//
// This is also the HTTP/1.1 fallback of the ALPN transport, which delegates here.
func TestConformance_RFC9112_Sec6_3_FastReusePoisonNotPooled_SingleConn(t *testing.T) {
	var accepted atomic.Int64
	addr, poison := poisonAfterFirstRead(t, &accepted)

	c, err := client.NewH1Client(addr, dialTCP(), client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1Client")
	defer c.Close()

	var resp1 client.Response
	err1 := doOnce(t, c, &resp1)
	poison()
	var resp2 client.Response
	err2 := doOnce(t, c, &resp2)

	require.NoError(t, err1, "request 1")
	// CONTROL: see the pool test above — request 1 must have read the well-framed
	// response, or the poison landed too early.
	require.Equal(t, "HELLO", string(resp1.Body),
		"request 1 did not read the well-framed response, so the poison landed too early")
	require.NoError(t, err2, "request 2: want success on a fresh connection")
	assert.Equal(t, "ok", string(resp2.Body),
		"request 2 body — the single-conn transport reused a connection with an "+
			"unsolicited response waiting on it")
	assert.EqualValues(t, 2, accepted.Load(),
		"want 2 accepted connections — anything less means the poisoned connection was reused")
}

// TestConformance_RFC9112_Sec6_3_FastReuse_ControlNoPoison_Pool is the control
// arm for the two poison tests: same shape, nothing injected. It exists because
// "accepted == 2" is evidence of eviction only if the pool would otherwise have
// reused the connection — a pool that never reuses, or a peer that hangs up
// after every response, satisfies "accepted == 2" while testing nothing. Here
// the second request must run on connection 1.
func TestConformance_RFC9112_Sec6_3_FastReuse_ControlNoPoison_Pool(t *testing.T) {
	var accepted atomic.Int64
	addr := cleanKeepAlivePeer(t, &accepted)

	c, err := client.NewH1PoolClient(addr, dialTCP(),
		client.PoolOptions{MaxConnsPerHost: 1}, client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()

	var resp1, resp2 client.Response
	err1 := doOnce(t, c, &resp1)
	err2 := doOnce(t, c, &resp2)

	require.NoError(t, err1, "request 1 against a clean keep-alive peer")
	require.NoError(t, err2, "request 2 against a clean keep-alive peer")
	assert.EqualValues(t, 1, accepted.Load(),
		"the pool must reuse a clean connection inside the probe threshold; if it dials "+
			"again here then \"accepted == 2\" in the poison tests proves nothing about eviction")
}

// TestConformance_RFC9112_Sec6_3_FastReuse_ControlNoPoison_SingleConn is the
// single-connection control arm; see the pool one above for why it exists.
func TestConformance_RFC9112_Sec6_3_FastReuse_ControlNoPoison_SingleConn(t *testing.T) {
	var accepted atomic.Int64
	addr := cleanKeepAlivePeer(t, &accepted)

	c, err := client.NewH1Client(addr, dialTCP(), client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1Client")
	defer c.Close()

	var resp1, resp2 client.Response
	err1 := doOnce(t, c, &resp1)
	err2 := doOnce(t, c, &resp2)

	require.NoError(t, err1, "request 1 against a clean keep-alive peer")
	require.NoError(t, err2, "request 2 against a clean keep-alive peer")
	assert.EqualValues(t, 1, accepted.Load(),
		"the single-conn transport must reuse a clean connection; if it redials here then "+
			"\"accepted == 2\" in the poison test proves nothing about eviction")
}

// cleanKeepAlivePeer answers every request on every connection with the same
// well-framed keep-alive response and never closes. It is the no-injection twin
// of poisonAfterFirstRead.
func cleanKeepAlivePeer(t *testing.T, accepted *atomic.Int64) string {
	t.Helper()
	const clean = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			accepted.Add(1)
			go func(nc net.Conn) {
				defer nc.Close()
				buf := make([]byte, 4096)
				for {
					if _, rerr := nc.Read(buf); rerr != nil {
						return
					}
					if _, werr := nc.Write([]byte(clean)); werr != nil {
						return
					}
				}
			}(nc)
		}
	}()
	return ln.Addr().String()
}
