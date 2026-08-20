package client_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
)

// TestConformance_RFC9112_Sec6_3_PoolFallthroughIsChecked pins that the pool
// never hands out an unchecked connection.
//
// h1Pool.acquire probes inside a loop bounded by MaxConnsPerHost and then fell
// out of it with a bare `return p.acquireOnce(ctx)` — no residue check. With the
// default MaxConnsPerHost of 1 that is two checked attempts followed by one
// unchecked one, and every rejected connection is evicted and redialled, so a
// peer that writes an unsolicited response on ACCEPT fails the checked attempts
// by construction. It only had to be persistent to be handed a poisoned
// connection anyway.
//
// Failing the request is the correct answer: a connection with residue cannot be
// framed, so there is nothing safe to return.
func TestConformance_RFC9112_Sec6_3_PoolFallthroughIsChecked(t *testing.T) {
	const poison = "HTTP/1.1 418 I am a teapot\r\nContent-Length: 6\r\n\r\nPOISON"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	defer ln.Close()
	// One token per connection, sent after the poison write has RETURNED. The
	// dialer below waits for it, so the octets are demonstrably on their way
	// before the client ever inspects the connection.
	//
	// Without that handshake this fixture measures something else entirely: the
	// dial completes in microseconds and the check runs before the server's write
	// lands, so the request reads the poison as its response no matter what the
	// pool does. That is the irreducible TOCTOU every checkout-time design has,
	// net/http included — not the fallthrough this test is about.
	written := make(chan struct{}, 8)
	var accepted atomic.Int64
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			accepted.Add(1)
			go func(nc net.Conn) {
				defer nc.Close()
				// Unsolicited, before any request is even read.
				_, _ = nc.Write([]byte(poison))
				written <- struct{}{}
				buf := make([]byte, 4096)
				_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _ = nc.Read(buf)
			}(nc)
		}
	}()
	dialer := h1clDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		nc, derr := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if derr != nil {
			return nil, derr
		}
		select {
		case <-written:
		case <-time.After(2 * time.Second):
		}
		// The write has returned; give loopback delivery a beat so the check sees
		// a socket that genuinely has octets on it.
		time.Sleep(20 * time.Millisecond)
		return nc, nil
	})
	c, err := client.NewH1PoolClient(ln.Addr().String(), dialer,
		client.PoolOptions{MaxConnsPerHost: 1}, client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()

	var resp client.Response
	err = doOnce(t, c, &resp)

	require.Errorf(t, err,
		"Do succeeded with body %q — a connection carrying an unsolicited response "+
			"was handed out unchecked", resp.Body)
	assert.Truef(t, errors.Is(err, client.ErrResidueOnAcquire),
		"error = %v, want ErrResidueOnAcquire so a caller can tell this apart "+
			"from an ordinary dial or transport failure", err)
}

// TestClient_Warmup_ConcurrentWithRequest pins that Warmup cannot disturb a live
// exchange on the single-connection transport.
//
// Warmup calls acquireConn from its own goroutine, and acquireConn calls
// HasResidue — which moves the read deadline and peeks at the reader, so it may
// only run when no exchange is in flight. openExchange takes s.inFlight for
// exactly that reason; Warmup skipped it. Under -race that is a reported race on
// the bufio reader; without it the in-flight response's own octets read as
// residue and the connection is closed out from under the request, which then
// fails with "use of closed network connection".
//
// Client.Warmup documents itself as idempotent and a no-op on an already-warm
// client, so it must be exactly that.
func TestClient_Warmup_ConcurrentWithRequest(t *testing.T) {
	const resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	defer ln.Close()
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(nc net.Conn) {
				defer nc.Close()
				buf := make([]byte, 4096)
				for {
					_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
					if _, rerr := nc.Read(buf); rerr != nil {
						return
					}
					// A beat before answering, so Warmup lands while the client is
					// waiting on the response rather than before it is sent.
					time.Sleep(20 * time.Millisecond)
					if _, werr := nc.Write([]byte(resp)); werr != nil {
						return
					}
				}
			}(nc)
		}
	}()
	c, err := client.NewH1Client(ln.Addr().String(), dialTCP(), client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1Client")
	defer c.Close()
	// Establish the connection so Warmup finds one to probe.
	var first client.Response
	require.NoError(t, doOnce(t, c, &first), "priming request")

	// A steady stream of requests with Warmup hammering alongside. Overlap has to
	// be produced, not hoped for: warmup self-guards on an in-progress warmup, so
	// a single concurrent pair mostly misses the window, and the server holds each
	// response 20ms specifically to keep a reader parked on the connection.
	var wg sync.WaitGroup
	var warmups atomic.Int64
	errs := make(chan error, 64)
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 40; i++ {
			var r client.Response
			if derr := doOnce(t, c, &r); derr != nil {
				errs <- derr
				return
			}
			if got := string(r.Body); got != "ok" {
				errs <- errors.New("body = " + got)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.Warmup(1)
			warmups.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(errs)

	// The injection count: with no concurrent Warmup calls at all this test would
	// be 40 plain requests and would pass for nothing.
	t.Logf("%d Warmup calls landed alongside 40 requests", warmups.Load())
	require.Positive(t, warmups.Load(),
		"no Warmup ever ran, so nothing was concurrent with the in-flight exchanges")
	for err := range errs {
		assert.NoErrorf(t, err,
			"request failed while Warmup ran concurrently: %v — Warmup must not "+
				"touch a connection an exchange is using", err)
	}
}

// TestConformance_RFC9110_Sec8_6_204WithZeroContentLengthStaysPoolable pins that
// an explicit "Content-Length: 0" on a 204 does not cost the connection.
//
// §8.6 forbids the field on a 204, and this branch exists because a declared
// body on a bodyless status means body-shaped octets may be sitting on the
// socket. A zero describes no octets, so keying on PRESENCE bought no safety and
// cost a connection per request against the many endpoints that answer 204 with
// an explicit zero. A false eviction is a self-inflicted outage, not strictness.
func TestConformance_RFC9110_Sec8_6_204WithZeroContentLengthStaysPoolable(t *testing.T) {
	const resp = "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"

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
			accepted.Add(1)
			go func(nc net.Conn) {
				defer nc.Close()
				buf := make([]byte, 4096)
				for {
					_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
					if _, rerr := nc.Read(buf); rerr != nil {
						return
					}
					if _, werr := nc.Write([]byte(resp)); werr != nil {
						return
					}
				}
			}(nc)
		}
	}()
	c, err := client.NewH1PoolClient(ln.Addr().String(), dialTCP(),
		client.PoolOptions{MaxConnsPerHost: 1}, client.WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1PoolClient")
	defer c.Close()

	statuses := make([]int, 0, 5)
	for i := 0; i < 5; i++ {
		var r client.Response
		require.NoErrorf(t, doOnce(t, c, &r), "request %d", i)
		statuses = append(statuses, r.Status)
	}

	for i, st := range statuses {
		assert.Equalf(t, 204, st, "request %d status", i)
	}
	assert.EqualValues(t, 1, accepted.Load(),
		"a connection was evicted per request: an explicit Content-Length: 0 on a 204 "+
			"describes no body and must not evict")
}
