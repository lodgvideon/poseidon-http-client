package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// h1PoolClient builds a pooled HTTP/1.1 client over the fake dialer.
func h1PoolClient(t *testing.T, d *h1FakeDialer, po PoolOptions) *Client {
	t.Helper()
	c, err := NewH1PoolClient("h:80", d, po, WithDefaultScheme("http"))
	require.NoError(t, err, "NewH1PoolClient over the fake dialer")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// h1Get issues one buffered GET through c.
func h1Get(ctx context.Context, c *Client) (*Response, error) {
	var resp Response
	resp.Reset()
	err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)
	return &resp, err
}

// TestH1PoolTransport_KeepAlive_TwoRequestsReuseOneConn drives two sequential
// requests end-to-end through the pooled transport and proves the connection is
// recycled: one dial, two exchanges on the same socket.
func TestH1PoolTransport_KeepAlive_TwoRequestsReuseOneConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer() // default respFn → keep-alive 200
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second})

	resps := make([]*Response, 0, 2)
	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		require.NoErrorf(t, err, "request %d", i)
		resps = append(resps, resp)
	}

	for i, resp := range resps {
		assert.Equalf(t, 200, resp.Status, "request %d status", i)
		assert.Equalf(t, "ok", string(resp.Body), "request %d body", i)
	}
	assert.Equal(t, 1, d.count("h:80"),
		"more than one conn was dialed: keep-alive reuse did not happen")
	assert.EqualValues(t, 2, d.conns("h:80")[0].reqs.Load(),
		"the single pooled conn did not serve both requests")
}

// TestH1PoolTransport_ConnectionClose_DiscardsAndRedials proves a response saying
// the connection will not persist causes the conn to be discarded, so the next
// request dials a fresh one instead of writing to a socket the peer is closing.
func TestH1PoolTransport_ConnectionClose_DiscardsAndRedials(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.respFn = func(_, _ int) string { return h1CloseResponse }
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second})

	statuses := make([]int, 0, 2)
	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		require.NoErrorf(t, err, "request %d", i)
		statuses = append(statuses, resp.Status)
	}

	for i, st := range statuses {
		assert.Equalf(t, 200, st, "request %d status", i)
	}
	conns := d.conns("h:80")
	require.Len(t, conns, 2,
		"Connection: close must force a redial, so exactly two conns must have been dialed")
	assert.True(t, conns[0].closed.Load(),
		"the conn answering Connection: close was not closed")
	for i, fc := range conns {
		assert.EqualValuesf(t, 1, fc.reqs.Load(),
			"conn %d served the wrong number of requests: a discarded conn must not be reused", i)
	}
}

// TestH1PoolTransport_ExchangeError_DiscardsConn proves a failed exchange discards
// its connection rather than returning a poisoned one to the idle set: the
// following request must land on a freshly dialed conn.
func TestH1PoolTransport_ExchangeError_DiscardsConn(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.respFn = func(connIdx, _ int) string {
		if connIdx == 0 {
			return "GARBAGE NOT HTTP\r\n\r\n" // poisons the first conn
		}
		return h1OKResponse
	}
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second})

	_, firstErr := h1Get(context.Background(), c)
	resp, secondErr := h1Get(context.Background(), c)

	require.Error(t, firstErr, "a request against a malformed response must fail, not succeed")
	require.NoError(t, secondErr,
		"the request after a failed exchange must succeed on a freshly dialed conn")
	assert.Equal(t, 200, resp.Status, "status on the fresh conn")
	conns := d.conns("h:80")
	require.Len(t, conns, 2, "a failed exchange must discard its conn, forcing a second dial")
	assert.True(t, conns[0].closed.Load(), "the poisoned conn was not closed")
}

// TestH1PoolTransport_ConcurrentRequests_BoundedByMaxConns proves the transport
// honours MaxConnsPerHost under concurrent load while every request still
// completes (waiters proceed as conns free).
func TestH1PoolTransport_ConcurrentRequests_BoundedByMaxConns(t *testing.T) {
	t.Parallel()
	const maxConns = 3
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: maxConns, HealthCheckPeriod: time.Second})

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := h1Get(ctx, c)
			if !assert.NoError(t, err, "Do under concurrent load: every request must still complete") {
				return
			}
			assert.Equal(t, 200, resp.Status, "status under concurrent load")
		}()
	}
	wg.Wait()

	assert.LessOrEqualf(t, d.count("h:80"), maxConns,
		"pool opened more conns than MaxConnsPerHost=%d under concurrent load", maxConns)
}

func TestH1PoolTransport_AfterClose_ReturnsErrPoolClosed(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 1})
	_ = c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := h1Get(ctx, c)

	assert.Truef(t, errors.Is(err, ErrPoolClosed),
		"request after Close = %v; a caller classifying this cannot tell a closed pool "+
			"from a transport failure", err)
}

func TestH1PoolTransport_PoolStats_Reported(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 2, HealthCheckPeriod: time.Second})

	_, err := h1Get(context.Background(), c)

	require.NoError(t, err, "Do")
	assert.Equal(t, 1, c.PoolStats().ActiveConns,
		"PoolStats must report the conn the request opened")
}

func TestH1PoolTransport_Warmup_PreDials(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 3, HealthCheckPeriod: time.Second})

	c.Warmup(3)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && d.count("h:80") != 3 {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, 3, d.count("h:80"),
		"warmup did not open the requested number of conns")
}

// TestConformance_RFC9112_Sec6_3_Rule3_SmuggledResponseNotPooled proves the
// payoff of RFC 9112 §6.3 rule 4 at the layer that matters. A response carrying
// both Transfer-Encoding and Content-Length "might indicate an attempt to
// perform request smuggling (§11.2)" and the connection MUST be closed after
// it. http1 signals that with KeepAlive()=false, but that is only a proxy — the
// security claim is that the next request does not land on the poisoned socket.
// So this asserts through the pool: two requests, two dials, and the first conn
// closed.
func TestConformance_RFC9112_Sec6_3_Rule3_SmuggledResponseNotPooled(t *testing.T) {
	t.Parallel()
	// Chunked framing is unambiguous and the body is well formed, so the
	// exchange succeeds: nothing but rule 4 can force the redial here.
	const smuggled = "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		"5\r\nhello\r\n0\r\n\r\n"
	d := newH1FakeDialer()
	d.respFn = func(_, _ int) string { return smuggled }
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second})

	resps := make([]*Response, 0, 2)
	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		require.NoErrorf(t, err, "request %d", i)
		resps = append(resps, resp)
	}

	for i, resp := range resps {
		assert.Equalf(t, 200, resp.Status, "request %d status", i)
		assert.Equalf(t, "hello", string(resp.Body), "request %d body", i)
	}
	conns := d.conns("h:80")
	require.Len(t, conns, 2,
		"a TE+CL response must not be reused, so the second request needs its own dial")
	assert.True(t, conns[0].closed.Load(),
		"the conn that sent a TE+CL response was returned to the pool, not closed")
}
