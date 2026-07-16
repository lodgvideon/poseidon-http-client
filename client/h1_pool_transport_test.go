package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// h1PoolClient builds a pooled HTTP/1.1 client over the fake dialer.
func h1PoolClient(t *testing.T, d *h1FakeDialer, po PoolOptions) *Client {
	t.Helper()
	c, err := NewH1PoolClient("h:80", d, po, WithDefaultScheme("http"))
	if err != nil {
		t.Fatalf("NewH1PoolClient: %v", err)
	}
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

	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 || string(resp.Body) != "ok" {
			t.Fatalf("request %d: status=%d body=%q", i, resp.Status, resp.Body)
		}
	}

	if n := d.count("h:80"); n != 1 {
		t.Fatalf("dialed %d conns, want 1 (keep-alive reuse)", n)
	}
	if got := d.conns("h:80")[0].reqs.Load(); got != 2 {
		t.Fatalf("conn served %d requests, want 2", got)
	}
}

// TestH1PoolTransport_ConnectionClose_DiscardsAndRedials proves a response saying
// the connection will not persist causes the conn to be discarded, so the next
// request dials a fresh one instead of writing to a socket the peer is closing.
func TestH1PoolTransport_ConnectionClose_DiscardsAndRedials(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	d.respFn = func(_, _ int) string { return h1CloseResponse }
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 4, HealthCheckPeriod: time.Second})

	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 {
			t.Fatalf("request %d: status = %d, want 200", i, resp.Status)
		}
	}

	conns := d.conns("h:80")
	if len(conns) != 2 {
		t.Fatalf("dialed %d conns, want 2 (Connection: close must force a redial)", len(conns))
	}
	if !conns[0].closed.Load() {
		t.Fatal("conn answering Connection: close was not closed")
	}
	for i, fc := range conns {
		if got := fc.reqs.Load(); got != 1 {
			t.Fatalf("conn %d served %d requests, want 1", i, got)
		}
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

	if _, err := h1Get(context.Background(), c); err == nil {
		t.Fatal("request against a malformed response = nil error, want failure")
	}

	resp, err := h1Get(context.Background(), c)
	if err != nil {
		t.Fatalf("request after a failed exchange: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}

	conns := d.conns("h:80")
	if len(conns) != 2 {
		t.Fatalf("dialed %d conns, want 2 (a failed exchange must discard its conn)", len(conns))
	}
	if !conns[0].closed.Load() {
		t.Fatal("poisoned conn was not closed")
	}
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
			if err != nil {
				t.Errorf("Do: %v", err)
				return
			}
			if resp.Status != 200 {
				t.Errorf("status = %d, want 200", resp.Status)
			}
		}()
	}
	wg.Wait()

	if n := d.count("h:80"); n > maxConns {
		t.Fatalf("dialed %d conns, want <= %d", n, maxConns)
	}
}

func TestH1PoolTransport_AfterClose_ReturnsErrPoolClosed(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 1})
	_ = c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := h1Get(ctx, c); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("request after Close = %v, want ErrPoolClosed", err)
	}
}

func TestH1PoolTransport_PoolStats_Reported(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 2, HealthCheckPeriod: time.Second})

	if _, err := h1Get(context.Background(), c); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if s := c.PoolStats(); s.ActiveConns != 1 {
		t.Fatalf("PoolStats = %+v, want ActiveConns=1", s)
	}
}

func TestH1PoolTransport_Warmup_PreDials(t *testing.T) {
	t.Parallel()
	d := newH1FakeDialer()
	c := h1PoolClient(t, d, PoolOptions{MaxConnsPerHost: 3, HealthCheckPeriod: time.Second})
	c.Warmup(3)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if d.count("h:80") == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("warmup dialed %d conns, want 3", d.count("h:80"))
}

// TestConformance_RFC9112_Sec6_3_Rule4_SmuggledResponseNotPooled proves the
// payoff of RFC 9112 §6.3 rule 4 at the layer that matters. A response carrying
// both Transfer-Encoding and Content-Length "might indicate an attempt to
// perform request smuggling (§11.2)" and the connection MUST be closed after
// it. http1 signals that with KeepAlive()=false, but that is only a proxy — the
// security claim is that the next request does not land on the poisoned socket.
// So this asserts through the pool: two requests, two dials, and the first conn
// closed.
func TestConformance_RFC9112_Sec6_3_Rule4_SmuggledResponseNotPooled(t *testing.T) {
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

	for i := 0; i < 2; i++ {
		resp, err := h1Get(context.Background(), c)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status != 200 || string(resp.Body) != "hello" {
			t.Fatalf("request %d: status=%d body=%q", i, resp.Status, resp.Body)
		}
	}

	conns := d.conns("h:80")
	if len(conns) != 2 {
		t.Fatalf("dialed %d conns, want 2 (a TE+CL response must not be reused)", len(conns))
	}
	if !conns[0].closed.Load() {
		t.Fatal("conn that sent a TE+CL response was returned to the pool, not closed")
	}
}
