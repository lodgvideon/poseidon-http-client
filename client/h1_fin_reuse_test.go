package client_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// A server reaping an idle keep-alive connection is the most ordinary event on a
// persistent HTTP/1.1 path, and this client is deliberately built not to detect
// it at checkout inside the fast-reuse window: HasResidue is explicitly not a
// liveness check (FIONREAD reports 0 on a closed socket, http1/residue.go), and
// the pool's ProbeIdle only runs once a connection has sat idle longer than
// h1ProbeIdleAfter (250ms, client/h1_pool.go). The single-connection transport
// never probes at all, by design — a bounded ~1ms probe on every request costs
// more than the request.
//
// So inside 250ms the recovery is not detection but replay: the write lands in
// the half-closed socket, the status-line read returns EOF, http1 raises
// ErrServerClosedIdle, and the Retryer replays onto a fresh connection. Under
// the load this library targets, reuse inside 250ms is every reuse, so that
// chain carries the common case.
//
// Both halves of it were tested and the join was not: http1/serverclosed_test.go
// classifies the error over a pipe, client/retry_h1_test.go feeds
// builtinShouldRetry a hand-built one. Neither drives a real client whose peer
// reaps the connection. These tests are that join.

// finPeer is a scripted origin that answers requests and reaps the first
// connection shortly after responding on it. It counts accepted connections,
// because the whole assertion is that the client had to open a second one.
type finPeer struct {
	ln net.Listener

	mu       sync.Mutex
	accepted int
}

func newFINPeer(t *testing.T, reapAfter time.Duration) *finPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &finPeer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			p.mu.Lock()
			p.accepted++
			n := p.accepted
			p.mu.Unlock()
			go p.serve(nc, n == 1, reapAfter)
		}
	}()
	return p
}

// serve answers every request on one connection with a keep-alive response, so
// the client is entitled to pool it. On the first connection it then closes,
// which is the event under test.
func (p *finPeer) serve(nc net.Conn, reap bool, after time.Duration) {
	defer func() { _ = nc.Close() }()
	buf := make([]byte, 4096)
	for {
		_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := nc.Read(buf); err != nil {
			return
		}
		if _, err := nc.Write([]byte(
			"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nhi",
		)); err != nil {
			return
		}
		if reap {
			// Reap well inside h1ProbeIdleAfter, so the pool's checkout probe is
			// still gated off and the connection is handed out unchecked. That is
			// the whole point: a longer wait would test the probe instead.
			time.Sleep(after)
			return
		}
	}
}

func (p *finPeer) acceptedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

func (p *finPeer) addr() string { return p.ln.Addr().String() }

// doGet issues one GET and reports the error, if any.
func doGet(ctx context.Context, r *client.Retryer) error {
	var resp client.Response
	resp.Reset()
	return r.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)
}

// runFINReuse drives two sequential requests against a peer that reaps the
// first connection between them, and returns the second request's error. The
// connection count is read from the peer itself, so the caller can assert a
// redial actually happened.
func runFINReuse(t *testing.T, c *client.Client) error {
	t.Helper()
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		// No backoff: the wait would be dead time here, and a jittered default
		// makes the test's duration a function of the retry policy rather than
		// of the behaviour under test.
		Backoff: func(int) time.Duration { return 0 },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := doGet(ctx, r); err != nil {
		t.Fatalf("first request failed before the connection was ever reaped: %v", err)
	}
	// Long enough for the FIN to arrive, short enough that the pool's 250ms
	// probe gate stays shut.
	time.Sleep(60 * time.Millisecond)

	return doGet(ctx, r)
}

// TestIntegration_H1Pool_ServerReapsIdleConn_RequestIsReplayed is the pooled
// path: MaxConnsPerHost 1 guarantees the second request is offered the same
// pooled connection the peer has just closed.
func TestIntegration_H1Pool_ServerReapsIdleConn_RequestIsReplayed(t *testing.T) {
	t.Parallel()
	peer := newFINPeer(t, 10*time.Millisecond)

	c, err := client.NewH1PoolClient(peer.addr(), &conn.PlaintextDialer{},
		client.PoolOptions{MaxConnsPerHost: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err2 := runFINReuse(t, c)
	if err2 != nil {
		t.Fatalf("second request failed after the peer reaped the pooled connection: %v\n"+
			"inside h1ProbeIdleAfter the pool hands the connection out unchecked by "+
			"design, so recovery depends entirely on ErrServerClosedIdle reaching the "+
			"Retryer and the request being replayed on a fresh connection", err2)
	}
	if got := peer.acceptedCount(); got < 2 {
		t.Errorf("peer accepted %d connections, want at least 2 — the second request "+
			"did not actually redial, so this passed without exercising the replay", got)
	}
}

// TestIntegration_H1SingleConn_ServerReapsIdleConn_RequestIsReplayed is the
// single-connection transport, which never probes at checkout at all
// (client/h1_transport.go calls only HasResidue). It is separate code from the
// pool and fails separately.
func TestIntegration_H1SingleConn_ServerReapsIdleConn_RequestIsReplayed(t *testing.T) {
	t.Parallel()
	peer := newFINPeer(t, 10*time.Millisecond)

	c, err := client.NewH1Client(peer.addr(), &conn.PlaintextDialer{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	err2 := runFINReuse(t, c)
	if err2 != nil {
		t.Fatalf("second request failed after the peer reaped the connection: %v\n"+
			"this transport never probes at checkout, so the replay is the only "+
			"recovery there is", err2)
	}
	if got := peer.acceptedCount(); got < 2 {
		t.Errorf("peer accepted %d connections, want at least 2 — no redial happened", got)
	}
}
