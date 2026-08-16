package client_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// A server that sends 1xx and then closes is the boundary of the one HTTP/1.1
// failure this client is allowed to replay. ErrServerClosedIdle means no part of a
// response ever arrived, which is what makes a replay safe; an interim response is
// the opposite — 100 Continue is the server saying it has the request head and wants
// the body, the strongest evidence available on that connection that it is acting on
// the request. Replaying it duplicates work the peer has already begun.
//
// http1 pins the classification over a pipe (serverclosed_test.go) and client pins
// how the retry classifier reads the error value (retry_h1_test.go). Neither drives
// the retry loop, so nothing proved what actually happens to a request whose peer
// answered with an interim and vanished (#677). These are that half.

// interimPeer is a scripted origin that counts the requests it reads across every
// connection — the count is the whole assertion, because a replay is visible only as
// the peer being asked twice. Its first connection either answers with an interim
// and closes, or closes having said nothing at all.
type interimPeer struct {
	ln          net.Listener
	sendInterim bool

	mu       sync.Mutex
	requests int
}

func newInterimPeer(t *testing.T, sendInterim bool) *interimPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &interimPeer{ln: ln, sendInterim: sendInterim}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go p.serve(nc)
		}
	}()
	return p
}

// serve reads one request, records it, and then either sends 100 Continue and closes
// or closes immediately. It never sends a final response either way, so the only
// difference between the two arms is whether any part of a response arrived.
func (p *interimPeer) serve(nc net.Conn) {
	defer func() { _ = nc.Close() }()
	buf := make([]byte, 4096)
	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := nc.Read(buf); err != nil {
		return
	}
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	if p.sendInterim {
		_, _ = nc.Write([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
		// Give the client time to consume the interim and loop back for a final
		// status line, so the close it then meets is on the SECOND status-line read.
		// Closing in the same instant would race the interim into the same event and
		// could exercise the first-read path instead — the one being distinguished.
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *interimPeer) requestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests
}

func (p *interimPeer) addr() string { return p.ln.Addr().String() }

// runReplayProbe issues one GET through a Retryer that would replay if allowed, and
// returns the error the caller sees.
//
// GET on purpose, not POST: canRetry refuses a non-idempotent method outright, so a
// POST would be un-replayed for a reason that has nothing to do with the interim and
// the test would pass with the classification broken. With GET, every other gate is
// open and the error classification is the only thing that can prevent the replay.
func runReplayProbe(t *testing.T, addr string) error {
	t.Helper()
	c, err := client.NewH1Client(addr, &conn.PlaintextDialer{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	return r.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)
}

// TestIntegration_H1_InterimThenClose_IsNotReplayed is the gate. The peer answers
// with 100 Continue and closes; the request must reach it exactly once.
func TestIntegration_H1_InterimThenClose_IsNotReplayed(t *testing.T) {
	t.Parallel()
	peer := newInterimPeer(t, true)

	err := runReplayProbe(t, peer.addr())

	if err == nil {
		t.Fatal("the request succeeded, but the peer never sent a final response")
	}
	if errors.Is(err, http1.ErrServerClosedIdle) {
		t.Errorf("error is %v, want anything but ErrServerClosedIdle: the peer sent "+
			"100 Continue before closing, so it had the request and had begun answering "+
			"it, and that type is the licence to replay", err)
	}
	if got := peer.requestCount(); got != 1 {
		t.Errorf("the peer received the request %d times, want exactly 1 — it answered "+
			"with an interim, so it was already acting on the request, and a replay "+
			"duplicates work it had begun", got)
	}
}

// TestIntegration_H1_ClosedWithNoResponse_IsReplayed is the control, and without it
// the gate above is satisfied by a client that never retries anything: the same
// fixture, the same method, the same Retryer, differing only in whether any part of
// a response arrived — and here the replay must happen.
func TestIntegration_H1_ClosedWithNoResponse_IsReplayed(t *testing.T) {
	t.Parallel()
	peer := newInterimPeer(t, false)

	err := runReplayProbe(t, peer.addr())

	if err == nil {
		t.Fatal("the request succeeded, but the peer never sent any response")
	}
	if got := peer.requestCount(); got < 2 {
		t.Errorf("the peer received the request %d times, want at least 2 — nothing of a "+
			"response ever arrived, which is the one H1 failure that is safe to replay, "+
			"so this arm proves the retry path is reachable at all and the gate above is "+
			"not passing merely because no request is ever retried", got)
	}
}
