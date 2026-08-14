package http1_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// silentPeer returns a Conn whose peer accepts and then says nothing, so any
// read blocks until a deadline elapses.
func silentPeer(t *testing.T) *http1.Conn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			accepted <- nil
			return
		}
		accepted <- c // held open, never written to
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	srv := <-accepted
	if srv == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return http1.NewConn(cli)
}

// TestReadResponse_CtxCancelUnblocksSilentPeer pins that cancelling a context
// with no deadline unblocks a response read against a peer that never answers.
//
// The write half honoured cancellation; the read half did not — ReadResponse
// consulted only ctx.Deadline(). A caller with a cancellable, deadline-less
// context therefore hung forever, and because the exchange is only released when
// the response completes or the caller closes it, each hung request also held its
// pool slot for good.
func TestReadResponse_CtxCancelUnblocksSilentPeer(t *testing.T) {
	ex := silentPeer(t).NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, rerr := ex.ReadResponse(ctx)
		done <- rerr
	}()

	time.Sleep(100 * time.Millisecond) // let the read actually block
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ReadResponse returned nil after cancellation; want an error")
		}
		// The mechanism that unblocks the read is a deadline in the past, so the
		// socket reports `i/o timeout` — a word this caller never used. Saying so
		// verbatim tells a caller its request timed out when in fact the caller
		// cancelled it, and it is what put a cancellation outside isHardStop's
		// reach one layer up. See deadlineCause.
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ReadResponse after cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ReadResponse did not return after ctx cancellation — a cancellable " +
			"context with no deadline must still unwind a silent peer")
	}
}

// TestReadResponse_AlreadyCancelledCtxFailsFast is the deterministic half: a
// context already cancelled when the read starts must not block at all.
func TestReadResponse_AlreadyCancelledCtxFailsFast(t *testing.T) {
	ex := silentPeer(t).NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, _, rerr := ex.ReadResponse(ctx)
		done <- rerr
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("ReadResponse with an already-cancelled context returned nil, want an error")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ReadResponse with an already-cancelled context = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ReadResponse hung on an already-cancelled context")
	}
}

// TestReadBodyChunk_CtxCancelUnblocks pins the same for the body read, which has
// no ctx parameter of its own and so reuses the one ReadResponse was given.
func TestReadBodyChunk_CtxCancelUnblocks(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			accepted <- nil
			return
		}
		// A head promising a body, then silence: the head parses, the body read
		// blocks.
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n"))
		accepted <- c
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	srv := <-accepted
	if srv == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = srv.Close() })

	ex := http1.NewConn(cli).NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, berr := ex.ReadBodyChunk(make([]byte, 64))
		done <- berr
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ReadBodyChunk returned nil after cancellation; want an error")
		}
		if ex.KeepAlive() {
			t.Error("KeepAlive() = true after a failed body read, want false")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ReadBodyChunk did not return after ctx cancellation")
	}
}

// deadlineCtx carries a deadline and nothing else: Done returns nil, so it can
// never be cancelled. context.Context allows exactly this — "Done may return nil
// if this context can never be canceled" — and a caller is entitled to have the
// deadline it declares enforced either way.
//
// It exists to separate the two mechanisms that can end a stalled read, which a
// context.WithTimeout cannot. ReadResponse installs ctx's deadline on the socket;
// the watchdog independently installs one in the past when ctx is cancelled, and
// a context.WithTimeout is cancelled at its deadline. So against a WithTimeout
// either mechanism ALONE finishes the read with the same error, and a test using
// one cannot tell which did the work — deleting the socket-deadline arming
// outright leaves the whole http1 and client suites green.
//
// armWatch declines a context whose Done() is nil, so with this one there is no
// watchdog and exactly one thing left that can end the read: the deadline
// ReadResponse puts on the socket.
type deadlineCtx struct{ dl time.Time }

func (c deadlineCtx) Deadline() (time.Time, bool) { return c.dl, true }
func (c deadlineCtx) Done() <-chan struct{}       { return nil }
func (c deadlineCtx) Err() error                  { return nil }
func (c deadlineCtx) Value(any) any               { return nil }

// TestReadResponse_StalledPeerIsThisContextsDeadline pins the promise a caller
// was given: a peer that accepts the request and then says nothing ends the
// request at the caller's own budget, reported as context.DeadlineExceeded.
//
// This is the mechanism the fix for a stalled read rests on, and nothing pinned
// it. Request.Timeout promises context.DeadlineExceeded and CLIENT_GUIDE repeats
// that such a request is never retried; the socket says `i/o timeout`, which does
// not match it, so isHardStop missed the case and a load generator's IsRetryable
// — a net.Error whose Timeout() is true is transient — replayed a request that
// had already spent its whole budget against the same silent peer.
//
// Both halves are asserted, as the integration leg does: the error still names
// the socket so a log can say what stalled, AND it classifies as this context's
// deadline so a retry classifier stops. Checking only the second would also pass
// for a request that failed before reaching http1 at all.
func TestReadResponse_StalledPeerIsThisContextsDeadline(t *testing.T) {
	ex := silentPeer(t).NewExchange()
	ctx := deadlineCtx{dl: time.Now().Add(300 * time.Millisecond)}

	if err := ex.WriteRequest(ctx, getFields(), true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, rerr := ex.ReadResponse(ctx)
		done <- rerr
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ReadResponse against a peer that never answers returned nil, want an error")
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("ReadResponse against a stalled peer = %v, want the socket error kept "+
				"as a cause — a log needs to name what stalled", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("ReadResponse against a stalled peer = %v, want context.DeadlineExceeded — "+
				"a read deadline on this connection can only be this exchange's own budget "+
				"running out, and a caller classifies retries on that", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ReadResponse never returned against a silent peer: the deadline its " +
			"context carries was never installed on the socket, so nothing bounds the read")
	}
}

// TestReadResponse_ClearsADeadlineItsContextDoesNotCarry pins the other half of
// the same unconditional arming: an exchange whose context has NO deadline must
// run without one, whatever was on the socket before it.
//
// TestReadDeadline_DoesNotLeakToTheNextExchange covers a deadline this package
// installed itself and so cannot fail once the arming is gone — with nothing
// installing a deadline there is nothing left to leak. The deadline here arrives
// from outside instead: a caller that set one for its own dial or handshake
// bookkeeping and handed the net.Conn over without clearing it. Only the zero
// value written on entry rescues that request.
func TestReadResponse_ClearsADeadlineItsContextDoesNotCarry(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		drainReqHead(br)
		time.Sleep(150 * time.Millisecond) // let the caller's stale deadline lapse first
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
		time.Sleep(100 * time.Millisecond)
	}()

	// The caller's leftover, elapsing well before the response arrives.
	if err := client.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	ex := http1.NewConn(client).NewExchange()
	if err := ex.WriteRequest(context.Background(), getFields(), true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse with a context carrying no deadline = %v, want nil — "+
			"a deadline this exchange never asked for ended it at an instant that had "+
			"nothing to do with it", err)
	}
}
