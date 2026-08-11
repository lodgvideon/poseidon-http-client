package http1_test

import (
	"context"
	"net"
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
