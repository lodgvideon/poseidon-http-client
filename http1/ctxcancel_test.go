package http1_test

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestWriteBody_CtxCancelUnblocksWedgedWrite pins that cancelling a context with
// no deadline unblocks a request-body write against a peer that has stopped
// reading.
//
// This exchange is single-goroutine by design — it writes the whole body before
// reading anything — which is a defensible simplification only while a wedged
// upload can still be unwound. A blocking Write cannot be selected on, and
// http1 previously honoured ctx.Deadline() but never ctx.Done(), so a caller
// passing a cancellable-but-deadline-less context hung forever against a server
// that simply stopped reading.
func TestWriteBody_CtxCancelUnblocksWedgedWrite(t *testing.T) {
	wc := newWedgeConn()
	ex := http1.NewConn(wc).NewExchange()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// No Content-Length → chunked framing, so nothing reconciles the length.
	if err := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	wc.arm()
	done := make(chan error, 1)
	go func() { done <- ex.WriteBody(ctx, []byte("wedged"), true) }()

	// The write is blocked in the peer stand-in; only setting a past write
	// deadline can release it, which is precisely what cancellation must do.
	<-wc.blocked
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WriteBody returned nil after cancellation; want an error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("WriteBody did not return after ctx cancellation — a cancellable " +
			"context with no deadline must still unwind a wedged upload")
	}
}

// wedgeConn stands in for a peer that has stopped reading: Write blocks until a
// write deadline in the past is set, then fails the way a real socket does.
//
// A real loopback socket is not usable for this: its buffers (with TCP
// autotuning) absorb tens of megabytes before a write blocks, and SetWriteBuffer
// is not reliably honoured, so the write completes and the test proves nothing.
// This isolates exactly the contract under test — cancellation must set a past
// deadline, and that must release a blocked write.
type wedgeConn struct {
	net.Conn // nil: only the methods below are exercised
	armed    atomic.Bool
	blocked  chan struct{}
	release  chan struct{}
	once     sync.Once
	blockOne sync.Once
}

func newWedgeConn() *wedgeConn {
	return &wedgeConn{blocked: make(chan struct{}), release: make(chan struct{})}
}

// arm makes every subsequent Write block. The request head has to go out first,
// and net.Buffers.WriteTo falls back to one Write per buffer on a conn that is
// not a *net.TCPConn, so "block from the second write" would stall mid-head.
func (c *wedgeConn) arm() { c.armed.Store(true) }

func (c *wedgeConn) Write(p []byte) (int, error) {
	if !c.armed.Load() {
		return len(p), nil
	}
	c.blockOne.Do(func() { close(c.blocked) })
	<-c.release
	return 0, os.ErrDeadlineExceeded
}

func (c *wedgeConn) SetWriteDeadline(t time.Time) error {
	// A deadline at or before now is the "unblock the write" signal a real
	// netpoll implements; a zero time is the release path clearing it.
	if !t.IsZero() && !t.After(time.Now()) {
		c.once.Do(func() { close(c.release) })
	}
	return nil
}

func (c *wedgeConn) SetReadDeadline(time.Time) error { return nil }
func (c *wedgeConn) Close() error                    { return nil }

// TestWriteBody_AlreadyCancelledCtxFailsFast is the deterministic half of the
// same contract: a context already cancelled when the write starts must make it
// fail rather than proceed. It exercises the same watchdog/deadline mechanism
// without depending on socket buffers filling.
func TestWriteBody_AlreadyCancelledCtxFailsFast(t *testing.T) {
	ex, _ := rawCapture(t)
	if err := ex.WriteRequest(context.Background(), []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- ex.WriteBody(ctx, []byte("hello"), true) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("WriteBody with an already-cancelled context returned nil, want an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WriteBody hung on an already-cancelled context")
	}
}

// TestWriteBody_NoDeadlineNoCancelStillWrites is the over-rejection guard: a
// background context (not cancellable, no deadline) must not have a deadline
// latched onto the connection by the arming helper.
func TestWriteBody_NoDeadlineNoCancelStillWrites(t *testing.T) {
	ex, capture := rawCapture(t)
	if err := ex.WriteRequest(context.Background(), reqCL("POST",
		header.Field{Name: []byte("Content-Length"), Value: []byte("5")}), false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(context.Background(), []byte("HELLO"), true); err != nil {
		t.Fatalf("WriteBody: %v", err)
	}
	if wire := capture(); wire == "" {
		t.Error("nothing reached the wire")
	}
}
