package conn

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// countingConn counts Read calls on the underlying socket. On a real TCP
// connection each Read is (at least) one read(2) syscall, so the count is a
// direct, cross-platform proxy for read-syscall volume — no strace needed.
type countingConn struct {
	net.Conn
	reads atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

// TestReadBuffer_FewerSyscalls proves the receive-path optimization in
// NewClientConn: wrapping the transport reader in bufio collapses the per-frame
// 2× ReadFull (9-byte header + payload) into far fewer socket reads when frames
// arrive together — the h2c response-receive hot path. It compares a raw-reader
// Framer against a bufio-wrapped one (size readBufferSize, as NewClientConn
// uses) reading the same burst of frames off a real TCP loopback socket. A real
// socket is required: the kernel coalesces the writes so one read can drain many
// frames; net.Pipe delivers exactly one Write per Read and could not show this.
func TestReadBuffer_FewerSyscalls(t *testing.T) {
	const frames = 256
	raw := countReadSyscalls(t, frames, false)
	buffered := countReadSyscalls(t, frames, true)
	t.Logf("socket reads to drain %d frames: raw=%d buffered=%d", frames, raw, buffered)
	if buffered >= raw {
		t.Fatalf("buffered reader did not reduce reads: raw=%d buffered=%d", raw, buffered)
	}
	// The win must be large, not marginal: raw is ~2 reads/frame, buffered
	// should be a small constant once the burst is in the kernel buffer.
	if buffered > raw/4 {
		t.Fatalf("buffered reads %d not < raw/4 (%d): batching ineffective", buffered, raw/4)
	}
}

// countReadSyscalls writes nframes small DATA frames into a TCP socket, then
// drains them through a Framer whose reader is raw or bufio-wrapped, returning
// the number of socket Read calls the client made.
func countReadSyscalls(t *testing.T, nframes int, buffered bool) int64 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	payload := make([]byte, 64) // small frames -> batching matters most
	ready := make(chan struct{})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		sc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = sc.Close() }()
		sf := frame.NewFramer(sc, sc)
		for i := 0; i < nframes; i++ {
			if werr := sf.WriteData(1, i == nframes-1, payload); werr != nil {
				return
			}
		}
		close(ready) // all frames are now in the kernel send/recv buffer
		<-stop
	}()

	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = cc.Close() }()
	counter := &countingConn{Conn: cc}

	var r io.Reader = counter
	if buffered {
		r = bufio.NewReaderSize(counter, readBufferSize)
	}
	cf := frame.NewFramer(io.Discard, r)
	cf.SetMaxReadFrameSize(1 << 20)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not finish writing in time")
	}

	_ = cc.SetReadDeadline(time.Now().Add(10 * time.Second))
	var h nilHandler
	for got := 0; got < nframes; got++ {
		if _, rerr := cf.ReadFrame(context.Background(), &h); rerr != nil {
			t.Fatalf("ReadFrame %d/%d: %v", got, nframes, rerr)
		}
	}
	return counter.reads.Load()
}
