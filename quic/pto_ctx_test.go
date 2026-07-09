package quic

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestReadWithPTO_AlreadyCancelled: a context already done returns its error
// without reading.
func TestReadWithPTO_AlreadyCancelled(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &Conn{pc: client}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.readWithPTO(ctx, make([]byte, 64)); err != context.Canceled {
		t.Fatalf("readWithPTO with cancelled ctx = %v, want context.Canceled", err)
	}
}

// TestReadWithPTO_CancelUnblocksRead: a cancel during a blocking read interrupts
// it promptly via the watchdog (which pokes the read deadline into the past). Run
// under -race, this also validates the watchdog goroutine's concurrent
// SetReadDeadline against the in-flight Read.
func TestReadWithPTO_CancelUnblocksRead(t *testing.T) {
	client, server := net.Pipe() // Read blocks (server never writes) but honors deadlines
	defer client.Close()
	defer server.Close()
	c := &Conn{pc: client}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := c.readWithPTO(ctx, make([]byte, 2048)); err != context.Canceled {
		t.Fatalf("readWithPTO = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("cancel took %v — the watchdog did not interrupt the read promptly", d)
	}
}

// TestReadWithPTO_CtxCancelNoPTOSpin: with data in flight (the PTO path armed), a
// context cancel is a terminal exit — it must not fire a probe (onPTO would bump
// ptoCount) or spin.
func TestReadWithPTO_CtxCancelNoPTOSpin(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &Conn{pc: client}
	c.sent[spaceApp].onSent(0, time.Now(), true, nil) // an unacked ack-eliciting packet → hasInFlight
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if _, err := c.readWithPTO(ctx, make([]byte, 2048)); err != context.Canceled {
		t.Fatalf("readWithPTO = %v, want context.Canceled", err)
	}
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d, a context cancel must not trigger a probe", c.ptoCount)
	}
}
