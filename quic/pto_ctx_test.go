package quic

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	_, err := c.readWithPTO(ctx, make([]byte, 64))

	require.ErrorIsf(t, err, context.Canceled,
		"readWithPTO with cancelled ctx = %v, want context.Canceled", err)
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
	_, err := c.readWithPTO(ctx, make([]byte, 2048))
	elapsed := time.Since(start)

	require.ErrorIsf(t, err, context.Canceled, "readWithPTO = %v, want context.Canceled", err)
	assert.Lessf(t, elapsed, 2*time.Second,
		"cancel took %v — the watchdog did not interrupt the read promptly", elapsed)
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

	_, err := c.readWithPTO(ctx, make([]byte, 2048))

	require.ErrorIsf(t, err, context.Canceled, "readWithPTO = %v, want context.Canceled", err)
	assert.Zerof(t, c.ptoCount,
		"ptoCount = %d, a context cancel must not trigger a probe", c.ptoCount)
}
