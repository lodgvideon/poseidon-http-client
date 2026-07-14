package quic

import (
	"net"
	"os"
	"sync"
	"time"
)

// This file holds the server-role listener: one UDP socket serving many QUIC
// connections. connPacketConn is the per-connection view of that socket, so an
// accepted Conn drives its ordinary Poll loop without knowing it shares the
// socket with other connections.

// connPacketConn is one connection's view of a listener's shared UDP socket: it
// reads the datagrams the listener routed to this connection and writes back to
// that connection's peer through the shared socket.
//
// It implements SetReadDeadline because a Conn drives its Poll loop with read
// deadlines (loss timers, idle timeout, cancellation) and pokes the deadline into
// the past to unblock a parked Read. Per the net.Conn contract that must be safe
// while a Read is blocked and take effect immediately, so a parked Read here
// re-evaluates whenever the deadline changes.
type connPacketConn struct {
	shared *net.UDPConn
	remote *net.UDPAddr
	in     chan []byte // datagrams the listener demuxed for this connection

	mu       sync.Mutex
	deadline time.Time
	changed  chan struct{} // replaced on every SetReadDeadline to wake a parked Read

	closeOnce sync.Once
	closed    chan struct{}
}

// newConnPacketConn builds a per-connection view of shared addressed to remote.
// depth bounds the inbound queue; a full queue drops datagrams like a kernel
// receive buffer would.
func newConnPacketConn(shared *net.UDPConn, remote *net.UDPAddr, depth int) *connPacketConn {
	return &connPacketConn{
		shared:  shared,
		remote:  remote,
		in:      make(chan []byte, depth),
		changed: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// Write sends one datagram to this connection's peer over the shared socket.
func (c *connPacketConn) Write(p []byte) (int, error) {
	return c.shared.WriteToUDP(p, c.remote)
}

// Read returns the next datagram the listener routed to this connection. It
// honours the read deadline, returning os.ErrDeadlineExceeded (a net.Error whose
// Timeout reports true) when it expires, and net.ErrClosed after Close.
func (c *connPacketConn) Read(p []byte) (int, error) {
	for {
		c.mu.Lock()
		dl, changed := c.deadline, c.changed
		c.mu.Unlock()

		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			d := time.Until(dl)
			if d <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(d)
			timeout = timer.C
		}

		select {
		case dg := <-c.in:
			stopTimer(timer)
			return copy(p, dg), nil
		case <-timeout:
			return 0, os.ErrDeadlineExceeded
		case <-c.closed:
			stopTimer(timer)
			return 0, net.ErrClosed
		case <-changed:
			stopTimer(timer) // the deadline moved; re-evaluate it
		}
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

// SetReadDeadline sets the deadline for future and in-flight Reads; a zero time
// clears it. It wakes a parked Read so a deadline moved into the past unblocks it
// at once (RFC-independent net.Conn semantics the Conn's PTO path relies on).
func (c *connPacketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	close(c.changed) // wake any parked Read to re-evaluate
	c.changed = make(chan struct{})
	c.mu.Unlock()
	return nil
}

// Close releases this connection's view. The shared socket stays open for the
// listener's other connections; only the listener closes it.
func (c *connPacketConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// deliver hands a demuxed datagram to this connection. It never blocks: a full
// queue drops the datagram, as a kernel receive buffer would, because stalling
// the listener's demux loop would starve every other connection. QUIC loss
// recovery handles the drop.
func (c *connPacketConn) deliver(dg []byte) {
	select {
	case c.in <- dg:
	default:
	}
}
