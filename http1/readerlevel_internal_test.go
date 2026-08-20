package http1

// The reader-level check that ProbeIdle and HasResidue each open with, and the
// masking that hid the whole pair (#824, #831).
//
// Both functions ask three layers in turn — this Conn's bufio reader, then the
// kernel receive queue, then (under TLS) the transport's own buffer — and the
// suite only ever put the octets on the SOCKET. Every existing case therefore
// let a later layer answer, so the first check was removable in both functions
// with the package green.
//
// These are internal tests on purpose. The state that matters is "octets in
// c.br with the kernel queue drained", and from outside the package neither
// half of that is observable — a fixture that merely hopes for it passes just as
// well when it never arranged it. Each test asserts its precondition before
// acting.
//
// One finding here is a NON-test. ProbeIdle's `c.br.Buffered() > 0`
// short-circuit reads like a second detection and is not one: peekUnder is
// `c.br.Peek(1)`, and Peek returns from the buffer with a nil error whenever
// anything is buffered, so the socket-level check reaches the same
// `return false`. Measured over ProbeIdle's whole input partition —
// buffered+drained, buffered with more still on the socket, empty+readable,
// empty+silent, empty+FIN, locally closed — disabling the short-circuit changed
// no verdict at all. It is an equivalent mutant with respect to what ProbeIdle
// returns (a fast path worth two SetReadDeadline syscalls), and what it really
// does is MASK the socket check in the buffered states, which is why #824's
// "disable both" row was caught while either alone survived. So only the socket
// check is pinned below; there is nothing to write for the other, and writing
// something would pin the optimisation rather than any behaviour.

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// internalProbePair returns a Conn wired to a live TCP peer plus that peer.
// net.Pipe is avoided for the same reason probeidle_test.go avoids it: its
// deadline behaviour is not a kernel socket's, and these tests are about what
// the kernel has.
func internalProbePair(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "dial")
	t.Cleanup(func() { _ = cli.Close() })

	srv := <-accepted
	require.NotNil(t, srv, "accept failed")
	t.Cleanup(func() { _ = srv.Close() })
	return NewConn(cli), srv
}

// queueDepth reports the kernel receive-queue depth through the same FIONREAD
// path HasResidue uses, and whether the platform could answer. It consumes
// nothing and buffers nothing, which is what makes it usable as a precondition
// check in a test whose subject is the FIRST call to a probe.
func queueDepth(c *Conn) (int, bool) {
	if c.rawCtl == nil {
		return 0, false
	}
	c.pendN, c.pendOK = 0, false
	if cerr := c.rawCtl.Control(c.ctlFn); cerr == nil && c.pendOK {
		return c.pendN, true
	}
	return 0, false
}

// awaitDelivered waits for the peer's octets to reach the kernel receive queue
// WITHOUT touching c.br, so the caller's next call really is the first one that
// can buffer anything. On a platform with no FIONREAD it falls back to a fixed
// wait: loopback delivery of a few dozen octets is microseconds, so 200ms is
// four orders of magnitude of headroom, and the caller asserts c.br is still
// empty either way.
func awaitDelivered(t *testing.T, c *Conn) {
	t.Helper()
	for i := 0; i < 100; i++ {
		n, known := queueDepth(c)
		if !known {
			time.Sleep(200 * time.Millisecond)
			return
		}
		if n > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	require.Fail(t, "the peer's octets never reached the receive queue")
}

// TestProbeIdle_SocketReadableEvictsOnFirstProbe pins the socket-level detection
// on its own (#824).
//
// TestProbeIdle_UnsolicitedDataEvicts, which looks like it covers this, polls in
// a loop — and the first call that reaches the socket check succeeds at Peek,
// which LEAVES the octet in the bufio reader, so every later call is answered by
// the buffered short-circuit instead. Under a socket check inverted to report a
// readable socket as healthy, that loop still went green on its second poll. The
// verdict has to be taken on the FIRST call, with delivery awaited outside the
// probe, or the loop keeps rescuing the assertion.
func TestProbeIdle_SocketReadableEvictsOnFirstProbe(t *testing.T) {
	c, srv := internalProbePair(t)
	_, err := srv.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
	require.NoError(t, err, "server write")
	awaitDelivered(t, c)
	require.Zero(t, c.br.Buffered(),
		"precondition: nothing may be buffered yet, or the buffered short-circuit "+
			"and not the socket check is what produces the verdict below")

	reusable := c.ProbeIdle()

	pending, _ := queueDepth(c)
	assert.Falsef(t, reusable,
		"ProbeIdle() = true on the first probe of a socket carrying unsolicited "+
			"octets. RFC 9110 makes data arriving with no outstanding request not a "+
			"valid response, so handing this conn to the next request makes it parse "+
			"peer-chosen bytes as its own status line — and %d octets are sitting "+
			"there right now", pending)
}

// TestHasResidue_BufferedInReaderWithSocketDrained pins HasResidue's first check
// (#831): octets in this Conn's own bufio reader while the kernel queue is
// empty.
//
// That is what an over-read leaves behind, and it is not hypothetical — driving
// one complete exchange whose wire carried a trailing unsolicited response left
// 41 octets in c.br with FIONREAD reporting 0, five runs out of five. It is also
// the state the guard is most load-bearing in, because every layer below it
// answers "clean": with the check removed, FIONREAD reports 0 on a plain socket
// and HasResidue returns false while a complete attacker-chosen response is
// already inside the Conn.
//
// The reader is primed with Peek rather than by driving an exchange, because
// Peek(n) blocks until exactly n octets are buffered and drains the socket doing
// it — no dependence on how the peer's write happened to be segmented.
func TestHasResidue_BufferedInReaderWithSocketDrained(t *testing.T) {
	const smuggled = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\npwn"
	c, srv := internalProbePair(t)
	_, err := srv.Write([]byte(smuggled))
	require.NoError(t, err, "server write")
	require.NoError(t, c.nc.SetReadDeadline(time.Now().Add(5*time.Second)), "arm a bounded prime")
	_, err = c.br.Peek(len(smuggled))
	require.NoError(t, err, "priming the bufio reader")
	require.NoError(t, c.nc.SetReadDeadline(time.Time{}), "clear the prime deadline")
	require.Equal(t, len(smuggled), c.br.Buffered(),
		"precondition: every octet must be in c.br, or this is not the over-read state")
	pending, known := queueDepth(c)
	require.True(t, known, "precondition: this platform must be able to report the queue depth")
	require.Zerof(t, pending,
		"precondition: the kernel queue must be drained (%d octets left), or FIONREAD "+
			"and not the reader check is what answers", pending)
	require.False(t, c.layered,
		"precondition: a plain socket, so the FIONREAD branch below is the one that "+
			"would report this connection clean")

	residue := c.HasResidue()

	assert.True(t, residue,
		"HasResidue() = false with a complete unsolicited response already inside "+
			"this Conn's reader. Every layer under the reader check answers clean here "+
			"— the socket really is empty — so pooling this connection hands the next "+
			"request an attacker-chosen response with err=nil")
}
