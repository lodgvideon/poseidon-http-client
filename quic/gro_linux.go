//go:build linux

package quic

import (
	"encoding/binary"
	"errors"
	"io"
	"syscall"
)

// groCanCoalesce reports whether this platform can actually coalesce datagrams
// on receive. Only Linux has UDP_GRO; everywhere else RecvGRO degrades to a plain
// single-datagram Read, so a transport that implements ReadGRO there is still one
// datagram per syscall and must not be given the large burst buffer.
const groCanCoalesce = true

// udpGRO is the Linux UDP generic-receive-offload socket option (linux/udp.h:
// UDP_GRO 104), the receive-side companion to UDP_SEGMENT (udpSegment, 103). The
// stdlib syscall package does not export it, and this client takes no third-party
// dependency (golang.org/x/sys is deliberately avoided), so it is defined here from
// the stable kernel UAPI. solUDP (SOL_UDP, 17) is shared with the GSO send path.
const udpGRO = 104

// EnableGRO turns on UDP_GRO for the socket behind rc, asking the kernel to
// coalesce consecutive same-size datagrams into one recvmsg and report the segment
// size out of band. It is best-effort: a kernel or socket that rejects the option
// simply never attaches a GRO control message, so RecvGRO reports segSize 0 and the
// receive path processes one datagram per read, exactly as before. A nil rc (no raw
// fd) is a no-op.
func EnableGRO(rc syscall.RawConn) error {
	if rc == nil {
		return nil
	}
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		opErr = syscall.SetsockoptInt(int(fd), solUDP, udpGRO, 1)
	}); err != nil {
		return err
	}
	return opErr
}

// GROState carries the receive state RecvGRO's syscall closure would otherwise
// capture, plus the ancillary-data buffer.
//
// RawConn.Read takes a func through an interface, so the closure escapes and
// drags every variable it captures to the heap with it — four heap objects on
// every recvmsg, on the QUIC receive hot path. Hoisting them onto a struct that
// lives as long as the connection makes that zero: the closure is built once in
// NewGROState and reads and writes fields instead of captured locals. It is
// stored rather than written inline at the call because a method value is itself
// allocated where it is taken, which would put the allocation straight back.
//
// A GROState is single-goroutine. http3's udpConn drives it from the QUIC reader
// goroutine only — the same contract the oob buffer it replaces already had.
type GROState struct {
	oob []byte // reused ancillary-data buffer receiving the GRO control message
	buf []byte // the caller's read buffer, held only for the call in progress
	// n, oobn and opErr are the closure's outputs.
	n     int
	oobn  int
	opErr error
	fn    func(uintptr) bool
}

// NewGROState returns receive state whose ancillary-data buffer is oobLen bytes,
// which must be large enough for the UDP_GRO control message.
func NewGROState(oobLen int) *GROState {
	g := &GROState{oob: make([]byte, oobLen)}
	g.fn = g.recv
	return g
}

// recv is the raw-fd callback. It is taken as a method value exactly once, in
// NewGROState.
func (g *GROState) recv(fd uintptr) bool {
	g.n, g.oobn, _, _, g.opErr = syscall.Recvmsg(int(fd), g.buf, g.oob, 0)
	// errors.Is, not !=: syscall.Errno implements Is, and a field-held error is
	// not provably unwrapped the way the local this replaced was.
	return !errors.Is(g.opErr, syscall.EAGAIN) // wait for readability while the socket would block
}

// RecvGRO reads one datagram, or one GRO-coalesced burst of datagrams, from the
// socket behind rc using the raw file descriptor, parsing the ancillary data for
// the UDP_GRO segment size. It returns n bytes read into buf and segSize: 0 (or
// >= n) for a single datagram, or 0 < segSize < n when buf[:n] holds several
// coalesced datagrams of segSize bytes (the last may be shorter). g supplies the
// control-message buffer and the reused syscall state.
//
// It degrades gracefully to a single plain Read (segSize 0) when g or rc is nil
// or a recvmsg quirk occurs, so a GRO bug can never mis-frame or drop a datagram —
// correctness over the syscall saving. A read-deadline expiry is surfaced verbatim:
// RawConn.Read honors the socket deadline through the runtime poller, so the
// receive path's PTO/idle timing and its non-blocking drain are unchanged.
//
// stdlib-only: the datagram is read with syscall.Recvmsg and the control message
// parsed with syscall.ParseSocketControlMessage; no golang.org/x/sys.
func RecvGRO(g *GROState, rc syscall.RawConn, fallback io.Reader, buf []byte) (n, segSize int, err error) {
	if g == nil || rc == nil {
		n, err = fallback.Read(buf)
		return n, 0, err
	}
	g.buf = buf
	rerr := rc.Read(g.fn)
	// Released immediately: holding the caller's read buffer between calls would
	// pin it for the life of the connection.
	g.buf = nil
	if rerr != nil {
		return 0, 0, rerr // includes a read-deadline expiry, surfaced as a timeout
	}
	if g.opErr != nil {
		// A recvmsg quirk (a non-EAGAIN syscall error): fall back to a plain Read so a
		// datagram is still delivered rather than lost.
		n, err = fallback.Read(buf)
		return n, 0, err
	}
	return g.n, parseGROSegmentSize(g.oob[:g.oobn]), nil
}

// parseGROSegmentSize returns the UDP_GRO segment size carried in the recvmsg
// ancillary data, or 0 when none is present (a single, non-coalesced datagram).
// The kernel reports the size in a {SOL_UDP, UDP_GRO} control message as a
// host-order int; a shorter body is read defensively as a u16 (a segment size is
// MTU-bounded, so it never needs more than 16 bits). A malformed control buffer
// yields 0, degrading to single-datagram processing.
func parseGROSegmentSize(oob []byte) int {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return 0
	}
	for _, m := range msgs {
		if m.Header.Level != solUDP || m.Header.Type != udpGRO {
			continue
		}
		switch {
		case len(m.Data) >= 4:
			return int(binary.NativeEndian.Uint32(m.Data))
		case len(m.Data) >= 2:
			return int(binary.NativeEndian.Uint16(m.Data))
		}
	}
	return 0
}
