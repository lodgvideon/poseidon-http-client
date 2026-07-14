//go:build linux

package quic

import (
	"encoding/binary"
	"io"
	"syscall"
)

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

// RecvGRO reads one datagram, or one GRO-coalesced burst of datagrams, from the
// socket behind rc using the raw file descriptor, parsing the ancillary data for
// the UDP_GRO segment size. It returns n bytes read into buf and segSize: 0 (or
// >= n) for a single datagram, or 0 < segSize < n when buf[:n] holds several
// coalesced datagrams of segSize bytes (the last may be shorter). oob is a reused
// scratch buffer that receives the control message.
//
// It degrades gracefully to a single plain Read (segSize 0) when rc is nil or a
// recvmsg quirk occurs, so a GRO bug can never mis-frame or drop a datagram —
// correctness over the syscall saving. A read-deadline expiry is surfaced verbatim:
// RawConn.Read honors the socket deadline through the runtime poller, so the
// receive path's PTO/idle timing and its non-blocking drain are unchanged.
//
// stdlib-only: the datagram is read with syscall.Recvmsg and the control message
// parsed with syscall.ParseSocketControlMessage; no golang.org/x/sys.
func RecvGRO(rc syscall.RawConn, fallback io.Reader, buf, oob []byte) (n, segSize int, err error) {
	if rc == nil {
		n, err = fallback.Read(buf)
		return n, 0, err
	}
	var oobn int
	var opErr error
	if rerr := rc.Read(func(fd uintptr) bool {
		n, oobn, _, _, opErr = syscall.Recvmsg(int(fd), buf, oob, 0)
		return opErr != syscall.EAGAIN // wait for readability while the socket would block
	}); rerr != nil {
		return 0, 0, rerr // includes a read-deadline expiry, surfaced as a timeout
	}
	if opErr != nil {
		// A recvmsg quirk (a non-EAGAIN syscall error): fall back to a plain Read so a
		// datagram is still delivered rather than lost.
		n, err = fallback.Read(buf)
		return n, 0, err
	}
	return n, parseGROSegmentSize(oob[:oobn]), nil
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
