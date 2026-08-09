//go:build linux

package quic

import (
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"unsafe"
)

// Linux UDP generic-segmentation-offload socket-option constants. The stdlib
// syscall package does not export them for every GOARCH, and this client takes no
// third-party dependency (golang.org/x/sys is deliberately avoided), so they are
// defined here. The values are part of the stable kernel UAPI:
//
//	SOL_UDP     17  — the UDP protocol level for setsockopt/cmsg (linux/udp.h)
//	UDP_SEGMENT 103 — GSO: the per-sendmsg segment size (linux/udp.h)
const (
	solUDP     = 17
	udpSegment = 103
)

// SendGSO writes buf as consecutive UDP datagrams of segSize bytes (the last may
// be shorter) in ONE sendmsg via the UDP_SEGMENT offload, using the raw file
// descriptor behind rc. It is the Linux transport primitive the udpConn's WriteGSO
// delegates to. It degrades gracefully to a per-datagram loop over fallback when:
//   - rc is nil or there is nothing to segment (a single datagram);
//   - the kernel/NIC rejects the offload at send time (e.g. EIO on a path that
//     cannot segment) — every datagram still goes out, correctness over the
//     syscall saving.
//
// stdlib-only: the control message is built by hand (gsoControl) and handed to
// syscall.SendmsgN; no golang.org/x/sys.
func SendGSO(g *GSOState, rc syscall.RawConn, fallback io.Writer, buf []byte, segSize int) (int, error) {
	if g == nil || rc == nil || segSize <= 0 || len(buf) <= segSize {
		return writeSegmentsTo(fallback, buf, segSize)
	}
	g.setSegSize(segSize)
	g.buf = buf
	err := rc.Write(g.fn)
	// Released immediately: holding the caller's send buffer between calls would
	// pin it for the life of the connection.
	g.buf = nil
	if err != nil {
		return 0, err
	}
	if g.opErr != nil {
		// The offload was rejected for this buffer/path: fall back so the datagrams
		// are still delivered. A GSO bug must never drop or corrupt a datagram.
		return writeSegmentsTo(fallback, buf, segSize)
	}
	return g.n, nil
}

// GSOState carries the send state SendGSO's syscall closure would otherwise
// capture, plus the ancillary-data buffer.
//
// Same shape and same reason as GROState on the receive side: RawConn.Write takes
// a func through an interface, so the closure escapes and takes its captures with
// it. Here it cost four heap objects per offloaded send — n, opErr, the closure,
// and the control buffer gsoControl built fresh every time even though only two
// of its bytes ever change.
//
// A GSOState is single-goroutine. http3's udpConn drives it from the sender only,
// never from the QUIC reader goroutine that owns the GROState — which is why the
// two directions get separate objects rather than one shared one.
type GSOState struct {
	oob []byte // ancillary data; the cmsg header is written once, the u16 per send
	buf []byte // the caller's send buffer, held only for the call in progress
	// n and opErr are the closure's outputs.
	n     int
	opErr error
	fn    func(uintptr) bool
}

// NewGSOState returns send state with its control message pre-built: a cmsghdr
// {level=SOL_UDP, type=UDP_SEGMENT} whose body is the uint16 segment size. It is
// sized and aligned with the stdlib's Cmsg helpers and overlays syscall.Cmsghdr on
// its own buffer, so the layout is correct on every GOARCH without hard-coding
// struct offsets.
func NewGSOState() *GSOState {
	g := &GSOState{oob: make([]byte, syscall.CmsgSpace(2))}
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&g.oob[0])) //nolint:gosec // overlay cmsghdr on its own buffer
	h.Level = solUDP
	h.Type = udpSegment
	h.SetLen(syscall.CmsgLen(2))
	g.fn = g.send
	return g
}

// setSegSize writes the segment size into the pre-built control message. The size
// is a host-order u16 (a socket option, not something on the wire), so native byte
// order is correct.
func (g *GSOState) setSegSize(segSize int) {
	//nolint:gosec // segSize is a bounded datagram length (<= maxDatagramSize), never overflows u16
	binary.NativeEndian.PutUint16(g.oob[syscall.CmsgLen(0):], uint16(segSize))
}

// send is the raw-fd callback. It is taken as a method value exactly once, in
// NewGSOState.
func (g *GSOState) send(fd uintptr) bool {
	g.n, g.opErr = syscall.SendmsgN(int(fd), g.buf, g.oob, nil, 0)
	// errors.Is, not !=: see the matching note in GROState.recv.
	return !errors.Is(g.opErr, syscall.EAGAIN) // retry only while the socket is not writable
}
