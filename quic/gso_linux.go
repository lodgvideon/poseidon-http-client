//go:build linux

package quic

import (
	"encoding/binary"
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
func SendGSO(rc syscall.RawConn, fallback io.Writer, buf []byte, segSize int) (int, error) {
	if rc == nil || segSize <= 0 || len(buf) <= segSize {
		return writeSegmentsTo(fallback, buf, segSize)
	}
	oob := gsoControl(segSize)
	var n int
	var opErr error
	if err := rc.Write(func(fd uintptr) bool {
		n, opErr = syscall.SendmsgN(int(fd), buf, oob, nil, 0)
		return opErr != syscall.EAGAIN // retry only while the socket is not writable
	}); err != nil {
		return 0, err
	}
	if opErr != nil {
		// The offload was rejected for this buffer/path: fall back so the datagrams
		// are still delivered. A GSO bug must never drop or corrupt a datagram.
		return writeSegmentsTo(fallback, buf, segSize)
	}
	return n, nil
}

// gsoControl builds the ancillary data (a single cmsg) carrying UDP_SEGMENT set to
// segSize: a cmsghdr {level=SOL_UDP, type=UDP_SEGMENT} followed by the uint16
// segment size. It sizes and aligns the buffer with the stdlib's Cmsg helpers and
// overlays syscall.Cmsghdr on it, so the layout is correct on every GOARCH without
// hard-coding struct offsets. The segment size is a host-order u16 (a socket
// option, not on the wire), so native byte order is correct.
func gsoControl(segSize int) []byte {
	oob := make([]byte, syscall.CmsgSpace(2))
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&oob[0])) //nolint:gosec // overlay cmsghdr on its own buffer
	h.Level = solUDP
	h.Type = udpSegment
	h.SetLen(syscall.CmsgLen(2))
	//nolint:gosec // segSize is a bounded datagram length (<= maxDatagramSize), never overflows u16
	binary.NativeEndian.PutUint16(oob[syscall.CmsgLen(0):], uint16(segSize))
	return oob
}
