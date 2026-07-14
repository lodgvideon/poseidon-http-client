//go:build !linux

package quic

import (
	"io"
	"syscall"
)

// SendGSO on platforms without UDP generic segmentation offload (Windows — this
// dev host — macOS, the BSDs) writes buf as consecutive UDP datagrams of segSize
// bytes, the last of which may be shorter, with one Write per datagram. The result
// is byte-identical on the wire to the pre-GSO send path; rc is ignored. This keeps
// udpConn.WriteGSO functional and the whole tree building off Linux with no
// platform-specific syscalls.
func SendGSO(_ syscall.RawConn, fallback io.Writer, buf []byte, segSize int) (int, error) {
	return writeSegmentsTo(fallback, buf, segSize)
}
