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
func SendGSO(_ *GSOState, _ syscall.RawConn, fallback io.Writer, buf []byte, segSize int) (int, error) {
	return writeSegmentsTo(fallback, buf, segSize)
}

// GSOState is the handle SendGSO takes on every platform. Off Linux there is no
// syscall closure and so nothing to hoist out of it; it exists, empty, so that
// http3's udpConn is one piece of code that compiles and behaves identically
// everywhere. See the Linux file for what it carries there.
type GSOState struct{}

// NewGSOState returns empty send state: there is no UDP_SEGMENT control message
// to build on a platform that cannot segment.
func NewGSOState() *GSOState { return &GSOState{} }
