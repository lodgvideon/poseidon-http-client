//go:build !linux

package quic

import (
	"io"
	"syscall"
)

// groCanCoalesce is false off Linux: see the Linux file. RecvGRO here is a plain
// Read, so the burst buffer would be dead memory on every connection.
const groCanCoalesce = false

// EnableGRO is a no-op on platforms without UDP generic receive offload (Windows —
// this dev host — macOS, the BSDs): there is no socket option to set, so the
// receive path reads one datagram per syscall exactly as before. rc is ignored.
func EnableGRO(_ syscall.RawConn) error { return nil }

// GROState is the handle RecvGRO takes on every platform. Off Linux there is no
// syscall closure and so nothing to hoist out of it; it exists, empty, so that
// http3's udpConn is one piece of code that compiles and behaves identically
// everywhere. See the Linux file for what it carries there.
type GROState struct{}

// NewGROState returns empty receive state. oobLen is ignored: no control message
// is read on a platform with no UDP_GRO to report one.
func NewGROState(_ int) *GROState { return &GROState{} }

// RecvGRO on a platform without GRO reads a single datagram with a plain Read and
// reports segSize 0 (one datagram), so udpConn.ReadGRO stays functional and the
// whole tree builds off Linux with no platform-specific syscalls. g and rc are
// ignored. The result is byte-identical to the pre-GRO receive path.
func RecvGRO(_ *GROState, _ syscall.RawConn, fallback io.Reader, buf []byte) (n, segSize int, err error) {
	n, err = fallback.Read(buf)
	return n, 0, err
}
