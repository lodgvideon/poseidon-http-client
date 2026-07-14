//go:build !linux

package quic

import (
	"io"
	"syscall"
)

// EnableGRO is a no-op on platforms without UDP generic receive offload (Windows —
// this dev host — macOS, the BSDs): there is no socket option to set, so the
// receive path reads one datagram per syscall exactly as before. rc is ignored.
func EnableGRO(_ syscall.RawConn) error { return nil }

// RecvGRO on a platform without GRO reads a single datagram with a plain Read and
// reports segSize 0 (one datagram), so udpConn.ReadGRO stays functional and the
// whole tree builds off Linux with no platform-specific syscalls. rc and oob are
// ignored. The result is byte-identical to the pre-GRO receive path.
func RecvGRO(_ syscall.RawConn, fallback io.Reader, buf, _ []byte) (n, segSize int, err error) {
	n, err = fallback.Read(buf)
	return n, 0, err
}
