//go:build windows

package http1

import (
	"syscall"
	"unsafe"
)

// fionread is the Winsock FIONREAD ioctl: report the octets readable without
// blocking. Same value as the BSD one — _IOR('f', 127, u_long).
const fionread = 0x4004667F

// socketPending reports the octets queued on fd, and whether the query worked.
//
// WSAIoctl rather than a raw recv with MSG_PEEK|MSG_DONTWAIT: Go's Windows
// sockets are overlapped handles associated with an IOCP, so a raw recv on one
// can block and syscall.RawConn.Read does not carry the same semantics it has on
// Unix. FIONREAD is query-only — it consumes nothing and cannot block — which
// makes it the symmetric primitive across both platforms.
func socketPending(fd uintptr) (int, bool) {
	var n, ret uint32
	// G103: the ioctl writes the count into n; passing its address is the only
	// way to receive an out-parameter from WSAIoctl. n is a local of exactly the
	// width the call is told to write (4), so the conversion is bounded by
	// construction and nothing escapes.
	err := syscall.WSAIoctl(syscall.Handle(fd), fionread, nil, 0,
		(*byte)(unsafe.Pointer(&n)), 4, &ret, nil, 0) //nolint:gosec // audited above
	if err != nil {
		return 0, false
	}
	return int(n), true
}
