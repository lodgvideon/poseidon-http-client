//go:build linux || darwin

package http1

import (
	"syscall"
	"unsafe"
)

// socketPending reports the octets queued on fd, and whether the query worked.
//
// FIONREAD is query-only: it consumes nothing, cannot block, and needs no
// deadline. See the Windows sibling for why this is preferred over a
// MSG_PEEK|MSG_DONTWAIT recv.
func socketPending(fd uintptr) (int, bool) {
	var n int32
	// G103: FIONREAD writes the count through the third argument, so the ioctl
	// needs the address of n. n is a local of exactly the width the kernel writes
	// and nothing escapes.
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, fionread, uintptr(unsafe.Pointer(&n))) //nolint:gosec // audited above
	if errno != 0 {
		return 0, false
	}
	return int(n), true
}
