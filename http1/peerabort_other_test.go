//go:build !windows

package http1

import "syscall"

// testAbortErrno is what a read on a connection the peer has reset returns on the
// Unix platforms.
var testAbortErrno error = syscall.ECONNRESET
