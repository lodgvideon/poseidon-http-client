//go:build windows

package http1

import "syscall"

// testAbortErrno is what a read on a keep-alive the peer has reaped actually
// returns here, measured rather than assumed: WSAECONNABORTED (10053). Not
// syscall.ECONNABORTED, which Go defines on Windows as a synthetic
// APPLICATION_ERROR value no socket ever produces — a test built on it would pass
// against a classifier that matches nothing real.
var testAbortErrno error = syscall.WSAECONNABORTED
