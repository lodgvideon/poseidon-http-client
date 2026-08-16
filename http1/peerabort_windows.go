//go:build windows

package http1

import (
	"errors"
	"syscall"
)

// isPeerAbort reports whether err is the local stack saying the peer has already
// destroyed this connection.
//
// The Winsock codes, not the ECONNRESET / ECONNABORTED that the same expression
// would use on Unix: Go's syscall package defines those two on Windows as
// synthetic APPLICATION_ERROR values (536870935 and 536870933) that no socket
// call ever returns, so a portable-looking errors.Is against them compiles here
// and matches nothing. A read on a reaped keep-alive returns errno 10053,
// WSAECONNABORTED.
func isPeerAbort(err error) bool {
	return errors.Is(err, syscall.WSAECONNABORTED) || errors.Is(err, syscall.WSAECONNRESET)
}
