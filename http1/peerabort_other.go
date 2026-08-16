//go:build !windows

package http1

import (
	"errors"
	"syscall"
)

// isPeerAbort reports whether err is the local stack saying the peer has already
// destroyed this connection. See the Windows sibling for why the two platforms
// cannot share one expression.
func isPeerAbort(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED)
}
