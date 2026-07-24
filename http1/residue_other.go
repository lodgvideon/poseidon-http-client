//go:build !windows && !linux && !darwin

package http1

// socketPending reports that the kernel receive queue cannot be inspected on
// this platform.
//
// HasResidue degrades to its reader-and-TLS-buffer checks, and the caller's
// existing ProbeIdle path — correct, just ~1ms — remains the backstop. Every
// dialer this module ships produces a *net.TCPConn or a *tls.Conn wrapping one,
// so no built-in path reaches here.
func socketPending(uintptr) (int, bool) {
	return 0, false
}
