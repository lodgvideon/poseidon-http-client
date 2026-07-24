//go:build linux

package http1

// fionread is Linux's FIONREAD (== TIOCINQ): report the octets readable without
// blocking.
const fionread = 0x541B
