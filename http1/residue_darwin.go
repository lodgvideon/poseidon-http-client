//go:build darwin

package http1

// fionread is the BSD FIONREAD — _IOR('f', 127, u_long) — reporting the octets
// readable without blocking.
const fionread = 0x4004667F
