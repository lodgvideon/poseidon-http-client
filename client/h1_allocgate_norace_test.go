//go:build !race

package client

// h1AllocGateLimit is the bytes/request ceiling TestH1_AllocatedBytesPerRequest
// enforces. It is split by build tag because the race detector's own overhead
// moves the floor by ~4.5 KB, and a single value that is safe under -race is too
// loose to be a guard in plain mode — the case that matters, since a per-request
// buffer hidden behind a pointer is invisible to the struct-size test and would
// otherwise sail through both.
//
// Measured against h1RawServer (client-only; see the test's doc comment for why
// the peer is not httptest), 2 KiB response, steady state:
//
//	before the pooled-buffer fix : 23,066 B/req
//	after it                     :  2,596 B/req
//	after the per-conn watchdog  :  1,590 B/req  (29 allocs)
//
// 4 KiB leaves ~2.5x headroom over the current floor and trips on any
// per-request allocation from ~2.4 KiB up. Re-measure and re-set it when a perf
// change moves the floor again — a gate whose headroom silently grows stops
// being a gate.
const h1AllocGateLimit = 4 * 1024
