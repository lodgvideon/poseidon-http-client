//go:build race

package client

// h1AllocGateLimit under -race. See the !race file for the measured floors and
// the reasoning.
//
// This is the value that actually guards CI: every workflow job that runs this
// test runs it with -race (ci.yml's `test` and `coverage` jobs), while the
// remaining jobs filter to -run=Fuzz, -run=Conformance or -run=^$ and never
// reach it. So the race ceiling has to be tight enough to catch a regression on
// its own, not merely wide enough to avoid false alarms.
//
// Race floor is 6,070 B/req (31 allocs). 9 KiB leaves ~1.5x headroom and trips
// from ~2.9 KiB up — verified against the pointer-held 4 KiB buffer that the
// struct-size test cannot see, which lands at ~10.1 KB and fails here. A looser
// 10 KiB let that exact regression through.
const h1AllocGateLimit = 9 * 1024
