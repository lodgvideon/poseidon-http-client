package quic

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// maxInlineMapElem is the largest map element Go stores inline in the table.
// Above it the compiler switches to indirect storage: the map holds a pointer
// and every insert heap-allocates the value separately. The threshold is a
// compiler constant, not something this package can influence.
const maxInlineMapElem = 128

// TestSentPacketStaysInlineInMap pins the size of sentPacket, because sentSpace
// keeps them in a map[uint64]sentPacket and one is inserted per packet sent.
//
// This is not a style preference about struct padding. Measured on go1.25.0 with
// a steady-state window of 64 entries in an already-grown map, so no growth or
// rehashing is involved:
//
//	element 120 bytes    0 allocs/op    29.6 ns/op
//	element 128 bytes    0 allocs/op    30.9 ns/op
//	element 129 bytes    1 allocs/op    59.3 ns/op   144 B/op
//	element 152 bytes    1 allocs/op    60.6 ns/op   160 B/op
//
// sentPacket was 152 bytes, so every sent packet allocated: profiling attributed
// 20.05% of the HTTP/3 arm's allocation count to the single insert in onSent
// (#475). The fields were then ordered so the three bools sit together instead of
// each taking a full 8-byte slot, which brought the struct under the threshold
// without changing a single field's type or meaning.
//
// The margin is thin by construction, so this test exists to make the next field
// addition a deliberate decision: if it fails, either pack the new field into
// existing padding, or accept an allocation per sent packet and say so here.
func TestSentPacketStaysInlineInMap(t *testing.T) {
	got := unsafe.Sizeof(sentPacket{})

	assert.LessOrEqualf(t, got, uintptr(maxInlineMapElem),
		"sizeof(sentPacket) = %d bytes, want <= %d.\n"+
			"Above %d Go stores map elements out of line, so sentSpace.onSent "+
			"allocates once per sent packet. See the measurements in this test's doc.",
		got, maxInlineMapElem, maxInlineMapElem)
	t.Logf("sizeof(sentPacket) = %d bytes (limit %d, margin %d)",
		got, maxInlineMapElem, int(maxInlineMapElem)-int(got))
}

// TestRetransFrameSize pins retransFrame for the same reason: it is embedded in
// sentPacket by value, so its own padding is charged to every sent packet.
func TestRetransFrameSize(t *testing.T) {
	got := unsafe.Sizeof(retransFrame{})

	// 56 = 3x uint64 + a 24-byte slice header + kind and fin packed into the tail.
	assert.LessOrEqualf(t, got, uintptr(56),
		"sizeof(retransFrame) = %d bytes, want <= 56 — it is embedded in "+
			"sentPacket, which must stay under %d", got, maxInlineMapElem)
}
