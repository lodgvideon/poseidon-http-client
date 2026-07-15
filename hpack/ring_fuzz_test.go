package hpack

import (
	"encoding/hex"
	"testing"
)

// maxLiveEntries is how many entries a 4096-octet table can hold: every entry
// costs 32 octets of overhead before a single byte of name or value
// (RFC 7541 §4.1).
const maxLiveEntries = int(defaultMaxDynamicTableSize) / 32

// FuzzHPACKDecode_PersistentDecoder feeds a sequence of header blocks to ONE
// decoder and asserts the dynamic table's entry ring stays bounded by what the
// table's size limit allows to be live.
//
// FuzzHPACKDecode builds a fresh NewDecoder() per iteration, which makes every
// cross-block accumulation bug invisible by construction — a leak that needs two
// blocks to show cannot be expressed there. A real connection decodes every
// block through one long-lived decoder (conn/conn.go), so this is the shape that
// matches production. It is the shape that would have caught the ring leak: a
// peer sending empty-name/empty-value literals grew the ring one slot per header
// with no ceiling, because the table's size stayed at its 4096-octet cap while
// its slot count tracked the header COUNT instead.
//
// The input is a sequence of length-prefixed blocks: one length byte, then that
// many bytes of HPACK. This lets the fuzzer place block boundaries itself, which
// is the axis that matters here.
func FuzzHPACKDecode_PersistentDecoder(f *testing.F) {
	// Two blocks of C.3.1, exercising the table across a boundary.
	c31, _ := hex.DecodeString("828684410f7777772e6578616d706c652e636f6d")
	seed := append([]byte{byte(len(c31))}, c31...)
	seed = append(seed, byte(len(c31)))
	seed = append(seed, c31...)
	f.Add(seed)

	// The leak's shape, sized to actually trip the bound: empty-name/empty-value
	// literals, enough of them that a ring tracking header count would exceed
	// 4*maxLiveEntries. A seed of a handful would exercise the path but assert
	// nothing — the bound only bites above 512 slots.
	var empties []byte
	for b := 0; b < 12; b++ {
		block := make([]byte, 0, 255)
		for i := 0; i < 84; i++ {
			block = append(block, 0x40, 0x00, 0x00)
		}
		empties = append(empties, byte(len(block)))
		empties = append(empties, block...)
	}
	f.Add(empties)

	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder()
		visit := func(HeaderField) error { return nil }

		for len(data) > 0 {
			n := int(data[0])
			data = data[1:]
			if n > len(data) {
				n = len(data)
			}
			// A block that errors leaves the decoder's table in whatever state the
			// error left it. That state is exactly what we want to keep checking:
			// a peer is not obliged to send only valid blocks.
			_ = d.DecodeBlock(data[:n], visit)
			data = data[n:]

			dt := d.dt
			if dt.count > maxLiveEntries {
				t.Fatalf("count=%d exceeds the %d entries a %d-octet table can hold",
					dt.count, maxLiveEntries, dt.maxSize)
			}
			if dt.size > dt.maxSize {
				t.Fatalf("size=%d exceeds maxSize=%d", dt.size, dt.maxSize)
			}
			// Loose: doubling plus compaction leaves slack. This pins the absence of
			// growth driven by header count, not an exact ring size.
			if len(dt.entries) > 4*maxLiveEntries {
				t.Fatalf("entry ring grew to %d slots holding %d live entries "+
					"(~%d B retained); the ring must track the table's size limit, "+
					"not how many headers the peer sent",
					len(dt.entries), dt.count, len(dt.entries)*16)
			}
			if dt.count > 0 && dt.head >= len(dt.entries) {
				t.Fatalf("head=%d out of range for a %d-slot ring", dt.head, len(dt.entries))
			}
		}
	})
}
