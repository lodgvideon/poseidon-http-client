package hpack

import (
	"encoding/hex"
	"testing"
)

// Seed corpus from RFC App. C vectors. Fuzzer ensures no panics on arbitrary input.
func FuzzHPACKDecode(f *testing.F) {
	for _, hexStr := range []string{
		"82",                                                       // C.2.4
		"400a637573746f6d2d6b65790d637573746f6d2d686561646572",     // C.2.1
		"040c2f73616d706c652f70617468",                             // C.2.2
		"100870617373776f726406736563726574",                       // C.2.3
		"828684410f7777772e6578616d706c652e636f6d",                 // C.3.1
		"828684418cf1e3c2e5f23a6ba0ab90f4ff",                       // C.4.1
	} {
		seed, _ := hex.DecodeString(hexStr)
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder()
		_ = d.DecodeBlock(data, func(_ HeaderField) error { return nil })
		// Invariant: no panic. Errors are fine.
	})
}
