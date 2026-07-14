package bytesx

import "testing"

// FuzzReadVarint feeds arbitrary bytes to the QUIC variable-length integer
// decoder (RFC 9000 §16). The decoder underlies every frame and packet field, so
// it must never panic or over-read: on a short/incomplete input it returns
// (0, 0), and on a complete prefix it returns a value within the 62-bit varint
// space that re-encodes and decodes back to itself.
func FuzzReadVarint(f *testing.F) {
	f.Add([]byte{})                                                 // empty
	f.Add([]byte{0x00})                                             // 1-byte, value 0
	f.Add([]byte{0x25})                                             // 1-byte, value 37
	f.Add([]byte{0x40, 0x25})                                       // 2-byte
	f.Add([]byte{0x80, 0x01, 0x02, 0x03})                           // 4-byte
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})   // 8-byte, MaxVarint
	f.Add([]byte{0x40})                                             // truncated 2-byte
	f.Add([]byte{0x80, 0x01})                                       // truncated 4-byte
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00})                           // truncated 8-byte

	f.Fuzz(func(t *testing.T, b []byte) {
		v, n := ReadVarint(b)
		if n == 0 {
			return // incomplete input: a valid "need more bytes" signal
		}
		// A decoded varint value is always within the 62-bit space by construction.
		if v > MaxVarint {
			t.Fatalf("ReadVarint returned %d, above MaxVarint %d", v, MaxVarint)
		}
		if n != 1 && n != 2 && n != 4 && n != 8 {
			t.Fatalf("ReadVarint consumed %d bytes, want 1/2/4/8", n)
		}
		// Round-trip: the minimal re-encoding of v must decode back to v. The
		// on-wire n may exceed the minimal length (non-minimal encodings are legal,
		// RFC 9000 §16), so only compare values, not lengths.
		var buf [8]byte
		m := WriteVarint(buf[:], v)
		v2, n2 := ReadVarint(buf[:m])
		if v2 != v || n2 != m {
			t.Fatalf("round-trip mismatch: v=%d n=%d -> re-encoded m=%d decoded v2=%d n2=%d", v, n, m, v2, n2)
		}
	})
}
