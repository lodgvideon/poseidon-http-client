package bytesx

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

// varintCases pairs values with their minimal QUIC varint encodings, including
// the four worked examples from RFC 9000 Appendix A.1 plus every length
// boundary.
var varintCases = []struct {
	name string
	v    uint64
	enc  []byte
}{
	{"zero", 0, []byte{0x00}},
	{"one_byte_max", 63, []byte{0x3f}},
	{"appendixA_37", 37, []byte{0x25}},
	{"two_byte_min", 64, []byte{0x40, 0x40}},
	{"appendixA_15293", 15293, []byte{0x7b, 0xbd}},
	{"two_byte_max", 16383, []byte{0x7f, 0xff}},
	{"four_byte_min", 16384, []byte{0x80, 0x00, 0x40, 0x00}},
	{"appendixA_494878333", 494878333, []byte{0x9d, 0x7f, 0x3e, 0x7d}},
	{"four_byte_max", (1 << 30) - 1, []byte{0xbf, 0xff, 0xff, 0xff}},
	{"eight_byte_min", 1 << 30, []byte{0xc0, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00}},
	{"appendixA_151288809941952652", 151288809941952652, []byte{0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c}},
	{"eight_byte_max", MaxVarint, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
}

// TestConformance_RFC9000_Sec16_VarintRoundTrip checks each RFC 9000 §16 value
// encodes to its minimal on-wire form and decodes back, at every length
// boundary and for the Appendix A.1 examples.
func TestConformance_RFC9000_Sec16_VarintRoundTrip(t *testing.T) {
	for _, tc := range varintCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := VarintLen(tc.v); got != len(tc.enc) {
				t.Fatalf("VarintLen(%d) = %d, want %d", tc.v, got, len(tc.enc))
			}
			var buf [8]byte
			n := WriteVarint(buf[:], tc.v)
			if !bytes.Equal(buf[:n], tc.enc) {
				t.Fatalf("WriteVarint(%d) = %x, want %x", tc.v, buf[:n], tc.enc)
			}
			v, m := ReadVarint(tc.enc)
			if v != tc.v || m != len(tc.enc) {
				t.Fatalf("ReadVarint(%x) = (%d, %d), want (%d, %d)", tc.enc, v, m, tc.v, len(tc.enc))
			}
		})
	}
}

// TestConformance_RFC9000_Sec16_NonMinimalDecode verifies the decoder reads a
// non-minimally-encoded varint faithfully (RFC 9000 §16 permits them): the
// two-byte form 0x4025 decodes to 37, the same value 0x25 encodes minimally.
func TestConformance_RFC9000_Sec16_NonMinimalDecode(t *testing.T) {
	v, n := ReadVarint([]byte{0x40, 0x25})
	if v != 37 || n != 2 {
		t.Fatalf("ReadVarint(0x4025) = (%d, %d), want (37, 2)", v, n)
	}
	if VarintLen(37) != 1 {
		t.Fatalf("VarintLen(37) = %d, want 1 (minimal)", VarintLen(37))
	}
}

// TestConformance_RFC9000_Sec16_IncompleteInput checks the streaming-parser
// contract: input shorter than the first byte's length prefix returns (0, 0).
func TestConformance_RFC9000_Sec16_IncompleteInput(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"two_byte_prefix_one_byte", []byte{0x40}},
		{"four_byte_prefix_three_bytes", []byte{0x80, 0x00, 0x40}},
		{"eight_byte_prefix_two_bytes", []byte{0xc0, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if v, n := ReadVarint(tc.in); v != 0 || n != 0 {
				t.Fatalf("ReadVarint(%x) = (%d, %d), want (0, 0)", tc.in, v, n)
			}
		})
	}
}

// TestVarint_ExhaustiveRoundTrip round-trips a spread of values across all four
// lengths (including every boundary ±1) through Write then Read.
func TestVarint_ExhaustiveRoundTrip(t *testing.T) {
	vals := []uint64{0, 1, 62, 63, 64, 65, 16382, 16383, 16384, 16385,
		(1 << 30) - 2, (1 << 30) - 1, 1 << 30, (1 << 30) + 1, MaxVarint - 1, MaxVarint}
	for _, v := range vals {
		var buf [8]byte
		n := WriteVarint(buf[:], v)
		if n != VarintLen(v) {
			t.Fatalf("WriteVarint(%d) wrote %d, VarintLen=%d", v, n, VarintLen(v))
		}
		got, m := ReadVarint(buf[:n])
		if got != v || m != n {
			t.Fatalf("round-trip %d: ReadVarint = (%d, %d), want (%d, %d)", v, got, m, v, n)
		}
	}
}

// TestReadVarintIsInlinable pins the property ReadVarint's shape exists for.
// The decoder sits under every QUIC and HTTP/3 packet parse, several calls per
// packet, so it is written to fit the inliner's cost budget of 80 rather than to
// read as compactly as possible — see the note on ReadVarint itself. Nothing
// else in this suite notices if an edit pushes it back over: the decode stays
// correct, it just silently becomes a real call again and every caller pays for
// it. The shift-or form it replaced cost 169 and was never inlined.
func TestReadVarintIsInlinable(t *testing.T) {
	out, err := exec.Command("go", "build", "-gcflags=-m", ".").CombinedOutput()
	if err != nil {
		t.Skipf("go build unavailable here (%v); cannot check inlining", err)
	}
	if !strings.Contains(string(out), "can inline ReadVarint") {
		t.Fatalf("ReadVarint is no longer inlinable: every QUIC/HTTP-3 varint decode "+
			"is a function call again. Bring its inline cost back under budget.\n"+
			"go build -gcflags=-m reported:\n%s", out)
	}
}

func BenchmarkWriteVarint_1(b *testing.B) { benchWrite(b, 37) }
func BenchmarkWriteVarint_2(b *testing.B) { benchWrite(b, 15293) }
func BenchmarkWriteVarint_4(b *testing.B) { benchWrite(b, 494878333) }
func BenchmarkWriteVarint_8(b *testing.B) { benchWrite(b, MaxVarint) }

func benchWrite(b *testing.B, v uint64) {
	b.Helper()
	var buf [8]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteVarint(buf[:], v)
	}
}

func BenchmarkReadVarint_1(b *testing.B) { benchRead(b, 37) }
func BenchmarkReadVarint_2(b *testing.B) { benchRead(b, 15293) }
func BenchmarkReadVarint_4(b *testing.B) { benchRead(b, 494878333) }
func BenchmarkReadVarint_8(b *testing.B) { benchRead(b, MaxVarint) }

// sinkVarint keeps the decoded value observable to the compiler. ReadVarint is
// inlinable (TestReadVarintIsInlinable), so a loop that discards its results is
// dead code: the optimiser deletes the decode and the benchmark times an empty
// loop. Measured on this host, the same 4-byte decode reports 0.59 ns/op with
// the results dropped against 1.15 ns/op with this sink in place. The sink adds
// one add per iteration and allocates nothing, so bench-gate stays satisfied.
var sinkVarint uint64

func benchRead(b *testing.B, v uint64) {
	b.Helper()
	var buf [8]byte
	n := WriteVarint(buf[:], v)
	p := buf[:n]
	b.ReportAllocs()
	b.ResetTimer()
	var acc uint64
	for i := 0; i < b.N; i++ {
		x, m := ReadVarint(p)
		acc += x + uint64(m)
	}
	sinkVarint = acc
}
