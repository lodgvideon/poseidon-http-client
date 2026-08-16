package bytesx

import (
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
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
			var buf [8]byte

			gotLen := VarintLen(tc.v)
			n := WriteVarint(buf[:], tc.v)
			gotV, gotN := ReadVarint(tc.enc)

			require.Equalf(t, len(tc.enc), gotLen,
				"VarintLen(%d) must report the minimal on-wire length; a caller sizes its buffer and its frame-length field from this, so a wrong answer writes the wrong number of bytes into the packet", tc.v)
			require.Equalf(t, tc.enc, buf[:n],
				"WriteVarint(%d) must emit exactly the RFC 9000 §16 encoding %x, length prefix included; one byte off is a packet no conformant peer can parse", tc.v, tc.enc)
			require.Equalf(t, tc.v, gotV,
				"ReadVarint(%x) must recover the encoded value; a decoder that disagrees with the encoder corrupts every length, stream ID, offset and frame type on the wire", tc.enc)
			require.Equalf(t, len(tc.enc), gotN,
				"ReadVarint(%x) must consume exactly the bytes its length prefix claims; a wrong count desynchronises the parse of everything after it in the packet", tc.enc)
		})
	}
}

// TestConformance_RFC9000_Sec16_NonMinimalDecode verifies the decoder reads a
// non-minimally-encoded varint faithfully (RFC 9000 §16 permits them): the
// two-byte form 0x4025 decodes to 37, the same value 0x25 encodes minimally.
func TestConformance_RFC9000_Sec16_NonMinimalDecode(t *testing.T) {
	nonMinimal := []byte{0x40, 0x25} // 37, spelled in the two-byte form

	v, n := ReadVarint(nonMinimal)
	minimalLen := VarintLen(37)

	require.Equalf(t, uint64(37), v,
		"ReadVarint(%x) must decode the non-minimal two-byte form to 37; RFC 9000 §16 permits non-minimal encodings, so a decoder that mis-reads them drops legal peer traffic", nonMinimal)
	require.Equalf(t, 2, n,
		"ReadVarint(%x) must consume the two bytes its length prefix claims, not the one the minimal form would have used; consuming fewer leaves a stray byte that reparses as a bogus frame", nonMinimal)
	require.Equal(t, 1, minimalLen,
		"VarintLen(37) must still report the minimal length 1. A caller whose field requires minimality detects a non-minimal encoding by comparing ReadVarint's n against this, and that check only works while the two disagree here")
}

// TestConformance_RFC9000_Sec16_IncompleteInput checks the streaming-parser
// contract: input shorter than the first byte's length prefix returns (0, 0).
func TestConformance_RFC9000_Sec16_IncompleteInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"two_byte_prefix_one_byte", []byte{0x40}},
		{"four_byte_prefix_three_bytes", []byte{0x80, 0x00, 0x40}},
		{"eight_byte_prefix_two_bytes", []byte{0xc0, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in

			v, n := ReadVarint(in)

			require.Equalf(t, 0, n,
				"ReadVarint(%x) must report 0 bytes consumed when the input is shorter than its length prefix claims; a streaming parser reads that as \"need more bytes\" and retries, and any other count makes it advance over bytes that have not arrived", in)
			require.Equalf(t, uint64(0), v,
				"ReadVarint(%x) must pair the incomplete signal with a zero value; a caller that checks only the value would otherwise act on a half-read integer assembled from absent bytes", in)
		})
	}
}

// TestVarint_ExhaustiveRoundTrip round-trips a spread of values across all four
// lengths (including every boundary ±1) through Write then Read.
func TestVarint_ExhaustiveRoundTrip(t *testing.T) {
	vals := []uint64{0, 1, 62, 63, 64, 65, 16382, 16383, 16384, 16385,
		(1 << 30) - 2, (1 << 30) - 1, 1 << 30, (1 << 30) + 1, MaxVarint - 1, MaxVarint}
	for _, v := range vals {
		t.Run(strconv.FormatUint(v, 10), func(t *testing.T) {
			var buf [8]byte
			wantLen := VarintLen(v)

			n := WriteVarint(buf[:], v)
			got, m := ReadVarint(buf[:n])

			require.Equalf(t, wantLen, n,
				"WriteVarint(%d) must write exactly VarintLen(%d) bytes; when the two disagree a caller that reserved space from VarintLen either truncates the value or leaves a gap in the packet", v, v)
			require.Equalf(t, v, got,
				"round-trip of %d must recover the value; every QUIC frame length, stream ID and offset passes through this pair, so a value that survives encoding but not decoding is silent wire corruption", v)
			require.Equalf(t, n, m,
				"round-trip of %d must consume exactly the bytes the encoder wrote; a mismatch desynchronises the parse of everything after it", v)
		})
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
	cmd := exec.Command("go", "build", "-gcflags=-m", ".")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("go build unavailable here (%v); cannot check inlining", err)
	}

	require.Containsf(t, string(out), "can inline ReadVarint",
		"ReadVarint is no longer inlinable: every QUIC/HTTP-3 varint decode is a function call again. "+
			"Bring its inline cost back under the budget of 80.\ngo build -gcflags=-m reported:\n%s", out)
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
