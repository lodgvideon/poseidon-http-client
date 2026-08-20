package bufx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadUint24(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want uint32
	}{
		{"zero", []byte{0x00, 0x00, 0x00}, 0},
		{"max", []byte{0xff, 0xff, 0xff}, 0xff_ff_ff},
		{"big_endian", []byte{0x12, 0x34, 0x56}, 0x12_34_56},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in

			got := ReadUint24(in)

			require.Equalf(t, tc.want, got,
				"ReadUint24(%x) = %#x, want %#x — the frame length is big-endian (RFC 7540 §4.1), and a byte-order slip here mis-frames every frame after it",
				in, got, tc.want)
		})
	}
}

func TestWriteUint24(t *testing.T) {
	cases := []struct {
		name string
		in   uint32
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00}},
		{"max", 0xff_ff_ff, []byte{0xff, 0xff, 0xff}},
		{"big_endian", 0x12_34_56, []byte{0x12, 0x34, 0x56}},
		{"truncates_high_byte", 0xab_12_34_56, []byte{0x12, 0x34, 0x56}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [3]byte

			WriteUint24(buf[:], tc.in)

			require.Equalf(t, tc.want, buf[:],
				"WriteUint24(%#x) = %x, want %x — the field is three bytes wide, so anything above 24 bits is dropped silently and the reader must still see the low three",
				tc.in, buf[:], tc.want)
		})
	}
}

func TestReadUint31(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want uint32
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0},
		{"max_31bit", []byte{0x7f, 0xff, 0xff, 0xff}, 0x7fff_ffff},
		{"r_bit_set_is_masked", []byte{0xff, 0xff, 0xff, 0xff}, 0x7fff_ffff},
		// The reserved bit as the ONLY set bit. It is what separates "masks the
		// R bit" from "saturates at 0x7fffffff": under a saturating reader the
		// row above still yields 0x7fffffff and passes, while this one must
		// yield 0 and would not. Nothing else in the table can tell the two
		// apart.
		{"r_bit_only_is_masked", []byte{0x80, 0x00, 0x00, 0x00}, 0},
		{"stream_id_1", []byte{0x00, 0x00, 0x00, 0x01}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in

			got := ReadUint31(in)

			require.Equalf(t, tc.want, got,
				"ReadUint31(%x) = %#x, want %#x — RFC 7540 §4.1 makes the high bit reserved and requires a receiver to ignore it; carrying it through turns stream 1 into stream 0x80000001",
				in, got, tc.want)
		})
	}
}

func TestWriteUint31(t *testing.T) {
	cases := []struct {
		name string
		in   uint32
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"max_31bit", 0x7fff_ffff, []byte{0x7f, 0xff, 0xff, 0xff}},
		{"high_bit_cleared", 0xffff_ffff, []byte{0x7f, 0xff, 0xff, 0xff}},
		// Same discriminator on the write side: 0xffffffff produces 7fffffff
		// under both "clear the reserved bit" and "saturate", so only the input
		// whose sole set bit IS the reserved one distinguishes them — it must
		// write four zero bytes, not 7fffffff.
		{"r_bit_only_writes_zero", 0x8000_0000, []byte{0x00, 0x00, 0x00, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [4]byte

			WriteUint31(buf[:], tc.in)

			require.Equalf(t, tc.want, buf[:],
				"WriteUint31(%#x) = %x, want %x — RFC 7540 §4.1 requires the reserved bit to be sent unset, and a peer is entitled to treat a set one as a protocol error",
				tc.in, buf[:], tc.want)
		})
	}
}
