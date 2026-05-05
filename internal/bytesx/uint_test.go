package bytesx

import (
	"bytes"
	"testing"
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
			got := ReadUint24(tc.in)
			if got != tc.want {
				t.Fatalf("ReadUint24(%x) = %#x, want %#x", tc.in, got, tc.want)
			}
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
			if !bytes.Equal(buf[:], tc.want) {
				t.Fatalf("WriteUint24(%#x) = %x, want %x", tc.in, buf, tc.want)
			}
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
		{"stream_id_1", []byte{0x00, 0x00, 0x00, 0x01}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReadUint31(tc.in)
			if got != tc.want {
				t.Fatalf("ReadUint31(%x) = %#x, want %#x", tc.in, got, tc.want)
			}
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [4]byte
			WriteUint31(buf[:], tc.in)
			if !bytes.Equal(buf[:], tc.want) {
				t.Fatalf("WriteUint31(%#x) = %x, want %x", tc.in, buf, tc.want)
			}
		})
	}
}
