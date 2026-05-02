package bytesx

import (
	"bytes"
	"errors"
	"testing"
)

func TestStripPadding(t *testing.T) {
	cases := []struct {
		name        string
		raw         []byte
		wantPayload []byte
		wantPadLen  uint8
		wantErr     error
	}{
		{
			name:        "no_padding_byte_present",
			raw:         []byte{0x00, 0xaa, 0xbb},
			wantPayload: []byte{0xaa, 0xbb},
			wantPadLen:  0,
		},
		{
			name:        "padding_3_bytes",
			raw:         []byte{0x03, 0xaa, 0xbb, 0x00, 0x00, 0x00},
			wantPayload: []byte{0xaa, 0xbb},
			wantPadLen:  3,
		},
		{
			name:        "all_padding_no_payload",
			raw:         []byte{0x02, 0x00, 0x00},
			wantPayload: []byte{},
			wantPadLen:  2,
		},
		{
			name:    "padlen_exceeds_payload",
			raw:     []byte{0x05, 0xaa},
			wantErr: ErrInvalidPadding,
		},
		{
			name:    "empty",
			raw:     []byte{},
			wantErr: ErrInvalidPadding,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, padLen, err := StripPadding(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !bytes.Equal(payload, tc.wantPayload) {
				t.Fatalf("payload = %x, want %x", payload, tc.wantPayload)
			}
			if padLen != tc.wantPadLen {
				t.Fatalf("padLen = %d, want %d", padLen, tc.wantPadLen)
			}
		})
	}
}
