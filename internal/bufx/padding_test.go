package bufx

import (
	"testing"

	"github.com/stretchr/testify/require"
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
			wantErr: errInvalidPadding,
		},
		{
			name:    "empty",
			raw:     []byte{},
			wantErr: errInvalidPadding,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw

			payload, padLen, err := StripPadding(raw)

			if tc.wantErr != nil {
				require.ErrorIsf(t, err, tc.wantErr,
					"err = %v, want %v — RFC 7540 §6.1 makes a pad length that eats its own frame a connection error, and a caller that cannot match the sentinel cannot raise one",
					err, tc.wantErr)
				return
			}
			require.NoErrorf(t, err, "unexpected err: %v", err)
			require.Equalf(t, tc.wantPayload, payload,
				"payload = %x, want %x — the returned slice must exclude both the pad-length byte and the trailing padding, or the padding reaches the HPACK decoder as header bytes",
				payload, tc.wantPayload)
			require.Equalf(t, tc.wantPadLen, padLen,
				"padLen = %d, want %d — the flow-control accounting charges the padding, so a wrong count desynchronises the connection window",
				padLen, tc.wantPadLen)
		})
	}
}
