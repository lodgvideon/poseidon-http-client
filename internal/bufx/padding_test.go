package bufx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStripPadding walks the pad-length guard across its whole range. For raw of
// length n the bytes after the pad-length byte are n-1, so the legal padLen
// values are 0..n-1 and n is the first illegal one — and n is where an off-by-one
// in the guard stops returning a PROTOCOL_ERROR and starts evaluating
// raw[1:len(raw)-padLen] as raw[1:0], i.e. panicking on peer bytes. StripPadding
// is called straight off the wire from frame.dispatchData, dispatchHeaders and
// dispatchPushPromise, so the boundary rows below are the ones that keep an
// attacker-supplied pad length a connection error instead of a crash.
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
			// padLen == len(raw): the FIRST illegal value, and the one an
			// off-by-one guard lets through into raw[1:0].
			name:    "padlen_equals_raw_len",
			raw:     []byte{0x02, 0xaa},
			wantErr: errInvalidPadding,
		},
		{
			name:    "padlen_one_past_raw_len",
			raw:     []byte{0x03, 0xaa},
			wantErr: errInvalidPadding,
		},
		{
			name:    "padlen_exceeds_payload",
			raw:     []byte{0x05, 0xaa},
			wantErr: errInvalidPadding,
		},
		{
			// The shortest non-empty frame: one pad-length byte and nothing
			// else, where 0 is the only legal count.
			name:        "min_frame_zero_padding",
			raw:         []byte{0x00},
			wantPayload: []byte{},
			wantPadLen:  0,
		},
		{
			name:    "min_frame_padlen_one",
			raw:     []byte{0x01},
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
				assert.Nilf(t, payload,
					"payload = %x on the failure path, want nil — a caller that reads the payload before the error hands frame bytes chosen by the peer to the HPACK decoder",
					payload)
				assert.Zerof(t, padLen,
					"padLen = %d on the failure path, want 0 — a non-zero count here would be charged to the flow-control window for a frame that was never accepted",
					padLen)
				return
			}
			require.NoErrorf(t, err, "unexpected err: %v", err)
			require.Equalf(t, tc.wantPayload, payload,
				"payload = %x, want %x — the returned slice must exclude both the pad-length byte and the trailing padding, or the padding reaches the HPACK decoder as header bytes",
				payload, tc.wantPayload)
			require.Equalf(t, tc.wantPadLen, padLen,
				"padLen = %d, want %d — the flow-control accounting charges the padding, so a wrong count desynchronises the connection window",
				padLen, tc.wantPadLen)
			if len(payload) > 0 {
				assert.Samef(t, &raw[1], &payload[0],
					"payload must alias raw at offset 1, not copy it — the doc comment binds callers to the visitor lifetime contract precisely because nothing is copied, and a version that allocated would satisfy every value comparison above while adding one allocation per DATA frame")
			}
		})
	}
}
