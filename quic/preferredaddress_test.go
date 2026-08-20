package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preferredAddressValue builds a preferred_address transport-parameter value:
// 24-byte fixed prefix (IPv4+ports+IPv6+ports), a 1-byte Connection ID Length,
// the CID, and a 16-byte stateless reset token.
func preferredAddressValue(cid []byte) []byte {
	v := make([]byte, preferredAddressFixedLen)
	v = append(v, byte(len(cid)))
	v = append(v, cid...)
	return append(v, make([]byte, 16)...)
}

// TestConformance_RFC9000_Sec96_PreferredAddressValidated checks that a
// preferred_address transport parameter (0x0d) is structurally validated: a
// well-formed one is accepted, but a zero-length connection ID (RFC 9000 §9.6), a
// value too short to hold the Connection ID Length, or a length that disagrees with
// that field is a TRANSPORT_PARAMETER_ERROR (§18.2).
func TestConformance_RFC9000_Sec96_PreferredAddressValidated(t *testing.T) {
	lengthMismatch := preferredAddressValue([]byte{1, 2, 3, 4})
	lengthMismatch = lengthMismatch[:len(lengthMismatch)-1] // declares a 4-byte CID but is one byte short
	cases := []struct {
		name  string
		value []byte
		want  error
		why   string
	}{
		{"well-formed", preferredAddressValue([]byte{1, 2, 3, 4}), nil,
			"a structurally valid preferred_address must be accepted"},
		{"zero-length CID", preferredAddressValue(nil), ErrTransportParameter,
			"§9.6: the preferred address's connection ID MUST NOT be zero length"},
		{"truncated before the CID-length byte", make([]byte, preferredAddressFixedLen), ErrTransportParameter,
			"a value with no Connection ID Length byte cannot be parsed"},
		{"length disagrees with the CID-length field", lengthMismatch, ErrTransportParameter,
			"§18.2: a parameter whose length contradicts its own CID length is malformed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTransportParams(tpBytes(tpPreferredAddress, tc.value))

			if tc.want == nil {
				require.NoError(t, err, tc.why)
				return
			}
			assert.ErrorIsf(t, err, tc.want, "preferred_address %s = %v, want ErrTransportParameter — %s",
				tc.name, err, tc.why)
		})
	}
}
