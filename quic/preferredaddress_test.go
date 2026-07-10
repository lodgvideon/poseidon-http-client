package quic

import "testing"

// TestConformance_RFC9000_Sec96_PreferredAddressValidated checks that a
// preferred_address transport parameter (0x0d) is structurally validated: a
// well-formed one is accepted, but a zero-length connection ID (RFC 9000 §9.6), a
// value too short to hold the Connection ID Length, or a length that disagrees with
// that field is a TRANSPORT_PARAMETER_ERROR (§18.2).
func TestConformance_RFC9000_Sec96_PreferredAddressValidated(t *testing.T) {
	// pa builds a preferred_address value: 24-byte fixed prefix (IPv4+ports+IPv6+
	// ports), a 1-byte Connection ID Length, the CID, and a 16-byte reset token.
	pa := func(cid []byte) []byte {
		v := make([]byte, preferredAddressFixedLen)
		v = append(v, byte(len(cid)))
		v = append(v, cid...)
		return append(v, make([]byte, 16)...)
	}

	if _, err := ParseTransportParams(tpBytes(tpPreferredAddress, pa([]byte{1, 2, 3, 4}))); err != nil {
		t.Fatalf("valid preferred_address: %v", err)
	}
	if _, err := ParseTransportParams(tpBytes(tpPreferredAddress, pa(nil))); err != ErrTransportParameter {
		t.Fatalf("zero-length connection ID = %v, want ErrTransportParameter (§9.6)", err)
	}
	if _, err := ParseTransportParams(tpBytes(tpPreferredAddress, make([]byte, preferredAddressFixedLen))); err != ErrTransportParameter {
		t.Fatalf("truncated preferred_address = %v, want ErrTransportParameter", err)
	}
	short := pa([]byte{1, 2, 3, 4})
	short = short[:len(short)-1] // declares a 4-byte CID but is one byte short
	if _, err := ParseTransportParams(tpBytes(tpPreferredAddress, short)); err != ErrTransportParameter {
		t.Fatalf("length-mismatched preferred_address = %v, want ErrTransportParameter", err)
	}
}
