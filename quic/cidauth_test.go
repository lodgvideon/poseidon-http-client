package quic

import "testing"

// TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated checks that the server's
// initial_source_connection_id is authenticated against the source connection ID
// the client adopted from the server's Initial packet (RFC 9000 §7.3): a match is
// accepted, a mismatch or an absent parameter is a TRANSPORT_PARAMETER_ERROR.
func TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated(t *testing.T) {
	scid := []byte{0x11, 0x22, 0x33, 0x44}

	// Matching initial_source_connection_id → accepted.
	c := &Conn{dcid: append([]byte(nil), scid...)}
	good := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: scid})
	if err := c.PeerTransportParameters(good); err != nil {
		t.Fatalf("matching initial_source_connection_id: %v", err)
	}

	// Mismatched value → TRANSPORT_PARAMETER_ERROR.
	c2 := &Conn{dcid: append([]byte(nil), scid...)}
	bad := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: []byte{0x99}})
	if err := c2.PeerTransportParameters(bad); err != ErrTransportParameter {
		t.Fatalf("mismatched initial_source_connection_id = %v, want ErrTransportParameter", err)
	}

	// Absent parameter (params carry no 0x0f) → TRANSPORT_PARAMETER_ERROR.
	c3 := &Conn{dcid: append([]byte(nil), scid...)}
	absent := appendTPInt(nil, tpInitialMaxData, 1000)
	if err := c3.PeerTransportParameters(absent); err != ErrTransportParameter {
		t.Fatalf("absent initial_source_connection_id = %v, want ErrTransportParameter", err)
	}
}
