package quic

import "testing"

// TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated checks that the server's
// initial_source_connection_id is authenticated against the source connection ID
// the client adopted from the server's Initial packet (RFC 9000 §7.3): a match is
// accepted, a mismatch or an absent parameter is a TRANSPORT_PARAMETER_ERROR.
func TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated(t *testing.T) {
	scid := []byte{0x11, 0x22, 0x33, 0x44}
	odcid := []byte("origdcid")
	// A full, §7.3-valid server parameter block (no Retry): matching ISCID plus the
	// mandatory original_destination_connection_id.
	valid := func() []byte {
		p := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: scid})
		return append(p, tpBytes(tpOriginalDestinationConnectionID, odcid)...)
	}
	newConn := func() *Conn {
		return &Conn{serverSCID: append([]byte(nil), scid...), origDCID: append([]byte(nil), odcid...)}
	}

	// Matching initial_source_connection_id → accepted.
	if err := newConn().PeerTransportParameters(valid()); err != nil {
		t.Fatalf("matching initial_source_connection_id: %v", err)
	}

	// Mismatched value → TRANSPORT_PARAMETER_ERROR.
	bad := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: []byte{0x99}})
	bad = append(bad, tpBytes(tpOriginalDestinationConnectionID, odcid)...)
	if err := newConn().PeerTransportParameters(bad); err != ErrTransportParameter {
		t.Fatalf("mismatched initial_source_connection_id = %v, want ErrTransportParameter", err)
	}

	// Absent parameter (params carry no 0x0f) → TRANSPORT_PARAMETER_ERROR.
	absent := appendTPInt(nil, tpInitialMaxData, 1000)
	absent = append(absent, tpBytes(tpOriginalDestinationConnectionID, odcid)...)
	if err := newConn().PeerTransportParameters(absent); err != ErrTransportParameter {
		t.Fatalf("absent initial_source_connection_id = %v, want ErrTransportParameter", err)
	}
}
