package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConformance_RFC9000_Sec73_ConnectionIDValidation checks the RFC 9000 §7.3
// rules for the server's connection-ID transport parameters: the
// original_destination_connection_id MUST be present and equal the client's first
// Initial DCID, and the retry_source_connection_id MUST be present and equal the
// Retry's SCID exactly when a Retry was received (and absent otherwise).
func TestConformance_RFC9000_Sec73_ConnectionIDValidation(t *testing.T) {
	scid := []byte{0x11, 0x22, 0x33, 0x44} // server SCID (initial_source_connection_id)
	odcid := []byte("origdcid")            // the client's first-Initial DCID
	rscid := []byte("retryscd")            // a Retry's SCID

	// base builds a server parameter block with a valid ISCID and ODCID, plus any
	// extra raw parameter tuples.
	base := func(extra ...[]byte) []byte {
		p := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: scid})
		p = append(p, tpBytes(tpOriginalDestinationConnectionID, odcid)...)
		for _, e := range extra {
			p = append(p, e...)
		}
		return p
	}
	noRetry := func() *Conn {
		return &Conn{serverSCID: append([]byte(nil), scid...), origDCID: append([]byte(nil), odcid...)}
	}
	withRetry := func() *Conn {
		c := noRetry()
		c.handledRetry, c.retrySCID = true, append([]byte(nil), rscid...)
		return c
	}
	wantErr := func(t *testing.T, params []byte, c *Conn) {
		t.Helper()

		err := c.PeerTransportParameters(params)

		assert.ErrorIsf(t, err, ErrTransportParameter,
			"PeerTransportParameters = %v, want ErrTransportParameter", err)
	}

	// original_destination_connection_id: mandatory, must match.
	t.Run("odcid-absent", func(t *testing.T) {
		p := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: scid})

		wantErr(t, p, noRetry())
	})
	t.Run("odcid-mismatch", func(t *testing.T) {
		p := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1000, SourceConnectionID: scid})
		p = append(p, tpBytes(tpOriginalDestinationConnectionID, []byte("wrongdci"))...)

		wantErr(t, p, noRetry())
	})

	// retry_source_connection_id: present iff a Retry was received, and must match.
	t.Run("rscid-present-without-retry", func(t *testing.T) {
		wantErr(t, base(tpBytes(tpRetrySourceConnectionID, rscid)), noRetry())
	})
	t.Run("rscid-matching-after-retry", func(t *testing.T) {
		params := base(tpBytes(tpRetrySourceConnectionID, rscid))

		err := withRetry().PeerTransportParameters(params)

		assert.NoErrorf(t, err, "matching retry_source_connection_id after a Retry = %v, want nil", err)
	})
	t.Run("rscid-absent-after-retry", func(t *testing.T) {
		wantErr(t, base(), withRetry())
	})
	t.Run("rscid-mismatch-after-retry", func(t *testing.T) {
		wantErr(t, base(tpBytes(tpRetrySourceConnectionID, []byte("wrongscd"))), withRetry())
	})
}
