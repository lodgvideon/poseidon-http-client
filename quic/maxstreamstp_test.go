package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConformance_RFC9000_Sec46_MaxStreamsTransportParamBound checks that an
// initial_max_streams_bidi or initial_max_streams_uni transport parameter greater
// than 2^60 is a TRANSPORT_PARAMETER_ERROR, while the exact boundary 2^60 is
// accepted (RFC 9000 §4.6).
func TestConformance_RFC9000_Sec46_MaxStreamsTransportParamBound(t *testing.T) {
	ids := []struct {
		name string
		id   uint64
	}{
		{"bidi", tpInitialMaxStreamsBidi},
		{"uni", tpInitialMaxStreamsUni},
	}
	over := uint64(1)<<60 + 1

	for _, tc := range ids {
		_, errOver := ParseTransportParams(appendTPInt(nil, tc.id, over))
		_, errAtBound := ParseTransportParams(appendTPInt(nil, tc.id, 1<<60))

		assert.Equalf(t, ErrTransportParameter, errOver,
			"%s max_streams = 2^60+1: err = %v, want ErrTransportParameter", tc.name, errOver)
		assert.NoErrorf(t, errAtBound,
			"%s max_streams = 2^60 (the boundary): err = %v, want nil", tc.name, errAtBound)
	}
}
