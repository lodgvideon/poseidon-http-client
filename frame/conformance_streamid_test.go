package frame

// Receiver-side stream-identifier conformance for the frame types RFC 9113
// scopes to a specific stream id. The Framer rejects each on read with the
// connection-fatal ErrInvalidStreamID sentinel (the conn layer maps it to a
// connection teardown; §5.4.1's typed-GOAWAY-with-code is only SHOULD, so these
// assert the sentinel, not a PROTOCOL_ERROR code).
//
// These close a sibling-divergence coverage gap: HEADERS and CONTINUATION each
// had a receiver-side stream-0 test (TestFramer_dispatchHeaders_ErrorStreamID0),
// but DATA/PRIORITY/RST_STREAM (must be nonzero) and PING/GOAWAY (must be zero)
// had only write-side guards or truncated-length tests on the valid stream id.
// The stream-id check runs before the length check in each dispatch, so a
// well-formed payload still trips it — which also makes each test fail if the
// guard is removed (the frame would dispatch to the handler and return nil).

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9113_Sec6_1_DataFrameOnStreamZero_FramerRejects pins §6.1:
// a DATA frame on stream 0 is rejected (connection error PROTOCOL_ERROR at the
// conn layer; ErrInvalidStreamID at the framer).
func TestConformance_RFC9113_Sec6_1_DataFrameOnStreamZero_FramerRejects(t *testing.T) {
	raw := frameBytes(1, FrameData, 0, 0, []byte{0x00})
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "DATA on stream 0: err = %v, want ErrInvalidStreamID", err)
}

// TestConformance_RFC9113_Sec6_3_PriorityFrameOnStreamZero_FramerRejects pins
// §6.3: a PRIORITY frame on stream 0 is rejected. The stream-id check precedes
// the length check, so a valid 5-octet payload still trips it.
func TestConformance_RFC9113_Sec6_3_PriorityFrameOnStreamZero_FramerRejects(t *testing.T) {
	raw := frameBytes(5, FramePriority, 0, 0, []byte{0, 0, 0, 0, 0})
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "PRIORITY on stream 0: err = %v, want ErrInvalidStreamID", err)
}

// TestConformance_RFC9113_Sec6_4_RSTStreamFrameOnStreamZero_FramerRejects pins
// §6.4: a RST_STREAM frame on stream 0 is rejected (before the length check).
func TestConformance_RFC9113_Sec6_4_RSTStreamFrameOnStreamZero_FramerRejects(t *testing.T) {
	raw := frameBytes(4, FrameRSTStream, 0, 0, []byte{0, 0, 0, 0})
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "RST_STREAM on stream 0: err = %v, want ErrInvalidStreamID", err)
}

// TestConformance_RFC9113_Sec6_7_PingFrameOnNonzeroStream_FramerRejects pins
// §6.7: a PING frame on a nonzero stream is rejected (before the length check),
// even with a valid 8-octet payload.
func TestConformance_RFC9113_Sec6_7_PingFrameOnNonzeroStream_FramerRejects(t *testing.T) {
	raw := frameBytes(8, FramePing, 0, 1, make([]byte, 8))
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "PING on stream 1: err = %v, want ErrInvalidStreamID", err)
}

// TestConformance_RFC9113_Sec6_8_GoAwayFrameOnNonzeroStream_FramerRejects pins
// §6.8: a GOAWAY frame on a nonzero stream is rejected (before the length
// check), even with a valid 8-octet payload.
func TestConformance_RFC9113_Sec6_8_GoAwayFrameOnNonzeroStream_FramerRejects(t *testing.T) {
	raw := frameBytes(8, FrameGoAway, 0, 1, make([]byte, 8))
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "GOAWAY on stream 1: err = %v, want ErrInvalidStreamID", err)
}
