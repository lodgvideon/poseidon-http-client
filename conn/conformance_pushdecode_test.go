package conn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC9113_Sec4_3_PushPromiseDecodeError_ConnCompressionError is
// the sibling of TestConformance_RFC9113_Sec4_3_HeaderDecodeError_ConnCompressionError
// on the push path.
//
// §4.3 binds every field block — "A decoding error in a field block MUST be
// treated as a connection error (Section 5.4.1) of type COMPRESSION_ERROR" —
// and a PUSH_PROMISE carries one. Only the response path was pinned; the push
// tests that reach a malformed promise assert a stream-level PROTOCOL_ERROR for
// FIELD VALIDATION, which is a different mechanism that runs after the block has
// already decoded cleanly. Reporting a decode failure as PROTOCOL_ERROR tears
// the connection down with a code that tells the peer its HPACK state is intact
// when it is not, and it survived the whole package twice (#814).
//
// Both entry points reach the same decode, so both are driven: a promise whose
// block arrives whole, and one reassembled from a CONTINUATION.
func TestConformance_RFC9113_Sec4_3_PushPromiseDecodeError_ConnCompressionError(t *testing.T) {
	// RFC 7541 §6.1: "The index value of 0 is not used.  It MUST be treated as a
	// decoding error if found in an indexed header field representation."
	bad := []byte{0x80}

	for _, tc := range []struct {
		name string
		feed func(h *connHandler) error
	}{
		{"promise carried by one frame", func(h *connHandler) error {
			return h.OnPushPromise(frame.FrameHeader{
				Type:     frame.FramePushPromise,
				Length:   uint32(len(bad)),
				Flags:    frame.FlagPushPromiseEndHeaders,
				StreamID: 1,
			}, 2, bad, 0)
		}},
		{"promise reassembled from a CONTINUATION", func(h *connHandler) error {
			if err := h.OnPushPromise(frame.FrameHeader{
				Type: frame.FramePushPromise, StreamID: 1,
			}, 2, nil, 0); err != nil {
				return err
			}
			return h.OnContinuation(frame.FrameHeader{
				Type:     frame.FrameContinuation,
				Length:   uint32(len(bad)),
				Flags:    frame.FlagContinuationEndHeaders,
				StreamID: 1,
			}, bad)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			m.pushEnabled = true
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)

			err := tc.feed(h)

			var ce *ConnError
			require.Truef(t, errors.As(err, &ce),
				"err = %v (%T), want *ConnError — a decode error desyncs the one decoder every "+
					"stream shares, so §4.3 makes it connection-scoped, never a stream reset", err, err)
			assert.Equalf(t, frame.ErrCodeCompressionError, ce.Code,
				"code = %v, want COMPRESSION_ERROR (§4.3). PROTOCOL_ERROR keeps the same "+
					"teardown but tells the peer its HPACK context survived, which is the one "+
					"thing that certainly did not", ce.Code)
		})
	}
}
