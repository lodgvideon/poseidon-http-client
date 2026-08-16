package frame

// The three receiver-side stream-id rules the sibling file stopped short of.
//
// conformance_streamid_test.go pins DATA, PRIORITY, RST_STREAM (must be
// nonzero) and PING, GOAWAY (must be zero) — five of the eight frame types RFC
// 9113 scopes to a stream id. SETTINGS and PUSH_PROMISE were missing, and
// mutation-checking found exactly that: deleting either guard broke no test,
// while every guard the sibling file covers was caught. The lines are
// *executed* by every valid frame of those types, so line coverage showed
// nothing.
//
// CONTINUATION's guard is the third of the trio and is NOT tested here — see
// the note at the bottom of this file.

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9113_Sec6_5_SettingsFrameOnNonzeroStream_FramerRejects
// pins §6.5: SETTINGS applies to the connection, so a SETTINGS frame carrying a
// stream id is rejected. The payload is a well-formed single setting (6 octets),
// so the length rule cannot be what rejects it — the stream-id check runs first.
func TestConformance_RFC9113_Sec6_5_SettingsFrameOnNonzeroStream_FramerRejects(t *testing.T) {
	raw := frameBytes(6, FrameSettings, 0, 1, []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x64})
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "SETTINGS on stream 1: err = %v, want ErrInvalidStreamID", err)
}

// TestConformance_RFC9113_Sec6_6_PushPromiseFrameOnStreamZero_FramerRejects
// pins §6.6: PUSH_PROMISE is sent on an existing stream, so stream 0 is
// rejected. The payload carries a well-formed promised stream id, so the
// short-read rule cannot be what rejects it.
func TestConformance_RFC9113_Sec6_6_PushPromiseFrameOnStreamZero_FramerRejects(t *testing.T) {
	raw := frameBytes(4, FramePushPromise, FlagPushPromiseEndHeaders, 0, []byte{0x00, 0x00, 0x00, 0x02})
	fr := NewFramer(nil, bytes.NewReader(raw))

	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "PUSH_PROMISE on stream 0: err = %v, want ErrInvalidStreamID", err)
}

// No CONTINUATION-on-stream-0 test, deliberately.
//
// dispatchContinuation carries the same `StreamID == 0` guard, but nothing can
// reach it. A CONTINUATION arriving outside a field block is rejected earlier as
// ErrUnexpectedContinuation, and one arriving inside a block must match the
// stream id the open HEADERS set or it is ErrContinuationExpected — and that id
// can never be 0, because HEADERS on stream 0 is itself rejected. The guard is
// unreachable defence, which is why removing it breaks no test: there is no
// input that distinguishes its presence. Asserting on it would need a test that
// reaches into the Framer's continuation state, which pins the test's own setup
// rather than any behaviour a peer can trigger.
