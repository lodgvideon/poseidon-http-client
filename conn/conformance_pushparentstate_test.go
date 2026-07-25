package conn

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// pushPromiseHeader is a complete (END_HEADERS) PUSH_PROMISE frame header on the
// given parent stream.
func pushPromiseHeader(parent uint32) frame.FrameHeader {
	return frame.FrameHeader{
		Type:     frame.FramePushPromise,
		StreamID: parent,
		Flags:    frame.FlagPushPromiseEndHeaders,
	}
}

// validPromiseFor builds an HPACK block for a valid safe/cacheable GET promise
// whose :authority matches the given parent authority.
func validPromiseFor(t *testing.T, authority string) []byte {
	t.Helper()
	return encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte(authority)},
	})
}

// TestConformance_RFC9113_Sec6_6_PushPromiseOnHalfClosedRemoteParent_ConnError
// pins RFC 9113 §6.6: "A receiver MUST treat the receipt of a PUSH_PROMISE on a
// stream ... as a connection error (Section 5.4.1) of type PROTOCOL_ERROR" when
// the associated stream is neither "open" nor "half-closed (local)". A
// half-closed(remote) parent — the server already sent END_STREAM while the
// client's upload is still open, so the stream stays registered — is neither of
// those. This is distinct from the RST-then-PP race (closed parent, tolerated as
// CANCEL): here the parent is live but the server's half is done.
func TestConformance_RFC9113_Sec6_6_PushPromiseOnHalfClosedRemoteParent_ConnError(t *testing.T) {
	m := newFakeStreamMap()
	m.pushEnabled = true
	h := newConnHandler(m, hpack.NewDecoder())

	s := m.addStream(1)
	s.remoteEnded = true // server sent END_STREAM
	s.localEnded = false // client upload still open -> stream stays registered
	s.reqAuthority = "example.com"

	err := h.OnPushPromise(pushPromiseHeader(1), 2, validPromiseFor(t, "example.com"), 0)
	var ce *ConnError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v (%T), want *ConnError — §6.6: a PUSH_PROMISE on a stream neither open nor "+
			"half-closed(local) MUST be a connection error PROTOCOL_ERROR", err, err)
	}
	if ce.Code != frame.ErrCodeProtocolError {
		t.Errorf("code = %v, want PROTOCOL_ERROR (§6.6)", ce.Code)
	}
}

// TestConformance_RFC9113_Sec6_6_PushPromiseOnValidParentStates_Accepted is the
// over-rejection guard: a PUSH_PROMISE on a parent whose server half is still
// open — "open" (neither end closed) or "half-closed (local)" (client sent
// END_STREAM, server still responding) — is a valid target and MUST NOT be
// rejected. This is the conformant-server path push_test.go exercises.
func TestConformance_RFC9113_Sec6_6_PushPromiseOnValidParentStates_Accepted(t *testing.T) {
	for _, tc := range []struct {
		name       string
		localEnded bool
	}{
		{"open", false},
		{"half_closed_local", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			m.pushEnabled = true
			h := newConnHandler(m, hpack.NewDecoder())

			s := m.addStream(1)
			s.localEnded = tc.localEnded
			s.remoteEnded = false // server half open — a valid PP target
			s.reqAuthority = "example.com"

			if err := h.OnPushPromise(pushPromiseHeader(1), 2, validPromiseFor(t, "example.com"), 0); err != nil {
				t.Errorf("%s parent: PUSH_PROMISE rejected (%v) — a parent with the server half still "+
					"open is a valid target (§6.6)", tc.name, err)
			}
		})
	}
}
