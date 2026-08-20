package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// altSvcCaptureHandler records ALTSVC entries for verification.
type altSvcCaptureHandler struct {
	entries []AltSvcEntry
	hdr     FrameHeader
}

func (h *altSvcCaptureHandler) OnData(FrameHeader, []byte, uint8) error { return nil }
func (h *altSvcCaptureHandler) OnHeaders(FrameHeader, HeaderBlock, *Priority, uint8) error {
	return nil
}
func (h *altSvcCaptureHandler) OnPriority(FrameHeader, Priority) error       { return nil }
func (h *altSvcCaptureHandler) OnRSTStream(FrameHeader, ErrCode) error       { return nil }
func (h *altSvcCaptureHandler) OnSettings(FrameHeader, SettingsParams) error { return nil }
func (h *altSvcCaptureHandler) OnPushPromise(FrameHeader, uint32, HeaderBlock, uint8) error {
	return nil
}
func (h *altSvcCaptureHandler) OnPing(FrameHeader, [8]byte) error                   { return nil }
func (h *altSvcCaptureHandler) OnGoAway(FrameHeader, uint32, ErrCode, []byte) error { return nil }
func (h *altSvcCaptureHandler) OnWindowUpdate(FrameHeader, uint32) error            { return nil }
func (h *altSvcCaptureHandler) OnContinuation(FrameHeader, HeaderBlock) error       { return nil }
func (h *altSvcCaptureHandler) OnOrigin(FrameHeader, []string) error                { return nil }
func (h *altSvcCaptureHandler) OnAltSvc(fh FrameHeader, entries []AltSvcEntry) error {
	h.hdr = fh
	h.entries = entries
	return nil
}

func TestFramer_AltSvc_RoundTrip(t *testing.T) {
	// A server-wide (stream-0) alternative: non-empty Origin, one field value.
	want := AltSvcEntry{Origin: "https://example.com", AltValue: `h2=":443"`}
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	require.NoError(t, fw.WriteAltSvc(0, []AltSvcEntry{want}), "WriteAltSvc")
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoError(t, err, "ReadFrame")
	require.Lenf(t, h.entries, 1, "got %d entries, want 1", len(h.entries))
	assert.Equalf(t, want.Origin, h.entries[0].Origin, "entry = %+v, want %+v", h.entries[0], want)
	assert.Equalf(t, want.AltValue, h.entries[0].AltValue, "entry = %+v, want %+v", h.entries[0], want)
	assert.Equalf(t, FrameAltSvc, h.hdr.Type,
		"frame = (type %d, stream %d), want (%d, 0)", h.hdr.Type, h.hdr.StreamID, FrameAltSvc)
	assert.Zerof(t, h.hdr.StreamID,
		"frame = (type %d, stream %d), want (%d, 0)", h.hdr.Type, h.hdr.StreamID, FrameAltSvc)
}

func TestFramer_AltSvc_PerStream_RoundTrip(t *testing.T) {
	// A per-request (non-zero-stream) alternative: empty Origin.
	want := AltSvcEntry{Origin: "", AltValue: `h2="alt.example.com:443"`}
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	require.NoError(t, fw.WriteAltSvc(5, []AltSvcEntry{want}), "WriteAltSvc")
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoError(t, err, "ReadFrame")
	require.Lenf(t, h.entries, 1, "got %d entries, want 1", len(h.entries))
	assert.Equalf(t, want.AltValue, h.entries[0].AltValue,
		"alt value: got %q, want %q", h.entries[0].AltValue, want.AltValue)
	assert.EqualValuesf(t, 5, h.hdr.StreamID, "stream ID: got %d, want 5", h.hdr.StreamID)
}

func TestFramer_AltSvc_EmptyClears(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	require.NoError(t, fw.WriteAltSvc(0, nil), "WriteAltSvc(empty)")
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoError(t, err, "ReadFrame")
	assert.Nilf(t, h.entries, "got %d entries, want nil", len(h.entries))
}

// TestFramer_AltSvc_RejectsMultipleEntries pins that the writer refuses to
// pack more than one Origin into a single ALTSVC frame — RFC 7838 §4 defines
// exactly one Origin per frame.
func TestFramer_AltSvc_RejectsMultipleEntries(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)

	err := fw.WriteAltSvc(0, []AltSvcEntry{
		{Origin: "https://a.example", AltValue: `h2=":443"`},
		{Origin: "https://b.example", AltValue: `h2=":443"`},
	})

	require.ErrorIsf(t, err, ErrTooManyAltSvc, "WriteAltSvc(2 entries) = %v, want ErrTooManyAltSvc", err)
}

func TestDispatchAltSvc_OriginOverflow(t *testing.T) {
	// Claim origin length 0x00FF but the payload is short.
	payload := []byte{0x00, 0xFF, 'x'}
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}

	err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc}, payload, h)

	require.Error(t, err, "expected error for origin overflow, got nil")
}

// TestConformance_RFC7838_Sec4_AltSvcWireFormat pins the RFC 7838 §4 ALTSVC
// payload layout: a uint16 Origin-Len, the Origin, then a single
// Alt-Svc-Field-Value that is the remainder of the frame (its "length
// determined by subtracting the length of all preceding fields from the frame
// length") — NOT a length-prefixed, repeated-entry list. The prior codec
// invented a uint24 length prefix per value, so a fully compliant frame from a
// real server was misparsed into ErrProtocolError (or truncated garbage). The
// §4 receiver-side ignore rules for invalid Origin/stream combinations are
// pinned too.
func TestConformance_RFC7838_Sec4_AltSvcWireFormat(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)

	t.Run("compliant stream-0 frame parses to one entry", func(t *testing.T) {
		origin := "https://example.com"
		value := `h2=":443"`
		payload := append([]byte{byte(len(origin) >> 8), byte(len(origin))}, origin...)
		payload = append(payload, value...)
		h := &altSvcCaptureHandler{}

		err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 0}, payload, h)

		require.NoErrorf(t, err, "dispatchAltSvc(compliant frame) = %v, want nil", err)
		require.Lenf(t, h.entries, 1, "entries = %+v, want one {%q, %q}", h.entries, origin, value)
		assert.Equalf(t, origin, h.entries[0].Origin,
			"entries = %+v, want one {%q, %q}", h.entries, origin, value)
		assert.Equalf(t, value, h.entries[0].AltValue,
			"entries = %+v, want one {%q, %q}", h.entries, origin, value)
	})

	t.Run("field value is the opaque remainder, not length-prefixed", func(t *testing.T) {
		// Empty Origin on a non-zero stream. The value's leading bytes 'h','3','='
		// would be read as a 6.8M uint24 length by the old codec → ErrProtocolError.
		value := `h3=":443"`
		payload := append([]byte{0x00, 0x00}, value...)
		h := &altSvcCaptureHandler{}

		err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 5}, payload, h)

		require.NoErrorf(t, err,
			"dispatchAltSvc(per-stream frame) = %v, want nil (a valid frame, not ErrProtocolError)", err)
		require.Lenf(t, h.entries, 1, "entries = %+v, want the field value %q delivered intact", h.entries, value)
		assert.Equalf(t, value, h.entries[0].AltValue,
			"entries = %+v, want the field value %q delivered intact", h.entries, value)
	})

	t.Run("stream-0 empty-origin frame is ignored", func(t *testing.T) {
		payload := append([]byte{0x00, 0x00}, []byte(`h2=":443"`)...)
		h := &altSvcCaptureHandler{}

		err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 0}, payload, h)

		require.NoErrorf(t, err, "dispatchAltSvc = %v, want nil (ignored)", err)
		require.Nilf(t, h.entries,
			"entries = %+v, want none — a stream-0 frame with empty Origin is ignored (§4)", h.entries)
	})

	t.Run("non-zero-stream non-empty-origin frame is ignored", func(t *testing.T) {
		origin := "https://example.com"
		payload := append([]byte{byte(len(origin) >> 8), byte(len(origin))}, origin...)
		payload = append(payload, []byte(`h2=":443"`)...)
		h := &altSvcCaptureHandler{}

		err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 5}, payload, h)

		require.NoErrorf(t, err, "dispatchAltSvc = %v, want nil (ignored)", err)
		require.Nilf(t, h.entries,
			"entries = %+v, want none — a non-zero-stream frame with non-empty Origin is ignored (§4)", h.entries)
	})

	t.Run("writer emits the RFC 7838 layout", func(t *testing.T) {
		var wbuf bytes.Buffer
		fw := NewFramer(&wbuf, &wbuf)
		origin := "https://example.com"
		value := `h2=":443"`

		err := fw.WriteAltSvc(0, []AltSvcEntry{{Origin: origin, AltValue: value}})

		require.NoError(t, err, "WriteAltSvc")
		// Skip the 9-byte frame header; the payload must be Origin-Len, Origin,
		// value-as-remainder with no length prefix on the value.
		wantPayload := append([]byte{byte(len(origin) >> 8), byte(len(origin))}, origin...)
		wantPayload = append(wantPayload, value...)
		got := wbuf.Bytes()[9:]
		require.Truef(t, bytes.Equal(got, wantPayload), "payload = % x, want % x", got, wantPayload)
	})
}

// TestFramer_AltSvc_PayloadTooShortForOriginLen sends the one payload length
// nothing in this file ever sent: a single octet (#781).
//
// RFC 7838 §4 puts a uint16 Origin-Len at the front of the payload, so `payload[0]`
// and `payload[1]` are read unconditionally once the empty-payload case is past.
// The existing cases send length 0 (handled by the clear-all branch) and length 3
// (long enough), which left the `len(payload) < 2` guard between an ALTSVC frame
// a peer can send in one line and an index-out-of-range panic inside the frame
// parser. This is the repo's peer-input shape: the guard is only worth having if
// something proves the input reaches it.
//
// Both boundary values, because the guard is a boundary: 1 must be refused and 2
// — Origin-Len present, origin empty, value empty — must be accepted. A guard
// written `< 3` would look just as defensive and would drop a legal frame.
func TestFramer_AltSvc_PayloadTooShortForOriginLen(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  []byte
		streamID uint32
		wantErr  error
	}{
		{"one octet cannot hold Origin-Len", []byte{0x00}, 0, ErrProtocolError},
		{"two octets are exactly Origin-Len", []byte{0x00, 0x00}, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := frameBytes(uint32(len(tc.payload)), FrameAltSvc, 0, tc.streamID, tc.payload)
			fr := NewFramer(nil, bytes.NewReader(raw))
			h := &altSvcCaptureHandler{}

			_, err := fr.ReadFrame(context.Background(), h)

			if tc.wantErr == nil {
				require.NoErrorf(t, err,
					"a two-octet ALTSVC was refused (%v); it carries a complete Origin-Len "+
						"of 0, which §4 makes a well-formed per-stream frame with an empty "+
						"origin — refusing it would drop a legal frame", err)
				return
			}
			require.ErrorIsf(t, err, tc.wantErr,
				"a one-octet ALTSVC gave %v, want ErrProtocolError — the payload cannot "+
					"hold the uint16 Origin-Len, and reading it anyway is an "+
					"index-out-of-range on a frame any peer can send", err)
		})
	}
}
