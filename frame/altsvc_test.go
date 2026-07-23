package frame

import (
	"bytes"
	"context"
	"testing"
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
	if err := fw.WriteAltSvc(0, []AltSvcEntry{want}); err != nil {
		t.Fatalf("WriteAltSvc: %v", err)
	}
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(h.entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(h.entries))
	}
	if h.entries[0].Origin != want.Origin || h.entries[0].AltValue != want.AltValue {
		t.Errorf("entry = %+v, want %+v", h.entries[0], want)
	}
	if h.hdr.Type != FrameAltSvc || h.hdr.StreamID != 0 {
		t.Errorf("frame = (type %d, stream %d), want (%d, 0)", h.hdr.Type, h.hdr.StreamID, FrameAltSvc)
	}
}

func TestFramer_AltSvc_PerStream_RoundTrip(t *testing.T) {
	// A per-request (non-zero-stream) alternative: empty Origin.
	want := AltSvcEntry{Origin: "", AltValue: `h2="alt.example.com:443"`}
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	if err := fw.WriteAltSvc(5, []AltSvcEntry{want}); err != nil {
		t.Fatalf("WriteAltSvc: %v", err)
	}
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(h.entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(h.entries))
	}
	if h.entries[0].AltValue != want.AltValue {
		t.Errorf("alt value: got %q, want %q", h.entries[0].AltValue, want.AltValue)
	}
	if h.hdr.StreamID != 5 {
		t.Errorf("stream ID: got %d, want 5", h.hdr.StreamID)
	}
}

func TestFramer_AltSvc_EmptyClears(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	if err := fw.WriteAltSvc(0, nil); err != nil {
		t.Fatalf("WriteAltSvc(empty): %v", err)
	}
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if h.entries != nil {
		t.Errorf("got %d entries, want nil", len(h.entries))
	}
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
	if err != ErrTooManyAltSvc {
		t.Fatalf("WriteAltSvc(2 entries) = %v, want ErrTooManyAltSvc", err)
	}
}

func TestDispatchAltSvc_OriginOverflow(t *testing.T) {
	// Claim origin length 0x00FF but the payload is short.
	payload := []byte{0x00, 0xFF, 'x'}
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	if err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc}, payload, h); err == nil {
		t.Fatal("expected error for origin overflow, got nil")
	}
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
		if err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 0}, payload, h); err != nil {
			t.Fatalf("dispatchAltSvc(compliant frame) = %v, want nil", err)
		}
		if len(h.entries) != 1 || h.entries[0].Origin != origin || h.entries[0].AltValue != value {
			t.Fatalf("entries = %+v, want one {%q, %q}", h.entries, origin, value)
		}
	})

	t.Run("field value is the opaque remainder, not length-prefixed", func(t *testing.T) {
		// Empty Origin on a non-zero stream. The value's leading bytes 'h','3','='
		// would be read as a 6.8M uint24 length by the old codec → ErrProtocolError.
		value := `h3=":443"`
		payload := append([]byte{0x00, 0x00}, value...)
		h := &altSvcCaptureHandler{}
		if err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 5}, payload, h); err != nil {
			t.Fatalf("dispatchAltSvc(per-stream frame) = %v, want nil (a valid frame, not ErrProtocolError)", err)
		}
		if len(h.entries) != 1 || h.entries[0].AltValue != value {
			t.Fatalf("entries = %+v, want the field value %q delivered intact", h.entries, value)
		}
	})

	t.Run("stream-0 empty-origin frame is ignored", func(t *testing.T) {
		payload := append([]byte{0x00, 0x00}, []byte(`h2=":443"`)...)
		h := &altSvcCaptureHandler{}
		if err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 0}, payload, h); err != nil {
			t.Fatalf("dispatchAltSvc = %v, want nil (ignored)", err)
		}
		if h.entries != nil {
			t.Fatalf("entries = %+v, want none — a stream-0 frame with empty Origin is ignored (§4)", h.entries)
		}
	})

	t.Run("non-zero-stream non-empty-origin frame is ignored", func(t *testing.T) {
		origin := "https://example.com"
		payload := append([]byte{byte(len(origin) >> 8), byte(len(origin))}, origin...)
		payload = append(payload, []byte(`h2=":443"`)...)
		h := &altSvcCaptureHandler{}
		if err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc, StreamID: 5}, payload, h); err != nil {
			t.Fatalf("dispatchAltSvc = %v, want nil (ignored)", err)
		}
		if h.entries != nil {
			t.Fatalf("entries = %+v, want none — a non-zero-stream frame with non-empty Origin is ignored (§4)", h.entries)
		}
	})

	t.Run("writer emits the RFC 7838 layout", func(t *testing.T) {
		var wbuf bytes.Buffer
		fw := NewFramer(&wbuf, &wbuf)
		origin := "https://example.com"
		value := `h2=":443"`
		if err := fw.WriteAltSvc(0, []AltSvcEntry{{Origin: origin, AltValue: value}}); err != nil {
			t.Fatalf("WriteAltSvc: %v", err)
		}
		// Skip the 9-byte frame header; the payload must be Origin-Len, Origin,
		// value-as-remainder with no length prefix on the value.
		wantPayload := append([]byte{byte(len(origin) >> 8), byte(len(origin))}, origin...)
		wantPayload = append(wantPayload, value...)
		got := wbuf.Bytes()[9:]
		if !bytes.Equal(got, wantPayload) {
			t.Fatalf("payload = % x, want % x", got, wantPayload)
		}
	})
}
