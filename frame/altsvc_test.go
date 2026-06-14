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

func (h *altSvcCaptureHandler) OnData(FrameHeader, []byte, uint8) error         { return nil }
func (h *altSvcCaptureHandler) OnHeaders(FrameHeader, HeaderBlock, *Priority, uint8) error {
	return nil
}
func (h *altSvcCaptureHandler) OnPriority(FrameHeader, Priority) error          { return nil }
func (h *altSvcCaptureHandler) OnRSTStream(FrameHeader, ErrCode) error          { return nil }
func (h *altSvcCaptureHandler) OnSettings(FrameHeader, SettingsParams) error    { return nil }
func (h *altSvcCaptureHandler) OnPushPromise(FrameHeader, uint32, HeaderBlock, uint8) error {
	return nil
}
func (h *altSvcCaptureHandler) OnPing(FrameHeader, [8]byte) error               { return nil }
func (h *altSvcCaptureHandler) OnGoAway(FrameHeader, uint32, ErrCode, []byte) error { return nil }
func (h *altSvcCaptureHandler) OnWindowUpdate(FrameHeader, uint32) error        { return nil }
func (h *altSvcCaptureHandler) OnContinuation(FrameHeader, HeaderBlock) error   { return nil }
func (h *altSvcCaptureHandler) OnOrigin(FrameHeader, []string) error            { return nil }
func (h *altSvcCaptureHandler) OnAltSvc(fh FrameHeader, entries []AltSvcEntry) error {
	h.hdr = fh
	h.entries = entries
	return nil
}

func TestFramer_AltSvc_RoundTrip(t *testing.T) {
	want := []AltSvcEntry{
		{Origin: "https://example.com", AltValue: `h2=":443"`},
		{Origin: "https://other.example.com", AltValue: `h2="alt.example.com:8443"`},
	}
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	if err := fw.WriteAltSvc(0, want); err != nil {
		t.Fatalf("WriteAltSvc: %v", err)
	}
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if len(h.entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(h.entries), len(want))
	}
	for i, e := range h.entries {
		if e.Origin != want[i].Origin || e.AltValue != want[i].AltValue {
			t.Errorf("entry %d: got %+v, want %+v", i, e, want[i])
		}
	}
	if h.hdr.Type != FrameAltSvc {
		t.Errorf("frame type: got %d, want %d", h.hdr.Type, FrameAltSvc)
	}
	if h.hdr.StreamID != 0 {
		t.Errorf("stream ID: got %d, want 0", h.hdr.StreamID)
	}
}

func TestFramer_AltSvc_PerStream_RoundTrip(t *testing.T) {
	want := []AltSvcEntry{
		{Origin: "", AltValue: `h2="alt.example.com:443"`},
	}
	var buf bytes.Buffer
	fw := NewFramer(&buf, &buf)
	if err := fw.WriteAltSvc(5, want); err != nil {
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
	if h.entries[0].AltValue != want[0].AltValue {
		t.Errorf("alt value: got %q, want %q", h.entries[0].AltValue, want[0].AltValue)
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

func TestDispatchAltSvc_MalformedTrailingBytes(t *testing.T) {
	// One valid entry, then 1 trailing byte (< 5 min).
	payload := []byte{
		0x00, 0x05, 'h', 't', 't', 'p', 's', // origin "https"
		0x00, 0x00, 0x03, 'a', 'b', 'c', // alt-value "abc"
		0x00, // trailing byte
	}
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc}, payload, h)
	if err == nil {
		t.Fatal("expected error for trailing bytes, got nil")
	}
}

func TestDispatchAltSvc_OriginOverflow(t *testing.T) {
	// Claim origin length 0x00FF but payload is short.
	payload := []byte{0x00, 0xFF, 'x'}
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc}, payload, h)
	if err == nil {
		t.Fatal("expected error for origin overflow, got nil")
	}
}

func TestDispatchAltSvc_AltValueOverflow(t *testing.T) {
	// Origin OK but alt-value length overflows remaining payload.
	payload := []byte{
		0x00, 0x05, 'h', 't', 't', 'p', 's', // origin "https"
		0x00, 0xFF, 0xFF, 0xFF, // claim 16M alt-value
	}
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	h := &altSvcCaptureHandler{}
	err := fr.dispatchAltSvc(FrameHeader{Type: FrameAltSvc}, payload, h)
	if err == nil {
		t.Fatal("expected error for alt-value overflow, got nil")
	}
}
