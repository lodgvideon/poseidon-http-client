package frame

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestDispatchOrigin_Valid(t *testing.T) {
	t.Parallel()

	o1 := "https://example.com"
	o2 := "https://cdn.example.com"

	payload := make([]byte, 0, 2+len(o1)+2+len(o2))
	payload = append(payload, byte(len(o1)>>8), byte(len(o1)))
	payload = append(payload, o1...)
	payload = append(payload, byte(len(o2)>>8), byte(len(o2)))
	payload = append(payload, o2...)

	raw := frameBytes(uint32(len(payload)), FrameOrigin, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))

	rh := &originRecordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), rh)
	if err != nil {
		t.Fatalf("ReadFrame error: %v", err)
	}
	if fh.Type != FrameOrigin {
		t.Fatalf("type = %v, want FrameOrigin", fh.Type)
	}
	if len(rh.origins) != 2 {
		t.Fatalf("expected 2 origins, got %d", len(rh.origins))
	}
	if rh.origins[0] != o1 || rh.origins[1] != o2 {
		t.Fatalf("origins = %v", rh.origins)
	}
}

func TestDispatchOrigin_RejectsNonZeroStream(t *testing.T) {
	t.Parallel()

	raw := frameBytes(0, FrameOrigin, 0, 1, nil)
	fr := NewFramer(nil, bytes.NewReader(raw))

	rh := &originRecordingHandler{}
	_, err := fr.ReadFrame(context.Background(), rh)
	if !errors.Is(err, ErrProtocolError) {
		t.Fatalf("expected ErrProtocolError, got %v", err)
	}
}

func TestDispatchOrigin_MalformedTrailingByte(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00}
	raw := frameBytes(uint32(len(payload)), FrameOrigin, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))

	rh := &originRecordingHandler{}
	_, err := fr.ReadFrame(context.Background(), rh)
	if !errors.Is(err, ErrProtocolError) {
		t.Fatalf("expected ErrProtocolError, got %v", err)
	}
}

func TestDispatchOrigin_LengthOverflow(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00, 99, 'a', 'b', 'c'}
	raw := frameBytes(uint32(len(payload)), FrameOrigin, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))

	rh := &originRecordingHandler{}
	_, err := fr.ReadFrame(context.Background(), rh)
	if !errors.Is(err, ErrProtocolError) {
		t.Fatalf("expected ErrProtocolError, got %v", err)
	}
}

func TestDispatchOrigin_Empty(t *testing.T) {
	t.Parallel()

	raw := frameBytes(0, FrameOrigin, 0, 0, nil)
	fr := NewFramer(nil, bytes.NewReader(raw))

	rh := &originRecordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), rh)
	if err != nil {
		t.Fatalf("ReadFrame error: %v", err)
	}
	if fh.Type != FrameOrigin {
		t.Fatalf("type = %v, want FrameOrigin", fh.Type)
	}
	if len(rh.origins) != 0 {
		t.Fatalf("expected 0 origins, got %d", len(rh.origins))
	}
}

type originRecordingHandler struct {
	origins []string
}

func (h *originRecordingHandler) OnData(FrameHeader, []byte, uint8) error { return nil }
func (h *originRecordingHandler) OnHeaders(FrameHeader, HeaderBlock, *Priority, uint8) error {
	return nil
}
func (h *originRecordingHandler) OnPriority(FrameHeader, Priority) error              { return nil }
func (h *originRecordingHandler) OnRSTStream(FrameHeader, ErrCode) error              { return nil }
func (h *originRecordingHandler) OnSettings(FrameHeader, SettingsParams) error        { return nil }
func (h *originRecordingHandler) OnPushPromise(FrameHeader, uint32, HeaderBlock, uint8) error {
	return nil
}
func (h *originRecordingHandler) OnPing(FrameHeader, [8]byte) error                   { return nil }
func (h *originRecordingHandler) OnGoAway(FrameHeader, uint32, ErrCode, []byte) error { return nil }
func (h *originRecordingHandler) OnWindowUpdate(FrameHeader, uint32) error            { return nil }
func (h *originRecordingHandler) OnContinuation(FrameHeader, HeaderBlock) error       { return nil }
func (h *originRecordingHandler) OnOrigin(_ FrameHeader, origins []string) error {
	h.origins = origins
	return nil
}
