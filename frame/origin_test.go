package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoErrorf(t, err, "ReadFrame error: %v", err)
	assert.Equalf(t, FrameOrigin, fh.Type, "type = %v, want FrameOrigin", fh.Type)
	require.Lenf(t, rh.origins, 2, "expected 2 origins, got %d", len(rh.origins))
	assert.Equalf(t, o1, rh.origins[0], "origins = %v", rh.origins)
	assert.Equalf(t, o2, rh.origins[1], "origins = %v", rh.origins)
}

func TestDispatchOrigin_IgnoresNonZeroStream(t *testing.T) {
	t.Parallel()

	// RFC 8336 §2.2: "an ORIGIN frame on any other stream is invalid and MUST be
	// ignored." Ignored, not rejected — a plain error here would fall through to
	// the reader loop's teardown path and kill the connection over a droppable
	// frame. So ReadFrame must return nil and OnOrigin must NOT fire.
	raw := frameBytes(0, FrameOrigin, 0, 1, nil)
	fr := NewFramer(nil, bytes.NewReader(raw))
	rh := &originRecordingHandler{}

	_, err := fr.ReadFrame(context.Background(), rh)

	require.NoErrorf(t, err, "expected nil (frame ignored), got %v", err)
	assert.False(t, rh.called,
		"OnOrigin fired for an ORIGIN frame on a non-zero stream; it must be ignored")
}

func TestDispatchOrigin_MalformedTrailingByte(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00}
	raw := frameBytes(uint32(len(payload)), FrameOrigin, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	rh := &originRecordingHandler{}

	_, err := fr.ReadFrame(context.Background(), rh)

	require.ErrorIsf(t, err, ErrProtocolError, "expected ErrProtocolError, got %v", err)
}

func TestDispatchOrigin_LengthOverflow(t *testing.T) {
	t.Parallel()

	payload := []byte{0x00, 99, 'a', 'b', 'c'}
	raw := frameBytes(uint32(len(payload)), FrameOrigin, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	rh := &originRecordingHandler{}

	_, err := fr.ReadFrame(context.Background(), rh)

	require.ErrorIsf(t, err, ErrProtocolError, "expected ErrProtocolError, got %v", err)
}

func TestDispatchOrigin_Empty(t *testing.T) {
	t.Parallel()

	raw := frameBytes(0, FrameOrigin, 0, 0, nil)
	fr := NewFramer(nil, bytes.NewReader(raw))
	rh := &originRecordingHandler{}

	fh, err := fr.ReadFrame(context.Background(), rh)

	require.NoErrorf(t, err, "ReadFrame error: %v", err)
	assert.Equalf(t, FrameOrigin, fh.Type, "type = %v, want FrameOrigin", fh.Type)
	assert.Emptyf(t, rh.origins, "expected 0 origins, got %d", len(rh.origins))
}

type originRecordingHandler struct {
	origins []string
	called  bool
}

func (h *originRecordingHandler) OnData(FrameHeader, []byte, uint8) error { return nil }
func (h *originRecordingHandler) OnHeaders(FrameHeader, HeaderBlock, *Priority, uint8) error {
	return nil
}
func (h *originRecordingHandler) OnPriority(FrameHeader, Priority) error       { return nil }
func (h *originRecordingHandler) OnRSTStream(FrameHeader, ErrCode) error       { return nil }
func (h *originRecordingHandler) OnSettings(FrameHeader, SettingsParams) error { return nil }
func (h *originRecordingHandler) OnPushPromise(FrameHeader, uint32, HeaderBlock, uint8) error {
	return nil
}
func (h *originRecordingHandler) OnPing(FrameHeader, [8]byte) error                   { return nil }
func (h *originRecordingHandler) OnGoAway(FrameHeader, uint32, ErrCode, []byte) error { return nil }
func (h *originRecordingHandler) OnWindowUpdate(FrameHeader, uint32) error            { return nil }
func (h *originRecordingHandler) OnContinuation(FrameHeader, HeaderBlock) error       { return nil }
func (h *originRecordingHandler) OnAltSvc(_ FrameHeader, entries []AltSvcEntry) error { return nil }

func (h *originRecordingHandler) OnOrigin(_ FrameHeader, origins []string) error {
	h.origins = origins
	h.called = true
	return nil
}
