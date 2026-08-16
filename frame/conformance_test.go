package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// frameBytes builds a 9-byte header followed by payload.
func frameBytes(length uint32, typ FrameType, flags Flags, streamID uint32, payload []byte) []byte {
	hdr := make([]byte, FrameHeaderSize)
	hdr[0] = byte(length >> 16)
	hdr[1] = byte(length >> 8)
	hdr[2] = byte(length)
	hdr[3] = byte(typ)
	hdr[4] = byte(flags)
	hdr[5] = byte(streamID >> 24)
	hdr[6] = byte(streamID >> 16)
	hdr[7] = byte(streamID >> 8)
	hdr[8] = byte(streamID)
	return append(hdr, payload...)
}

func readOneFrame(t *testing.T, raw []byte, h Handler) FrameHeader {
	t.Helper()
	fr := NewFramer(nil, bytes.NewReader(raw))
	fh, err := fr.ReadFrame(context.Background(), h)
	require.NoError(t, err, "ReadFrame")
	return fh
}

// RFC 7540 §4.1 — receivers MUST ignore the reserved (R) bit on StreamID.
func TestConformance_RFC7540_Sec41_FrameHeader_RBitMasked(t *testing.T) {
	raw := []byte{
		0x00, 0x00, 0x08,
		0x06,
		0x00,
		0x80, 0x00, 0x00, 0x00,
		0, 0, 0, 0, 0, 0, 0, 0,
	}
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	require.Zerof(t, fh.StreamID, "R-bit not masked: StreamID = %d, want 0", fh.StreamID)
	assert.Equalf(t, FramePing, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 8, fh.Length, "hdr = %+v", fh)
}

// RFC 7540 §6.1 — DATA: optional Pad Length octet then data then padding.
func TestConformance_RFC7540_Sec61_DataFrame_PaddedEndStream(t *testing.T) {
	payload := []byte{
		0x03,
		'h', 'i',
		0x00, 0x00, 0x00,
	}
	raw := frameBytes(uint32(len(payload)), FrameData,
		FlagDataEndStream|FlagDataPadded, 1, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameData, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 1, fh.StreamID, "hdr = %+v", fh)
	assert.NotZerof(t, fh.Flags&FlagDataEndStream, "hdr = %+v", fh)
	assert.Equalf(t, "hi", string(h.dataPayload), "data = %q pad = %d", h.dataPayload, h.dataPad)
	assert.EqualValuesf(t, 3, h.dataPad, "data = %q pad = %d", h.dataPayload, h.dataPad)
}

// RFC 7540 §6.2 — HEADERS with Pad Length, Priority, fragment, padding.
func TestConformance_RFC7540_Sec62_HeadersFrame_PriorityPaddedEndHeaders(t *testing.T) {
	payload := []byte{
		0x02,
		0x80, 0x00, 0x00, 0x07,
		0x10,
		0x82, 0x84,
		0x00, 0x00,
	}
	raw := frameBytes(uint32(len(payload)), FrameHeaders,
		FlagHeadersEndStream|FlagHeadersEndHeaders|FlagHeadersPadded|FlagHeadersPriority,
		1, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameHeaders, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 1, fh.StreamID, "hdr = %+v", fh)
	require.NotNilf(t, h.prio, "prio = %+v", h.prio)
	assert.EqualValuesf(t, 7, h.prio.StreamDep, "prio = %+v", h.prio)
	assert.Truef(t, h.prio.Exclusive, "prio = %+v", h.prio)
	assert.EqualValuesf(t, 0x10, h.prio.Weight, "prio = %+v", h.prio)
	assert.Truef(t, bytes.Equal(h.hb, []byte{0x82, 0x84}), "hb = %x", h.hb)
	assert.EqualValuesf(t, 2, h.hbPad, "pad = %d", h.hbPad)
}

// RFC 7540 §6.3 — PRIORITY: 5-byte payload (E+StreamDep+Weight).
func TestConformance_RFC7540_Sec63_PriorityFrame(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x09, 0x20}
	raw := frameBytes(5, FramePriority, 0, 1, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FramePriority, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 5, fh.Length, "hdr = %+v", fh)
	assert.EqualValuesf(t, 9, h.priorityVal.StreamDep, "prio = %+v", h.priorityVal)
	assert.Falsef(t, h.priorityVal.Exclusive, "prio = %+v", h.priorityVal)
	assert.EqualValuesf(t, 0x20, h.priorityVal.Weight, "prio = %+v", h.priorityVal)
}

// RFC 7540 §6.4 — RST_STREAM: 4-byte error code.
func TestConformance_RFC7540_Sec64_RstStreamFrame(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x08}
	raw := frameBytes(4, FrameRSTStream, 0, 3, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameRSTStream, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 3, fh.StreamID, "hdr = %+v", fh)
	assert.Equalf(t, ErrCodeCancel, h.rstCode, "code = %v", h.rstCode)
}

// RFC 7540 §6.5.1 — SETTINGS entry: 2-byte ID + 4-byte Value.
func TestConformance_RFC7540_Sec65_SettingsFrame(t *testing.T) {
	payload := []byte{
		0x00, 0x03, 0x00, 0x00, 0x00, 0x64,
		0x00, 0x04, 0x00, 0x00, 0xFF, 0xFF,
	}
	raw := frameBytes(uint32(len(payload)), FrameSettings, 0, 0, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameSettings, fh.Type, "hdr = %+v", fh)
	assert.Zerof(t, fh.StreamID, "hdr = %+v", fh)
	assert.Zerof(t, fh.Flags, "hdr = %+v", fh)
	require.Equalf(t, 2, h.settings.N, "settings = %+v", h.settings)
	assert.Equalf(t, SettingMaxConcurrentStreams, h.settings.Pairs[0].ID, "settings = %+v", h.settings)
	assert.EqualValuesf(t, 100, h.settings.Pairs[0].Value, "settings = %+v", h.settings)
	assert.Equalf(t, SettingInitialWindowSize, h.settings.Pairs[1].ID, "settings = %+v", h.settings)
	assert.EqualValuesf(t, 65535, h.settings.Pairs[1].Value, "settings = %+v", h.settings)
}

// RFC 7540 §6.5 — SETTINGS with ACK flag has zero-length payload.
func TestConformance_RFC7540_Sec65_SettingsAck(t *testing.T) {
	raw := frameBytes(0, FrameSettings, FlagSettingsAck, 0, nil)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.NotZerof(t, fh.Flags&FlagSettingsAck, "hdr = %+v", fh)
	assert.Zerof(t, fh.Length, "hdr = %+v", fh)
}

// RFC 7540 §6.6 — PUSH_PROMISE: PadLen + R+PromisedID + fragment + padding.
func TestConformance_RFC7540_Sec66_PushPromiseFrame(t *testing.T) {
	payload := []byte{
		0x01,
		0x80, 0x00, 0x00, 0x04,
		0x82,
		0x00,
	}
	raw := frameBytes(uint32(len(payload)), FramePushPromise,
		FlagPushPromiseEndHeaders|FlagPushPromisePadded, 1, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FramePushPromise, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 4, h.promID, "R-bit not masked on Promised Stream ID: %d", h.promID)
	assert.Truef(t, bytes.Equal(h.hb, []byte{0x82}), "hb = %x pad = %d", h.hb, h.promPad)
	assert.EqualValuesf(t, 1, h.promPad, "hb = %x pad = %d", h.hb, h.promPad)
}

// RFC 7540 §6.7 — PING: 8-byte opaque payload.
func TestConformance_RFC7540_Sec67_PingFrame(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	raw := frameBytes(8, FramePing, 0, 0, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FramePing, fh.Type, "hdr = %+v", fh)
	assert.Zerof(t, fh.StreamID, "hdr = %+v", fh)
	assert.EqualValuesf(t, 8, fh.Length, "hdr = %+v", fh)
	assert.Equalf(t, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}, h.pingData, "data = %v", h.pingData)
}

// RFC 7540 §6.8 — GOAWAY: R+LastStreamID + ErrorCode + Debug Data.
func TestConformance_RFC7540_Sec68_GoAwayFrame(t *testing.T) {
	payload := []byte{
		0x80, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x01,
		'o', 'o', 'p', 's',
	}
	raw := frameBytes(uint32(len(payload)), FrameGoAway, 0, 0, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameGoAway, fh.Type, "hdr = %+v", fh)
	assert.Zerof(t, fh.StreamID, "hdr = %+v", fh)
	assert.EqualValuesf(t, 7, h.goLastID, "R-bit not masked on Last-Stream-ID: %d", h.goLastID)
	assert.Equalf(t, ErrCodeProtocolError, h.goCode, "code = %v debug = %q", h.goCode, h.goDebug)
	assert.Equalf(t, "oops", string(h.goDebug), "code = %v debug = %q", h.goCode, h.goDebug)
}

// RFC 7540 §6.9 — WINDOW_UPDATE: 4-byte R+Window-Size-Increment.
func TestConformance_RFC7540_Sec69_WindowUpdateFrame(t *testing.T) {
	payload := []byte{0x80, 0x00, 0x04, 0x00}
	raw := frameBytes(4, FrameWindowUpdate, 0, 1, payload)
	h := &recordingHandler{}

	fh := readOneFrame(t, raw, h)

	assert.Equalf(t, FrameWindowUpdate, fh.Type, "hdr = %+v", fh)
	assert.EqualValuesf(t, 1024, h.winInc, "R-bit not masked or value wrong: %d", h.winInc)
}

// RFC 7540 §6.10 — CONTINUATION: opaque header block fragment. Per RFC 9113
// §6.10 a CONTINUATION is valid only inside an open field block, so a HEADERS
// without END_HEADERS precedes it on the same stream.
func TestConformance_RFC7540_Sec610_ContinuationFrame(t *testing.T) {
	open := frameBytes(1, FrameHeaders, 0, 1, []byte{0x82}) // HEADERS, no END_HEADERS
	payload := []byte{0x82, 0x84}
	cont := frameBytes(uint32(len(payload)), FrameContinuation,
		FlagContinuationEndHeaders, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(append(append([]byte{}, open...), cont...)))
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.NoError(t, err, "read HEADERS")

	fh, err := fr.ReadFrame(context.Background(), h)

	require.NoError(t, err, "read CONTINUATION")
	assert.Equalf(t, FrameContinuation, fh.Type, "hdr = %+v", fh)
	assert.NotZerof(t, fh.Flags&FlagContinuationEndHeaders, "hdr = %+v", fh)
	assert.Truef(t, bytes.Equal(h.contHB, payload), "hb = %x", h.contHB)
}

// RFC 7540 §3.5 — Connection Preface octets.
func TestConformance_RFC7540_Sec35_ClientPreface(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)

	err := fr.WriteClientPreface()

	require.NoError(t, err, "WriteClientPreface")
	want := []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
	require.Truef(t, bytes.Equal(buf.Bytes(), want), "preface = %q, want %q", buf.Bytes(), want)
}
