package frame

import (
	"bytes"
	"context"
	"testing"
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
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
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
	if fh.StreamID != 0 {
		t.Fatalf("R-bit not masked: StreamID = %d, want 0", fh.StreamID)
	}
	if fh.Type != FramePing || fh.Length != 8 {
		t.Fatalf("hdr = %+v", fh)
	}
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
	if fh.Type != FrameData || fh.StreamID != 1 || fh.Flags&FlagDataEndStream == 0 {
		t.Fatalf("hdr = %+v", fh)
	}
	if string(h.dataPayload) != "hi" || h.dataPad != 3 {
		t.Fatalf("data = %q pad = %d", h.dataPayload, h.dataPad)
	}
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
	if fh.Type != FrameHeaders || fh.StreamID != 1 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.prio == nil || h.prio.StreamDep != 7 || !h.prio.Exclusive || h.prio.Weight != 0x10 {
		t.Fatalf("prio = %+v", h.prio)
	}
	if !bytes.Equal(h.hb, []byte{0x82, 0x84}) {
		t.Fatalf("hb = %x", h.hb)
	}
	if h.hbPad != 2 {
		t.Fatalf("pad = %d", h.hbPad)
	}
}

// RFC 7540 §6.3 — PRIORITY: 5-byte payload (E+StreamDep+Weight).
func TestConformance_RFC7540_Sec63_PriorityFrame(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x09, 0x20}
	raw := frameBytes(5, FramePriority, 0, 1, payload)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Type != FramePriority || fh.Length != 5 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.priorityVal.StreamDep != 9 || h.priorityVal.Exclusive || h.priorityVal.Weight != 0x20 {
		t.Fatalf("prio = %+v", h.priorityVal)
	}
}

// RFC 7540 §6.4 — RST_STREAM: 4-byte error code.
func TestConformance_RFC7540_Sec64_RstStreamFrame(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x08}
	raw := frameBytes(4, FrameRSTStream, 0, 3, payload)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Type != FrameRSTStream || fh.StreamID != 3 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.rstCode != ErrCodeCancel {
		t.Fatalf("code = %v", h.rstCode)
	}
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
	if fh.Type != FrameSettings || fh.StreamID != 0 || fh.Flags != 0 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.settings.N != 2 ||
		h.settings.Pairs[0].ID != SettingMaxConcurrentStreams || h.settings.Pairs[0].Value != 100 ||
		h.settings.Pairs[1].ID != SettingInitialWindowSize || h.settings.Pairs[1].Value != 65535 {
		t.Fatalf("settings = %+v", h.settings)
	}
}

// RFC 7540 §6.5 — SETTINGS with ACK flag has zero-length payload.
func TestConformance_RFC7540_Sec65_SettingsAck(t *testing.T) {
	raw := frameBytes(0, FrameSettings, FlagSettingsAck, 0, nil)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Flags&FlagSettingsAck == 0 || fh.Length != 0 {
		t.Fatalf("hdr = %+v", fh)
	}
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
	if fh.Type != FramePushPromise {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.promID != 4 {
		t.Fatalf("R-bit not masked on Promised Stream ID: %d", h.promID)
	}
	if !bytes.Equal(h.hb, []byte{0x82}) || h.promPad != 1 {
		t.Fatalf("hb = %x pad = %d", h.hb, h.promPad)
	}
}

// RFC 7540 §6.7 — PING: 8-byte opaque payload.
func TestConformance_RFC7540_Sec67_PingFrame(t *testing.T) {
	payload := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	raw := frameBytes(8, FramePing, 0, 0, payload)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Type != FramePing || fh.StreamID != 0 || fh.Length != 8 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.pingData != [8]byte{1, 2, 3, 4, 5, 6, 7, 8} {
		t.Fatalf("data = %v", h.pingData)
	}
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
	if fh.Type != FrameGoAway || fh.StreamID != 0 {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.goLastID != 7 {
		t.Fatalf("R-bit not masked on Last-Stream-ID: %d", h.goLastID)
	}
	if h.goCode != ErrCodeProtocolError || string(h.goDebug) != "oops" {
		t.Fatalf("code = %v debug = %q", h.goCode, h.goDebug)
	}
}

// RFC 7540 §6.9 — WINDOW_UPDATE: 4-byte R+Window-Size-Increment.
func TestConformance_RFC7540_Sec69_WindowUpdateFrame(t *testing.T) {
	payload := []byte{0x80, 0x00, 0x04, 0x00}
	raw := frameBytes(4, FrameWindowUpdate, 0, 1, payload)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Type != FrameWindowUpdate {
		t.Fatalf("hdr = %+v", fh)
	}
	if h.winInc != 1024 {
		t.Fatalf("R-bit not masked or value wrong: %d", h.winInc)
	}
}

// RFC 7540 §6.10 — CONTINUATION: opaque header block fragment.
func TestConformance_RFC7540_Sec610_ContinuationFrame(t *testing.T) {
	payload := []byte{0x82, 0x84}
	raw := frameBytes(uint32(len(payload)), FrameContinuation,
		FlagContinuationEndHeaders, 1, payload)
	h := &recordingHandler{}
	fh := readOneFrame(t, raw, h)
	if fh.Type != FrameContinuation || fh.Flags&FlagContinuationEndHeaders == 0 {
		t.Fatalf("hdr = %+v", fh)
	}
	if !bytes.Equal(h.contHB, payload) {
		t.Fatalf("hb = %x", h.contHB)
	}
}

// RFC 7540 §3.5 — Connection Preface octets.
func TestConformance_RFC7540_Sec35_ClientPreface(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	if err := fr.WriteClientPreface(); err != nil {
		t.Fatalf("WriteClientPreface: %v", err)
	}
	want := []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("preface = %q, want %q", buf.Bytes(), want)
	}
}
