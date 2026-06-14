package frame

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

type recordingHandler struct {
	header     FrameHeader
	dataPayload []byte
	dataPad    uint8
	hb         []byte
	prio       *Priority
	hbPad      uint8
	rstCode    ErrCode
	settings   SettingsParams
	promID     uint32
	promPad    uint8
	pingData   [8]byte
	goLastID   uint32
	goCode     ErrCode
	goDebug    []byte
	winInc     uint32
	contHB     []byte
	priorityVal Priority
}

func (h *recordingHandler) OnData(fh FrameHeader, p []byte, pad uint8) error {
	h.header = fh
	h.dataPayload = append(h.dataPayload[:0], p...)
	h.dataPad = pad
	return nil
}
func (h *recordingHandler) OnHeaders(fh FrameHeader, hb HeaderBlock, prio *Priority, pad uint8) error {
	h.header = fh
	h.hb = append(h.hb[:0], hb...)
	if prio != nil {
		v := *prio
		h.prio = &v
	} else {
		h.prio = nil
	}
	h.hbPad = pad
	return nil
}
func (h *recordingHandler) OnPriority(fh FrameHeader, p Priority) error {
	h.header = fh
	h.priorityVal = p
	return nil
}
func (h *recordingHandler) OnRSTStream(fh FrameHeader, code ErrCode) error {
	h.header = fh
	h.rstCode = code
	return nil
}
func (h *recordingHandler) OnSettings(fh FrameHeader, s SettingsParams) error {
	h.header = fh
	h.settings = s
	return nil
}
func (h *recordingHandler) OnPushPromise(fh FrameHeader, pid uint32, hb HeaderBlock, pad uint8) error {
	h.header = fh
	h.promID = pid
	h.hb = append(h.hb[:0], hb...)
	h.promPad = pad
	return nil
}
func (h *recordingHandler) OnPing(fh FrameHeader, data [8]byte) error {
	h.header = fh
	h.pingData = data
	return nil
}
func (h *recordingHandler) OnGoAway(fh FrameHeader, last uint32, code ErrCode, debug []byte) error {
	h.header = fh
	h.goLastID = last
	h.goCode = code
	h.goDebug = append(h.goDebug[:0], debug...)
	return nil
}
func (h *recordingHandler) OnWindowUpdate(fh FrameHeader, inc uint32) error {
	h.header = fh
	h.winInc = inc
	return nil
}
func (h *recordingHandler) OnContinuation(fh FrameHeader, hb HeaderBlock) error {
	h.header = fh
	h.contHB = append(h.contHB[:0], hb...)
	return nil
}
func (h *recordingHandler) OnOrigin(fh FrameHeader, origins []string) error { return nil }
func (h *recordingHandler) OnAltSvc(fh FrameHeader, entries []AltSvcEntry) error { return nil }

func newFramerWithBuffer() (*Framer, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewFramer(&buf, &buf), &buf
}

func TestFramer_Data_Roundtrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteData(1, true, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), h)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if fh.Type != FrameData || fh.StreamID != 1 || fh.Flags&FlagDataEndStream == 0 {
		t.Fatalf("hdr: %+v", fh)
	}
	if string(h.dataPayload) != "hello" {
		t.Fatalf("data: %q", h.dataPayload)
	}
}

func TestFramer_DataPadded_Roundtrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteDataPadded(3, false, []byte("xy"), 4); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(h.dataPayload) != "xy" || h.dataPad != 4 {
		t.Fatalf("got %q pad=%d", h.dataPayload, h.dataPad)
	}
}

func TestFramer_Headers_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82, 0x84}, EndStream: true, EndHeaders: true,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), h)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if fh.Type != FrameHeaders || fh.Flags&FlagHeadersEndStream == 0 || fh.Flags&FlagHeadersEndHeaders == 0 {
		t.Fatalf("hdr flags: %+v", fh)
	}
	if !bytes.Equal(h.hb, []byte{0x82, 0x84}) {
		t.Fatalf("hb: %x", h.hb)
	}
	if h.prio != nil {
		t.Fatalf("unexpected prio")
	}
}

func TestFramer_HeadersWithPriority_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	prio := &Priority{StreamDep: 7, Exclusive: true, Weight: 16}
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82}, Priority: prio, EndHeaders: true,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.prio == nil || h.prio.StreamDep != 7 || !h.prio.Exclusive || h.prio.Weight != 16 {
		t.Fatalf("prio: %+v", h.prio)
	}
}

func TestFramer_HeadersPadded_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82}, PadLength: 3, EndHeaders: true,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.hbPad != 3 || !bytes.Equal(h.hb, []byte{0x82}) {
		t.Fatalf("pad=%d hb=%x", h.hbPad, h.hb)
	}
}

func TestFramer_Priority_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WritePriority(1, Priority{StreamDep: 9, Exclusive: false, Weight: 32}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.priorityVal.StreamDep != 9 || h.priorityVal.Exclusive || h.priorityVal.Weight != 32 {
		t.Fatalf("prio: %+v", h.priorityVal)
	}
}

func TestFramer_RSTStream_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteRSTStream(2, ErrCodeCancel); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.rstCode != ErrCodeCancel {
		t.Fatalf("code: %v", h.rstCode)
	}
}

func TestFramer_Settings_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	s := SettingsParams{N: 2}
	s.Pairs[0] = SettingPair{ID: SettingMaxConcurrentStreams, Value: 100}
	s.Pairs[1] = SettingPair{ID: SettingInitialWindowSize, Value: 65535}
	if err := fr.WriteSettings(s); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.settings.N != 2 ||
		h.settings.Pairs[0].ID != SettingMaxConcurrentStreams || h.settings.Pairs[0].Value != 100 ||
		h.settings.Pairs[1].ID != SettingInitialWindowSize || h.settings.Pairs[1].Value != 65535 {
		t.Fatalf("settings: %+v", h.settings)
	}
}

func TestFramer_SettingsAck_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteSettingsAck(); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), h)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if fh.Flags&FlagSettingsAck == 0 || fh.Length != 0 {
		t.Fatalf("ack: %+v", fh)
	}
}

func TestFramer_Ping_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	data := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	if err := fr.WritePing(false, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.pingData != data {
		t.Fatalf("data: %v", h.pingData)
	}
}

func TestFramer_GoAway_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteGoAway(7, ErrCodeProtocolError, []byte("oops")); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.goLastID != 7 || h.goCode != ErrCodeProtocolError || string(h.goDebug) != "oops" {
		t.Fatalf("got last=%d code=%v debug=%q", h.goLastID, h.goCode, h.goDebug)
	}
}

func TestFramer_WindowUpdate_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteWindowUpdate(1, 1024); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.winInc != 1024 {
		t.Fatalf("inc: %d", h.winInc)
	}
}

func TestFramer_WindowUpdate_ZeroIncrementRejected(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteWindowUpdate(1, 0); !errors.Is(err, ErrZeroIncrement) {
		t.Fatalf("err = %v, want ErrZeroIncrement", err)
	}
}

func TestFramer_Continuation_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteContinuation(1, true, []byte{0x82, 0x84}); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(h.contHB, []byte{0x82, 0x84}) {
		t.Fatalf("hb: %x", h.contHB)
	}
}

func TestFramer_PushPromise_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.promID != 4 || !bytes.Equal(h.hb, []byte{0x82}) {
		t.Fatalf("got promID=%d hb=%x", h.promID, h.hb)
	}
}

func TestFramer_ClientPreface(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	if err := fr.WriteClientPreface(); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}

func TestFramer_DataStreamID0Rejected(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	if err := fr.WriteData(0, false, []byte("x")); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
}

func TestFramer_FrameTooLargeOnRead(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	fr.SetMaxReadFrameSize(2)
	if err := fr.WriteData(1, false, []byte("hello")); err != nil {
		// write side may also reject when over its own limit; that's acceptable
		if errors.Is(err, ErrFrameTooLarge) {
			return
		}
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// === Bench gates ===

func BenchmarkFramer_WriteData_1KB(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WriteData(1, false, data)
	}
}

func BenchmarkFramer_WriteHeaders_minimal(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	block := []byte{0x82, 0x84, 0x86}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: block, EndHeaders: true})
	}
}

func BenchmarkFramer_ReadFrame_DATA_1KB(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	data := make([]byte, 1024)
	_ = fr.WriteData(1, false, data)
	raw := append([]byte{}, buf.Bytes()...)
	rdr := bytes.NewReader(raw)
	fr.r = rdr
	h := &recordingHandler{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rdr.Reset(raw)
		_, _ = fr.ReadFrame(context.Background(), h)
	}
}
