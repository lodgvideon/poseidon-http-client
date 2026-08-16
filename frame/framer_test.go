package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingHandler struct {
	header      FrameHeader
	dataPayload []byte
	dataPad     uint8
	hb          []byte
	prio        *Priority
	hbPad       uint8
	rstCode     ErrCode
	settings    SettingsParams
	promID      uint32
	promPad     uint8
	pingData    [8]byte
	goLastID    uint32
	goCode      ErrCode
	goDebug     []byte
	winInc      uint32
	contHB      []byte
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
func (h *recordingHandler) OnOrigin(fh FrameHeader, origins []string) error      { return nil }
func (h *recordingHandler) OnAltSvc(fh FrameHeader, entries []AltSvcEntry) error { return nil }

func newFramerWithBuffer() (*Framer, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewFramer(&buf, &buf), &buf
}

func TestFramer_Data_Roundtrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteData(1, true, []byte("hello")), "write")
	h := &recordingHandler{}

	fh, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.Equalf(t, FrameData, fh.Type, "hdr: %+v", fh)
	assert.EqualValuesf(t, 1, fh.StreamID, "hdr: %+v", fh)
	assert.NotZerof(t, fh.Flags&FlagDataEndStream, "hdr: %+v", fh)
	assert.Equalf(t, "hello", string(h.dataPayload), "data: %q", h.dataPayload)
}

func TestFramer_DataPadded_Roundtrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteDataPadded(3, false, []byte("xy"), 4), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.Equalf(t, "xy", string(h.dataPayload), "got %q pad=%d", h.dataPayload, h.dataPad)
	assert.EqualValuesf(t, 4, h.dataPad, "got %q pad=%d", h.dataPayload, h.dataPad)
}

func TestFramer_Headers_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82, 0x84}, EndStream: true, EndHeaders: true,
	}), "write")
	h := &recordingHandler{}

	fh, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.Equalf(t, FrameHeaders, fh.Type, "hdr flags: %+v", fh)
	assert.NotZerof(t, fh.Flags&FlagHeadersEndStream, "hdr flags: %+v", fh)
	assert.NotZerof(t, fh.Flags&FlagHeadersEndHeaders, "hdr flags: %+v", fh)
	assert.Truef(t, bytes.Equal(h.hb, []byte{0x82, 0x84}), "hb: %x", h.hb)
	assert.Nil(t, h.prio, "unexpected prio")
}

func TestFramer_HeadersWithPriority_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	prio := &Priority{StreamDep: 7, Exclusive: true, Weight: 16}
	require.NoError(t, fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82}, Priority: prio, EndHeaders: true,
	}), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	require.NotNilf(t, h.prio, "prio: %+v", h.prio)
	assert.EqualValuesf(t, 7, h.prio.StreamDep, "prio: %+v", h.prio)
	assert.Truef(t, h.prio.Exclusive, "prio: %+v", h.prio)
	assert.EqualValuesf(t, 16, h.prio.Weight, "prio: %+v", h.prio)
}

func TestFramer_HeadersPadded_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82}, PadLength: 3, EndHeaders: true,
	}), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, 3, h.hbPad, "pad=%d hb=%x", h.hbPad, h.hb)
	assert.Truef(t, bytes.Equal(h.hb, []byte{0x82}), "pad=%d hb=%x", h.hbPad, h.hb)
}

func TestFramer_Priority_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WritePriority(1, Priority{StreamDep: 9, Exclusive: false, Weight: 32}), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, 9, h.priorityVal.StreamDep, "prio: %+v", h.priorityVal)
	assert.Falsef(t, h.priorityVal.Exclusive, "prio: %+v", h.priorityVal)
	assert.EqualValuesf(t, 32, h.priorityVal.Weight, "prio: %+v", h.priorityVal)
}

func TestFramer_RSTStream_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteRSTStream(2, ErrCodeCancel), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.Equalf(t, ErrCodeCancel, h.rstCode, "code: %v", h.rstCode)
}

func TestFramer_Settings_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	s := SettingsParams{N: 2}
	s.Pairs[0] = SettingPair{ID: SettingMaxConcurrentStreams, Value: 100}
	s.Pairs[1] = SettingPair{ID: SettingInitialWindowSize, Value: 65535}
	require.NoError(t, fr.WriteSettings(s), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	require.Equalf(t, 2, h.settings.N, "settings: %+v", h.settings)
	assert.Equalf(t, SettingMaxConcurrentStreams, h.settings.Pairs[0].ID, "settings: %+v", h.settings)
	assert.EqualValuesf(t, 100, h.settings.Pairs[0].Value, "settings: %+v", h.settings)
	assert.Equalf(t, SettingInitialWindowSize, h.settings.Pairs[1].ID, "settings: %+v", h.settings)
	assert.EqualValuesf(t, 65535, h.settings.Pairs[1].Value, "settings: %+v", h.settings)
}

func TestFramer_SettingsAck_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteSettingsAck(), "write")
	h := &recordingHandler{}

	fh, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.NotZerof(t, fh.Flags&FlagSettingsAck, "ack: %+v", fh)
	assert.Zerof(t, fh.Length, "ack: %+v", fh)
}

func TestFramer_Ping_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	data := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	require.NoError(t, fr.WritePing(false, data), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.Equalf(t, data, h.pingData, "data: %v", h.pingData)
}

func TestFramer_GoAway_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteGoAway(7, ErrCodeProtocolError, []byte("oops")), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, 7, h.goLastID, "got last=%d code=%v debug=%q", h.goLastID, h.goCode, h.goDebug)
	assert.Equalf(t, ErrCodeProtocolError, h.goCode,
		"got last=%d code=%v debug=%q", h.goLastID, h.goCode, h.goDebug)
	assert.Equalf(t, "oops", string(h.goDebug),
		"got last=%d code=%v debug=%q", h.goLastID, h.goCode, h.goDebug)
}

func TestFramer_WindowUpdate_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WriteWindowUpdate(1, 1024), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, 1024, h.winInc, "inc: %d", h.winInc)
}

func TestFramer_WindowUpdate_ZeroIncrementRejected(t *testing.T) {
	fr, _ := newFramerWithBuffer()

	err := fr.WriteWindowUpdate(1, 0)

	require.ErrorIsf(t, err, ErrZeroIncrement, "err = %v, want ErrZeroIncrement", err)
}

func TestFramer_Continuation_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	// RFC 9113 §6.10: a CONTINUATION is valid only after a HEADERS/PUSH_PROMISE
	// without END_HEADERS opens a field block on the same stream. Open one first.
	require.NoError(t,
		fr.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: []byte{0x82}, EndHeaders: false}),
		"write HEADERS")
	require.NoError(t, fr.WriteContinuation(1, true, []byte{0x82, 0x84}), "write CONTINUATION")
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.NoErrorf(t, err, "read HEADERS: %v", err)

	_, err = fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read CONTINUATION: %v", err)
	assert.Truef(t, bytes.Equal(h.contHB, []byte{0x82, 0x84}), "hb: %x", h.contHB)
}

func TestFramer_PushPromise_RoundTrip(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	require.NoError(t, fr.WritePushPromise(1, 4, []byte{0x82}, true, 0), "write")
	h := &recordingHandler{}

	_, err := fr.ReadFrame(context.Background(), h)

	require.NoErrorf(t, err, "read: %v", err)
	assert.EqualValuesf(t, 4, h.promID, "got promID=%d hb=%x", h.promID, h.hb)
	assert.Truef(t, bytes.Equal(h.hb, []byte{0x82}), "got promID=%d hb=%x", h.promID, h.hb)
}

func TestFramer_ClientPreface(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)

	err := fr.WriteClientPreface()

	require.NoErrorf(t, err, "write: %v", err)
	want := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	assert.Equalf(t, want, buf.String(), "got %q, want %q", buf.String(), want)
}

func TestFramer_DataStreamID0Rejected(t *testing.T) {
	fr, _ := newFramerWithBuffer()

	err := fr.WriteData(0, false, []byte("x"))

	require.ErrorIsf(t, err, ErrInvalidStreamID, "err = %v, want ErrInvalidStreamID", err)
}

func TestFramer_FrameTooLargeOnRead(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	fr.SetMaxReadFrameSize(2)

	writeErr := fr.WriteData(1, false, []byte("hello"))

	if writeErr != nil {
		// write side may also reject when over its own limit; that's acceptable
		require.ErrorIsf(t, writeErr, ErrFrameTooLarge, "write: %v", writeErr)
		return
	}
	h := &recordingHandler{}
	_, err := fr.ReadFrame(context.Background(), h)
	require.ErrorIsf(t, err, ErrFrameTooLarge, "err = %v, want ErrFrameTooLarge", err)
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

// BenchmarkFramer_WritePing exists to keep WritePing under the bench-gate's
// zero-allocation guarantee. It used to allocate 8 B/op: the by-value [8]byte
// argument escaped through writeFrame's io.Writer, and nothing checked it
// because no PING bench was on the gate. The inbound-PING echo path reaches it
// for every PING the peer sends.
func BenchmarkFramer_WritePing(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	var data [8]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WritePing(true, data)
	}
}

// BenchmarkFramer_WriteWindowUpdate covers the control-frame write path the
// flow-controlled read side emits steadily; it has always been zero-alloc and
// this pins it.
func BenchmarkFramer_WriteWindowUpdate(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WriteWindowUpdate(1, 65535)
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

// TestFramer_WriteHeaders_PayloadNearBufBoundary guards the WriteHeaders
// fast-path: the 9-byte frame header plus the block fragment must fit in
// the 256-byte writeBuf. Blocks of 248..256 bytes (no padding/priority)
// previously satisfied the guard but overflowed writeBuf and panicked.
func TestFramer_WriteHeaders_PayloadNearBufBoundary(t *testing.T) {
	for _, blockLen := range []int{247, 248, 256, 257} {
		fr, _ := newFramerWithBuffer()
		block := bytes.Repeat([]byte{0x00}, blockLen)
		require.NoErrorf(t, fr.WriteHeaders(WriteHeadersParams{
			StreamID: 1, BlockFragment: block, EndStream: true, EndHeaders: true,
		}), "blockLen=%d: write", blockLen)
		h := &recordingHandler{}

		fh, err := fr.ReadFrame(context.Background(), h)

		require.NoErrorf(t, err, "blockLen=%d: read: %v", blockLen, err)
		assert.Equalf(t, FrameHeaders, fh.Type, "blockLen=%d: type=%v", blockLen, fh.Type)
		assert.Lenf(t, h.hb, blockLen,
			"blockLen=%d: round-tripped %d bytes, want %d", blockLen, len(h.hb), blockLen)
	}
}
