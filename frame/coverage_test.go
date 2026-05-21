package frame

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// errWriter is an io.Writer that always returns an error after writing n bytes.
type errWriter struct {
	n   int // number of bytes to accept before failing
	buf bytes.Buffer
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, errors.New("injected write error")
	}
	if len(p) > e.n {
		written := e.n
		e.buf.Write(p[:written])
		e.n = 0
		return written, errors.New("injected write error")
	}
	e.buf.Write(p)
	e.n -= len(p)
	return len(p), nil
}

// alwaysErrWriter rejects every write immediately.
type alwaysErrWriter struct{}

func (alwaysErrWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("injected write error")
}

// === writeFrame error path ===

// TestFramer_writeFrame_ErrorOnHeaderWrite exercises the writeFrame path when
// the underlying writer fails during header write.
func TestFramer_writeFrame_ErrorOnHeaderWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	err := fr.WritePriority(1, Priority{StreamDep: 1, Weight: 10})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestFramer_writeFrame_ErrorOnPayloadWrite exercises the writeFrame path when
// the header succeeds but payload write fails.
func TestFramer_writeFrame_ErrorOnPayloadWrite(t *testing.T) {
	// Accept exactly 9 bytes (frame header) then fail.
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WritePriority(1, Priority{StreamDep: 1, Weight: 10})
	if err == nil {
		t.Fatal("expected error on payload write, got nil")
	}
}

// === ReadFrame: unknown frame type ===

// TestFramer_ReadFrame_UnknownFrameType verifies that an unknown frame type is
// silently ignored per RFC 7540 §5.5.
func TestFramer_ReadFrame_UnknownFrameType(t *testing.T) {
	// Build a frame with type 0xFF (unknown).
	payload := []byte{0x01, 0x02, 0x03}
	raw := frameBytes(uint32(len(payload)), FrameType(0xFF), 0, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	h := &recordingHandler{}
	fh, err := fr.ReadFrame(context.Background(), h)
	if err != nil {
		t.Fatalf("expected no error for unknown frame type, got: %v", err)
	}
	if fh.Type != FrameType(0xFF) {
		t.Fatalf("unexpected frame type: %v", fh.Type)
	}
}

// TestFramer_ReadFrame_MaxFrameSizeViolation verifies ErrFrameTooLarge when
// the peer sends a frame exceeding our advertised max.
func TestFramer_ReadFrame_MaxFrameSizeViolation(t *testing.T) {
	// Write a large DATA frame but set a small max on read.
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	// Write a 100-byte frame without enforcing size on write.
	fr.maxReadFrameSize = 16384 // temporarily large for write
	if err := fr.WriteData(1, false, make([]byte, 100)); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	fr.SetMaxReadFrameSize(50) // now smaller than the written frame
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// TestFramer_ReadFrame_CancelledContext verifies that a pre-cancelled context
// causes ReadFrame to return immediately.
func TestFramer_ReadFrame_CancelledContext(t *testing.T) {
	fr := NewFramer(nil, bytes.NewReader([]byte{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fr.ReadFrame(ctx, &recordingHandler{})
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}

// === dispatchRSTStream error: wrong length ===

// TestFramer_dispatchRSTStream_TruncatedPayload sends an RST_STREAM with wrong
// length and expects ErrRSTWrongLength.
func TestFramer_dispatchRSTStream_TruncatedPayload(t *testing.T) {
	// RST_STREAM with length=3 instead of required 4.
	raw := frameBytes(3, FrameRSTStream, 0, 1, []byte{0x00, 0x00, 0x08})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrRSTWrongLength) {
		t.Fatalf("err = %v, want ErrRSTWrongLength", err)
	}
}

// === dispatchContinuation error: stream id 0 ===

// TestFramer_dispatchContinuation_ErrorStreamID0 verifies that a CONTINUATION
// with stream ID 0 is rejected.
func TestFramer_dispatchContinuation_ErrorStreamID0(t *testing.T) {
	raw := frameBytes(2, FrameContinuation, FlagContinuationEndHeaders, 0, []byte{0x82, 0x84})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
}

// === WriteDataPadded error paths ===

// TestFramer_WriteDataPadded_ErrorOnPadLenWrite verifies the error when writing
// the pad-length byte fails.
func TestFramer_WriteDataPadded_ErrorOnPadLenWrite(t *testing.T) {
	// Allow only the 9-byte header through.
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WriteDataPadded(1, false, []byte("hi"), 4)
	if err == nil {
		t.Fatal("expected error when writing pad-length byte, got nil")
	}
}

// TestFramer_WriteDataPadded_ErrorOnDataWrite verifies the error when writing
// the data portion of a padded DATA frame fails.
func TestFramer_WriteDataPadded_ErrorOnDataWrite(t *testing.T) {
	// Allow header (9) + pad-length byte (1) = 10 bytes.
	ew := &errWriter{n: FrameHeaderSize + 1}
	fr := NewFramer(ew, nil)
	err := fr.WriteDataPadded(1, false, []byte("hello"), 4)
	if err == nil {
		t.Fatal("expected error when writing data, got nil")
	}
}

// TestFramer_WriteDataPadded_ErrorOnPaddingWrite verifies error when writing
// padding bytes fails.
func TestFramer_WriteDataPadded_ErrorOnPaddingWrite(t *testing.T) {
	// Allow header (9) + pad-length (1) + data (2) = 12 bytes.
	ew := &errWriter{n: FrameHeaderSize + 1 + 2}
	fr := NewFramer(ew, nil)
	err := fr.WriteDataPadded(1, false, []byte("hi"), 4)
	if err == nil {
		t.Fatal("expected error when writing padding, got nil")
	}
}

// === dispatchPriority: wrong length ===

// TestFramer_dispatchPriority_TruncatedPayload sends a PRIORITY with wrong
// length and expects ErrPriorityWrongLength.
func TestFramer_dispatchPriority_TruncatedPayload(t *testing.T) {
	raw := frameBytes(4, FramePriority, 0, 1, []byte{0x00, 0x00, 0x00, 0x09})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrPriorityWrongLength) {
		t.Fatalf("err = %v, want ErrPriorityWrongLength", err)
	}
}

// === dispatchPing: wrong length ===

// TestFramer_dispatchPing_TruncatedPayload sends a PING with wrong length
// and expects ErrPingWrongLength.
func TestFramer_dispatchPing_TruncatedPayload(t *testing.T) {
	raw := frameBytes(4, FramePing, 0, 0, []byte{1, 2, 3, 4})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrPingWrongLength) {
		t.Fatalf("err = %v, want ErrPingWrongLength", err)
	}
}

// === dispatchWindowUpdate: wrong length ===

// TestFramer_dispatchWindowUpdate_TruncatedPayload sends a WINDOW_UPDATE with
// wrong length and expects ErrWindowWrongLength.
func TestFramer_dispatchWindowUpdate_TruncatedPayload(t *testing.T) {
	raw := frameBytes(3, FrameWindowUpdate, 0, 1, []byte{0x00, 0x00, 0x04})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrWindowWrongLength) {
		t.Fatalf("err = %v, want ErrWindowWrongLength", err)
	}
}

// === dispatchSettings error paths ===

// TestFramer_dispatchSettings_OddSizedPayload sends SETTINGS with a payload
// not divisible by 6 and expects ErrSettingsLength.
func TestFramer_dispatchSettings_OddSizedPayload(t *testing.T) {
	raw := frameBytes(5, FrameSettings, 0, 0, []byte{0x00, 0x03, 0x00, 0x00, 0x00})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrSettingsLength) {
		t.Fatalf("err = %v, want ErrSettingsLength", err)
	}
}

// TestFramer_dispatchSettings_AckWithNonEmptyPayload sends SETTINGS ACK with
// non-zero length and expects ErrSettingsAck.
func TestFramer_dispatchSettings_AckWithNonEmptyPayload(t *testing.T) {
	raw := frameBytes(6, FrameSettings, FlagSettingsAck, 0, []byte{0x00, 0x03, 0x00, 0x00, 0x00, 0x64})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrSettingsAck) {
		t.Fatalf("err = %v, want ErrSettingsAck", err)
	}
}

// TestFramer_dispatchSettings_TooManyPairs sends SETTINGS with more than 16
// pairs and expects ErrSettingsLength.
func TestFramer_dispatchSettings_TooManyPairs(t *testing.T) {
	// 17 pairs × 6 bytes = 102 bytes.
	payload := make([]byte, 17*6)
	raw := frameBytes(uint32(len(payload)), FrameSettings, 0, 0, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	fr.SetMaxReadFrameSize(16384)
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrSettingsLength) {
		t.Fatalf("err = %v, want ErrSettingsLength", err)
	}
}

// === WritePing error path ===

// TestFramer_WritePing_ErrorOnWrite verifies that a write failure from the
// underlying writer surfaces correctly.
func TestFramer_WritePing_ErrorOnWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	err := fr.WritePing(false, [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// === WriteGoAway error paths ===

// TestFramer_WriteGoAway_ErrorOnHeaderWrite verifies WriteGoAway returns an
// error when the header write fails.
func TestFramer_WriteGoAway_ErrorOnHeaderWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	err := fr.WriteGoAway(1, ErrCodeNoError, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestFramer_WriteGoAway_ErrorOnFixedPayloadWrite verifies WriteGoAway returns
// an error when writing the 8-byte fixed payload fails.
func TestFramer_WriteGoAway_ErrorOnFixedPayloadWrite(t *testing.T) {
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WriteGoAway(1, ErrCodeNoError, nil)
	if err == nil {
		t.Fatal("expected error writing goaway fixed payload, got nil")
	}
}

// TestFramer_WriteGoAway_ErrorOnDebugWrite verifies WriteGoAway returns an
// error when writing the debug data fails.
func TestFramer_WriteGoAway_ErrorOnDebugWrite(t *testing.T) {
	// Allow header (9) + fixed 8 bytes = 17 bytes.
	ew := &errWriter{n: FrameHeaderSize + 8}
	fr := NewFramer(ew, nil)
	err := fr.WriteGoAway(1, ErrCodeNoError, []byte("debug info"))
	if err == nil {
		t.Fatal("expected error writing goaway debug data, got nil")
	}
}

// TestFramer_WriteGoAway_TooLarge verifies WriteGoAway returns ErrFrameTooLarge
// when the debug data makes the frame exceed max size.
func TestFramer_WriteGoAway_TooLarge(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetMaxReadFrameSize(10) // too small for 8-byte fixed + any debug
	err := fr.WriteGoAway(1, ErrCodeNoError, []byte("debug"))
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// === dispatchGoAway error: truncated ===

// TestFramer_dispatchGoAway_TruncatedPayload sends a GOAWAY with fewer than 8
// bytes and expects ErrShortRead.
func TestFramer_dispatchGoAway_TruncatedPayload(t *testing.T) {
	raw := frameBytes(4, FrameGoAway, 0, 0, []byte{0x00, 0x00, 0x00, 0x07})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("err = %v, want ErrShortRead", err)
	}
}

// === dispatchData: padding error ===

// TestFramer_dispatchData_ErrorInvalidPadding sends a padded DATA frame where
// the declared pad length exceeds the payload.
func TestFramer_dispatchData_ErrorInvalidPadding(t *testing.T) {
	// pad_length byte = 10, but only 2 bytes of actual payload follow (impossible).
	payload := []byte{0x0A, 0x01} // padLen=10, only 1 byte remaining
	raw := frameBytes(uint32(len(payload)), FrameData, FlagDataPadded, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if err == nil {
		t.Fatal("expected padding error, got nil")
	}
}

// === WritePriority error path ===

// TestFramer_WritePriority_ErrorOnWrite verifies WritePriority propagates write
// errors from the underlying writer.
func TestFramer_WritePriority_ErrorOnWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	err := fr.WritePriority(1, Priority{StreamDep: 1, Weight: 10})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// === WriteContinuation error path ===

// TestFramer_WriteContinuation_ErrorOnWrite verifies WriteContinuation
// propagates write errors.
func TestFramer_WriteContinuation_ErrorOnWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	err := fr.WriteContinuation(1, true, []byte{0x82})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// === dispatchHeaders error paths ===

// TestFramer_dispatchHeaders_ErrorStreamID0 verifies that a HEADERS frame
// on stream 0 is rejected.
func TestFramer_dispatchHeaders_ErrorStreamID0(t *testing.T) {
	raw := frameBytes(1, FrameHeaders, 0, 0, []byte{0x82})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
}

// TestFramer_dispatchHeaders_PriorityTruncated sends a HEADERS frame with
// PRIORITY flag but too short body (< 5 bytes) and expects ErrShortRead.
func TestFramer_dispatchHeaders_PriorityTruncated(t *testing.T) {
	// Only 4 bytes for priority field (need 5).
	payload := []byte{0x00, 0x00, 0x00, 0x09}
	raw := frameBytes(uint32(len(payload)), FrameHeaders, FlagHeadersPriority, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("err = %v, want ErrShortRead", err)
	}
}

// TestFramer_dispatchHeaders_InvalidPadding sends a HEADERS frame with a pad
// length that exceeds the payload size.
func TestFramer_dispatchHeaders_InvalidPadding(t *testing.T) {
	// pad_length = 10, only 1 remaining byte.
	payload := []byte{0x0A, 0x82}
	raw := frameBytes(uint32(len(payload)), FrameHeaders, FlagHeadersPadded, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if err == nil {
		t.Fatal("expected padding error, got nil")
	}
}

// === WriteHeaders error paths ===

// TestFramer_WriteHeaders_ErrorOnPadLenWrite verifies that WriteHeaders returns
// an error when writing the padding-length byte fails.
func TestFramer_WriteHeaders_ErrorOnPadLenWrite(t *testing.T) {
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WriteHeaders(WriteHeadersParams{
		StreamID:      1,
		BlockFragment: []byte{0x82},
		PadLength:     4,
		EndHeaders:    true,
	})
	if err == nil {
		t.Fatal("expected error on pad-length write, got nil")
	}
}

// TestFramer_WriteHeaders_ErrorOnPriorityWrite verifies that WriteHeaders
// returns an error when writing the priority field fails.
func TestFramer_WriteHeaders_ErrorOnPriorityWrite(t *testing.T) {
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WriteHeaders(WriteHeadersParams{
		StreamID:      1,
		BlockFragment: []byte{0x82},
		Priority:      &Priority{StreamDep: 3, Weight: 10},
		EndHeaders:    true,
	})
	if err == nil {
		t.Fatal("expected error on priority write, got nil")
	}
}

// TestFramer_WriteHeaders_ErrorOnBlockWrite verifies that WriteHeaders returns
// an error when writing the block fragment fails.
func TestFramer_WriteHeaders_ErrorOnBlockWrite(t *testing.T) {
	// Allow header (9) only; block fragment write fails.
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WriteHeaders(WriteHeadersParams{
		StreamID:      1,
		BlockFragment: []byte{0x82},
		EndHeaders:    true,
	})
	if err == nil {
		t.Fatal("expected error on block write, got nil")
	}
}

// TestFramer_WriteHeaders_ErrorOnPaddingWrite verifies that WriteHeaders
// returns an error when writing padding bytes fails.
func TestFramer_WriteHeaders_ErrorOnPaddingWrite(t *testing.T) {
	// Allow header (9) + pad-length byte (1) + block (1) = 11 bytes.
	ew := &errWriter{n: FrameHeaderSize + 1 + 1}
	fr := NewFramer(ew, nil)
	err := fr.WriteHeaders(WriteHeadersParams{
		StreamID:      1,
		BlockFragment: []byte{0x82},
		PadLength:     4,
		EndHeaders:    true,
	})
	if err == nil {
		t.Fatal("expected error on padding write, got nil")
	}
}

// === WritePushPromise error paths ===

// TestFramer_WritePushPromise_WithPadding verifies WritePushPromise with
// padding writes correct frame.
func TestFramer_WritePushPromise_WithPadding(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 3)
	if err != nil {
		t.Fatalf("WritePushPromise with padding: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if h.promID != 4 || len(h.hb) != 1 || h.hb[0] != 0x82 || h.promPad != 3 {
		t.Fatalf("got promID=%d hb=%x pad=%d", h.promID, h.hb, h.promPad)
	}
}

// TestFramer_WritePushPromise_TooLarge verifies that WritePushPromise rejects
// oversized frames.
func TestFramer_WritePushPromise_TooLarge(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetMaxReadFrameSize(10)
	err := fr.WritePushPromise(1, 4, make([]byte, 100), true, 0)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// TestFramer_WritePushPromise_ErrorOnPadLenWrite tests error when writing pad-
// length byte for WritePushPromise fails.
func TestFramer_WritePushPromise_ErrorOnPadLenWrite(t *testing.T) {
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 3)
	if err == nil {
		t.Fatal("expected error on pad-length write, got nil")
	}
}

// TestFramer_WritePushPromise_ErrorOnPromisedIDWrite tests error when writing
// the promised stream ID fails.
func TestFramer_WritePushPromise_ErrorOnPromisedIDWrite(t *testing.T) {
	// No padding: allow header (9) only; promised-ID write fails.
	ew := &errWriter{n: FrameHeaderSize}
	fr := NewFramer(ew, nil)
	err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 0)
	if err == nil {
		t.Fatal("expected error on promised-ID write, got nil")
	}
}

// TestFramer_WritePushPromise_ErrorOnBlockWrite tests error when writing the
// header block fragment fails.
func TestFramer_WritePushPromise_ErrorOnBlockWrite(t *testing.T) {
	// Allow header (9) + promised-ID (4) = 13 bytes.
	ew := &errWriter{n: FrameHeaderSize + 4}
	fr := NewFramer(ew, nil)
	err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 0)
	if err == nil {
		t.Fatal("expected error on block write, got nil")
	}
}

// TestFramer_WritePushPromise_ErrorOnPaddingWrite tests error when writing the
// padding bytes of a PUSH_PROMISE fails.
func TestFramer_WritePushPromise_ErrorOnPaddingWrite(t *testing.T) {
	// With padding: header (9) + pad-len (1) + promised-ID (4) + block (1) = 15.
	ew := &errWriter{n: FrameHeaderSize + 1 + 4 + 1}
	fr := NewFramer(ew, nil)
	err := fr.WritePushPromise(1, 4, []byte{0x82}, true, 3)
	if err == nil {
		t.Fatal("expected error on padding write, got nil")
	}
}

// === dispatchPushPromise error paths ===

// TestFramer_dispatchPushPromise_TruncatedPayload sends a PUSH_PROMISE without
// padding but fewer than 4 bytes for promised-ID and expects ErrShortRead.
func TestFramer_dispatchPushPromise_TruncatedPayload(t *testing.T) {
	raw := frameBytes(3, FramePushPromise, FlagPushPromiseEndHeaders, 1, []byte{0x00, 0x00, 0x04})
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("err = %v, want ErrShortRead", err)
	}
}

// TestFramer_dispatchPushPromise_InvalidPadding sends a padded PUSH_PROMISE
// frame where the declared pad length exceeds the payload.
func TestFramer_dispatchPushPromise_InvalidPadding(t *testing.T) {
	// pad_length = 20, only 2 bytes remaining.
	payload := []byte{0x14, 0x82}
	raw := frameBytes(uint32(len(payload)), FramePushPromise,
		FlagPushPromisePadded|FlagPushPromiseEndHeaders, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if err == nil {
		t.Fatal("expected padding error, got nil")
	}
}

// === WriteSettings error path ===

// TestFramer_WriteSettings_ErrorOnWrite verifies that WriteSettings propagates
// write errors.
func TestFramer_WriteSettings_ErrorOnWrite(t *testing.T) {
	fr := NewFramer(alwaysErrWriter{}, nil)
	s := SettingsParams{N: 1}
	s.Pairs[0] = SettingPair{ID: SettingMaxConcurrentStreams, Value: 100}
	err := fr.WriteSettings(s)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// === ReadFrame: no reader ===

// TestFramer_ReadFrame_NoReader verifies ReadFrame returns an error when the
// Framer has no reader configured.
func TestFramer_ReadFrame_NoReader(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if err == nil {
		t.Fatal("expected error for nil reader, got nil")
	}
}

// === ReadFrame: EOF on header read ===

// TestFramer_ReadFrame_EOFOnHeader verifies ReadFrame returns io.EOF when the
// reader is empty.
func TestFramer_ReadFrame_EOFOnHeader(t *testing.T) {
	fr := NewFramer(nil, bytes.NewReader([]byte{}))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}

// TestFramer_dispatchWindowUpdate_ZeroIncrement sends a WINDOW_UPDATE with
// zero increment value and expects ErrZeroIncrement.
func TestFramer_dispatchWindowUpdate_ZeroIncrement(t *testing.T) {
	// Valid 4-byte payload with increment = 0.
	payload := []byte{0x00, 0x00, 0x00, 0x00}
	raw := frameBytes(4, FrameWindowUpdate, 0, 1, payload)
	fr := NewFramer(nil, bytes.NewReader(raw))
	_, err := fr.ReadFrame(context.Background(), &recordingHandler{})
	if !errors.Is(err, ErrZeroIncrement) {
		t.Fatalf("err = %v, want ErrZeroIncrement", err)
	}
}
