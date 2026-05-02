package frame

import (
	"bytes"
	"testing"
)

func TestReadFrameHeader_Sample(t *testing.T) {
	// length=10, type=0x1 (HEADERS), flags=0x05 (END_STREAM|END_HEADERS), stream=1
	raw := []byte{0x00, 0x00, 0x0a, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01}
	h, err := ReadFrameHeader(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if h.Length != 10 || h.Type != FrameHeaders || h.Flags != 0x05 || h.StreamID != 1 {
		t.Fatalf("got %+v", h)
	}
}

func TestReadFrameHeader_RBitMasked(t *testing.T) {
	raw := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x01}
	h, err := ReadFrameHeader(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if h.StreamID != 1 {
		t.Fatalf("StreamID = %d, want 1 (R-bit must be masked)", h.StreamID)
	}
}

func TestReadFrameHeader_Short(t *testing.T) {
	_, err := ReadFrameHeader([]byte{0x00, 0x00})
	if err == nil {
		t.Fatalf("want error on short header")
	}
}

func TestWriteFrameHeader(t *testing.T) {
	h := FrameHeader{Length: 0x1234, Type: FrameSettings, Flags: 0x01, StreamID: 0}
	var buf [9]byte
	WriteFrameHeader(buf[:], h)
	want := []byte{0x00, 0x12, 0x34, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(buf[:], want) {
		t.Fatalf("got %x, want %x", buf, want)
	}
}
