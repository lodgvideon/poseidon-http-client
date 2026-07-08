package http3

import (
	"bytes"
	"testing"
)

func TestReadStreamType(t *testing.T) {
	for _, typ := range []uint64{StreamTypeControl, StreamTypePush, StreamTypeQPACKEncoder, StreamTypeQPACKDecoder} {
		enc := appendV(nil, typ)
		got, n, err := ReadStreamType(enc)
		if err != nil || got != typ || n != len(enc) {
			t.Fatalf("ReadStreamType(%#x) = (%#x,%d,%v)", typ, got, n, err)
		}
	}
	if _, _, err := ReadStreamType(nil); err != ErrNeedMore {
		t.Fatalf("empty: err = %v, want ErrNeedMore", err)
	}
	// A 2-byte varint type with only its first byte present.
	if _, _, err := ReadStreamType([]byte{0x40}); err != ErrNeedMore {
		t.Fatalf("truncated: err = %v, want ErrNeedMore", err)
	}
}

func TestFrameReader_MultipleFrames(t *testing.T) {
	var stream []byte
	stream = AppendData(stream, []byte("hello"))
	stream = AppendHeaders(stream, []byte{0x00, 0x00, 0xc1})

	var r FrameReader
	r.Feed(stream)

	typ, payload, err := r.ReadFrame()
	if err != nil || typ != FrameData || !bytes.Equal(payload, []byte("hello")) {
		t.Fatalf("frame 1 = (%#x,%q,%v)", typ, payload, err)
	}
	typ, payload, err = r.ReadFrame()
	if err != nil || typ != FrameHeaders || !bytes.Equal(payload, []byte{0x00, 0x00, 0xc1}) {
		t.Fatalf("frame 2 = (%#x,%x,%v)", typ, payload, err)
	}
	if _, _, err := r.ReadFrame(); err != ErrNeedMore {
		t.Fatalf("drained: err = %v, want ErrNeedMore", err)
	}
}

// TestFrameReader_SplitAcrossFeeds feeds a single frame one byte at a time,
// verifying the reader signals ErrNeedMore until the frame is complete — the
// case that arises when a frame spans multiple QUIC STREAM frames.
func TestFrameReader_SplitAcrossFeeds(t *testing.T) {
	frame := AppendData(nil, []byte("streamed body"))
	var r FrameReader
	for i := 0; i < len(frame)-1; i++ {
		r.Feed(frame[i : i+1])
		if _, _, err := r.ReadFrame(); err != ErrNeedMore {
			t.Fatalf("after %d/%d bytes: err = %v, want ErrNeedMore", i+1, len(frame), err)
		}
	}
	r.Feed(frame[len(frame)-1:])
	typ, payload, err := r.ReadFrame()
	if err != nil || typ != FrameData || !bytes.Equal(payload, []byte("streamed body")) {
		t.Fatalf("completed = (%#x,%q,%v)", typ, payload, err)
	}
}

// TestFrameReader_HugeLength ensures a frame header announcing an enormous
// length does not overflow or panic — it stays ErrNeedMore (a real payload
// would never arrive; bounding inbound data is receive-side flow control's job).
func TestFrameReader_HugeLength(t *testing.T) {
	hdr := AppendFrameHeader(nil, FrameData, 1<<61) // ~2 EiB, no payload
	var r FrameReader
	r.Feed(hdr)
	if _, _, err := r.ReadFrame(); err != ErrNeedMore {
		t.Fatalf("huge length: err = %v, want ErrNeedMore", err)
	}
}

// TestConformance_RFC9114_Sec62_ControlStream builds a client control stream
// (stream-type 0x00 + a first SETTINGS frame) and reads it back the way a peer
// would: peel the stream type, then the first frame must be SETTINGS
// (RFC 9114 §6.2, §6.2.1).
func TestConformance_RFC9114_Sec62_ControlStream(t *testing.T) {
	settings := []Setting{{SettingQPACKMaxTableCapacity, 0}, {SettingMaxFieldSectionSize, 1 << 16}}
	stream := AppendClientControlStream(nil, settings)

	typ, n, err := ReadStreamType(stream)
	if err != nil {
		t.Fatal(err)
	}
	if typ != StreamTypeControl {
		t.Fatalf("stream type = %#x, want control 0x00", typ)
	}

	var r FrameReader
	r.Feed(stream[n:])
	ftyp, payload, err := r.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if ftyp != FrameSettings {
		t.Fatalf("first control frame = %#x, want SETTINGS (§6.2.1)", ftyp)
	}
	got, err := ParseSettings(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(settings) {
		t.Fatalf("got %d settings, want %d", len(got), len(settings))
	}
	for i, s := range settings {
		if got[i] != s {
			t.Fatalf("setting %d = %+v, want %+v", i, got[i], s)
		}
	}
}

func BenchmarkReadStreamType(b *testing.B) {
	enc := appendV(nil, StreamTypeQPACKEncoder)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = ReadStreamType(enc)
	}
}
