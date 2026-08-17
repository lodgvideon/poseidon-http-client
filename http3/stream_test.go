package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadStreamType(t *testing.T) {
	types := []uint64{StreamTypeControl, StreamTypePush, StreamTypeQPACKEncoder, StreamTypeQPACKDecoder}
	encoded := make([][]byte, len(types))
	for i, typ := range types {
		encoded[i] = appendV(nil, typ)
	}

	type result struct {
		typ uint64
		n   int
		err error
	}
	got := make([]result, len(types))
	for i, enc := range encoded {
		got[i].typ, got[i].n, got[i].err = ReadStreamType(enc)
	}
	_, _, emptyErr := ReadStreamType(nil)
	// A 2-byte varint type with only its first byte present.
	_, _, truncatedErr := ReadStreamType([]byte{0x40})

	for i, typ := range types {
		require.NoErrorf(t, got[i].err, "ReadStreamType(%#x) = (%#x,%d,%v)",
			typ, got[i].typ, got[i].n, got[i].err)
		assert.Equalf(t, typ, got[i].typ, "ReadStreamType(%#x) = (%#x,%d,%v)",
			typ, got[i].typ, got[i].n, got[i].err)
		assert.Equalf(t, len(encoded[i]), got[i].n, "ReadStreamType(%#x) = (%#x,%d,%v): the "+
			"caller resumes at the wrong offset if the consumed count is off",
			typ, got[i].typ, got[i].n, got[i].err)
	}
	assert.Equalf(t, ErrNeedMore, emptyErr, "empty: err = %v, want ErrNeedMore", emptyErr)
	assert.Equalf(t, ErrNeedMore, truncatedErr, "truncated: err = %v, want ErrNeedMore — a "+
		"half-arrived varint must be waited for, not read as a type", truncatedErr)
}

func TestFrameReader_MultipleFrames(t *testing.T) {
	var stream []byte
	stream = AppendData(stream, []byte("hello"))
	stream = AppendHeaders(stream, []byte{0x00, 0x00, 0xc1})
	var r FrameReader
	r.Feed(stream)

	// No Feed happens between these reads, so the earlier payloads stay valid.
	typ1, payload1, err1 := r.ReadFrame()
	typ2, payload2, err2 := r.ReadFrame()
	_, _, drainedErr := r.ReadFrame()

	require.NoErrorf(t, err1, "frame 1 = (%#x,%q,%v)", typ1, payload1, err1)
	assert.Equalf(t, FrameData, typ1, "frame 1 = (%#x,%q,%v)", typ1, payload1, err1)
	assert.Equalf(t, []byte("hello"), payload1, "frame 1 = (%#x,%q,%v)", typ1, payload1, err1)
	require.NoErrorf(t, err2, "frame 2 = (%#x,%x,%v)", typ2, payload2, err2)
	assert.Equalf(t, FrameHeaders, typ2, "frame 2 = (%#x,%x,%v) — the reader did not advance "+
		"past the first frame", typ2, payload2, err2)
	assert.Equalf(t, []byte{0x00, 0x00, 0xc1}, payload2, "frame 2 = (%#x,%x,%v)", typ2, payload2, err2)
	assert.Equalf(t, ErrNeedMore, drainedErr, "drained: err = %v, want ErrNeedMore", drainedErr)
}

// TestFrameReader_SplitAcrossFeeds feeds a single frame one byte at a time,
// verifying the reader signals ErrNeedMore until the frame is complete — the
// case that arises when a frame spans multiple QUIC STREAM frames.
func TestFrameReader_SplitAcrossFeeds(t *testing.T) {
	frame := AppendData(nil, []byte("streamed body"))
	var r FrameReader

	partial := make([]error, 0, len(frame)-1)
	for i := 0; i < len(frame)-1; i++ {
		r.Feed(frame[i : i+1])
		_, _, err := r.ReadFrame()
		partial = append(partial, err)
	}
	r.Feed(frame[len(frame)-1:])
	typ, payload, err := r.ReadFrame()

	for i, perr := range partial {
		require.Equalf(t, ErrNeedMore, perr, "after %d/%d bytes: err = %v, want ErrNeedMore — "+
			"handing out a half-arrived frame gives the caller a truncated field section",
			i+1, len(frame), perr)
	}
	require.NoErrorf(t, err, "completed = (%#x,%q,%v)", typ, payload, err)
	assert.Equalf(t, FrameData, typ, "completed = (%#x,%q,%v)", typ, payload, err)
	assert.Equalf(t, []byte("streamed body"), payload, "completed = (%#x,%q,%v)", typ, payload, err)
}

// TestFrameReader_HugeLength ensures a frame header announcing an enormous
// length does not overflow or panic — it stays ErrNeedMore (a real payload
// would never arrive; bounding inbound data is receive-side flow control's job).
func TestFrameReader_HugeLength(t *testing.T) {
	hdr := AppendFrameHeader(nil, FrameData, 1<<61) // ~2 EiB, no payload
	var r FrameReader
	r.Feed(hdr)

	_, _, err := r.ReadFrame()

	assert.Equalf(t, ErrNeedMore, err, "huge length: err = %v, want ErrNeedMore", err)
}

// TestConformance_RFC9114_Sec62_ControlStream builds a client control stream
// (stream-type 0x00 + a first SETTINGS frame) and reads it back the way a peer
// would: peel the stream type, then the first frame must be SETTINGS
// (RFC 9114 §6.2, §6.2.1).
func TestConformance_RFC9114_Sec62_ControlStream(t *testing.T) {
	settings := []Setting{{SettingQPACKMaxTableCapacity, 0}, {SettingMaxFieldSectionSize, 1 << 16}}
	stream := AppendClientControlStream(nil, settings)

	typ, n, err := ReadStreamType(stream)
	require.NoError(t, err, "the leading stream-type varint the client wrote must parse")
	var r FrameReader
	r.Feed(stream[n:])
	ftyp, payload, ferr := r.ReadFrame()
	require.NoError(t, ferr, "the first frame on the control stream must parse")
	got, perr := ParseSettings(payload)

	require.NoError(t, perr, "the SETTINGS payload the client wrote must parse")
	assert.Equalf(t, StreamTypeControl, typ, "stream type = %#x, want control 0x00", typ)
	assert.Equalf(t, FrameSettings, ftyp, "first control frame = %#x, want SETTINGS (§6.2.1)", ftyp)
	require.Lenf(t, got, len(settings), "got %d settings, want %d", len(got), len(settings))
	for i, s := range settings {
		assert.Equalf(t, s, got[i], "setting %d = %+v, want %+v", i, got[i], s)
	}
}

func BenchmarkReadStreamType(b *testing.B) {
	enc := appendV(nil, StreamTypeQPACKEncoder)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = ReadStreamType(enc)
	}
}
