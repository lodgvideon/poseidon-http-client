package frame

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

// recorder keeps every frame reported to it. Params is copied, not kept: the
// Tracer contract says the pointer aliases the Framer's own scratch.
type recorder struct{ got []trace.FrameInfo }

func (r *recorder) TraceFrame(info trace.FrameInfo) {
	if info.Params != nil {
		cp := *info.Params
		info.Params = &cp
	}
	r.got = append(r.got, info)
}

func (r *recorder) only(t *testing.T) trace.FrameInfo {
	t.Helper()
	if len(r.got) != 1 {
		t.Fatalf("traced %d frames, want exactly 1: %+v", len(r.got), r.got)
	}
	return r.got[0]
}

// discardTracer is non-nil and does nothing. It is what the allocation
// benchmarks measure: a nil check is obviously free, and the claim that needs
// pinning is that building a FrameInfo and passing it through an interface
// method costs nothing either.
type discardTracer struct{}

func (discardTracer) TraceFrame(trace.FrameInfo) {}

func tracedFramer(w *bytes.Buffer, r *bytes.Reader, tr trace.Tracer) *Framer {
	var rd interface {
		Read([]byte) (int, error)
	}
	if r != nil {
		rd = r
	}
	f := NewFramer(w, rd)
	f.SetTracer(tr)
	return f
}

func TestFramer_TraceOut_ReportsEveryFrameType(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Framer) error
		want  trace.FrameInfo
	}{
		{
			name:  "DATA with END_STREAM",
			write: func(f *Framer) error { return f.WriteData(3, true, []byte("hello")) },
			want: trace.FrameInfo{
				TypeName: "DATA", Type: 0x0, Flags: 0x1, FlagNames: "END_STREAM",
				StreamID: 3, Length: 5,
			},
		},
		{
			name: "HEADERS fast path",
			write: func(f *Framer) error {
				return f.WriteHeaders(WriteHeadersParams{
					StreamID: 5, BlockFragment: []byte{0x82}, EndStream: true, EndHeaders: true,
				})
			},
			want: trace.FrameInfo{
				TypeName: "HEADERS", Type: 0x1, Flags: 0x5, FlagNames: "END_STREAM|END_HEADERS",
				StreamID: 5, Length: 1,
			},
		},
		{
			name: "HEADERS slow path is padded",
			write: func(f *Framer) error {
				return f.WriteHeaders(WriteHeadersParams{
					StreamID: 7, BlockFragment: []byte{0x82}, EndHeaders: true, PadLength: 4,
				})
			},
			want: trace.FrameInfo{
				TypeName: "HEADERS", Type: 0x1, Flags: 0xc, FlagNames: "END_HEADERS|PADDED",
				StreamID: 7, Length: 6,
			},
		},
		{
			name:  "RST_STREAM carries its code",
			write: func(f *Framer) error { return f.WriteRSTStream(9, ErrCodeCancel) },
			want: trace.FrameInfo{
				TypeName: "RST_STREAM", Type: 0x3, StreamID: 9, Length: 4,
				Detail: trace.DetailErrCode, ErrCode: 0x8, ErrCodeName: "CANCEL",
			},
		},
		{
			name:  "WINDOW_UPDATE carries its increment",
			write: func(f *Framer) error { return f.WriteWindowUpdate(0, 32768) },
			want: trace.FrameInfo{
				TypeName: "WINDOW_UPDATE", Type: 0x8, StreamID: 0, Length: 4,
				Detail: trace.DetailIncrement, Increment: 32768,
			},
		},
		{
			name:  "PING ACK",
			write: func(f *Framer) error { return f.WritePing(true, [8]byte{1}) },
			want: trace.FrameInfo{
				TypeName: "PING", Type: 0x6, Flags: 0x1, FlagNames: "ACK", Length: 8,
			},
		},
		{
			name:  "SETTINGS ACK reports no parameters",
			write: func(f *Framer) error { return f.WriteSettingsAck() },
			want: trace.FrameInfo{
				TypeName: "SETTINGS", Type: 0x4, Flags: 0x1, FlagNames: "ACK", Length: 0,
			},
		},
		{
			name: "PUSH_PROMISE carries the promised id",
			write: func(f *Framer) error {
				return f.WritePushPromise(3, 4, []byte{0x82}, true, 0)
			},
			want: trace.FrameInfo{
				TypeName: "PUSH_PROMISE", Type: 0x5, Flags: 0x4, FlagNames: "END_HEADERS",
				StreamID: 3, Length: 5,
				Detail: trace.DetailPromisedID, PromisedID: 4,
			},
		},
		{
			name: "padded PUSH_PROMISE still carries the promised id",
			write: func(f *Framer) error {
				return f.WritePushPromise(3, 6, []byte{0x82}, true, 4)
			},
			want: trace.FrameInfo{
				TypeName: "PUSH_PROMISE", Type: 0x5, Flags: 0xc, FlagNames: "END_HEADERS|PADDED",
				StreamID: 3, Length: 10,
				Detail: trace.DetailPromisedID, PromisedID: 6,
			},
		},
		{
			name: "GOAWAY carries last-stream and code",
			write: func(f *Framer) error {
				return f.WriteGoAway(5, ErrCodeNoError, nil)
			},
			want: trace.FrameInfo{
				TypeName: "GOAWAY", Type: 0x7, Length: 8,
				Detail:  trace.DetailLastStreamID | trace.DetailErrCode,
				ErrCode: 0x0, ErrCodeName: "NO_ERROR", LastStreamID: 5,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			rec := &recorder{}
			f := tracedFramer(&buf, nil, rec)
			if err := tc.write(f); err != nil {
				t.Fatalf("write: %v", err)
			}
			got := rec.only(t)
			tc.want.Proto, tc.want.Dir = trace.ProtoH2, trace.DirOut
			assertFrameInfo(t, got, tc.want)
		})
	}
}

// TestFramer_TraceOut_SettingsParams pins the one detail field that is not a
// scalar: a SETTINGS frame's parameters, reported through Framer-owned storage.
func TestFramer_TraceOut_SettingsParams(t *testing.T) {
	var buf bytes.Buffer
	rec := &recorder{}
	f := tracedFramer(&buf, nil, rec)

	var s SettingsParams
	s.set(SettingInitialWindowSize, 1<<20)
	s.set(SettingMaxFrameSize, 16384)
	s.set(SettingID(0xff), 7) // outside the registry
	if err := f.WriteSettings(s); err != nil {
		t.Fatalf("WriteSettings: %v", err)
	}

	got := rec.only(t)
	if !got.Detail.Has(trace.DetailParams) || got.Params == nil {
		t.Fatalf("SETTINGS reported without parameters: %+v", got)
	}
	want := []trace.Param{
		{ID: 0x4, Name: "INITIAL_WINDOW_SIZE", Value: 1 << 20},
		{ID: 0x5, Name: "MAX_FRAME_SIZE", Value: 16384},
		{ID: 0xff, Name: "", Value: 7}, // unregistered: number, not the word UNKNOWN
	}
	if gotP := got.Params.All(); len(gotP) != len(want) {
		t.Fatalf("params = %+v, want %+v", gotP, want)
	} else {
		for i := range want {
			if gotP[i] != want[i] {
				t.Errorf("param %d = %+v, want %+v", i, gotP[i], want[i])
			}
		}
	}
}

func TestFramer_TraceIn_ReportsReceivedFrames(t *testing.T) {
	// Build a wire stream with a peer-side Framer, then read it back traced.
	var wire bytes.Buffer
	peer := NewFramer(&wire, nil)
	if err := peer.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x88}, EndHeaders: true,
	}); err != nil {
		t.Fatalf("peer HEADERS: %v", err)
	}
	if err := peer.WriteData(1, true, []byte("body")); err != nil {
		t.Fatalf("peer DATA: %v", err)
	}
	if err := peer.WriteGoAway(1, ErrCodeEnhanceYourCalm, []byte("slow down")); err != nil {
		t.Fatalf("peer GOAWAY: %v", err)
	}

	rec := &recorder{}
	f := tracedFramer(nil, bytes.NewReader(wire.Bytes()), rec)
	h := dropHandler{}
	for range 3 {
		if _, err := f.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
	}

	if len(rec.got) != 3 {
		t.Fatalf("traced %d frames, want 3", len(rec.got))
	}
	for i, got := range rec.got {
		if got.Dir != trace.DirIn || got.Proto != trace.ProtoH2 {
			t.Errorf("frame %d: dir/proto = %v/%v, want in/h2", i, got.Dir, got.Proto)
		}
	}
	if got := rec.got[0]; got.TypeName != "HEADERS" || got.StreamID != 1 {
		t.Errorf("frame 0 = %+v, want HEADERS on stream 1", got)
	}
	if got := rec.got[1]; got.TypeName != "DATA" || got.FlagNames != "END_STREAM" {
		t.Errorf("frame 1 = %+v, want DATA END_STREAM", got)
	}
	goaway := rec.got[2]
	if goaway.TypeName != "GOAWAY" || goaway.ErrCodeName != "ENHANCE_YOUR_CALM" || goaway.LastStreamID != 1 {
		t.Errorf("frame 2 = %+v, want GOAWAY ENHANCE_YOUR_CALM last=1", goaway)
	}
	if !goaway.Detail.Has(trace.DetailErrCode | trace.DetailLastStreamID) {
		t.Errorf("GOAWAY detail = %b, want code and last-stream set", goaway.Detail)
	}
}

// TestFramer_TraceIn_ReportsFrameThatFailsDispatch is the reason the emit sits
// ahead of validation. A frame log whose last line is missing exactly because
// the frame was malformed omits the one event worth seeing.
func TestFramer_TraceIn_ReportsFrameThatFailsDispatch(t *testing.T) {
	// RST_STREAM with a 5-byte payload: §6.4 makes it a FRAME_SIZE_ERROR.
	wire := []byte{0, 0, 5, byte(FrameRSTStream), 0, 0, 0, 0, 1, 0, 0, 0, 8, 0}

	rec := &recorder{}
	f := tracedFramer(nil, bytes.NewReader(wire), rec)
	if _, err := f.ReadFrame(context.Background(), dropHandler{}); err == nil {
		t.Fatal("ReadFrame accepted a 5-byte RST_STREAM")
	}
	got := rec.only(t)
	if got.TypeName != "RST_STREAM" || got.Length != 5 {
		t.Errorf("traced %+v, want the malformed RST_STREAM as it arrived", got)
	}
	if got.ErrCodeName != "CANCEL" {
		t.Errorf("code = %q, want CANCEL decoded from the leading four bytes", got.ErrCodeName)
	}
}

// TestFramer_TraceIn_UnknownTypeIsReported: §5.5 obliges us to ignore frame
// types we do not know, which is precisely when a frame log has to say
// something rather than nothing.
func TestFramer_TraceIn_UnknownTypeIsReported(t *testing.T) {
	wire := []byte{0, 0, 2, 0x1f, 0, 0, 0, 0, 0, 0xaa, 0xbb}

	rec := &recorder{}
	f := tracedFramer(nil, bytes.NewReader(wire), rec)
	if _, err := f.ReadFrame(context.Background(), dropHandler{}); err != nil {
		t.Fatalf("ReadFrame rejected an unknown type: %v", err)
	}
	got := rec.only(t)
	if got.Type != 0x1f || got.TypeName != trace.UnknownName || got.Length != 2 {
		t.Errorf("traced %+v, want type 0x1f reported as %s", got, trace.UnknownName)
	}
}

func TestFramer_SetTracer_NilTurnsTracingOff(t *testing.T) {
	var buf bytes.Buffer
	rec := &recorder{}
	f := tracedFramer(&buf, nil, rec)
	if err := f.WritePing(false, [8]byte{}); err != nil {
		t.Fatalf("WritePing: %v", err)
	}
	f.SetTracer(nil)
	if err := f.WritePing(false, [8]byte{}); err != nil {
		t.Fatalf("WritePing: %v", err)
	}
	if len(rec.got) != 1 {
		t.Fatalf("traced %d frames after SetTracer(nil), want 1", len(rec.got))
	}
}

// TestFramer_TraceOut_NotReportedWhenWriteFails: the event says a frame went
// out, so it must not be emitted for one that did not.
func TestFramer_TraceOut_NotReportedWhenWriteFails(t *testing.T) {
	rec := &recorder{}
	f := NewFramer(errWriterAt{0}, nil)
	f.SetTracer(rec)
	if err := f.WritePing(false, [8]byte{}); err == nil {
		t.Fatal("WritePing succeeded on a failing writer")
	}
	if len(rec.got) != 0 {
		t.Fatalf("traced %d frames for a write that failed: %+v", len(rec.got), rec.got)
	}
}

// errWriterAt fails after n bytes.
type errWriterAt struct{ n int }

func (e errWriterAt) Write(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, errShortWrite
	}
	return len(p), nil
}

var errShortWrite = errShortWriteType{}

type errShortWriteType struct{}

func (errShortWriteType) Error() string { return "short write" }

func assertFrameInfo(t *testing.T, got, want trace.FrameInfo) {
	t.Helper()
	got.Params = nil // compared separately where it matters
	if got != want {
		t.Errorf("frame info mismatch\n got %+v\nwant %+v", got, want)
	}
}

// === Naming ===

func TestFrameType_String(t *testing.T) {
	for typ, want := range map[FrameType]string{
		FrameData: "DATA", FrameHeaders: "HEADERS", FramePriority: "PRIORITY",
		FrameRSTStream: "RST_STREAM", FrameSettings: "SETTINGS",
		FramePushPromise: "PUSH_PROMISE", FramePing: "PING", FrameGoAway: "GOAWAY",
		FrameWindowUpdate: "WINDOW_UPDATE", FrameContinuation: "CONTINUATION",
		FrameAltSvc: "ALTSVC", FrameOrigin: "ORIGIN",
		FrameType(0xfe): trace.UnknownName,
	} {
		if got := typ.String(); got != want {
			t.Errorf("FrameType(%#x).String() = %q, want %q", uint8(typ), got, want)
		}
	}
}

func TestErrCode_String(t *testing.T) {
	for code, want := range map[ErrCode]string{
		ErrCodeNoError: "NO_ERROR", ErrCodeProtocolError: "PROTOCOL_ERROR",
		ErrCodeInternalError: "INTERNAL_ERROR", ErrCodeFlowControlError: "FLOW_CONTROL_ERROR",
		ErrCodeSettingsTimeout: "SETTINGS_TIMEOUT", ErrCodeStreamClosed: "STREAM_CLOSED",
		ErrCodeFrameSizeError: "FRAME_SIZE_ERROR", ErrCodeRefusedStream: "REFUSED_STREAM",
		ErrCodeCancel: "CANCEL", ErrCodeCompressionError: "COMPRESSION_ERROR",
		ErrCodeConnectError: "CONNECT_ERROR", ErrCodeEnhanceYourCalm: "ENHANCE_YOUR_CALM",
		ErrCodeInadequateSecurity: "INADEQUATE_SECURITY", ErrCodeHTTP11Required: "HTTP_1_1_REQUIRED",
		ErrCode(0xdead): trace.UnknownName,
	} {
		if got := code.String(); got != want {
			t.Errorf("ErrCode(%#x).String() = %q, want %q", uint32(code), got, want)
		}
	}
}

func TestSettingID_String(t *testing.T) {
	for id, want := range map[SettingID]string{
		SettingHeaderTableSize: "HEADER_TABLE_SIZE", SettingEnablePush: "ENABLE_PUSH",
		SettingMaxConcurrentStreams:  "MAX_CONCURRENT_STREAMS",
		SettingInitialWindowSize:     "INITIAL_WINDOW_SIZE",
		SettingMaxFrameSize:          "MAX_FRAME_SIZE",
		SettingMaxHeaderListSize:     "MAX_HEADER_LIST_SIZE",
		SettingEnableConnectProtocol: "ENABLE_CONNECT_PROTOCOL",
		SettingID(0x99):              trace.UnknownName,
	} {
		if got := id.String(); got != want {
			t.Errorf("SettingID(%#x).String() = %q, want %q", uint16(id), got, want)
		}
	}
}

// TestFrameType_FlagNames_MatchBits rebuilds every arm of the hand-written
// tables in names.go from the individual bit names, for all 256 flag values on
// every frame type. The tables are written out longhand so that each result is
// a constant string the emit path can return without allocating; this is what
// keeps a typo in one of the twenty-eight arms from being invisible.
func TestFrameType_FlagNames_MatchBits(t *testing.T) {
	// The flags each type defines, in the order FlagNames joins them.
	defined := map[FrameType][]struct {
		bit  Flags
		name string
	}{
		FrameData: {{FlagDataEndStream, "END_STREAM"}, {FlagDataPadded, "PADDED"}},
		FrameHeaders: {
			{FlagHeadersEndStream, "END_STREAM"}, {FlagHeadersEndHeaders, "END_HEADERS"},
			{FlagHeadersPadded, "PADDED"}, {FlagHeadersPriority, "PRIORITY"},
		},
		FrameSettings:     {{FlagSettingsAck, "ACK"}},
		FramePing:         {{FlagPingAck, "ACK"}},
		FramePushPromise:  {{FlagPushPromiseEndHeaders, "END_HEADERS"}, {FlagPushPromisePadded, "PADDED"}},
		FrameContinuation: {{FlagContinuationEndHeaders, "END_HEADERS"}},
		// Types with no flags of their own must name nothing, whatever bits arrive.
		FramePriority:     nil,
		FrameRSTStream:    nil,
		FrameGoAway:       nil,
		FrameWindowUpdate: nil,
		FrameAltSvc:       nil,
		FrameOrigin:       nil,
		FrameType(0xfe):   nil,
	}

	for typ, bits := range defined {
		for v := 0; v < 256; v++ {
			f := Flags(v)
			var parts []string
			for _, b := range bits {
				if f&b.bit != 0 {
					parts = append(parts, b.name)
				}
			}
			want := strings.Join(parts, "|")
			if got := typ.FlagNames(f); got != want {
				t.Errorf("%s.FlagNames(%#x) = %q, want %q", typ, uint8(f), got, want)
			}
		}
	}
}

// === Allocation gate ===
//
// The bench-gate fails the build on any Benchmark line in this package
// reporting a non-zero B/op or allocs/op, which is what makes these two the
// enforcement of "zero cost when off is a hard gate, not an aspiration" — and,
// with a non-nil tracer installed, of the stronger claim that building and
// passing a FrameInfo is free too.

func BenchmarkFramer_WriteData_Traced(b *testing.B) {
	f := NewFramer(discardWriter{}, nil)
	f.SetTracer(discardTracer{})
	payload := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := f.WriteData(1, false, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFramer_ReadFrame_Traced(b *testing.B) {
	var wire bytes.Buffer
	peer := NewFramer(&wire, nil)
	if err := peer.WriteData(1, false, make([]byte, 1024)); err != nil {
		b.Fatal(err)
	}
	frame := wire.Bytes()

	r := &repeatReader{buf: frame}
	f := NewFramer(nil, r)
	f.SetTracer(discardTracer{})
	h := dropHandler{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := f.ReadFrame(context.Background(), h); err != nil {
			b.Fatal(err)
		}
	}
}

// repeatReader replays one frame forever.
type repeatReader struct {
	buf []byte
	off int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := copy(p, r.buf[r.off:])
	r.off += n
	if r.off == len(r.buf) {
		r.off = 0
	}
	return n, nil
}

// TestFramer_TraceIn_ReportsOversizedFrame: the frame is refused before its
// payload is read, so it reaches the tracer without one. ErrFrameTooLarge names
// neither the type nor the size, which is the gap this closes.
func TestFramer_TraceIn_ReportsOversizedFrame(t *testing.T) {
	// A SETTINGS header claiming 1 MiB, against the default 16384 read limit.
	wire := []byte{0x10, 0x00, 0x00, byte(FrameSettings), 0, 0, 0, 0, 0}

	rec := &recorder{}
	f := tracedFramer(nil, bytes.NewReader(wire), rec)
	if _, err := f.ReadFrame(context.Background(), dropHandler{}); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadFrame err = %v, want ErrFrameTooLarge", err)
	}
	got := rec.only(t)
	if got.TypeName != "SETTINGS" || got.Length != 0x100000 || got.Dir != trace.DirIn {
		t.Errorf("traced %+v, want the inbound oversized SETTINGS with its claimed length", got)
	}
	if got.Detail != 0 {
		t.Errorf("detail = %b, want none — no payload was read", got.Detail)
	}
}
