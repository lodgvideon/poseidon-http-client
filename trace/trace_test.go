package trace

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestParseSpec(t *testing.T) {
	cases := []struct {
		in   string
		want Spec
	}{
		{"", Spec{}},
		{"   ", Spec{}},
		{"frames", Spec{Frames: true}},
		{"frame", Spec{Frames: true}},
		{"1", Spec{Frames: true}},
		{"true", Spec{Frames: true}},
		{"on", Spec{Frames: true}},
		{"FRAMES", Spec{Frames: true}},
		{" frames , flow ", Spec{Frames: true, Flow: true}},
		{"frames,streams,flow", Spec{Frames: true, Streams: true, Flow: true}},
		{"all", Spec{Frames: true, Streams: true, Flow: true}},
		{"payload", Spec{Frames: true, PayloadBytes: defaultPayloadBytes}},
		{"payload=16", Spec{Frames: true, PayloadBytes: 16}},
		{"payload=0", Spec{Frames: true, PayloadBytes: 0}},
		// Empty tokens from a trailing or doubled comma are not typos worth
		// failing over.
		{"frames,,", Spec{Frames: true}},
	}
	for _, c := range cases {
		got, err := ParseSpec(c.in)
		if err != nil {
			t.Errorf("ParseSpec(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSpec(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

// TestParseSpec_UnknownIsAnError pins the deliberate choice: a typo in a debug
// switch must not silently turn nothing on. Discovering that an hour later is
// the worst failure this feature has.
func TestParseSpec_UnknownIsAnError(t *testing.T) {
	for _, in := range []string{"framez", "frames,stremas", "yes", "payload=-1", "payload=lots"} {
		if _, err := ParseSpec(in); err == nil {
			t.Errorf("ParseSpec(%q) = nil error, want a complaint", in)
		} else if !strings.Contains(err.Error(), EnvVar) {
			t.Errorf("ParseSpec(%q) error %q does not name %s", in, err, EnvVar)
		}
	}
}

// TestParseSpec_AllExcludesPayload: reaching for the loudest switch is not the
// same as asking to put `authorization` headers and request bodies in a file
// you are about to attach to an issue.
func TestParseSpec_AllExcludesPayload(t *testing.T) {
	s, err := ParseSpec("all")
	if err != nil {
		t.Fatal(err)
	}
	if s.PayloadBytes != 0 {
		t.Fatalf("all enabled payload dumping (%d bytes); it must be opt-in by name", s.PayloadBytes)
	}
}

func TestSpec_Pending(t *testing.T) {
	s, err := ParseSpec("all")
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(s.Pending(), ",")
	if want := "streams,flow"; got != want {
		t.Errorf("Pending() = %q, want %q", got, want)
	}
	if p := (Spec{Frames: true}).Pending(); len(p) != 0 {
		t.Errorf("Pending() = %v for a frames-only spec, want empty", p)
	}
	if (Spec{}).Enabled() {
		t.Error("zero Spec reports Enabled")
	}
}

// newTestTracer builds a deterministic tracer: no timestamps, no background
// flusher, so the output is byte-comparable and appears when Flush says so.
func newTestTracer(w *bytes.Buffer, opts ...Option) *TextTracer {
	base := []Option{WithoutTimestamps(), WithFlushInterval(0)}
	return New(w, append(base, opts...)...)
}

func TestTextTracer_Lines(t *testing.T) {
	var buf bytes.Buffer
	tr := newTestTracer(&buf)

	settings := frame.SettingsParams{N: 2}
	settings.Pairs[0] = frame.SettingPair{ID: frame.SettingEnablePush, Value: 0}
	settings.Pairs[1] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 65535}

	events := []frame.FrameInfo{
		{Dir: frame.DirSend, Header: frame.FrameHeader{Type: frame.FrameSettings, Length: 12}, Settings: settings},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FrameSettings, Flags: frame.FlagSettingsAck}},
		{Dir: frame.DirSend, Header: frame.FrameHeader{
			Type: frame.FrameHeaders, Flags: frame.FlagHeadersEndStream | frame.FlagHeadersEndHeaders,
			StreamID: 1, Length: 54,
		}},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 1024}},
		{Dir: frame.DirSend, Header: frame.FrameHeader{Type: frame.FrameWindowUpdate, Length: 4}, WindowIncrement: 32768},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1, Length: 4}, ErrCode: frame.ErrCodeCancel},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FrameGoAway, Length: 8}, LastStreamID: 7, ErrCode: frame.ErrCodeEnhanceYourCalm},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FramePing, Flags: frame.FlagPingAck, Length: 8}, Ping: [8]byte{0xde, 0xad, 0xbe, 0xef}},
		{Dir: frame.DirRecv, Header: frame.FrameHeader{Type: frame.FramePushPromise, StreamID: 1, Length: 20}, PromisedID: 2},
	}
	for i := range events {
		tr.TraceFrame(&events[i])
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := strings.Join([]string{
		"-> SETTINGS stream=0 len=12 [SETTINGS_ENABLE_PUSH=0 SETTINGS_INITIAL_WINDOW_SIZE=65535]",
		"<- SETTINGS stream=0 len=0 flags=ACK",
		"-> HEADERS stream=1 len=54 flags=END_STREAM|END_HEADERS",
		"<- DATA stream=1 len=1024",
		"-> WINDOW_UPDATE stream=0 len=4 inc=32768",
		"<- RST_STREAM stream=1 len=4 code=CANCEL",
		"<- GOAWAY stream=0 len=8 last=7 code=ENHANCE_YOUR_CALM",
		"<- PING stream=0 len=8 flags=ACK data=deadbeef00000000",
		"<- PUSH_PROMISE stream=1 len=20 promised=2",
		"",
	}, "\n")
	if got := buf.String(); got != want {
		t.Errorf("output mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestTextTracer_PayloadOptIn(t *testing.T) {
	fi := frame.FrameInfo{
		Dir:     frame.DirRecv,
		Header:  frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 6},
		Payload: []byte("secret"),
	}

	// Default: the body never reaches the log.
	var off bytes.Buffer
	tr := newTestTracer(&off)
	tr.TraceFrame(&fi)
	_ = tr.Close()
	if strings.Contains(off.String(), "secret") || strings.Contains(off.String(), "payload=") {
		t.Fatalf("payload leaked into the default output: %q", off.String())
	}

	// Opt in, and it is hex and bounded.
	var on bytes.Buffer
	tr = newTestTracer(&on, WithPayload(3))
	tr.TraceFrame(&fi)
	_ = tr.Close()
	got := strings.TrimSpace(on.String())
	if want := "<- DATA stream=1 len=6 payload=736563+3"; got != want {
		t.Fatalf("payload output = %q, want %q", got, want)
	}
}

// TestTextTracer_NilSafe: FromEnv returns a nil *TextTracer when tracing is
// off, and `defer tr.Close()` must not need a guard for that to be safe.
func TestTextTracer_NilSafe(t *testing.T) {
	var tr *TextTracer
	tr.TraceFrame(&frame.FrameInfo{})
	if err := tr.Flush(); err != nil {
		t.Errorf("Flush on nil: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Errorf("Close on nil: %v", err)
	}
	if err := tr.Err(); err != nil {
		t.Errorf("Err on nil: %v", err)
	}
}

// TestTextTracer_TracerAvoidsTypedNil is the trap this whole helper exists for:
// assigning a nil *TextTracer straight into a frame.Tracer field produces a
// NON-nil interface, so every emit site's nil check passes and a build that
// asked for no tracing pays an interface call per frame.
func TestTextTracer_TracerAvoidsTypedNil(t *testing.T) {
	var tr *TextTracer
	if got := tr.Tracer(); got != nil {
		t.Fatal("nil *TextTracer.Tracer() produced a non-nil frame.Tracer")
	}
	// That assertion is the whole regression guard: a "simplification" of Tracer
	// to `return t` puts a nil *TextTracer inside a non-nil frame.Tracer, and
	// then every emit site's nil check passes on a build that asked for no
	// tracing. The naive assignment is not written out here to prove it —
	// go vet's nilness pass folds `frame.Tracer(tr) == nil` to a constant and
	// rejects the comparison, which is itself the language rule this guards.

	live := newTestTracer(&bytes.Buffer{})
	defer func() { _ = live.Close() }()
	if live.Tracer() == nil {
		t.Fatal("live tracer converted to a nil frame.Tracer")
	}
}

// TestTextTracer_Concurrent exercises the contract frame.Tracer states: one
// tracer, both directions, different goroutines. Meaningful under -race.
func TestTextTracer_Concurrent(t *testing.T) {
	var buf bytes.Buffer
	tr := newTestTracer(&buf)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			fi := frame.FrameInfo{
				Dir:             frame.Direction(g % 2),
				Header:          frame.FrameHeader{Type: frame.FrameWindowUpdate, StreamID: uint32(g), Length: 4},
				WindowIncrement: 1,
			}
			for i := 0; i < 200; i++ {
				tr.TraceFrame(&fi)
			}
		}(g)
	}
	wg.Wait()
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := strings.Count(buf.String(), "\n"); got != 800 {
		t.Fatalf("wrote %d lines, want 800", got)
	}
	// Interleaving must not tear a line in half.
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		if !strings.HasSuffix(line, "len=4 inc=1") {
			t.Fatalf("torn line: %q", line)
		}
	}
}

type errWriter struct{ err error }

func (e errWriter) Write([]byte) (int, error) { return 0, e.err }

// TestTextTracer_StickyError: a debug log that stopped writing an hour ago is
// worth hearing about at the end rather than never.
func TestTextTracer_StickyError(t *testing.T) {
	boom := errors.New("disk full")
	tr := New(errWriter{err: boom}, WithoutTimestamps(), WithFlushInterval(0))
	// Enough lines to overflow the 64 KiB buffer and force a real write.
	fi := frame.FrameInfo{Header: frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 1}}
	for i := 0; i < 4000; i++ {
		tr.TraceFrame(&fi)
	}
	if err := tr.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close = %v, want %v", err, boom)
	}
}

func TestTextTracer_Timestamps(t *testing.T) {
	var buf bytes.Buffer
	tr := New(&buf, WithFlushInterval(0))
	tr.TraceFrame(&frame.FrameInfo{Header: frame.FrameHeader{Type: frame.FramePing, Length: 8}})
	_ = tr.Close()
	line := strings.TrimRight(buf.String(), "\n")
	// "   0.000123 <- PING …" — four integer columns, a dot, six fraction
	// digits, then the arrow.
	if len(line) < 12 || line[4] != '.' || !strings.Contains(line, " <- PING ") {
		t.Fatalf("timestamped line has the wrong shape: %q", line)
	}
}

func TestTextTracer_AutoFlush(t *testing.T) {
	var mu sync.Mutex
	var got bytes.Buffer
	w := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return got.Write(p)
	})
	tr := New(w, WithoutTimestamps(), WithFlushInterval(5*time.Millisecond))
	defer func() { _ = tr.Close() }()
	tr.TraceFrame(&frame.FrameInfo{Header: frame.FrameHeader{Type: frame.FramePing, Length: 8}})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := got.Len()
		mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background flusher never wrote the buffered line")
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestTextTracer_CloseIdempotent: Close is deferred in the documented usage and
// may also be called explicitly.
func TestTextTracer_CloseIdempotent(t *testing.T) {
	tr := New(&bytes.Buffer{}, WithFlushInterval(time.Millisecond))
	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
