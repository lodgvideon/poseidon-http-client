package trace

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// render runs one FrameInfo through a TextTracer and returns the line with the
// leading timestamp removed, so the assertions are about content rather than
// about what time the test ran.
func render(t *testing.T, info FrameInfo) string {
	t.Helper()
	var buf bytes.Buffer
	tr := NewTextTracer(&buf)
	tr.TraceFrame(info)
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	line := strings.TrimSuffix(buf.String(), "\n")
	_, rest, ok := strings.Cut(line, " ")
	if !ok {
		t.Fatalf("line %q has no timestamp prefix", line)
	}
	return rest
}

func TestTextTracer_RendersFrames(t *testing.T) {
	tests := []struct {
		name string
		info FrameInfo
		want string
	}{
		{
			name: "outbound HEADERS with flags",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirOut, Type: 0x1, TypeName: "HEADERS",
				Flags: 0x5, FlagNames: "END_STREAM|END_HEADERS", StreamID: 3, Length: 54,
			},
			want: "h2 -> HEADERS stream=3 len=54 flags=END_STREAM|END_HEADERS",
		},
		{
			name: "inbound DATA without flags",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "DATA", StreamID: 3, Length: 1024,
			},
			want: "h2 <- DATA stream=3 len=1024",
		},
		{
			// The #570 shape: a GOAWAY whose code is NO_ERROR must print the code,
			// and it is exactly the value a "print non-zero fields" renderer drops.
			name: "GOAWAY with NO_ERROR still prints its code",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "GOAWAY", Length: 8,
				Detail:  DetailLastStreamID | DetailErrCode,
				ErrCode: 0, ErrCodeName: "NO_ERROR", LastStreamID: 7,
			},
			want: "h2 <- GOAWAY stream=0 len=8 last_stream=7 code=NO_ERROR",
		},
		{
			name: "unnamed error code falls back to the number",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "RST_STREAM", StreamID: 1, Length: 4,
				Detail: DetailErrCode, ErrCode: 0xbeef, ErrCodeName: UnknownName,
			},
			want: "h2 <- RST_STREAM stream=1 len=4 code=0xbeef",
		},
		{
			name: "unknown frame type prints its number",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, Type: 0x1f, TypeName: UnknownName, Length: 2,
			},
			want: "h2 <- UNKNOWN(0x1f) stream=0 len=2",
		},
		{
			name: "WINDOW_UPDATE increment",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirOut, TypeName: "WINDOW_UPDATE", Length: 4,
				Detail: DetailIncrement, Increment: 32768,
			},
			want: "h2 -> WINDOW_UPDATE stream=0 len=4 incr=32768",
		},
		{
			name: "PUSH_PROMISE promised id",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "PUSH_PROMISE", StreamID: 1, Length: 12,
				Detail: DetailPromisedID, PromisedID: 2,
			},
			want: "h2 <- PUSH_PROMISE stream=1 len=12 promised=2",
		},
		{
			name: "h3 protocol tag",
			info: FrameInfo{
				Proto: ProtoH3, Dir: DirOut, TypeName: "HEADERS", StreamID: 0, Length: 30,
			},
			want: "h3 -> HEADERS stream=0 len=30",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := render(t, tc.info); got != tc.want {
				t.Errorf("\n got %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestTextTracer_RendersSettingsParams(t *testing.T) {
	var p Params
	p.Add(0x4, "INITIAL_WINDOW_SIZE", 1<<20)
	p.Add(0x5, "MAX_FRAME_SIZE", 16384)
	p.Add(0xff, "", 7) // unregistered identifier: number, not a word

	got := render(t, FrameInfo{
		Proto: ProtoH2, Dir: DirIn, TypeName: "SETTINGS", Length: 18,
		Detail: DetailParams, Params: &p,
	})
	want := "h2 <- SETTINGS stream=0 len=18 params=INITIAL_WINDOW_SIZE=1048576,MAX_FRAME_SIZE=16384,0xff=7"
	if got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
}

func TestParams_AddStopsAtCapacity(t *testing.T) {
	var p Params
	for i := range MaxParams + 5 {
		p.Add(uint64(i), "", uint64(i))
	}
	if got := len(p.All()); got != MaxParams {
		t.Fatalf("held %d params, want the cap of %d", got, MaxParams)
	}
	if p.P[MaxParams-1].ID != MaxParams-1 {
		t.Errorf("last param = %+v, want the %dth added, not an overwrite", p.P[MaxParams-1], MaxParams)
	}
	p.Reset()
	if len(p.All()) != 0 {
		t.Errorf("Reset left %d params", len(p.All()))
	}
}

// TestTextTracer_DropsRatherThanBlocks builds the tracer without its flusher so
// nothing drains, then overruns the backlog. The count has to survive into the
// output: a gap in a debug log that does not say it is a gap is worse than the
// missing lines.
func TestTextTracer_DropsRatherThanBlocks(t *testing.T) {
	var buf bytes.Buffer
	tr := &TextTracer{
		w:        &buf,
		buf:      make([]byte, 0, textLineHint),
		spare:    make([]byte, 0, textLineHint),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	info := FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "DATA", StreamID: 1, Length: 1024}
	for range 40_000 {
		tr.TraceFrame(info)
	}
	if tr.dropped == 0 {
		t.Fatalf("buffered %d bytes without dropping; cap is %d", len(tr.buf), textMaxBuffered)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !strings.Contains(buf.String(), "frames dropped (tracer backlog)") {
		t.Error("flushed output does not report the dropped frames")
	}
}

func TestTextTracer_CloseIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTextTracer(&buf)
	tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})
	for range 3 {
		if err := tr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if n := strings.Count(buf.String(), "PING"); n != 1 {
		t.Errorf("PING appears %d times after three Closes, want 1", n)
	}
}

// TestTextTracer_ConcurrentEmit is the -race half of "one Tracer is shared by
// every connection a client owns, and a connection's reader and writer are
// different goroutines".
func TestTextTracer_ConcurrentEmit(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTextTracer(&buf)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				tr.TraceFrame(FrameInfo{
					Proto: ProtoH2, Dir: DirOut, TypeName: "DATA",
					StreamID: uint64(g*1000 + i), Length: 8,
				})
			}
		}()
	}
	wg.Wait()
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := strings.Count(buf.String(), "\n"); n != 8*200 {
		t.Errorf("wrote %d lines, want %d", n, 8*200)
	}
}

func TestDirectionAndProtocol_String(t *testing.T) {
	if DirIn.String() != "<-" || DirOut.String() != "->" {
		t.Errorf("directions render as %q/%q", DirIn, DirOut)
	}
	for p, want := range map[Protocol]string{
		ProtoH1: "h1", ProtoH2: "h2", ProtoH3: "h3", ProtoQUIC: "quic", Protocol(9): "?",
	} {
		if got := p.String(); got != want {
			t.Errorf("Protocol(%d).String() = %q, want %q", uint8(p), got, want)
		}
	}
}

func TestDetail_Has(t *testing.T) {
	d := DetailErrCode | DetailParams
	if !d.Has(DetailErrCode) || !d.Has(DetailErrCode|DetailParams) {
		t.Error("Has reports a set bit as unset")
	}
	if d.Has(DetailIncrement) || d.Has(DetailErrCode|DetailIncrement) {
		t.Error("Has reports an unset bit as set")
	}
}
