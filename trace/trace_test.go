package trace

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// render runs one FrameInfo through a TextTracer and returns the line with the
// leading timestamp removed, so the assertions are about content rather than
// about what time the test ran.
func render(t *testing.T, info FrameInfo) string {
	t.Helper()

	var buf bytes.Buffer
	tr := NewTextTracer(&buf)

	tr.TraceFrame(info)
	err := tr.Close()

	require.NoError(t, err, "Close is what flushes the buffered line; without it there is nothing to assert on")
	line := strings.TrimSuffix(buf.String(), "\n")
	_, rest, ok := strings.Cut(line, " ")
	require.Truef(t, ok, "line %q has no timestamp prefix", line)
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
			info := tc.info

			got := render(t, info)

			assert.Equalf(t, tc.want, got,
				"the rendered line is the whole contract: it is what somebody pastes into an issue, and a field the renderer silently omits is a field nobody knows to ask about")
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

	assert.Equal(t,
		"h2 <- SETTINGS stream=0 len=18 params=INITIAL_WINDOW_SIZE=1048576,MAX_FRAME_SIZE=16384,0xff=7",
		got,
		"an identifier the emitter does not recognise must still reach the log as its number; dropping it hides exactly the setting whose meaning is in dispute")
}

func TestParams_AddStopsAtCapacity(t *testing.T) {
	var p Params

	for i := range MaxParams + 5 {
		p.Add(uint64(i), "", uint64(i))
	}

	require.Lenf(t, p.All(), MaxParams,
		"held %d params, want the cap of %d: a debug path must not panic on a peer that sends more identifiers than this implementation knows about",
		len(p.All()), MaxParams)
	assert.Equalf(t, uint64(MaxParams-1), p.P[MaxParams-1].ID,
		"last param = %+v, want the %dth added: the overflow must be dropped, not wrapped over a parameter already held",
		p.P[MaxParams-1], MaxParams)
}

func TestParams_ResetEmpties(t *testing.T) {
	var p Params
	for i := range MaxParams {
		p.Add(uint64(i), "", uint64(i))
	}

	p.Reset()

	assert.Emptyf(t, p.All(),
		"Reset left %d params; the emitter reuses one Params for every settings frame, so a stale tail would be attributed to the next frame",
		len(p.All()))
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
	// Both are read before Flush: drain swaps the counter to zero and hands the
	// filled buffer away, so after it neither value says anything.
	dropped, buffered := tr.dropped.Load(), len(tr.buf)
	flushErr := tr.Flush()

	require.NotZerof(t, dropped,
		"buffered %d bytes without dropping; the cap is %d, and a tracer that waits on its writer instead stalls the connection it is observing",
		buffered, textMaxBuffered)
	require.NoError(t, flushErr, "Flush writes what the tracer buffered")
	assert.Containsf(t, buf.String(), "frames dropped (tracer backlog)",
		"the flushed output does not report the %d dropped frames; a gap in a debug log that does not say it is a gap is worse than the missing lines",
		dropped)
}

func TestTextTracer_CloseIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTextTracer(&buf)
	tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})

	errs := make([]error, 0, 3)
	for range 3 {
		errs = append(errs, tr.Close())
	}

	for i, err := range errs {
		require.NoErrorf(t, err, "Close #%d of 3", i+1)
	}
	assert.Equalf(t, 1, strings.Count(buf.String(), "PING"),
		"PING appears %d times after three Closes, want 1: a second Close must neither re-emit the batch nor close an already-closed channel",
		strings.Count(buf.String(), "PING"))
}

// TestTextTracer_ConcurrentEmit is the -race half of "one Tracer is shared by
// every connection a client owns, and a connection's reader and writer are
// different goroutines".
func TestTextTracer_ConcurrentEmit(t *testing.T) {
	const goroutines, perGoroutine = 8, 200
	var buf bytes.Buffer
	tr := NewTextTracer(&buf)

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perGoroutine {
				tr.TraceFrame(FrameInfo{
					Proto: ProtoH2, Dir: DirOut, TypeName: "DATA",
					StreamID: uint64(g*1000 + i), Length: 8,
				})
			}
		}()
	}
	wg.Wait()
	closeErr := tr.Close()

	require.NoError(t, closeErr, "Close flushes the final batch")
	assert.Equalf(t, goroutines*perGoroutine, strings.Count(buf.String(), "\n"),
		"wrote %d lines, want %d: every connection sharing this tracer appends under the same lock, and a lost line is a frame nobody can prove crossed the wire",
		strings.Count(buf.String(), "\n"), goroutines*perGoroutine)
}

func TestDirection_String(t *testing.T) {
	tests := []struct {
		name string
		dir  Direction
		want string
	}{
		{name: "inbound", dir: DirIn, want: "<-"},
		{name: "outbound", dir: DirOut, want: "->"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.dir

			got := dir.String()

			assert.Equalf(t, tc.want, got,
				"Direction(%d) renders as %q; the arrow is the only thing in the line that says which endpoint sent the frame",
				uint8(dir), got)
		})
	}
}

func TestProtocol_String(t *testing.T) {
	tests := []struct {
		name  string
		proto Protocol
		want  string
	}{
		{name: "h1", proto: ProtoH1, want: "h1"},
		{name: "h2", proto: ProtoH2, want: "h2"},
		{name: "h3", proto: ProtoH3, want: "h3"},
		{name: "quic", proto: ProtoQUIC, want: "quic"},
		// Not folded into "undefined value": ProtoUnknown is a DEFINED value
		// meaning "no protocol was settled", and rendering it as one of the
		// real protocols is the failure it exists to prevent.
		{name: "unknown", proto: ProtoUnknown, want: "?"},
		{name: "undefined value", proto: Protocol(9), want: "?"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proto := tc.proto

			got := proto.String()

			assert.Equalf(t, tc.want, got,
				"Protocol(%d).String() = %q, want %q: one Tracer serves a client speaking several protocols at once, so the tag is what separates their frames in one log",
				uint8(proto), got, tc.want)
		})
	}
}

func TestDetail_Has(t *testing.T) {
	const have = DetailErrCode | DetailParams

	tests := []struct {
		name string
		want Detail
		ok   bool
	}{
		{name: "one bit that is set", want: DetailErrCode, ok: true},
		{name: "both bits that are set", want: DetailErrCode | DetailParams, ok: true},
		{name: "one bit that is unset", want: DetailIncrement, ok: false},
		{name: "one set bit and one unset bit", want: DetailErrCode | DetailIncrement, ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := have

			got := d.Has(tc.want)

			assert.Equalf(t, tc.ok, got,
				"Detail(%b).Has(%b) = %v: Has means every bit, not any bit, and an emitter that filled one of two fields must not read as having filled both",
				d, tc.want, got)
		})
	}
}
