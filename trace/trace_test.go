package trace

import (
	"bytes"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timestampFormat is the shape appendFrameLine stamps every line with:
// HH:MM:SS with six fractional digits. render throws the stamp away so the
// per-case want strings do not depend on what time the test ran, which left
// the format itself pinned by nothing — strings.Cut at the first space
// succeeds for any prefix at all, so dropping the sub-second field, switching
// to RFC3339 or emitting no stamp would all have shipped green.
//
// The resolution is the load-bearing part. This renders on a connection
// carrying tens of thousands of frames per second: at second resolution a
// whole second of frames shares one stamp and the ordering information the log
// exists to convey is gone.
var timestampFormat = regexp.MustCompile(`^\d{2}:\d{2}:\d{2}\.\d{6}$`)

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
	stamp, rest, ok := strings.Cut(line, " ")
	require.Truef(t, ok, "line %q has no timestamp prefix", line)
	require.Regexpf(t, timestampFormat, stamp,
		"timestamp %q is not HH:MM:SS.microseconds; at coarser resolution a whole second of frames shares one stamp and the log stops saying which frame preceded which",
		stamp)
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
			// last_stream=0 is what a server sends when it refuses the
			// connection without having processed any stream, and it is
			// exactly the value a "print non-zero fields" renderer drops.
			name: "GOAWAY refusing everything still prints last_stream=0",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "GOAWAY", Length: 8,
				Detail: DetailLastStreamID, LastStreamID: 0,
			},
			want: "h2 <- GOAWAY stream=0 len=8 last_stream=0",
		},
		{
			// incr=0 is a PROTOCOL_ERROR under RFC 9113 §6.9 — the one
			// WINDOW_UPDATE actually worth logging, and the one a
			// non-zero-only renderer omits.
			name: "WINDOW_UPDATE with a zero increment still prints incr=0",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "WINDOW_UPDATE", Length: 4,
				Detail: DetailIncrement, Increment: 0,
			},
			want: "h2 <- WINDOW_UPDATE stream=0 len=4 incr=0",
		},
		{
			name: "PUSH_PROMISE with a zero promised id still prints promised=0",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "PUSH_PROMISE", StreamID: 1, Length: 12,
				Detail: DetailPromisedID, PromisedID: 0,
			},
			want: "h2 <- PUSH_PROMISE stream=1 len=12 promised=0",
		},
		{
			// DetailParams set with no Params attached is an emitter bug.
			// Params.All dereferences the pointer, so the guard in
			// appendFrameLine is what stops a debug path from panicking the
			// connection it was installed to observe.
			name: "DetailParams with a nil Params renders without panicking",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "SETTINGS", Length: 0,
				Detail: DetailParams, Params: nil,
			},
			want: "h2 <- SETTINGS stream=0 len=0",
		},
		{
			// An empty SETTINGS frame is legal and is how a peer acknowledges
			// nothing in particular; the field must still appear, empty.
			name: "DetailParams with an empty Params still prints the field",
			info: FrameInfo{
				Proto: ProtoH2, Dir: DirIn, TypeName: "SETTINGS", Length: 0,
				Detail: DetailParams, Params: &Params{},
			},
			want: "h2 <- SETTINGS stream=0 len=0 params=",
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

// errWriter fails every Write. Every other writer in this package's tests is a
// bytes.Buffer or bench_test.go's countingWriter, neither of which can fail —
// so the one piece of I/O the package performs, and the error both Flush and
// Close exist to return, had no failing arm at all.
type errWriter struct{ err error }

func (w *errWriter) Write(p []byte) (int, error) { return 0, w.err }

// newUndrainedTracer builds a TextTracer with no flusher goroutine, so nothing
// drains the buffer behind the test's back and Flush is the only writer. The
// same construction is used by TestTextTracer_DropsRatherThanBlocks and for the
// same reason: with the 20 ms flusher running, whether a buffered line is still
// there when the test looks is a race against a ticker.
func newUndrainedTracer(w io.Writer) *TextTracer {
	return &TextTracer{
		w:        w,
		buf:      make([]byte, 0, textLineHint),
		spare:    make([]byte, 0, textLineHint),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

func TestTextTracer_FlushPropagatesTheWriterError(t *testing.T) {
	sentinel := errors.New("write failed")
	tr := newUndrainedTracer(&errWriter{err: sentinel})
	tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})

	err := tr.Flush()

	require.ErrorIsf(t, err, sentinel,
		"Flush returned %v; it is what a caller reaches for after reproducing a bug and before reading the log, so a swallowed write error reads as \"the log is complete\" when the log is empty",
		err)
}

// TestTextTracer_CloseStopsTheFlusherEvenWhenTheFinalWriteFails pins the
// ordering inside Close: closeOnce runs BEFORE drain. Reordering them so a
// failed final write returns early leaks the flusher goroutine — one per
// tracer, forever — and every other test in this package stays green, because
// none of them has a writer that can fail.
//
// The flusher is stood in for by a pre-closed finished channel rather than a
// live goroutine: Close's observable effect is that it closed t.stop, and a
// real flusher would only make whether the line is still buffered a race
// against the 20 ms ticker.
func TestTextTracer_CloseStopsTheFlusherEvenWhenTheFinalWriteFails(t *testing.T) {
	sentinel := errors.New("write failed")
	tr := newUndrainedTracer(&errWriter{err: sentinel})
	close(tr.finished)
	tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})

	err := tr.Close()

	require.ErrorIsf(t, err, sentinel,
		"Close returned %v, not the failing writer's error; Close is the last chance to learn the log was never written", err)
	select {
	case <-tr.stop:
	default:
		assert.Fail(t,
			"Close reported the write error without stopping the flusher: closeOnce must run before drain, or a tracer over a broken writer leaks one goroutine per instance")
	}
}

func TestTextTracer_CloseIsIdempotentWhenTheWriterFails(t *testing.T) {
	sentinel := errors.New("write failed")
	tr := NewTextTracer(&errWriter{err: sentinel})
	// Close first, with nothing buffered: it stops the flusher and has nothing
	// to write, so the line traced next is provably still there when the second
	// Close drains it. Without that ordering the 20 ms flusher may drain first
	// and discard the error, and the assertion below becomes a coin toss.
	require.NoError(t, tr.Close(), "the first Close drains an empty buffer, so there is nothing to fail on")
	tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})

	second, third := tr.Close(), tr.Close()

	require.ErrorIsf(t, second, sentinel,
		"the Close that carried a buffered line returned %v; a failing writer must reach the caller", second)
	assert.NoErrorf(t, third,
		"a third Close returned %v; with nothing left to write it must neither re-report the failure nor close an already-closed channel", third)
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
