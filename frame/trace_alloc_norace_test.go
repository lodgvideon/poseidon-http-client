//go:build !race

package frame

import (
	"bytes"
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

// TestFramer_Trace_AddsNoAllocations measures each frame path twice — once with
// no tracer, once with a tracer that is installed and discards — and requires
// the two counts to be equal.
//
// A delta rather than an absolute zero, for two reasons. It states the property
// actually being promised ("observing a frame costs nothing"), and it states it
// about paths whose own allocation behaviour is not the tracer's business:
// WriteSettings has escaped its 96-byte encode scratch to the heap since long
// before any of this, and an absolute gate here would report that as a tracing
// regression.
//
// The two hot paths — a DATA write and a frame read — are separately pinned at
// an absolute zero by the traced benchmarks in trace_test.go, which the
// bench-gate enforces.
//
// !race because the detector adds allocations of its own, unevenly across the
// two sides; the client and conn alloc gates are split by this tag for the same
// reason.
func TestFramer_Trace_AddsNoAllocations(t *testing.T) {
	var settings SettingsParams
	settings.set(SettingInitialWindowSize, 1<<20)
	settings.set(SettingMaxFrameSize, 16384)
	block := []byte{0x82, 0x86, 0x84}
	payload := make([]byte, 1024)

	cases := []struct {
		name string
		run  func(*testing.T, *Framer)
	}{
		{"DATA", func(t *testing.T, f *Framer) { mustWrite(t, f.WriteData(1, false, payload)) }},
		{"HEADERS", func(t *testing.T, f *Framer) {
			mustWrite(t, f.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: block, EndHeaders: true}))
		}},
		{"SETTINGS", func(t *testing.T, f *Framer) { mustWrite(t, f.WriteSettings(settings)) }},
		{"RST_STREAM", func(t *testing.T, f *Framer) { mustWrite(t, f.WriteRSTStream(1, ErrCodeCancel)) }},
		{"GOAWAY", func(t *testing.T, f *Framer) { mustWrite(t, f.WriteGoAway(1, ErrCodeNoError, nil)) }},
		{"WINDOW_UPDATE", func(t *testing.T, f *Framer) { mustWrite(t, f.WriteWindowUpdate(1, 4096)) }},
		{"PING", func(t *testing.T, f *Framer) { mustWrite(t, f.WritePing(false, [8]byte{})) }},
		{"PUSH_PROMISE", func(t *testing.T, f *Framer) { mustWrite(t, f.WritePushPromise(1, 2, block, true, 0)) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			write := func(tr trace.Tracer) float64 {
				f := NewFramer(discardWriter{}, nil)
				f.SetTracer(tr)
				tc.run(t, f) // warm one-shot growth out of the measured window
				return testing.AllocsPerRun(200, func() { tc.run(t, f) })
			}
			if traced, plain := write(discardTracer{}), write(nil); traced != plain {
				t.Errorf("%v allocs traced vs %v untraced — tracing must be free", traced, plain)
			}
		})
	}

	t.Run("ReadFrame", func(t *testing.T) {
		// SETTINGS on purpose: it is the one inbound frame whose trace detail is
		// not a scalar, so it is the one that could plausibly allocate.
		var wire bytes.Buffer
		peer := NewFramer(&wire, nil)
		mustWrite(t, peer.WriteSettings(settings))
		one := bytes.Clone(wire.Bytes())

		read := func(tr trace.Tracer) float64 {
			f := NewFramer(nil, &repeatReader{buf: one})
			f.SetTracer(tr)
			h := dropHandler{}
			ctx := context.Background()
			return testing.AllocsPerRun(200, func() {
				if _, err := f.ReadFrame(ctx, h); err != nil {
					t.Fatal(err)
				}
			})
		}
		if traced, plain := read(discardTracer{}), read(nil); traced != plain {
			t.Errorf("%v allocs traced vs %v untraced — tracing must be free", traced, plain)
		}
	})
}

func mustWrite(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("write: %v", err)
	}
}
