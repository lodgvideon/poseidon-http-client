//go:build !race

package trace

import (
	"io"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestTextTracer_DoesNotAllocatePerFrame is this package's half of the #610 caveat
// that zero cost when off is a gate and not an aspiration. The frame package's
// half — that a Framer with a tracer installed still allocates nothing — is on
// the CI bench-gate. This is the other end: the built-in tracer that a user
// actually installs must not allocate per frame either, or switching tracing on
// adds GC pressure to the load test it was switched on to explain.
//
// Not on the bench-gate, because that job scans raw `go test -bench` output for
// ANY non-zero B/op line across seven packages, and this package will grow
// benchmarks (parsing, formatting variants) that legitimately allocate. The
// AllocsPerRun gate is the pattern conn, client and http1 use for exactly that
// reason, and it is two-sided in the same way: this fails if the number goes
// UP, and the assertion is 0, so it cannot be silently widened.
//
// //go:build !race because the race detector allocates on its own account and
// makes AllocsPerRun meaningless.
func TestTextTracer_DoesNotAllocatePerFrame(t *testing.T) {
	settings := frame.SettingsParams{N: 3}
	settings.Pairs[0] = frame.SettingPair{ID: frame.SettingHeaderTableSize, Value: 4096}
	settings.Pairs[1] = frame.SettingPair{ID: frame.SettingMaxConcurrentStreams, Value: 250}
	settings.Pairs[2] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 65535}

	cases := []struct {
		name string
		fi   frame.FrameInfo
	}{
		{"HEADERS", frame.FrameInfo{
			Dir: frame.DirSend,
			Header: frame.FrameHeader{
				Type: frame.FrameHeaders, StreamID: 1, Length: 54,
				Flags: frame.FlagHeadersEndStream | frame.FlagHeadersEndHeaders,
			},
		}},
		{"DATA", frame.FrameInfo{
			Dir:    frame.DirRecv,
			Header: frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 16384},
		}},
		{"WINDOW_UPDATE", frame.FrameInfo{
			Dir:             frame.DirSend,
			Header:          frame.FrameHeader{Type: frame.FrameWindowUpdate, Length: 4},
			WindowIncrement: 32768,
		}},
		{"GOAWAY", frame.FrameInfo{
			Dir:          frame.DirRecv,
			Header:       frame.FrameHeader{Type: frame.FrameGoAway, Length: 8},
			LastStreamID: 9, ErrCode: frame.ErrCodeEnhanceYourCalm,
		}},
		// The widest line the renderer produces: a decoded settings table.
		{"SETTINGS", frame.FrameInfo{
			Dir:      frame.DirRecv,
			Header:   frame.FrameHeader{Type: frame.FrameSettings, Length: 18},
			Settings: settings,
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Timestamps on: appendElapsed is part of the per-frame path in the
			// default configuration, so it is part of what must not allocate.
			tr := New(io.Discard, WithFlushInterval(0))
			defer func() { _ = tr.Close() }()
			fi := c.fi
			// One warm-up render so the line scratch has reached its steady
			// capacity; the first line's growth is setup, not per-frame cost.
			tr.TraceFrame(&fi)
			if got := testing.AllocsPerRun(200, func() { tr.TraceFrame(&fi) }); got != 0 {
				t.Fatalf("%s: %v allocs/frame, want 0", c.name, got)
			}
		})
	}
}

// TestTextTracer_PayloadRenderDoesNotAllocate keeps the opt-in payload path on the
// same footing — it is the one branch that reads a caller-owned slice, and hex
// encoding is the obvious place to reach for a helper that allocates.
func TestTextTracer_PayloadRenderDoesNotAllocate(t *testing.T) {
	tr := New(io.Discard, WithFlushInterval(0), WithPayload(32))
	defer func() { _ = tr.Close() }()
	fi := frame.FrameInfo{
		Dir:     frame.DirRecv,
		Header:  frame.FrameHeader{Type: frame.FrameData, StreamID: 1, Length: 1024},
		Payload: make([]byte, 1024),
	}
	tr.TraceFrame(&fi)
	if got := testing.AllocsPerRun(200, func() { tr.TraceFrame(&fi) }); got != 0 {
		t.Fatalf("%v allocs/frame with payload rendering on, want 0", got)
	}
}
