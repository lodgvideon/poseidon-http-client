package trace

import (
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// The tracer's own lock is the thing to watch. frame's emit sites take no lock
// at all, but TraceFrame fires on the connection's reader goroutine and, on the
// write side, while the connection's write lock is held — and one TextTracer is
// typically shared by every connection a client owns. So its critical section
// serializes across connections, which is exactly where a debug facility starts
// changing the timing of the thing being debugged.
//
// Every benchmark here reports drop-frac. It has to: a benchmark drives frames
// far faster than any writer drains them, so an unattended tracer saturates its
// 1 MiB backlog within milliseconds and every subsequent call takes the
// lock-free drop path. That reads as a spectacular 16 ns/op which measures
// nothing but an atomic load. drop-frac near 0 means the number above it is the
// render-and-append path; drop-frac near 1 means it is not.

func benchInfo() FrameInfo {
	return FrameInfo{
		Proto: ProtoH2, Dir: DirOut, Type: 0x0, TypeName: "DATA",
		StreamID: 1, Length: 16384,
	}
}

// countingWriter tallies the lines that actually reach the writer.
type countingWriter struct{ lines atomic.Int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	var n int64
	for _, c := range p {
		if c == '\n' {
			n++
		}
	}
	w.lines.Add(n)
	return len(p), nil
}

// keepDrained flushes far more often than the production 20 ms ticker, so the
// backlog never saturates and the benchmark measures the path that renders.
// The flusher contends for the same lock the emitters do, which is realistic —
// the real one does too, just less often.
func keepDrained(b *testing.B, t *TextTracer) {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = t.Flush()
				time.Sleep(20 * time.Microsecond)
			}
		}
	}()
	b.Cleanup(func() { close(stop); <-done })
}

func reportDrops(b *testing.B, w *countingWriter, t *TextTracer) {
	_ = t.Close()
	delivered := w.lines.Load()
	dropped := int64(b.N) - delivered
	if dropped < 0 {
		dropped = 0
	}
	b.ReportMetric(float64(dropped)/float64(b.N), "drop-frac")
}

func BenchmarkTextTracer_TraceFrame(b *testing.B) {
	w := &countingWriter{}
	t := NewTextTracer(w)
	keepDrained(b, t)
	info := benchInfo()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t.TraceFrame(info)
	}
	b.StopTimer()
	reportDrops(b, w, t)
}

// BenchmarkTextTracer_TraceFrameParallel is the contended case: N goroutines on
// one tracer, as N connections would be. -cpu sets N.
func BenchmarkTextTracer_TraceFrameParallel(b *testing.B) {
	w := &countingWriter{}
	t := NewTextTracer(w)
	keepDrained(b, t)
	info := benchInfo()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t.TraceFrame(info)
		}
	})
	b.StopTimer()
	reportDrops(b, w, t)
}

// BenchmarkTextTracer_SettingsLine renders the widest line the HTTP/2 emitter
// produces; the critical section is proportional to line length.
func BenchmarkTextTracer_SettingsLine(b *testing.B) {
	var p Params
	p.Add(0x1, "HEADER_TABLE_SIZE", 4096)
	p.Add(0x3, "MAX_CONCURRENT_STREAMS", 100)
	p.Add(0x4, "INITIAL_WINDOW_SIZE", 65535)
	p.Add(0x5, "MAX_FRAME_SIZE", 16384)
	p.Add(0x6, "MAX_HEADER_LIST_SIZE", 8<<20)
	info := FrameInfo{
		Proto: ProtoH2, Dir: DirOut, TypeName: "SETTINGS", Length: 30,
		Detail: DetailParams, Params: &p,
	}
	w := &countingWriter{}
	t := NewTextTracer(w)
	keepDrained(b, t)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t.TraceFrame(info)
	}
	b.StopTimer()
	reportDrops(b, w, t)
}

// BenchmarkTextTracer_Overloaded is the drop path on purpose — no drainer, so
// the backlog saturates and stays saturated. This is what a tracer costs a
// connection once it has given up keeping the log complete, and the reason the
// overload check sits ahead of rendering and takes no lock.
func BenchmarkTextTracer_Overloaded(b *testing.B) {
	t := NewTextTracer(io.Discard)
	defer func() { _ = t.Close() }()
	info := benchInfo()
	for range 40_000 { // saturate before measuring
		t.TraceFrame(info)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t.TraceFrame(info)
	}
}

// BenchmarkAppendFrameLine isolates rendering from the lock entirely.
func BenchmarkAppendFrameLine(b *testing.B) {
	info := benchInfo()
	now := time.Now()
	n := 0
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var scratch [textScratchSize]byte
		n += len(appendFrameLine(scratch[:0], now, info))
	}
	if n == 0 {
		b.Fatal("rendered nothing")
	}
}
