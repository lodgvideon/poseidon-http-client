package trace

import (
	"io"
	"strconv"
	"sync"
	"time"
)

// Text tracer tuning. All three are deliberately not options: they are the
// answer to "a slow tracer stalls the connection", and a caller who wants a
// different answer wants a different Tracer, which is a two-line type.
const (
	// textFlushInterval is how often the background flusher drains the buffer.
	// At 100k frames/sec this batches roughly two thousand lines into one Write.
	textFlushInterval = 20 * time.Millisecond
	// textMaxBuffered bounds the unflushed backlog. Past it TraceFrame drops
	// lines and counts them rather than waiting on the writer.
	textMaxBuffered = 1 << 20 // 1 MiB
	// textLineHint is the initial per-buffer capacity, sized so a busy
	// connection settles without regrowing every tick.
	textLineHint = 16 << 10
)

// TextTracer writes one line per frame to an io.Writer.
//
// It buffers. The frames it observes arrive on the connection's own goroutines
// — a log.Printf per frame would put a write syscall under the connection's
// write lock at whatever rate the peer sets — so TraceFrame only appends to an
// in-memory buffer and a background goroutine performs the writes. When the
// backlog outruns the writer anyway, TraceFrame drops lines and reports the
// count rather than blocking the connection; a debug log that changes the
// timing of the bug being debugged is worse than one with a gap in it.
//
// Close stops the flusher and writes what is left. A TextTracer that is never
// closed leaks one goroutine and loses its final partial batch.
//
// Safe for concurrent use.
type TextTracer struct {
	w io.Writer

	mu      sync.Mutex
	buf     []byte // filled by TraceFrame, drained by the flusher
	spare   []byte // handed back by the flusher for the next batch
	dropped uint64

	stop      chan struct{}
	finished  chan struct{}
	closeOnce sync.Once
}

var _ Tracer = (*TextTracer)(nil)

// NewTextTracer returns a TextTracer writing to w and starts its flusher.
// Call Close when done.
func NewTextTracer(w io.Writer) *TextTracer {
	t := &TextTracer{
		w:        w,
		buf:      make([]byte, 0, textLineHint),
		spare:    make([]byte, 0, textLineHint),
		stop:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	go t.flushLoop()
	return t
}

// TraceFrame renders info as one line. It never writes to the underlying
// io.Writer and never blocks on it.
func (t *TextTracer) TraceFrame(info FrameInfo) {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.buf) >= textMaxBuffered {
		t.dropped++
		return
	}
	t.buf = appendFrameLine(t.buf, now, info)
}

// Flush writes any buffered lines immediately. It is what a caller reaches for
// after reproducing a bug and before reading the log, rather than waiting out
// the flush interval.
func (t *TextTracer) Flush() error { return t.drain() }

// Close stops the background flusher and flushes what remains. It does not
// close the underlying io.Writer. Idempotent.
func (t *TextTracer) Close() error {
	t.closeOnce.Do(func() {
		close(t.stop)
		<-t.finished
	})
	return t.drain()
}

func (t *TextTracer) flushLoop() {
	defer close(t.finished)
	tick := time.NewTicker(textFlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			_ = t.drain()
		case <-t.stop:
			return
		}
	}
}

// drain swaps the filled buffer out under the lock and writes it outside,
// so an emitting goroutine never waits on the io.Writer.
func (t *TextTracer) drain() error {
	t.mu.Lock()
	if len(t.buf) == 0 && t.dropped == 0 {
		t.mu.Unlock()
		return nil
	}
	out := t.buf
	t.buf, t.spare = t.spare[:0], out
	dropped := t.dropped
	t.dropped = 0
	t.mu.Unlock()

	if dropped > 0 {
		out = append(out, "... "...)
		out = strconv.AppendUint(out, dropped, 10)
		out = append(out, " frames dropped (tracer backlog)\n"...)
	}
	_, err := t.w.Write(out)

	// Hand the drained buffer back as the next spare. Growing it past the hint
	// is kept — a connection that needed the room once will need it again — but
	// a one-off burst is not held forever.
	t.mu.Lock()
	if cap(out) <= 4*textLineHint {
		t.spare = out[:0]
	} else {
		t.spare = make([]byte, 0, textLineHint)
	}
	t.mu.Unlock()
	return err
}

// appendFrameLine renders one frame. fmt is avoided throughout: it allocates
// per call and this runs once per frame on a connection carrying tens of
// thousands per second.
func appendFrameLine(dst []byte, now time.Time, info FrameInfo) []byte {
	dst = now.AppendFormat(dst, "15:04:05.000000")
	dst = append(dst, ' ')
	dst = append(dst, info.Proto.String()...)
	dst = append(dst, ' ')
	dst = append(dst, info.Dir.String()...)
	dst = append(dst, ' ')
	dst = append(dst, info.TypeName...)
	if info.TypeName == "" || info.TypeName == UnknownName {
		dst = append(dst, '(')
		dst = appendHex(dst, info.Type)
		dst = append(dst, ')')
	}
	dst = append(dst, " stream="...)
	dst = strconv.AppendUint(dst, info.StreamID, 10)
	dst = append(dst, " len="...)
	dst = strconv.AppendUint(dst, uint64(info.Length), 10)
	if info.FlagNames != "" {
		dst = append(dst, " flags="...)
		dst = append(dst, info.FlagNames...)
	}
	if info.Detail.Has(DetailLastStreamID) {
		dst = append(dst, " last_stream="...)
		dst = strconv.AppendUint(dst, info.LastStreamID, 10)
	}
	if info.Detail.Has(DetailErrCode) {
		dst = append(dst, " code="...)
		if info.ErrCodeName != "" && info.ErrCodeName != UnknownName {
			dst = append(dst, info.ErrCodeName...)
		} else {
			dst = appendHex(dst, info.ErrCode)
		}
	}
	if info.Detail.Has(DetailIncrement) {
		dst = append(dst, " incr="...)
		dst = strconv.AppendUint(dst, info.Increment, 10)
	}
	if info.Detail.Has(DetailPromisedID) {
		dst = append(dst, " promised="...)
		dst = strconv.AppendUint(dst, info.PromisedID, 10)
	}
	if info.Detail.Has(DetailParams) && info.Params != nil {
		dst = append(dst, " params="...)
		for i, p := range info.Params.All() {
			if i > 0 {
				dst = append(dst, ',')
			}
			if p.Name != "" {
				dst = append(dst, p.Name...)
			} else {
				dst = appendHex(dst, p.ID)
			}
			dst = append(dst, '=')
			dst = strconv.AppendUint(dst, p.Value, 10)
		}
	}
	return append(dst, '\n')
}

func appendHex(dst []byte, v uint64) []byte {
	dst = append(dst, "0x"...)
	return strconv.AppendUint(dst, v, 16)
}
