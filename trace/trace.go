// Package trace turns the wire-level observation seams of this module into
// something a human reads.
//
// The codec packages define the seams and nothing else: frame.Tracer is an
// interface and a value struct, deliberately free of any opinion about
// formatting, buffering or destinations, because it fires on a connection's
// reader goroutine and under its write lock. This package holds the opinions.
//
// The usual shape is one line:
//
//	tr, err := trace.FromEnv(os.Stderr)   // reads POSEIDON_DEBUG
//	if err != nil {
//		return err
//	}
//	defer tr.Close()                      // nil-safe
//	opts.Tracer = tr.Tracer()             // nil-safe; a true nil interface when off
//
// or, without the environment variable in the way:
//
//	tr := trace.New(os.Stderr)
//	defer tr.Close()
//	opts.Tracer = tr
//
// Output looks like this — one line per frame, oldest first, both directions
// interleaved in the order they crossed the wire:
//
//	0.000412 -> SETTINGS stream=0 len=18 [SETTINGS_ENABLE_PUSH=0 SETTINGS_INITIAL_WINDOW_SIZE=65535]
//	0.001880 <- SETTINGS stream=0 len=24 [SETTINGS_MAX_CONCURRENT_STREAMS=250]
//	0.001901 -> SETTINGS stream=0 len=0 flags=ACK
//	0.002233 -> HEADERS stream=1 len=54 flags=END_STREAM|END_HEADERS
//	0.019004 <- HEADERS stream=1 len=91 flags=END_HEADERS
//	0.019055 <- DATA stream=1 len=1024
//	0.019102 -> WINDOW_UPDATE stream=0 len=4 inc=32768
//	0.031200 <- GOAWAY stream=0 len=8 last=1 code=NO_ERROR
//
// What is NOT in that output is the point of half the design: no header names,
// no header values, no body bytes. A debug log is the thing people paste into a
// public issue, and `authorization` and `cookie` live in the field block. DATA
// and header-block payloads are printed only when explicitly asked for, and
// truncated when they are.
package trace

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// EnvVar is the environment variable FromEnv reads.
//
// It is an environment variable and not a build tag on purpose. `-tags
// poseidondebug` already exists in this module and carries the Close-leak
// finalizer, and it cannot answer the question that matters here: a load
// generator is misbehaving in an environment you did not build, and you need it
// loud without a rebuild and a redeploy.
const EnvVar = "POSEIDON_DEBUG"

// Defaults for New. The buffer size is one 64 KiB write per ~700 frames at the
// line lengths above; the flush interval bounds how long the tail of a log sits
// unwritten when traffic stops, which is exactly the moment a hang is being
// investigated.
const (
	defaultBufSize       = 64 << 10
	defaultFlushInterval = 100 * time.Millisecond
	defaultPayloadBytes  = 64
)

// Spec is a parsed EnvVar value: which categories of wire event were asked for.
//
// It is exported so a program can report what it honoured. Two of the four
// categories are accepted and do nothing yet — see Pending — and a caller that
// silently ignores them sends its user hunting for output that was never going
// to appear.
type Spec struct {
	// Frames enables per-frame tracing. It is the only category with a seam
	// behind it today.
	Frames bool

	// Streams asks for stream lifecycle events. Accepted, not yet implemented:
	// the seam lives in conn, which #610 stages after this one.
	Streams bool

	// Flow asks for flow-control window accounting. Accepted, not yet
	// implemented, same reason. Note that WINDOW_UPDATE frames in both
	// directions are already visible under Frames — what is missing is the
	// running window balance, which only conn knows.
	Flow bool

	// PayloadBytes is how many bytes of each frame payload to render as hex.
	// Zero — the default — prints none. See the package doc for why that is
	// the default.
	PayloadBytes int
}

// Enabled reports whether any category was requested.
func (s Spec) Enabled() bool { return s.Frames || s.Streams || s.Flow }

// Pending returns the requested categories that have no seam behind them yet,
// in the order they appear in the type. Report it: a category that parsed
// cleanly and then produces nothing is indistinguishable from a broken build.
func (s Spec) Pending() []string {
	var out []string
	if s.Streams {
		out = append(out, "streams")
	}
	if s.Flow {
		out = append(out, "flow")
	}
	return out
}

// ParseSpec parses a POSEIDON_DEBUG value: a comma-separated list of
// categories, case-insensitive, surrounding whitespace ignored.
//
//	frames        per-frame tracing (aliases: frame, 1, true, on)
//	streams       stream lifecycle — accepted, not yet implemented
//	flow          flow-control accounting — accepted, not yet implemented
//	all           frames + streams + flow, but NOT payload
//	payload       frames, plus 64 bytes of each payload as hex
//	payload=N     frames, plus N bytes
//
// An empty or all-whitespace value is not an error; it parses to the zero Spec,
// whose Enabled reports false.
//
// An unrecognised token IS an error. A typo in a debug switch that silently
// turns nothing on is the worst possible failure for this feature — you spend
// the next hour concluding the bug does not reproduce under tracing.
//
// `all` deliberately excludes payload. Someone reaching for the loudest setting
// is not thereby asking to put request bodies and HPACK-coded `authorization`
// headers in a file they are about to attach to an issue; that has to be typed
// out.
func ParseSpec(v string) (Spec, error) {
	var s Spec
	for _, tok := range strings.Split(v, ",") {
		tok = strings.ToLower(strings.TrimSpace(tok))
		if tok == "" {
			continue
		}
		name, arg, hasArg := strings.Cut(tok, "=")
		switch name {
		case "frames", "frame", "1", "true", "on":
			s.Frames = true
		case "streams", "stream":
			s.Streams = true
		case "flow":
			s.Flow = true
		case "all":
			s.Frames, s.Streams, s.Flow = true, true, true
		case "payload", "payloads":
			s.Frames = true
			s.PayloadBytes = defaultPayloadBytes
			if hasArg {
				n, err := strconv.Atoi(arg)
				if err != nil || n < 0 {
					return Spec{}, fmt.Errorf("%s: payload=%q: want a non-negative byte count", EnvVar, arg)
				}
				s.PayloadBytes = n
			}
		default:
			return Spec{}, fmt.Errorf(
				"%s: unknown category %q; want frames, streams, flow, all, payload[=N]", EnvVar, tok)
		}
	}
	return s, nil
}

// Options returns the New options implied by s.
func (s Spec) Options() []Option {
	var out []Option
	if s.PayloadBytes > 0 {
		out = append(out, WithPayload(s.PayloadBytes))
	}
	return out
}

// FromEnv reads EnvVar and returns a text tracer configured from it, writing to
// w. It returns a nil *TextTracer — not an error — when the variable is unset,
// empty, or names only categories that have no frame seam.
//
// Every method on *TextTracer is nil-safe, so the result can be closed
// unconditionally; use Tracer to install it, which converts a nil pointer to a
// nil interface rather than the non-nil interface holding a nil pointer that a
// direct assignment would produce.
func FromEnv(w io.Writer) (*TextTracer, error) {
	spec, err := ParseSpec(os.Getenv(EnvVar))
	if err != nil {
		return nil, err
	}
	if !spec.Frames {
		return nil, nil
	}
	return New(w, spec.Options()...), nil
}

// Option configures a TextTracer.
type Option func(*TextTracer)

// WithPayload renders up to n bytes of each frame's payload as hex, truncating
// longer payloads with a `+N` count. n <= 0 turns payload rendering off.
//
// SEE THE PACKAGE DOC BEFORE USING IT. The payload of a HEADERS frame is an
// HPACK-coded field block — `authorization` and `cookie` included — and the
// payload of a DATA frame is the body. It is off by default and this is the
// only way to turn it on.
//
// Send-side frames have no payload to render: the Framer assembles them from
// the caller's own buffers. This affects received frames only.
func WithPayload(n int) Option {
	return func(t *TextTracer) {
		if n < 0 {
			n = 0
		}
		t.payloadBytes = n
	}
}

// WithoutTimestamps drops the leading elapsed-time column. Mostly for tests,
// whose expected output would otherwise change every run.
func WithoutTimestamps() Option {
	return func(t *TextTracer) { t.timestamps = false }
}

// WithFlushInterval sets how often the background flusher pushes buffered lines
// to the underlying writer. Zero or less disables the flusher entirely, leaving
// output to appear only when the buffer fills or Flush is called — which is
// what a test that wants deterministic timing should use.
func WithFlushInterval(d time.Duration) Option {
	return func(t *TextTracer) { t.flushInterval = d }
}

// TextTracer is a frame.Tracer that writes one human-readable line per frame.
//
// It is buffered, and that is not an optimisation. TraceFrame runs on the
// connection's reader goroutine and under its write lock, so whatever it does
// is time the connection is not moving bytes; at 100k frames/sec a write
// syscall per frame does not merely slow the run down, it changes what the run
// measures. Lines therefore accumulate in a 64 KiB buffer and reach the writer
// in batches, with a background flusher bounding the delay.
//
// What that buys is a bounded cost, not a zero one: when the buffer fills, the
// frame that filled it pays for the write to w inline. Point it at a file, a
// pipe or an in-memory sink. Pointing it at something slow — a network logger,
// a synchronous remote sink — will slow the connection down, and no amount of
// buffering in here can fix that.
//
// The zero value is not usable; construct with New. Every method tolerates a
// nil receiver.
type TextTracer struct {
	mu  sync.Mutex
	bw  *bufio.Writer
	buf []byte // reused line scratch; the reason a traced frame allocates nothing
	err error  // first write error, sticky

	start         time.Time
	payloadBytes  int
	timestamps    bool
	flushInterval time.Duration

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

// Verify at compile time that the thing this package exists to provide is in
// fact the thing frame asks for.
var _ frame.Tracer = (*TextTracer)(nil)

// New returns a TextTracer writing to w.
//
// It starts a background goroutine to flush buffered output, so the returned
// tracer MUST be closed — the same contract as time.Ticker. Pass
// WithFlushInterval(0) for a tracer with no goroutine at all.
func New(w io.Writer, opts ...Option) *TextTracer {
	t := &TextTracer{
		bw:            bufio.NewWriterSize(w, defaultBufSize),
		buf:           make([]byte, 0, 256),
		start:         time.Now(),
		timestamps:    true,
		flushInterval: defaultFlushInterval,
	}
	for _, o := range opts {
		o(t)
	}
	if t.flushInterval > 0 {
		t.stop = make(chan struct{})
		t.wg.Add(1)
		go t.flushLoop()
	}
	return t
}

// Tracer returns t as a frame.Tracer, or a nil interface when t is nil.
//
// Assigning a nil *TextTracer straight into a frame.Tracer field produces a
// non-nil interface holding a nil pointer, which defeats the nil check at every
// emit site: tracing would be "on", calling into a no-op, on a build that asked
// for no tracing at all. This is the conversion that does not do that.
func (t *TextTracer) Tracer() frame.Tracer {
	if t == nil {
		return nil
	}
	return t
}

// flushLoop pushes buffered output to the writer every flushInterval.
func (t *TextTracer) flushLoop() {
	defer t.wg.Done()
	tick := time.NewTicker(t.flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			_ = t.Flush()
		case <-t.stop:
			return
		}
	}
}

// Flush writes any buffered lines to the underlying writer.
func (t *TextTracer) Flush() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	err := t.bw.Flush()
	if err != nil && t.err == nil {
		t.err = err
	}
	return err
}

// Close stops the background flusher and flushes what is left. It is idempotent
// and safe to call on a nil *TextTracer, so `defer tr.Close()` needs no guard.
//
// It returns the first write error the tracer ever saw, not just this flush's:
// a debug log that silently stopped writing an hour ago is worth hearing about
// at the end.
func (t *TextTracer) Close() error {
	if t == nil {
		return nil
	}
	t.stopOnce.Do(func() {
		if t.stop != nil {
			close(t.stop)
		}
	})
	t.wg.Wait()
	_ = t.Flush()
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// Err returns the first write error the tracer saw, or nil.
func (t *TextTracer) Err() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// TraceFrame implements frame.Tracer.
//
// It holds a mutex for the length of one line render plus a buffered write.
// The lock is what makes a single tracer legal across the reader goroutine and
// the write side at once, which frame.Tracer requires of every implementation;
// the contention is between exactly two goroutines per connection.
func (t *TextTracer) TraceFrame(fi *frame.FrameInfo) {
	if t == nil || fi == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.buf[:0]
	if t.timestamps {
		b = appendElapsed(b, time.Since(t.start))
		b = append(b, ' ')
	}
	if fi.Dir == frame.DirSend {
		b = append(b, "-> "...)
	} else {
		b = append(b, "<- "...)
	}
	b = appendHeader(b, fi.Header)
	b = t.appendDetail(b, fi)
	b = append(b, '\n')
	t.buf = b
	if _, err := t.bw.Write(b); err != nil && t.err == nil {
		t.err = err
	}
}

// appendHeader renders the frame header. It does not use FrameHeader.String,
// which allocates: this runs once per frame.
func appendHeader(b []byte, h frame.FrameHeader) []byte {
	b = append(b, h.Type.String()...)
	// stream= is printed even when it is zero. On SETTINGS and PING it is noise,
	// but on WINDOW_UPDATE the difference between the connection window and a
	// stream's is the whole question, and a column that is sometimes there is
	// harder to read — and to grep — than one that always is.
	b = append(b, " stream="...)
	b = strconv.AppendUint(b, uint64(h.StreamID), 10)
	b = append(b, " len="...)
	b = strconv.AppendUint(b, uint64(h.Length), 10)
	if h.Flags != 0 {
		b = append(b, " flags="...)
		b = h.Flags.AppendFor(b, h.Type)
	}
	return b
}

// appendDetail renders the decoded fields that belong to this frame type, then
// the payload if one was asked for.
func (t *TextTracer) appendDetail(b []byte, fi *frame.FrameInfo) []byte {
	//exhaustive:ignore // Only the types carrying scalar detail appear here;
	// the rest are fully described by their header.
	switch fi.Header.Type {
	case frame.FrameRSTStream:
		b = append(b, " code="...)
		b = append(b, fi.ErrCode.String()...)
	case frame.FrameGoAway:
		b = append(b, " last="...)
		b = strconv.AppendUint(b, uint64(fi.LastStreamID), 10)
		b = append(b, " code="...)
		b = append(b, fi.ErrCode.String()...)
	case frame.FrameWindowUpdate:
		b = append(b, " inc="...)
		b = strconv.AppendUint(b, uint64(fi.WindowIncrement), 10)
	case frame.FramePushPromise:
		b = append(b, " promised="...)
		b = strconv.AppendUint(b, uint64(fi.PromisedID), 10)
	case frame.FramePing:
		b = append(b, " data="...)
		b = appendHex(b, fi.Ping[:])
	case frame.FrameSettings:
		if fi.Settings.N > 0 {
			b = append(b, " ["...)
			for i := 0; i < fi.Settings.N; i++ {
				if i > 0 {
					b = append(b, ' ')
				}
				p := fi.Settings.Pairs[i]
				b = append(b, p.ID.String()...)
				b = append(b, '=')
				b = strconv.AppendUint(b, uint64(p.Value), 10)
			}
			b = append(b, ']')
		}
	}
	if t.payloadBytes > 0 && len(fi.Payload) > 0 {
		b = append(b, " payload="...)
		n := min(len(fi.Payload), t.payloadBytes)
		b = appendHex(b, fi.Payload[:n])
		if rest := len(fi.Payload) - n; rest > 0 {
			b = append(b, "+"...)
			b = strconv.AppendInt(b, int64(rest), 10)
		}
	}
	return b
}

const hexDigits = "0123456789abcdef"

// appendHex renders p as lower-case hex without allocating.
func appendHex(b, p []byte) []byte {
	for _, c := range p {
		b = append(b, hexDigits[c>>4], hexDigits[c&0x0f])
	}
	return b
}

// appendElapsed renders d as seconds with six fractional digits, right-padded
// to a stable width so consecutive lines line up.
//
// Elapsed-since-start rather than wall clock: what a frame log answers is "how
// long after the request did the peer do that", and a column of 20:04:11.998412
// makes the reader do the subtraction on every line.
func appendElapsed(b []byte, d time.Duration) []byte {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	secs := us / 1e6
	// Pad to four integer digits, which keeps the column stable for the first
	// ~2.8 hours and then grows rather than misaligning silently.
	for pad := int64(1000); pad > 1 && secs < pad; pad /= 10 {
		b = append(b, ' ')
	}
	b = strconv.AppendInt(b, secs, 10)
	b = append(b, '.')
	frac := us % 1e6
	for div := int64(100000); div > 0; div /= 10 {
		b = append(b, byte('0'+(frac/div)%10))
	}
	return b
}
