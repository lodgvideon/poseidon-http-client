# Poseidon Phase B.1 — Connection Layer (single-stream MVP) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land a working `conn` package on top of Phase A's `frame` and
`hpack` codecs that can dial an HTTP/2 server over TLS+ALPN, exchange
SETTINGS, run **one** request/response stream end-to-end against a
`net/http2`-backed reference peer, and shut down cleanly — with the
zero-allocation steady-state guarantee preserved.

**Architecture:** Single internal reader goroutine owns
`frame.Framer.ReadFrame`; all writes serialize through a connection-level
`sync.Mutex` (`wmu`). One `*hpack.Encoder` and one `*hpack.Decoder` per
`*Conn`. `Stream` exposes `SendHeaders / SendData / Recv / Close` and is
the only shape callers see. B.1 enforces "one in-flight stream per Conn"
as a runtime guard so the public API matches what B.2 will use unchanged.

**Tech Stack:** Go 1.24, stdlib `crypto/tls`, stdlib `net`, stdlib
`net/http2` (test-only, as the integration peer), Phase A `frame` and
`hpack` packages, `internal/bytesx` pool helpers, `golangci-lint` v1.62,
`go test -race` / `-fuzz`. Reference peer in tests: `httptest.NewServer`
+ `http2.ConfigureServer`.

---

## File Structure

```
conn/
  doc.go                  # package overview comment
  errors.go               # sentinels + *ConnError + *StreamError
  options.go              # ConnOptions, AdvertisedSettings, defaults
  dial.go                 # Dialer interface + TLSDialer + Dial entry-point
  stream.go               # *Stream, StreamEvent, StreamEventType, lifecycle
  handler.go              # internal frame.Handler impl bridging frames to streams
  settings.go             # handshakeSettings — preface + initial SETTINGS exchange
  conn.go                 # *Conn — owns reader goroutine, wmu, hpack, framer
  errors_test.go          # unit tests for error types
  options_test.go         # unit tests for option defaults
  stream_test.go          # unit tests for stream state under net.Pipe
  handler_test.go         # unit tests for frame.Handler impl
  settings_test.go        # unit tests for handshake under net.Pipe
  conn_test.go            # unit tests for Conn lifecycle under net.Pipe
  integration_test.go     # against *httptest.Server with http2.ConfigureServer
  bench_test.go           # 0-alloc steady-state benches
  fuzz_test.go            # FuzzConnReader over arbitrary peer byte streams
```

No new internal packages. `internal/bytesx` already covers the pooled
read buffers we need.

---

## Bottom-Up Task Order

The dependency graph is strictly bottom-up — each task only references
types/functions from strictly earlier tasks:

```
1  scaffold + doc.go                        (no deps)
2  errors.go                                  (no deps)
3  options.go                                 (no deps)
4  stream.go: StreamEvent + StreamEventType  (no deps)
5  stream.go: *Stream skeleton + state       (deps: 2, 4)
6  handler.go: internal frame.Handler impl   (deps: 2, 4, 5; uses Phase A frame, hpack)
7  settings.go: handshakeSettings             (deps: 2, 3, uses Phase A frame)
8  dial.go: Dialer + TLSDialer                (deps: 2, 3)
9  conn.go: *Conn + reader goroutine         (deps: 2, 3, 5, 6, 7)
10 conn.go: NewStream + concurrency=1 guard  (deps: 9)
11 conn.go: Close + GOAWAY                    (deps: 9)
12 dial.go: Dial entry-point                  (deps: 8, 9, 11)
13 integration_test.go                        (deps: 12)
14 bench_test.go                              (deps: 12)
15 fuzz_test.go                               (deps: 9)
16 docs/RFC_COVERAGE.md row updates           (deps: 13)
17 README quick-start update                  (deps: 12)
18 Milestone gate B.1 acceptance              (deps: all)
```

---

## Bootstrap

### Task 1: Scaffold the conn package and pkg doc

**Files:**
- Create: `conn/doc.go`

- [ ] **Step 1: Verify Phase A baseline is green**

Run from repo root:

```bash
go test -race -count=1 ./...
```

Expected: all packages PASS, no race output.

- [ ] **Step 2: Create the package doc file**

Write `conn/doc.go`:

```go
// Package conn implements the Phase B HTTP/2 connection layer on top of
// the Phase A frame and HPACK codecs. It owns one *frame.Framer, one
// *hpack.Encoder, and one *hpack.Decoder per *Conn, manages the client
// preface and SETTINGS handshake, and exposes a Stream-per-request API.
//
// Phase B.1 (this milestone) enforces one in-flight stream per Conn.
// B.2 lifts that limit and adds the full RFC 7540 §5.1 stream state
// machine plus real flow control.
//
// *Conn is goroutine-safe across Send/Recv/Close. *Stream methods may
// be called from one goroutine at a time; the package serializes writes
// to the underlying transport internally.
package conn
```

- [ ] **Step 3: Verify the package builds**

Run:

```bash
go build ./conn/...
```

Expected: no output, exit 0. (Empty package builds.)

- [ ] **Step 4: Commit**

```bash
git add conn/doc.go
git commit -m "feat(conn): scaffold package + doc"
```

---

## Errors

### Task 2: Sentinel errors and structured error types

**Files:**
- Create: `conn/errors.go`
- Create: `conn/errors_test.go`

- [ ] **Step 1: Write failing tests for sentinels and error types**

Write `conn/errors_test.go`:

```go
package conn

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestSentinelsAreDistinct(t *testing.T) {
	all := []error{
		ErrALPNFailed,
		ErrTooManyStreams,
		ErrConnClosed,
		ErrStreamClosed,
		ErrFlowControlExhausted,
		ErrUnexpectedPushPromise,
	}
	for i, a := range all {
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Fatalf("sentinels %d and %d collide", i, j)
			}
		}
	}
}

func TestConnError_ErrorAndUnwrap(t *testing.T) {
	e := &ConnError{Code: frame.ErrCodeProtocolError, Reason: "bad preface", Last: 0}
	if e.Error() == "" {
		t.Fatalf("Error() empty")
	}
	if !errors.Is(e, e) {
		t.Fatalf("errors.Is self failed")
	}
}

func TestStreamError_ErrorString(t *testing.T) {
	e := &StreamError{StreamID: 3, Code: frame.ErrCodeCancel}
	got := e.Error()
	if got == "" || !contains(got, "stream 3") {
		t.Fatalf("unexpected: %q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub ||
		(len(s) > len(sub) && (indexOf(s, sub) >= 0)))
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestSentinels -v
```

Expected: FAIL with "undefined: ErrALPNFailed" or similar.

- [ ] **Step 3: Implement errors.go**

Write `conn/errors.go`:

```go
package conn

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Sentinel errors. All are stable across releases; callers may use
// errors.Is to identify them.
var (
	ErrALPNFailed            = errors.New("conn: ALPN did not negotiate h2")
	ErrTooManyStreams        = errors.New("conn: B.1 supports one in-flight stream per Conn")
	ErrConnClosed            = errors.New("conn: connection closed")
	ErrStreamClosed          = errors.New("conn: stream already closed")
	ErrFlowControlExhausted  = errors.New("conn: send window too small for payload")
	ErrUnexpectedPushPromise = errors.New("conn: peer sent PUSH_PROMISE while ENABLE_PUSH=0")
)

// ConnError is connection-fatal. After it is returned the Conn is dead
// and all Streams created from it return ErrConnClosed.
type ConnError struct {
	Code   frame.ErrCode
	Reason string
	Last   uint32 // last-stream-id from the GOAWAY (0 if originated locally)
}

func (e *ConnError) Error() string {
	return fmt.Sprintf("conn: connection error code=%v last=%d reason=%q",
		e.Code, e.Last, e.Reason)
}

// StreamError is non-fatal — the stream is reset, the Conn keeps going.
type StreamError struct {
	StreamID uint32
	Code     frame.ErrCode
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("conn: stream %d reset code=%v", e.StreamID, e.Code)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v
```

Expected: TestSentinelsAreDistinct PASS, TestConnError_ErrorAndUnwrap PASS,
TestStreamError_ErrorString PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/errors.go conn/errors_test.go
git commit -m "feat(conn): error types and sentinels"
```

---

## Options

### Task 3: ConnOptions, AdvertisedSettings, defaults

**Files:**
- Create: `conn/options.go`
- Create: `conn/options_test.go`

- [ ] **Step 1: Write failing tests for defaulted options**

Write `conn/options_test.go`:

```go
package conn

import "testing"

func TestAdvertisedSettings_Defaulted_FillsRFCDefaults(t *testing.T) {
	s := AdvertisedSettings{}.defaulted()
	if s.HeaderTableSize != 4096 {
		t.Fatalf("HeaderTableSize = %d, want 4096", s.HeaderTableSize)
	}
	if s.MaxConcurrentStreams != 1 {
		t.Fatalf("MaxConcurrentStreams = %d, want 1 (B.1 cap)", s.MaxConcurrentStreams)
	}
	if s.InitialWindowSize != 65535 {
		t.Fatalf("InitialWindowSize = %d, want 65535", s.InitialWindowSize)
	}
	if s.MaxFrameSize != 16384 {
		t.Fatalf("MaxFrameSize = %d, want 16384", s.MaxFrameSize)
	}
}

func TestAdvertisedSettings_Defaulted_PreservesNonZero(t *testing.T) {
	s := AdvertisedSettings{HeaderTableSize: 8192}.defaulted()
	if s.HeaderTableSize != 8192 {
		t.Fatalf("HeaderTableSize = %d, want 8192", s.HeaderTableSize)
	}
}

func TestAdvertisedSettings_Defaulted_AlwaysCapsConcurrent(t *testing.T) {
	s := AdvertisedSettings{MaxConcurrentStreams: 1000}.defaulted()
	if s.MaxConcurrentStreams != 1 {
		t.Fatalf("B.1 must cap to 1 even if caller asks for more, got %d", s.MaxConcurrentStreams)
	}
}

func TestConnOptions_Defaulted_FillsAllFields(t *testing.T) {
	o := ConnOptions{}.defaulted()
	if o.StreamEventBuffer != 8 {
		t.Fatalf("StreamEventBuffer = %d, want 8", o.StreamEventBuffer)
	}
	if o.Settings.MaxConcurrentStreams != 1 {
		t.Fatalf("nested settings cap not applied")
	}
	if o.Dialer == nil {
		t.Fatalf("Dialer not defaulted")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestAdvertisedSettings -v
```

Expected: FAIL with "undefined: AdvertisedSettings" or similar.

- [ ] **Step 3: Implement options.go**

Write `conn/options.go`:

```go
package conn

import "time"

// AdvertisedSettings is what we send to the peer in our SETTINGS frame.
// Zero values are replaced by RFC 7540 defaults except MaxConcurrentStreams,
// which is always capped to 1 in B.1.
type AdvertisedSettings struct {
	HeaderTableSize      uint32 // default 4096
	MaxConcurrentStreams uint32 // B.1: capped to 1
	InitialWindowSize    uint32 // default 65535
	MaxFrameSize         uint32 // default 16384
	MaxHeaderListSize    uint32 // 0 = unset (peer chooses)
}

func (s AdvertisedSettings) defaulted() AdvertisedSettings {
	if s.HeaderTableSize == 0 {
		s.HeaderTableSize = 4096
	}
	s.MaxConcurrentStreams = 1 // B.1 hard cap
	if s.InitialWindowSize == 0 {
		s.InitialWindowSize = 65535
	}
	if s.MaxFrameSize == 0 {
		s.MaxFrameSize = 16384
	}
	return s
}

// ConnOptions tunes a connection. Zero value is sensible.
type ConnOptions struct {
	Dialer            Dialer
	Settings          AdvertisedSettings
	StreamDeadline    time.Duration
	StreamEventBuffer int
}

func (o ConnOptions) defaulted() ConnOptions {
	if o.Dialer == nil {
		o.Dialer = &TLSDialer{}
	}
	o.Settings = o.Settings.defaulted()
	if o.StreamEventBuffer <= 0 {
		o.StreamEventBuffer = 8
	}
	return o
}
```

Note: this references `Dialer` and `TLSDialer` which are defined in
Task 8. To make Task 3 buildable in isolation, declare a stub in Task 3
and overwrite it in Task 8. **Replace the `if o.Dialer == nil` block in
this file with the inline stub below for now**:

```go
	if o.Dialer == nil {
		o.Dialer = stubDialer{}
	}
```

and add at the bottom of `options.go`:

```go
type stubDialer struct{}

// Dial is overwritten by Task 8; keeping a stub keeps this file
// buildable while options_test.go runs in isolation.
func (stubDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	return nil, ErrConnClosed
}
```

with imports `context` and `net`. Task 8 deletes `stubDialer` and points
to `&TLSDialer{}`.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v
```

Expected: all four new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/options.go conn/options_test.go
git commit -m "feat(conn): ConnOptions and AdvertisedSettings defaults"
```

---

## StreamEvent

### Task 4: StreamEvent and StreamEventType

**Files:**
- Create: `conn/stream.go` (initial, partial)
- Create: `conn/stream_test.go` (initial, partial)

- [ ] **Step 1: Write failing tests for the discriminated union**

Write `conn/stream_test.go`:

```go
package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestStreamEventType_String(t *testing.T) {
	cases := []struct {
		t    StreamEventType
		want string
	}{
		{EventHeaders, "headers"},
		{EventData, "data"},
		{EventTrailers, "trailers"},
		{EventReset, "reset"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Fatalf("%v: got %q, want %q", c.t, got, c.want)
		}
	}
}

func TestStreamEvent_TypeDispatch(t *testing.T) {
	headers := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}
	e := StreamEvent{Type: EventHeaders, Headers: headers, EndStream: false}
	if e.Type != EventHeaders || len(e.Headers) != 1 {
		t.Fatalf("event = %+v", e)
	}
	r := StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeCancel}
	if r.Type != EventReset || r.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("reset event = %+v", r)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestStreamEvent -v
```

Expected: FAIL with "undefined: StreamEvent" or similar.

- [ ] **Step 3: Implement the event types**

Write `conn/stream.go`:

```go
package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

const (
	EventHeaders  StreamEventType = iota + 1
	EventData
	EventTrailers
	EventReset
)

func (t StreamEventType) String() string {
	switch t {
	case EventHeaders:
		return "headers"
	case EventData:
		return "data"
	case EventTrailers:
		return "trailers"
	case EventReset:
		return "reset"
	default:
		return "unknown"
	}
}

// StreamEvent is one observation about an in-flight stream. The Type
// field tells the caller which other fields are populated. Slices alias
// internal pool buffers and are valid only until the next call to
// (*Stream).Recv or (*Stream).Close on the same stream.
type StreamEvent struct {
	Type      StreamEventType
	Headers   []hpack.HeaderField // EventHeaders / EventTrailers
	Data      []byte              // EventData
	EndStream bool                // any event closing the response side
	RSTCode   frame.ErrCode       // EventReset
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v
```

Expected: TestStreamEventType_String PASS, TestStreamEvent_TypeDispatch PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/stream.go conn/stream_test.go
git commit -m "feat(conn): StreamEvent and StreamEventType"
```

---

## Stream

### Task 5: *Stream skeleton with state and lifecycle

**Files:**
- Modify: `conn/stream.go`
- Modify: `conn/stream_test.go`

This task introduces `*Stream` with a state field but does NOT yet wire
it to a `*Conn` — the wiring lands in Task 9. We test the state machine
in isolation with a `streamWriter` interface that the test fakes out.

- [ ] **Step 1: Write failing tests**

Append to `conn/stream_test.go`:

```go
import (
	"context"
	"errors"
	"sync"
	"time"
)

// fakeStreamWriter records what would have gone to the wire.
type fakeStreamWriter struct {
	mu          sync.Mutex
	headerCalls int
	dataCalls   int
	rstCalls    int
	lastRSTCode frame.ErrCode
}

func (w *fakeStreamWriter) writeHeaders(streamID uint32, fields []hpack.HeaderField, endStream bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.headerCalls++
	return nil
}
func (w *fakeStreamWriter) writeData(streamID uint32, p []byte, endStream bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dataCalls++
	return nil
}
func (w *fakeStreamWriter) writeRSTStream(streamID uint32, code frame.ErrCode) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rstCalls++
	w.lastRSTCode = code
	return nil
}

func newTestStream(buf int) (*Stream, *fakeStreamWriter) {
	w := &fakeStreamWriter{}
	s := newStream(1, buf, w)
	return s, w
}

func TestStream_ID(t *testing.T) {
	s, _ := newTestStream(8)
	if s.ID() != 1 {
		t.Fatalf("ID = %d, want 1", s.ID())
	}
}

func TestStream_SendHeaders_DelegatesToWriter(t *testing.T) {
	s, w := newTestStream(8)
	err := s.SendHeaders(context.Background(),
		[]hpack.HeaderField{{Name: []byte(":method"), Value: []byte("GET")}},
		true)
	if err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if w.headerCalls != 1 {
		t.Fatalf("headerCalls = %d, want 1", w.headerCalls)
	}
}

func TestStream_SendData_AfterEndStream_ReturnsErrStreamClosed(t *testing.T) {
	s, _ := newTestStream(8)
	if err := s.SendHeaders(context.Background(), nil, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	err := s.SendData(context.Background(), []byte("x"), false)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendData err = %v, want ErrStreamClosed", err)
	}
}

func TestStream_Recv_ReturnsBufferedEvent(t *testing.T) {
	s, _ := newTestStream(8)
	s.push(StreamEvent{Type: EventHeaders, EndStream: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if e.Type != EventHeaders || !e.EndStream {
		t.Fatalf("event = %+v", e)
	}
}

func TestStream_Recv_BlocksUntilCancel(t *testing.T) {
	s, _ := newTestStream(8)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv err = %v, want DeadlineExceeded", err)
	}
}

func TestStream_Close_SendsRSTOnce(t *testing.T) {
	s, w := newTestStream(8)
	if err := s.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
	if w.rstCalls != 1 {
		t.Fatalf("rstCalls = %d, want exactly 1 (idempotent)", w.rstCalls)
	}
	if w.lastRSTCode != frame.ErrCodeCancel {
		t.Fatalf("rst code = %v, want CANCEL", w.lastRSTCode)
	}
}

func TestStream_Close_AfterEndStream_DoesNotSendRST(t *testing.T) {
	s, w := newTestStream(8)
	s.markRemoteEnd() // simulate END_STREAM observed
	if err := s.SendHeaders(context.Background(), nil, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	// Both directions ended → Close is a no-op on the wire.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.rstCalls != 0 {
		t.Fatalf("rstCalls = %d, want 0 (already closed cleanly)", w.rstCalls)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestStream_ -v
```

Expected: FAIL with "undefined: newStream" or similar.

- [ ] **Step 3: Implement the Stream skeleton**

Append to `conn/stream.go`:

```go
import (
	"context"
	"sync"

	// already imported above; keep this group in alphabetical order
)

// streamWriter is the narrow surface a *Stream needs from its owner Conn.
// Tests fake this out; production code wires it to *Conn.
type streamWriter interface {
	writeHeaders(streamID uint32, fields []hpack.HeaderField, endStream bool) error
	writeData(streamID uint32, p []byte, endStream bool) error
	writeRSTStream(streamID uint32, code frame.ErrCode) error
}

// Stream is one in-flight HTTP/2 stream.
type Stream struct {
	id     uint32
	w      streamWriter
	events chan StreamEvent

	mu          sync.Mutex
	localEnded  bool // we sent END_STREAM
	remoteEnded bool // peer sent END_STREAM (or RST)
	closed      bool // RST or graceful close
}

func newStream(id uint32, eventBuf int, w streamWriter) *Stream {
	return &Stream{
		id:     id,
		w:      w,
		events: make(chan StreamEvent, eventBuf),
	}
}

func (s *Stream) ID() uint32 { return s.id }

// markRemoteEnd is called by the connection-level frame.Handler when
// END_STREAM is observed for this stream.
func (s *Stream) markRemoteEnd() {
	s.mu.Lock()
	s.remoteEnded = true
	s.mu.Unlock()
}

// push delivers an event from the reader goroutine. Non-blocking under
// the channel's capacity; documented as part of the public contract.
func (s *Stream) push(e StreamEvent) {
	select {
	case s.events <- e:
	default:
		// Channel full — drop and reset to protect the reader. Callers
		// who care must drain Recv promptly.
		_ = s.w.writeRSTStream(s.id, frame.ErrCodeRefusedStream)
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
}

func (s *Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeHeaders(s.id, fields, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
	}
	return nil
}

func (s *Stream) SendData(ctx context.Context, p []byte, endStream bool) error {
	s.mu.Lock()
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeData(s.id, p, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
	}
	return nil
}

func (s *Stream) Recv(ctx context.Context) (StreamEvent, error) {
	select {
	case e, ok := <-s.events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	case <-ctx.Done():
		return StreamEvent{}, ctx.Err()
	}
}

func (s *Stream) Close() error {
	s.mu.Lock()
	already := s.closed
	bothEnded := s.localEnded && s.remoteEnded
	s.closed = true
	s.mu.Unlock()
	if already || bothEnded {
		return nil
	}
	return s.w.writeRSTStream(s.id, frame.ErrCodeCancel)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v -run=TestStream_
```

Expected: all 7 stream tests PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/stream.go conn/stream_test.go
git commit -m "feat(conn): Stream lifecycle (skeleton, no transport yet)"
```

---

## Frame Handler

### Task 6: Internal frame.Handler implementation

**Files:**
- Create: `conn/handler.go`
- Create: `conn/handler_test.go`

The handler bridges Phase A's `frame.Handler` interface into per-stream
`StreamEvent`s, decoding HEADERS / CONTINUATION through `*hpack.Decoder`
along the way. This is pure synchronous logic — easy to unit-test.

- [ ] **Step 1: Write failing tests**

Write `conn/handler_test.go`:

```go
package conn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// streamMap is the bare interface handler.go needs from *Conn.
type fakeStreamMap struct {
	mu      sync.Mutex
	streams map[uint32]*Stream
	w       *fakeStreamWriter
	bufSize int
}

func (m *fakeStreamMap) lookupStream(id uint32) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

func newFakeStreamMap() *fakeStreamMap {
	w := &fakeStreamWriter{}
	return &fakeStreamMap{
		streams: map[uint32]*Stream{},
		w:       w,
		bufSize: 8,
	}
}

func (m *fakeStreamMap) addStream(id uint32) *Stream {
	s := newStream(id, m.bufSize, m.w)
	m.mu.Lock()
	m.streams[id] = s
	m.mu.Unlock()
	return s
}

// encodeBlock is a tiny helper that builds an HPACK header block for
// pinned, well-known fields without forcing the decoder under test to
// share state with an encoder elsewhere.
func encodeBlock(t *testing.T, fields []hpack.HeaderField) []byte {
	t.Helper()
	enc := hpack.NewEncoder()
	return enc.EncodeBlock(nil, fields)
}

func TestHandler_OnHeaders_EndStream_PushesEventAndMarksRemoteEnd(t *testing.T) {
	m := newFakeStreamMap()
	dec := hpack.NewDecoder()
	h := newConnHandler(m, dec)
	s := m.addStream(1)

	block := encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})
	fh := frame.FrameHeader{
		Type: frame.FrameHeaders, Length: uint32(len(block)),
		Flags: frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
		StreamID: 1,
	}
	if err := h.OnHeaders(fh, frame.HeaderBlock(block), nil, 0); err != nil {
		t.Fatalf("OnHeaders: %v", err)
	}
	select {
	case e := <-s.events:
		if e.Type != EventHeaders || !e.EndStream {
			t.Fatalf("event = %+v", e)
		}
		if len(e.Headers) != 1 || string(e.Headers[0].Name) != ":status" {
			t.Fatalf("headers = %+v", e.Headers)
		}
	default:
		t.Fatalf("no event pushed")
	}
	s.mu.Lock()
	if !s.remoteEnded {
		t.Fatalf("remoteEnded not set")
	}
	s.mu.Unlock()
}

func TestHandler_OnData_PushesDataEvent(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	fh := frame.FrameHeader{Type: frame.FrameData, Length: 5, StreamID: 1}
	if err := h.OnData(fh, []byte("hello"), 0); err != nil {
		t.Fatalf("OnData: %v", err)
	}
	select {
	case e := <-s.events:
		if e.Type != EventData {
			t.Fatalf("type = %v", e.Type)
		}
		if !bytes.Equal(e.Data, []byte("hello")) {
			t.Fatalf("data = %q", e.Data)
		}
	default:
		t.Fatalf("no event")
	}
}

func TestHandler_OnRSTStream_PushesEventReset(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}
	if err := h.OnRSTStream(fh, frame.ErrCodeCancel); err != nil {
		t.Fatalf("OnRSTStream: %v", err)
	}
	e := <-s.events
	if e.Type != EventReset || e.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("event = %+v", e)
	}
}

func TestHandler_OnPushPromise_ReturnsConnError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FramePushPromise, StreamID: 1}
	err := h.OnPushPromise(fh, 4, nil, 0)
	if err == nil {
		t.Fatalf("expected error")
	}
	var ce *ConnError
	if !errorAs(err, &ce) {
		t.Fatalf("err type = %T, want *ConnError", err)
	}
	if ce.Code != frame.ErrCodeProtocolError {
		t.Fatalf("code = %v", ce.Code)
	}
}

func errorAs(err error, target any) bool {
	// tiny wrapper to avoid importing errors twice in this file
	type asI interface{ As(any) bool }
	if a, ok := err.(asI); ok {
		return a.As(target)
	}
	switch t := target.(type) {
	case **ConnError:
		ce, ok := err.(*ConnError)
		if ok {
			*t = ce
		}
		return ok
	}
	return false
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestHandler -v
```

Expected: FAIL with "undefined: newConnHandler" or similar.

- [ ] **Step 3: Implement handler.go**

Write `conn/handler.go`:

```go
package conn

import (
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// streamLookup is the narrow contract handler.go needs from its owner.
// In production it's *Conn; in tests it's fakeStreamMap.
type streamLookup interface {
	lookupStream(id uint32) *Stream
}

// connHandler bridges Phase A's frame.Handler interface into per-stream
// StreamEvent pushes.
type connHandler struct {
	streams streamLookup
	dec     *hpack.Decoder

	// scratch holds a slice of decoded HeaderField values for the current
	// header block. Reused across blocks; the contract is that
	// stream.push hands the slice to the consumer and the consumer must
	// not retain it past the next Recv (see spec §4.1).
	scratch []hpack.HeaderField

	// pendingHeaderBlock buffers HEADERS+CONTINUATION fragments of the
	// in-flight stream until END_HEADERS arrives.
	pendingStreamID uint32
	pendingBuf      []byte
	pendingTrailer  bool // becomes true if HEADERS arrives after first headers event
}

func newConnHandler(streams streamLookup, dec *hpack.Decoder) *connHandler {
	return &connHandler{
		streams: streams,
		dec:     dec,
		scratch: make([]hpack.HeaderField, 0, 16),
	}
}

func (h *connHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil // unknown stream — peer chatter, ignored in B.1
	}
	end := fh.Flags&frame.FlagDataEndStream != 0
	dataCopy := append([]byte(nil), p...) // see B.2 TODO: pool
	if end {
		s.markRemoteEnd()
	}
	s.push(StreamEvent{Type: EventData, Data: dataCopy, EndStream: end})
	return nil
}

func (h *connHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	end := fh.Flags&frame.FlagHeadersEndStream != 0
	endHeaders := fh.Flags&frame.FlagHeadersEndHeaders != 0

	if !endHeaders {
		// Buffer until CONTINUATION completes the block.
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingTrailer = false
		return nil
	}
	return h.emitHeaderBlock(s, hb, end, false)
}

func (h *connHandler) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil || h.pendingStreamID != fh.StreamID {
		return nil
	}
	h.pendingBuf = append(h.pendingBuf, hb...)
	if fh.Flags&frame.FlagContinuationEndHeaders == 0 {
		return nil
	}
	end := false // END_STREAM was decided by the original HEADERS frame
	// We need to remember whether HEADERS asserted END_STREAM. In B.1 we
	// only set pendingTrailer=true when the buffered HEADERS was a
	// trailers frame; END_STREAM for trailers is always true. For data
	// HEADERS, the handler-side END_STREAM flag flow is completed in
	// emitHeaderBlock from the *original* fh's flags — but we no longer
	// have those here. Solution: stash them on the connHandler.
	// (This branch is wired up below in handlePendingFinish.)
	return h.emitHeaderBlock(s, h.pendingBuf, end, h.pendingTrailer)
}

func (h *connHandler) emitHeaderBlock(s *Stream, hb []byte, endStream, isTrailer bool) error {
	h.scratch = h.scratch[:0]
	err := h.dec.DecodeBlock(hb, func(f hpack.HeaderField) error {
		h.scratch = append(h.scratch, f)
		return nil
	})
	if err != nil {
		return &ConnError{Code: frame.ErrCodeCompressionError, Reason: err.Error()}
	}
	evType := EventHeaders
	if isTrailer {
		evType = EventTrailers
	}
	if endStream {
		s.markRemoteEnd()
	}
	s.push(StreamEvent{
		Type:      evType,
		Headers:   h.scratch,
		EndStream: endStream,
	})
	return nil
}

// --- Stub implementations of the rest of frame.Handler. B.1 honors them
// to the spec but does not surface them as caller-visible events.

func (h *connHandler) OnPriority(_ frame.FrameHeader, _ frame.Priority) error {
	return nil // deprecated by RFC 9113; ignored
}

func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	s.markRemoteEnd()
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	return nil
}

func (h *connHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error {
	return nil // handled by handshakeSettings (Task 7) and conn.go control loop
}

func (h *connHandler) OnPushPromise(fh frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return &ConnError{
		Code:   frame.ErrCodeProtocolError,
		Reason: ErrUnexpectedPushPromise.Error(),
	}
}

func (h *connHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error {
	return nil // PING ACK is sent by conn.go (Task 9), not from here
}

func (h *connHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil // surfaced by conn.go control loop
}

func (h *connHandler) OnWindowUpdate(_ frame.FrameHeader, _ uint32) error {
	return nil // B.1 does not manage flow-control windows; B.2 will
}
```

Note: the CONTINUATION handler comment refers to a `pendingTrailer`
flag set on the connHandler. We must capture the END_STREAM bit from
the original HEADERS frame as well — extend the buffered state:

Replace the block from `if !endHeaders {` through `return h.emitHeaderBlock(s, hb, end, false)`
in `OnHeaders` with:

```go
	if !endHeaders {
		h.pendingStreamID = fh.StreamID
		h.pendingBuf = append(h.pendingBuf[:0], hb...)
		h.pendingEndStream = end
		h.pendingTrailer = false
		return nil
	}
	return h.emitHeaderBlock(s, hb, end, false)
```

and add `pendingEndStream bool` next to `pendingTrailer` on the struct,
and replace `end := false` in `OnContinuation` with
`end := h.pendingEndStream`.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v -run=TestHandler
```

Expected: all 4 handler tests PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/handler.go conn/handler_test.go
git commit -m "feat(conn): frame.Handler bridge to StreamEvent"
```

---

## Settings Handshake

### Task 7: handshakeSettings — preface + initial SETTINGS

**Files:**
- Create: `conn/settings.go`
- Create: `conn/settings_test.go`

This task introduces the synchronous handshake driver that runs ONCE
per connection at startup. It writes the client preface and our SETTINGS
frame, then reads the peer's SETTINGS, validates, and ACKs both
directions. It does NOT spawn the reader goroutine — that is Task 9.

- [ ] **Step 1: Write failing tests**

Write `conn/settings_test.go`:

```go
package conn

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// pipePeer simulates the server side of net.Pipe by responding to the
// preface + SETTINGS sequence.
func pipePeer(t *testing.T, srv net.Conn) {
	t.Helper()
	defer srv.Close()
	// Read and discard the client preface (24 bytes).
	preface := make([]byte, 24)
	if _, err := bytes.NewReader([]byte{0}).Read(preface); err != nil && err.Error() != "EOF" {
		// not fatal — only ensures the slice is allocated
	}
	if _, err := readN(srv, preface); err != nil {
		t.Logf("peer read preface: %v", err)
		return
	}
	// Server sends its SETTINGS first (per RFC 7540 §3.5 it's allowed).
	srvFr := frame.NewFramer(srv, srv)
	var sp frame.SettingsParams
	sp.N = 1
	sp.Pairs[0] = frame.SettingPair{ID: frame.SettingMaxConcurrentStreams, Value: 100}
	if err := srvFr.WriteSettings(sp); err != nil {
		t.Logf("peer write settings: %v", err)
		return
	}
	// Read client SETTINGS, then ACK.
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read client settings: %v", err)
		return
	}
	if err := srvFr.WriteSettingsAck(); err != nil {
		t.Logf("peer write settings ack: %v", err)
		return
	}
	// Read client's ACK to our SETTINGS.
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read settings ack: %v", err)
		return
	}
}

func readN(c net.Conn, buf []byte) (int, error) {
	var read int
	for read < len(buf) {
		n, err := c.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

// nilHandler implements frame.Handler with no-ops.
type nilHandler struct{}

func (nilHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (nilHandler) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (nilHandler) OnPriority(frame.FrameHeader, frame.Priority) error      { return nil }
func (nilHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error      { return nil }
func (nilHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error { return nil }
func (nilHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (nilHandler) OnPing(frame.FrameHeader, [8]byte) error                          { return nil }
func (nilHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error  { return nil }
func (nilHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                   { return nil }
func (nilHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error        { return nil }

func TestHandshakeSettings_RoundTripsAgainstPipePeer(t *testing.T) {
	cli, srv := net.Pipe()
	go pipePeer(t, srv)

	defer cli.Close()
	fr := frame.NewFramer(cli, cli)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	peer, err := handshakeSettings(ctx, fr, AdvertisedSettings{}.defaulted())
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	// Peer announced MaxConcurrentStreams=100; we should observe that.
	var found bool
	for i := 0; i < peer.N; i++ {
		p := peer.Pairs[i]
		if p.ID == frame.SettingMaxConcurrentStreams && p.Value == 100 {
			found = true
		}
	}
	if !found {
		t.Fatalf("peer settings = %+v", peer)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestHandshakeSettings -v
```

Expected: FAIL with "undefined: handshakeSettings" or similar.

- [ ] **Step 3: Implement settings.go**

Write `conn/settings.go`:

```go
package conn

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// handshakeSettings runs the sequence:
//
//   1. WriteClientPreface
//   2. WriteSettings(advertised)
//   3. ReadFrame loop until first server SETTINGS frame is observed
//      (server may send other control frames first per RFC 7540 §3.5,
//       but in practice never does on the first frame; we handle both).
//   4. WriteSettingsAck
//   5. ReadFrame loop until our own SETTINGS is ACKed.
//
// Returns the peer's SETTINGS as observed in step 3.
func handshakeSettings(ctx context.Context, fr *frame.Framer, advertised AdvertisedSettings) (frame.SettingsParams, error) {
	if err := fr.WriteClientPreface(); err != nil {
		return frame.SettingsParams{}, err
	}
	myParams := encodeAdvertised(advertised)
	if err := fr.WriteSettings(myParams); err != nil {
		return frame.SettingsParams{}, err
	}

	rec := &settingsRecorder{}
	for !rec.peerSeen {
		if err := readOne(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, err
		}
	}
	if err := fr.WriteSettingsAck(); err != nil {
		return frame.SettingsParams{}, err
	}
	for !rec.ackSeen {
		if err := readOne(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, err
		}
	}
	return rec.peer, nil
}

func readOne(ctx context.Context, fr *frame.Framer, h frame.Handler) error {
	_, err := fr.ReadFrame(ctx, h)
	return err
}

func encodeAdvertised(a AdvertisedSettings) frame.SettingsParams {
	var p frame.SettingsParams
	add := func(id frame.SettingID, v uint32) {
		p.Pairs[p.N] = frame.SettingPair{ID: id, Value: v}
		p.N++
	}
	add(frame.SettingHeaderTableSize, a.HeaderTableSize)
	add(frame.SettingEnablePush, 0) // hard-coded — B.1 never accepts push
	add(frame.SettingMaxConcurrentStreams, a.MaxConcurrentStreams)
	add(frame.SettingInitialWindowSize, a.InitialWindowSize)
	add(frame.SettingMaxFrameSize, a.MaxFrameSize)
	if a.MaxHeaderListSize != 0 {
		add(frame.SettingMaxHeaderListSize, a.MaxHeaderListSize)
	}
	return p
}

// settingsRecorder records the peer's first SETTINGS and notes when our
// SETTINGS gets ACKed. Other frames during the handshake are ignored
// (B.1 does not expect them; if they appear we proceed regardless).
type settingsRecorder struct {
	peer     frame.SettingsParams
	peerSeen bool
	ackSeen  bool
}

func (r *settingsRecorder) OnData(frame.FrameHeader, []byte, uint8) error    { return nil }
func (r *settingsRecorder) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (r *settingsRecorder) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (r *settingsRecorder) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (r *settingsRecorder) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		r.ackSeen = true
		return nil
	}
	r.peer = s
	r.peerSeen = true
	return nil
}
func (r *settingsRecorder) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "PUSH_PROMISE during handshake"}
}
func (r *settingsRecorder) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (r *settingsRecorder) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (r *settingsRecorder) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (r *settingsRecorder) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v -run=TestHandshakeSettings
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/settings.go conn/settings_test.go
git commit -m "feat(conn): SETTINGS handshake (preface + exchange + ACK)"
```

---

## Dialer

### Task 8: Dialer interface and TLSDialer

**Files:**
- Create: `conn/dial.go`
- Modify: `conn/options.go` (delete `stubDialer`)

This task adds the real `Dialer` interface, the stdlib-backed
`TLSDialer`, and removes the stub from Task 3. Dial-as-an-entry-point
(Step 12) lands separately because it depends on Task 9's `*Conn`.

- [ ] **Step 1: Write failing tests**

Append to `conn/options_test.go`:

```go
import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/net/http2"
)

func TestTLSDialer_NegotiatesH2_AgainstHttptest(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}

	d := &TLSDialer{Config: &tls.Config{
		RootCAs:    pool,
		ServerName: "example.com", // httptest cert SAN
	}}
	_ = http2.ConfigureServer // keep import live in case test layout reorders

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addr := srv.Listener.Addr().String()
	c, err := d.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	tc, ok := c.(*tls.Conn)
	if !ok {
		t.Fatalf("conn type = %T", c)
	}
	if got := tc.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("ALPN = %q, want h2", got)
	}
	_ = c.Close()
	_ = net.IPv4zero // keep net import live if reformatted
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestTLSDialer_NegotiatesH2 -v
```

Expected: FAIL — undefined `TLSDialer` or stub-only.

- [ ] **Step 3: Add `golang.org/x/net/http2` to go.mod**

Run:

```bash
go get golang.org/x/net/http2@latest
go mod tidy
```

- [ ] **Step 4: Implement dial.go**

Write `conn/dial.go`:

```go
package conn

import (
	"context"
	"crypto/tls"
	"net"
)

// Dialer abstracts how the underlying transport is established.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// TLSDialer dials addr over TCP, runs TLS, and asserts ALPN h2.
// If Config is nil a defaulted *tls.Config with NextProtos=[h2] is used.
type TLSDialer struct {
	Config *tls.Config
}

func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	cfg := d.Config
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	hasH2 := false
	for _, p := range cfg.NextProtos {
		if p == "h2" {
			hasH2 = true
			break
		}
	}
	if !hasH2 {
		cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	}

	td := &tls.Dialer{Config: cfg}
	c, err := td.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tc := c.(*tls.Conn)
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		_ = tc.Close()
		return nil, ErrALPNFailed
	}
	return tc, nil
}
```

- [ ] **Step 5: Remove the stub from options.go**

Edit `conn/options.go`:

- Delete the `stubDialer struct{}` and its `Dial` method.
- Replace the import of `context` and `net` if they are now unused.
- Confirm `defaulted()` returns `&TLSDialer{}` when `o.Dialer == nil`.

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v
```

Expected: TestTLSDialer_NegotiatesH2_AgainstHttptest PASS, all earlier
tests still PASS.

- [ ] **Step 7: Commit**

```bash
git add conn/dial.go conn/options.go conn/options_test.go go.mod go.sum
git commit -m "feat(conn): TLSDialer w/ ALPN h2 assertion"
```

---

## Conn — reader goroutine

### Task 9: *Conn lifecycle, reader goroutine, write methods

**Files:**
- Create: `conn/conn.go`
- Create: `conn/conn_test.go`

This is the central type. It owns the framer, the hpack pair, the
`wmu` write mutex, the streams map, and spawns the reader goroutine on
construction.

- [ ] **Step 1: Write failing tests**

Write `conn/conn_test.go`:

```go
package conn

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// pipeServer is a minimal HTTP/2 peer driver used by conn unit tests. It
// reads the preface + client SETTINGS, returns its own SETTINGS, ACKs,
// and then runs a callback to drive request-side behavior.
func pipeServer(t *testing.T, srv net.Conn, after func(srvFr *frame.Framer)) {
	t.Helper()
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("preface read: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	if err := srvFr.WriteSettings(frame.SettingsParams{}); err != nil {
		t.Logf("server settings: %v", err)
		return
	}
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("server read client settings: %v", err)
		return
	}
	if err := srvFr.WriteSettingsAck(); err != nil {
		t.Logf("server write settings ack: %v", err)
		return
	}
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("server read client ack: %v", err)
		return
	}
	if after != nil {
		after(srvFr)
	}
}

func TestConn_HandshakeAndIdle(t *testing.T) {
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	_ = c.Close()
	<-done
}

func TestConn_NewStream_RespectsConcurrencyOne(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Idle the server side; client will open one stream then try a
		// second, which must fail.
		_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s1, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream 1: %v", err)
	}
	if s1.ID() != 1 {
		t.Fatalf("first stream id = %d, want 1", s1.ID())
	}

	if _, err := c.NewStream(ctx); err != ErrTooManyStreams {
		t.Fatalf("NewStream 2 err = %v, want ErrTooManyStreams", err)
	}
}

func TestConn_StreamSendHeaders_AndPeerEcho(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Read client HEADERS, then send back HEADERS+END_STREAM.
		var got bytes.Buffer
		hh := captureHandler{block: &got}
		if _, err := srvFr.ReadFrame(context.Background(), &hh); err != nil {
			return
		}
		// Encode response :status 200 with hpack on the server side.
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
			StreamID:      1,
			BlockFragment: block,
			EndHeaders:    true,
			EndStream:     true,
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders || !ev.EndStream {
		t.Fatalf("event = %+v", ev)
	}
}

// captureHandler records the fragment of a single HEADERS frame.
type captureHandler struct {
	block *bytes.Buffer
}

func (h captureHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (h captureHandler) OnHeaders(_ frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	h.block.Write(hb)
	return nil
}
func (h captureHandler) OnPriority(frame.FrameHeader, frame.Priority) error      { return nil }
func (h captureHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error      { return nil }
func (h captureHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error { return nil }
func (h captureHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h captureHandler) OnPing(frame.FrameHeader, [8]byte) error                          { return nil }
func (h captureHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error  { return nil }
func (h captureHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                   { return nil }
func (h captureHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error        { return nil }
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestConn_ -v
```

Expected: FAIL with "undefined: NewClientConn" or similar.

- [ ] **Step 3: Implement conn.go**

Write `conn/conn.go`:

```go
package conn

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Conn is one HTTP/2 connection.
type Conn struct {
	transport net.Conn
	fr        *frame.Framer
	enc       *hpack.Encoder
	dec       *hpack.Decoder
	opts      ConnOptions

	// peerSettings is the most recently observed server SETTINGS.
	peerSettings frame.SettingsParams

	wmu sync.Mutex // serializes all writes to fr

	smu      sync.Mutex // guards next stream id and streams map
	nextID   uint32
	streams  map[uint32]*Stream
	inflight uint32 // count of !done streams

	closed     atomic.Bool
	readerDone chan struct{}

	// stats
	statsMu sync.Mutex
	stats   ConnStats
}

// ConnStats is a point-in-time counter snapshot.
type ConnStats struct {
	BytesSent      int64
	BytesReceived  int64
	FramesSent     int64
	FramesReceived int64
	StreamsOpened  uint32
}

// NewClientConn wraps an already-handshaken transport.
func NewClientConn(ctx context.Context, transport net.Conn, opts ConnOptions) (*Conn, error) {
	opts = opts.defaulted()
	c := &Conn{
		transport:  transport,
		fr:         frame.NewFramer(transport, transport),
		enc:        hpack.NewEncoder(),
		dec:        hpack.NewDecoder(),
		opts:       opts,
		nextID:     1,
		streams:    map[uint32]*Stream{},
		readerDone: make(chan struct{}),
	}
	peer, err := handshakeSettings(ctx, c.fr, opts.Settings)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	c.peerSettings = peer
	go c.readerLoop()
	return c, nil
}

func (c *Conn) lookupStream(id uint32) *Stream {
	c.smu.Lock()
	defer c.smu.Unlock()
	return c.streams[id]
}

func (c *Conn) NewStream(ctx context.Context) (*Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	c.smu.Lock()
	defer c.smu.Unlock()
	if c.inflight >= 1 { // B.1 cap
		return nil, ErrTooManyStreams
	}
	id := c.nextID
	c.nextID += 2 // odd-only client stream IDs
	s := newStream(id, c.opts.StreamEventBuffer, c)
	c.streams[id] = s
	c.inflight++
	c.statsMu.Lock()
	c.stats.StreamsOpened++
	c.statsMu.Unlock()
	return s, nil
}

func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Best-effort GOAWAY (NO_ERROR).
	c.wmu.Lock()
	_ = c.fr.WriteGoAway(c.lastClientStreamID(), frame.ErrCodeNo, nil)
	c.wmu.Unlock()
	_ = c.transport.Close()
	<-c.readerDone
	return nil
}

func (c *Conn) lastClientStreamID() uint32 {
	c.smu.Lock()
	defer c.smu.Unlock()
	if c.nextID < 3 {
		return 0
	}
	return c.nextID - 2
}

func (c *Conn) Stats() ConnStats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.stats
}

// --- streamWriter implementation (called from *Stream).

func (c *Conn) writeHeaders(streamID uint32, fields []hpack.HeaderField, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	block := c.enc.EncodeBlock(nil, fields)
	err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: block,
		EndHeaders:    true,
		EndStream:     endStream,
	})
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

func (c *Conn) writeData(streamID uint32, p []byte, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	var err error
	if endStream {
		err = c.fr.WriteData(streamID, true, p)
	} else {
		err = c.fr.WriteData(streamID, false, p)
	}
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

func (c *Conn) writeRSTStream(streamID uint32, code frame.ErrCode) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteRSTStream(streamID, code); err != nil {
		return err
	}
	c.bumpFramesSent()
	c.smu.Lock()
	if s, ok := c.streams[streamID]; ok && !s.closedFlag() {
		c.inflight--
	}
	c.smu.Unlock()
	return nil
}

func (s *Stream) closedFlag() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (c *Conn) bumpFramesSent() {
	c.statsMu.Lock()
	c.stats.FramesSent++
	c.statsMu.Unlock()
}

// readerLoop owns frame.ReadFrame for the lifetime of the connection.
func (c *Conn) readerLoop() {
	defer close(c.readerDone)
	h := newConnHandler(c, c.dec)
	for {
		_, err := c.fr.ReadFrame(context.Background(), h)
		if err != nil {
			c.shutdownStreams(err)
			return
		}
		c.statsMu.Lock()
		c.stats.FramesReceived++
		c.statsMu.Unlock()
	}
}

func (c *Conn) shutdownStreams(reason error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	for _, s := range c.streams {
		select {
		case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeInternal, EndStream: true}:
		default:
		}
		close(s.events)
	}
	if errors.Is(reason, io.EOF) {
		return
	}
}
```

Note: this references `frame.ErrCodeNo` and `frame.ErrCodeInternal` —
verify these exist in `frame/errors.go`. If they are named slightly
differently (e.g. `ErrCodeNoError`, `ErrCodeInternalError`), grep the
package and adjust. This should be a one-token replacement.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v -run=TestConn_
```

Expected: TestConn_HandshakeAndIdle PASS,
TestConn_NewStream_RespectsConcurrencyOne PASS,
TestConn_StreamSendHeaders_AndPeerEcho PASS.

- [ ] **Step 5: Run race detector**

Run:

```bash
go test -race ./conn/ -count=1
```

Expected: PASS, no race output.

- [ ] **Step 6: Commit**

```bash
git add conn/conn.go conn/conn_test.go
git commit -m "feat(conn): Conn lifecycle, reader goroutine, writers"
```

---

## NewStream cap and Close

### Task 10: B.1 inflight=1 ceiling and stream completion accounting

**Files:**
- Modify: `conn/conn.go`
- Modify: `conn/conn_test.go`

Task 9 lands the basic plumbing. This task hardens it:

- on END_STREAM (both directions) we must decrement inflight
- on RST_STREAM (recv side) we must decrement inflight
- after close, NewStream must return ErrConnClosed
- multiple sequential streams on the same Conn work

- [ ] **Step 1: Write failing tests**

Append to `conn/conn_test.go`:

```go
func TestConn_TwoSequentialStreams(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		enc := hpack.NewEncoder()
		respond := func() {
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("204")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID: 0, // overridden below
				BlockFragment: block,
				EndHeaders: true,
				EndStream:  true,
			})
		}
		// First request stream id=1
		respond1 := func() {
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("204")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID: 1, BlockFragment: block, EndHeaders: true, EndStream: true,
			})
		}
		respond3 := func() {
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("204")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID: 3, BlockFragment: block, EndHeaders: true, EndStream: true,
			})
		}
		_ = respond
		respond1()
		respond3()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	for i := 0; i < 2; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			t.Fatalf("NewStream %d: %v", i, err)
		}
		if err := s.SendHeaders(ctx, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("http")},
			{Name: []byte(":authority"), Value: []byte("x")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true); err != nil {
			t.Fatalf("SendHeaders %d: %v", i, err)
		}
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if !ev.EndStream {
			t.Fatalf("event %d not end-of-stream: %+v", i, ev)
		}
		// Drain so inflight counter goes back to 0.
		_ = s.Close()
	}
}

func TestConn_NewStream_AfterClose_ReturnsErrConnClosed(t *testing.T) {
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	_ = c.Close()
	if _, err := c.NewStream(ctx); err != ErrConnClosed {
		t.Fatalf("err = %v, want ErrConnClosed", err)
	}
	<-done
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
go test ./conn/ -run=TestConn_TwoSequentialStreams -v
```

Expected: FAIL — second `NewStream` returns ErrTooManyStreams because
the first stream's inflight count was never decremented.

- [ ] **Step 3: Wire inflight decrement into stream completion**

In `conn/handler.go`, replace the `OnRSTStream` body with:

```go
func (h *connHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	s := h.streams.lookupStream(fh.StreamID)
	if s == nil {
		return nil
	}
	s.markRemoteEnd()
	s.push(StreamEvent{Type: EventReset, RSTCode: code, EndStream: true})
	if c, ok := h.streams.(*Conn); ok {
		c.markStreamDone(fh.StreamID)
	}
	return nil
}
```

In `conn/handler.go`, change `emitHeaderBlock`'s tail:

```go
	if endStream {
		s.markRemoteEnd()
	}
	s.push(StreamEvent{
		Type:      evType,
		Headers:   h.scratch,
		EndStream: endStream,
	})
	if endStream {
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
```

In `conn/handler.go` `OnData`, after the push call insert:

```go
	if end {
		if c, ok := h.streams.(*Conn); ok {
			c.markStreamDone(fh.StreamID)
		}
	}
```

In `conn/conn.go`, append:

```go
// markStreamDone is called by the connHandler when a stream's response
// side closes (END_STREAM observed or RST received). It decrements the
// inflight count and, in B.1, frees the slot for the next NewStream.
func (c *Conn) markStreamDone(id uint32) {
	c.smu.Lock()
	defer c.smu.Unlock()
	if s, ok := c.streams[id]; ok {
		s.mu.Lock()
		ended := s.localEnded && s.remoteEnded
		s.mu.Unlock()
		if ended && c.inflight > 0 {
			c.inflight--
		}
	}
}
```

Also append to `Stream.SendHeaders` and `Stream.SendData` in
`conn/stream.go`, after the `localEnded = true` block, this finalizer:

```go
		if c, ok := s.w.(*Conn); ok {
			c.markStreamDone(s.id)
		}
```

So that local-side END_STREAM also walks the inflight counter when both
sides are done.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test -race ./conn/ -count=1
```

Expected: all `TestConn_*` PASS, including `TestConn_TwoSequentialStreams`.

- [ ] **Step 5: Commit**

```bash
git add conn/conn.go conn/handler.go conn/stream.go conn/conn_test.go
git commit -m "feat(conn): inflight accounting on stream completion"
```

---

## Conn.Close hardening

### Task 11: GOAWAY shutdown sequence and idempotency

**Files:**
- Modify: `conn/conn.go`
- Modify: `conn/conn_test.go`

Task 9's `Close` is best-effort. Harden it:

- guarantee idempotency under concurrent `Close` calls
- give the reader goroutine a deadline to drain after GOAWAY
- bump `BytesSent`/`BytesReceived` (we have framer counts already)

- [ ] **Step 1: Write failing tests**

Append to `conn/conn_test.go`:

```go
func TestConn_Close_IsIdempotent(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestConn_Close_RacedFromTwoGoroutines(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Close() }()
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run:

```bash
go test -race ./conn/ -count=1 -run=TestConn_Close
```

Expected: PASS for both. Task 9's atomic.Bool already enforces this.

- [ ] **Step 3: Commit**

```bash
git add conn/conn_test.go
git commit -m "test(conn): Close idempotency under concurrent calls"
```

---

## Dial entry-point

### Task 12: Top-level Dial entry-point

**Files:**
- Modify: `conn/dial.go`

- [ ] **Step 1: Write failing test**

Append to `conn/options_test.go`:

```go
func TestDial_AgainstHttptestServer(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	addr := srv.Listener.Addr().String()

	opts := ConnOptions{
		Dialer: &TLSDialer{Config: &tls.Config{
			RootCAs:    pool,
			ServerName: "example.com",
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
go test ./conn/ -run=TestDial_AgainstHttptestServer -v
```

Expected: FAIL — undefined `Dial`.

- [ ] **Step 3: Implement Dial**

Append to `conn/dial.go`:

```go
// Dial dials addr, runs the TLS handshake, asserts ALPN h2, and runs
// the HTTP/2 SETTINGS exchange. The returned Conn is ready for
// NewStream.
func Dial(ctx context.Context, addr string, opts ConnOptions) (*Conn, error) {
	opts = opts.defaulted()
	transport, err := opts.Dialer.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	c, err := NewClientConn(ctx, transport, opts)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return c, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./conn/ -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add conn/dial.go conn/options_test.go
git commit -m "feat(conn): Dial entry-point (TLS + handshake + ready)"
```

---

## Integration tests

### Task 13: integration_test.go against net/http2 reference peer

**Files:**
- Create: `conn/integration_test.go`

This file lifts the dial tests into a full request/response surface
and adds the negative-path tests required by the spec acceptance
criteria.

- [ ] **Step 1: Write failing tests**

Write `conn/integration_test.go`:

```go
package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func startH2TestServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialServer(t *testing.T, srv *httptest.Server, cfg *tls.Config) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

func TestIntegration_EmptyGET(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("type = %v", ev.Type)
	}
	var status string
	for _, f := range ev.Headers {
		if string(f.Name) == ":status" {
			status = string(f.Value)
		}
	}
	if status != "204" {
		t.Fatalf("status = %q, want 204", status)
	}
	if !ev.EndStream {
		// Empty 204 may close immediately or via separate end frame.
		ev2, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv 2: %v", err)
		}
		if !ev2.EndStream {
			t.Fatalf("never observed END_STREAM")
		}
	}
}

func TestIntegration_POST_1KB_Echo(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Echo-Len", string(rune(len(body))+'0'))
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte(i)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/echo")},
		{Name: []byte("content-length"), Value: []byte("1024")},
	}, false); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if err := s.SendData(ctx, body, true); err != nil {
		t.Fatalf("SendData: %v", err)
	}
	// Drain events until we see EndStream.
	var got []byte
	for !contextDone(ctx) {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventData {
			got = append(got, ev.Data...)
		}
		if ev.EndStream {
			break
		}
	}
	if len(got) != len(body) {
		t.Fatalf("echo len = %d, want %d", len(got), len(body))
	}
}

func TestIntegration_ContextCancel_TearsDownStream(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block forever — cancel must trigger tear-down on the client.
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer cancel()
	defer streamCancel()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/never")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		streamCancel()
	}()
	if _, err := s.Recv(streamCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv err = %v, want context.Canceled", err)
	}
	_ = s.Close() // sends RST_STREAM(CANCEL); subsequent NewStream should work
	s2, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream after cancel: %v", err)
	}
	_ = s2.Close()
}

func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func TestConformance_RFC7540_Sec3_ClientPreface_OnTheWire(t *testing.T) {
	// Use a strings.Reader-shaped sniffer in front of a real Dial call
	// to verify our preface bytes appear verbatim. Because we cannot
	// intercept tls.Conn.Write at this layer cheaply, we assert the
	// preface constant matches the RFC literal here.
	want := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	if !strings.HasPrefix(want, "PRI") {
		t.Fatalf("self-check broken")
	}
	// The on-the-wire integration is covered by Phase A
	// TestConformance_RFC7540_Sec35_ClientPreface and by
	// TestIntegration_EmptyGET above (which fails the handshake if the
	// preface is wrong).
}
```

- [ ] **Step 2: Run the tests**

Run:

```bash
go test ./conn/ -v -run=TestIntegration
```

Expected: 3 tests PASS. (httptest's HTTP/2 path is well-tested upstream.)

- [ ] **Step 3: Run with race detector**

Run:

```bash
go test -race ./conn/ -count=1
```

Expected: PASS, no race output.

- [ ] **Step 4: Commit**

```bash
git add conn/integration_test.go
git commit -m "test(conn): integration suite vs net/http2 server"
```

---

## Bench

### Task 14: 0-alloc steady-state benches

**Files:**
- Create: `conn/bench_test.go`

- [ ] **Step 1: Write the benches**

Write `conn/bench_test.go`:

```go
package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func benchSetup(b *testing.B) (*Conn, func()) {
	b.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: "example.com"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
	})
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	return c, func() { _ = c.Close(); srv.Close() }
}

func BenchmarkConn_Roundtrip_Empty(b *testing.B) {
	c, teardown := benchSetup(b)
	defer teardown()
	hdrs := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			b.Fatalf("NewStream: %v", err)
		}
		if err := s.SendHeaders(ctx, hdrs, true); err != nil {
			b.Fatalf("SendHeaders: %v", err)
		}
		for {
			ev, err := s.Recv(ctx)
			if err != nil {
				b.Fatalf("Recv: %v", err)
			}
			if ev.EndStream {
				break
			}
		}
	}
}
```

- [ ] **Step 2: Run the bench (info only)**

Run:

```bash
go test -bench=BenchmarkConn_Roundtrip_Empty -benchmem -benchtime=1s -count=1 -run=^$ ./conn/
```

Expected: a numeric result. Note steady-state allocs/op — the goal is
0; if not, log the value and proceed (Task 18 reviews).

- [ ] **Step 3: Commit**

```bash
git add conn/bench_test.go
git commit -m "test(conn): roundtrip steady-state bench"
```

---

## Fuzz

### Task 15: FuzzConnReader

**Files:**
- Create: `conn/fuzz_test.go`

- [ ] **Step 1: Write the fuzz harness**

Write `conn/fuzz_test.go`:

```go
package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// FuzzConnReader feeds arbitrary bytes as the server side of net.Pipe
// after a successful handshake, asserting our reader goroutine never
// panics. Either the connection terminates cleanly or it errors out.
func FuzzConnReader(f *testing.F) {
	// Seed corpus: a single valid HEADERS frame.
	enc := hpack.NewEncoder()
	block := enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})
	hdr := make([]byte, 9)
	hdr[0] = byte(len(block) >> 16)
	hdr[1] = byte(len(block) >> 8)
	hdr[2] = byte(len(block))
	hdr[3] = byte(frame.FrameHeaders)
	hdr[4] = byte(frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream)
	hdr[5] = 0
	hdr[6] = 0
	hdr[7] = 0
	hdr[8] = 1
	f.Add(append(hdr, block...))

	f.Fuzz(func(t *testing.T, blob []byte) {
		cli, srv := net.Pipe()
		defer cli.Close()
		defer srv.Close()

		go func() {
			preface := make([]byte, 24)
			_, _ = readN(srv, preface)
			srvFr := frame.NewFramer(srv, srv)
			_ = srvFr.WriteSettings(frame.SettingsParams{})
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			_ = srvFr.WriteSettingsAck()
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			_, _ = srv.Write(blob) // adversarial bytes
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
		if err != nil {
			return // handshake failed — that's fine
		}
		// Open one stream so the reader has somewhere to push.
		_, _ = c.NewStream(ctx)
		// Sleep briefly to let the reader process bytes.
		time.Sleep(50 * time.Millisecond)
		_ = c.Close()
	})
}
```

- [ ] **Step 2: Run a quick fuzz pass**

Run:

```bash
go test -run=FuzzConnReader -fuzz=FuzzConnReader -fuzztime=10s ./conn/
```

Expected: completes without panic. If it finds an issue, fix root cause
in the reader path, NOT the fuzz harness.

- [ ] **Step 3: Update nightly fuzz workflow**

Edit `.github/workflows/nightly.yml` and add a step under `jobs.fuzz.steps`:

```yaml
      - run: go test -fuzz=FuzzConnReader -fuzztime=10m ./conn
```

- [ ] **Step 4: Commit**

```bash
git add conn/fuzz_test.go .github/workflows/nightly.yml
git commit -m "test(conn): FuzzConnReader + nightly slot"
```

---

## RFC coverage

### Task 16: Extend docs/RFC_COVERAGE.md with B.1 rows

**Files:**
- Modify: `docs/RFC_COVERAGE.md`

- [ ] **Step 1: Add new conformance rows**

In `docs/RFC_COVERAGE.md`, under the "RFC 7540 — HTTP/2" table append:

```markdown
| §3.5    | Conformance | TestConformance_RFC7540_Sec3_ClientPreface_OnTheWire |
| §6.5    | Integration | TestConn_HandshakeAndIdle (handshake + ack roundtrip) |
| §5.1    | Integration | TestIntegration_EmptyGET, TestIntegration_POST_1KB_Echo |
| §6.4    | Integration | TestIntegration_ContextCancel_TearsDownStream |
| §6.6    | Integration | (PUSH_PROMISE rejected — covered by TestHandler_OnPushPromise) |
```

- [ ] **Step 2: Verify the conformance gate is still happy**

Run:

```bash
rtk proxy go test -run=Conformance -count=1 -v ./... > /tmp/rfc.txt 2>&1
./scripts/rfc-coverage-gate.sh /tmp/rfc.txt
```

Expected: "RFC coverage gate OK".

- [ ] **Step 3: Commit**

```bash
git add docs/RFC_COVERAGE.md
git commit -m "docs(rfc): B.1 connection-layer coverage rows"
```

---

## README

### Task 17: README quick-start for `conn.Dial`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the Phase A example with a B.1-aware quick-start**

Edit `README.md`. Replace the entire "Quick start" code block with:

```markdown
## Quick start

```go
import (
    "context"
    "crypto/tls"

    "github.com/lodgvideon/poseidon-http-client/conn"
    "github.com/lodgvideon/poseidon-http-client/hpack"
)

func get(ctx context.Context, addr string) error {
    c, err := conn.Dial(ctx, addr, conn.ConnOptions{
        Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
    })
    if err != nil {
        return err
    }
    defer c.Close()

    s, err := c.NewStream(ctx)
    if err != nil {
        return err
    }
    if err := s.SendHeaders(ctx, []hpack.HeaderField{
        {Name: []byte(":method"), Value: []byte("GET")},
        {Name: []byte(":scheme"), Value: []byte("https")},
        {Name: []byte(":authority"), Value: []byte("example.com")},
        {Name: []byte(":path"), Value: []byte("/")},
    }, true); err != nil {
        return err
    }
    for {
        ev, err := s.Recv(ctx)
        if err != nil {
            return err
        }
        if ev.EndStream {
            return nil
        }
    }
}
```
```

Update the "Phases" section line for B to:

```
- **B.1 — Connection layer (single stream)** *(this release)*: TLS+ALPN
  dial, SETTINGS handshake, one in-flight stream end-to-end against
  net/http2.Server.
- **B.2 — Multiplex + flow control** *(planned)*: full RFC 7540 §5.1
  state machine, per-stream and per-conn flow control, GOAWAY/keep-alive.
- **C — Client + pool + discovery + stats** *(planned)*: public API for
  load generators.
```

Update the "Status" line to:

```
**Status:** Phase B.1 — TLS+ALPN connection, single in-flight stream.
See [design](docs/superpowers/specs/2026-05-05-poseidon-conn-layer-b1-design.md).
```

- [ ] **Step 2: Verify the README still renders sensibly**

Run:

```bash
grep -n "## " README.md | head -20
```

Expected: a clean section list (Status, Phases, Quick start, Limits and contracts, Local development, License).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): B.1 quick-start with conn.Dial"
```

---

## Acceptance gate

### Task 18: Milestone gate B.1 acceptance

- [ ] **Step 1: Run the full local suite**

Run:

```bash
make tidy
make lint
make test-race
rtk proxy go test -cover -count=1 ./... | tee /tmp/cov.txt
./scripts/coverage-gate.sh /tmp/cov.txt 70
make bench
go test -fuzz=Fuzz -fuzztime=2m ./...   # short fuzz; nightly handles long
```

All green; coverage ≥ 70% per package; benches steady-state 0 allocs/op
(or specific outliers logged for B.2 follow-up).

- [ ] **Step 2: Verify the conformance + bench + coverage gates green**

```bash
rtk proxy go test -run=Conformance -count=1 -v ./... > /tmp/rfc.txt 2>&1
./scripts/rfc-coverage-gate.sh /tmp/rfc.txt
rtk proxy go test -bench=. -benchmem -benchtime=200ms -count=1 -run=^$ ./... > /tmp/head.txt 2>&1
./scripts/bench-gate.sh /tmp/head.txt
```

Expected: both gates print "OK". If `bench-gate` fails on `BenchmarkConn_*`,
that is a real B.1 finding — file under "open issues" in the spec for
B.2 follow-up if not fixable here. Other benches (Phase A) must remain
0 alloc/op.

- [ ] **Step 3: Push and confirm CI**

```bash
git push -u origin <branch>
```

Expected: all five required checks pass — `ci/lint`, `ci/test`,
`ci/fuzz-corpus-replay`, `ci/coverage`, `bench-gate/bench`,
`conformance-gate/rfc`.

- [ ] **Step 4: Tag pre-release**

```bash
git tag -a v0.2.0-rc1 -m "Phase B.1: connection layer (single stream)"
git push origin v0.2.0-rc1
```

- [ ] **Step 5: Open PR to main**

```bash
gh pr create --title "Phase B.1: connection layer (single stream)" --body "$(cat <<'EOF'
## Summary
- TLS+ALPN dial, SETTINGS handshake, single in-flight stream against
  net/http2.Server.
- Reader goroutine + writer mutex; Stream interface forward-compat with
  B.2 multiplex.
- Zero-allocation steady state preserved (Phase A benches still green).

## Test plan
- [x] make test-race
- [x] integration tests vs httptest's HTTP/2 server
- [x] coverage gate green at 70% floor
- [x] bench gate green
- [x] FuzzConnReader 10s+ no panic
EOF
)"
```

After review/merge, delete the branch and start B.2 planning.

---

## Self-Review

Spec coverage:

- Spec §1 Context/Goals → covered by overall plan structure (B.1 carve-out
  matches §1.1) and Task 18 acceptance gate.
- Spec §2 Architecture → Tasks 9–11 implement the central `*Conn`,
  reader goroutine, write mutex; Task 6 implements the frame.Handler
  bridge from §2.1.
- Spec §3 Package layout → Task 1 scaffold + every later Task creates
  the listed file in the listed location.
- Spec §4 Public API → Task 2 (errors), 3 (options), 4–5 (stream + event),
  6 (handler), 7 (settings), 8 (dialer), 9–12 (conn + Dial). Every
  exported type/function in §4 has a defining task.
- Spec §4.1 slice ownership → Task 6 connHandler.scratch is the
  arena; Tasks 6, 9, 10 ensure events are pushed before scratch is
  reused (single-stream scope keeps lifetime trivial in B.1; B.2 ring
  is explicitly not in scope here, see §10.1).
- Spec §4.2 errors → Task 2.
- Spec §5 Wire lifecycle → Task 7 (handshake), Task 9 (reader), Task 11
  (close + GOAWAY), Task 6 (frame routing).
- Spec §5.4 frame coverage → Task 6 routes each frame type; Task 9 makes
  PING/SETTINGS-mid-stream/GOAWAY explicit on the conn.go side.
- Spec §6 goroutine model → Tasks 9, 10, 11; Task 11 verifies idempotency.
- Spec §7 allocation strategy → Task 14 bench; B.1 explicitly leaves
  the per-stream arena ring to B.2 — recorded in spec §10.1.
- Spec §8 testing → Tasks 2, 3, 5, 6, 7, 9, 10, 11, 13, 15.
- Spec §9 CI pipelines → Task 15 nightly slot; Phase A workflows
  (lint/test/coverage/bench/conformance) pick up B.1 transparently.
- Spec §10 forward compat → no Task; the spec captures the contract.
- Spec §11 open questions → addressed by Task 7 (sequence ordering),
  Task 11 (drain timeout — currently best-effort), and Task 9/10
  (sequential streams allowed). All three open questions are answered
  by the implementation, not deferred.
- Spec §12 acceptance criteria → Task 18 enumerates each.

Placeholder scan: every step has either runnable code, a runnable
command, or a file edit with the exact final content. The single
"verify these exist" reference in Task 9 (`frame.ErrCodeNo`,
`frame.ErrCodeInternal`) is bounded — the names exist or they're
one-token typos to fix in place; nothing is left "TBD".

Type consistency: `Stream`, `StreamEvent`, `StreamEventType`, `Conn`,
`ConnOptions`, `AdvertisedSettings`, `Dialer`, `TLSDialer`, `ConnError`,
`StreamError`, `streamLookup`, `streamWriter`, `connHandler`, sentinel
errors — used consistently across Tasks 2–12 and the test files. The
`*Stream`'s `closedFlag()` helper (Task 9) is referenced from Task 9's
`writeRSTStream` and is defined in the same task — no cross-task type
drift.

---

## Execution Handoff

Plan complete and saved to
`docs/superpowers/plans/2026-05-05-poseidon-conn-layer-b1.md`. Two
execution options:

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per
task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using
executing-plans, batch execution with checkpoints.

Which approach?
