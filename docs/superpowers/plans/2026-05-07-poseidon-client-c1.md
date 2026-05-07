# Phase C.1 — Public Client API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public `client` package that wraps the existing `conn` layer with sync `Do` and streaming `DoStream` APIs, single-connection lazy-dial transport, and conn-level auto-redial — without churning any existing public API except adding `(*Conn).IsAlive()`.

**Architecture:** Three-layer design inside `client/`: `Client` (public, orchestration only) → `transport` (unexported interface) → `singleConn` (only impl in C.1; pool slot for C.2). Body upload happens before `DoStream` returns, so request/response are sequential per call. All response slices are copied before returning to the caller; opt-in `WantBody` / `WantTrailers` keep the unhappy-path zero-buffer.

**Tech Stack:** Go 1.24, no new external deps. Reuses `frame`, `hpack`, `conn`. Tests use `net.Pipe` (`pipeServer` helper) for unit and `httptest.NewUnstartedServer` + `EnableHTTP2 = true` for integration.

---

## Spec reference

Design spec: [`docs/superpowers/specs/2026-05-07-poseidon-client-c1-design.md`](../specs/2026-05-07-poseidon-client-c1-design.md). Read it first.

## Pre-implementation corrections to spec wording

These resolve naming drift discovered while reading the conn-layer source. Apply them in code; the spec does not need re-editing for these (engineering-level corrections only):

1. The conn-layer method to write a DATA frame is **`(*Stream).SendData`**, not `WriteData`. Use `SendData` everywhere in this plan.
2. The conn-layer method to send `RST_STREAM(CANCEL)` is **`(*Stream).Close()`** (idempotent; only resets if neither side reached END_STREAM). There is no `Stream.Reset(code)` API. For the body-reader error path, we therefore call `s.Close()` (sending CANCEL) and wrap the original reader error.
3. `conn.ConnOptions` already carries `Dialer` (`conn/options.go`). Therefore the `ClientOptions` struct in this plan **does not have a separate `Dialer` field**. The shape is:
   ```go
   type ClientOptions struct {
       Addr        string            // required, "host:port"
       ConnOpts    conn.ConnOptions  // forwarded to conn.Dial; ConnOpts.Dialer must be non-nil
       DialBackoff time.Duration     // delay before redial after a failed dial; default 0 (no backoff)
   }
   ```
4. `client.StreamEvent` reuses `conn.StreamEventType` constants by value via local aliases — but to avoid leaking the dependency, this plan defines fresh constants `EventData`, `EventTrailers`, `EventReset` of type `client.EventType`, and the implementation translates between them. (Spec already shows this shape.)

## File map

New files (all under `client/`):

| Path | Responsibility |
|---|---|
| `client/doc.go` | Package doc comment |
| `client/errors.go` | Error sentinels and typed errors (`ErrInvalidRequest`, `ErrClosed`, `ErrRedialBackoff`, `ErrEmptyResponse`, `*StreamResetError`, `*DialError`) |
| `client/request.go` | `Request` struct, `validate(*Request) error` |
| `client/response.go` | `Response`, `StreamResponse`, `StreamEvent`, `EventType`; helper `parseStatus` |
| `client/transport.go` | `transport` unexported interface |
| `client/single_conn.go` | `singleConn` impl |
| `client/client.go` | `Client`, `ClientOptions`, `NewClient`, `Do`, `DoStream`, `Close`; internal `buildHeaders`, `drainResponse`, header pool |
| `client/client_test.go` | Unit tests: validation, Do happy/edge, DoStream, singleConn |
| `client/integration_test.go` | Integration tests vs `net/http2.Server` |

Modified files:

| Path | Change |
|---|---|
| `conn/conn.go` | Add `(*Conn).IsAlive() bool` method |
| `conn/conn_test.go` | Add `TestConn_IsAlive_*` tests |
| `docs/RFC_COVERAGE.md` | Add row for `TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst` |
| `README.md` | Replace "Quick start" snippet with `client` package usage |

---

## Task 1: Add `(*conn.Conn).IsAlive()`

**Files:**
- Modify: `conn/conn.go` (insert after the `Stats` method)
- Modify: `conn/conn_test.go` (append at end)

- [ ] **Step 1: Write the failing tests**

Append to `conn/conn_test.go`:

```go
func TestConn_IsAlive_FreshConnTrue(t *testing.T) {
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, func(srvFr *frame.Framer) {
			// Hold the connection until the test finishes.
			<-done
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	if !c.IsAlive() {
		t.Fatal("fresh conn must be alive")
	}
}

func TestConn_IsAlive_AfterCloseFalse(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	_ = c.Close()
	if c.IsAlive() {
		t.Fatal("closed conn must not be alive")
	}
}

func TestConn_IsAlive_AfterPeerGoAwayFalse(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	// Wait for the reader to observe GOAWAY.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !c.IsAlive() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("conn still alive after peer GOAWAY")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./conn/ -run TestConn_IsAlive -count=1 -race -timeout 90s`
Expected: FAIL — `c.IsAlive` undefined.

- [ ] **Step 3: Add the method**

Insert into `conn/conn.go` immediately after the `Stats` method (find it via the symbols overview):

```go
// IsAlive reports whether the connection has neither been Closed nor
// received a GOAWAY frame from the peer. It is a cheap atomic check
// suitable for transport pools that need to decide whether to reuse
// or redial.
func (c *Conn) IsAlive() bool {
	return !c.closed.Load() && !c.goAwayReceived.Load()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./conn/ -run TestConn_IsAlive -count=1 -race -timeout 90s`
Expected: PASS for all three.

- [ ] **Step 5: Commit**

```bash
git add conn/conn.go conn/conn_test.go
git commit -m "feat(conn): IsAlive method for transport reuse decisions"
```

---

## Task 2: `client` package skeleton + doc comment + error types

**Files:**
- Create: `client/doc.go`
- Create: `client/errors.go`

- [ ] **Step 1: Create `client/doc.go`**

```go
// Package client provides a high-level HTTP/2 client API on top of
// the conn package. Phase C.1 ships a single-connection-per-Client
// transport with conn-level auto-redial; Phase C.2 will add a
// connection pool against the same internal transport interface.
//
// Two entry points:
//
//   - Client.Do is synchronous: it issues a request and returns a
//     fully-buffered Response. Response body and trailers are
//     opt-in via Request.WantBody and Request.WantTrailers; when
//     either is false the corresponding frames are still consumed
//     (so flow control refunds run) but the bytes are dropped.
//
//   - Client.DoStream returns a StreamResponse once the initial
//     HEADERS frame has arrived. The caller pumps StreamResponse.Recv
//     for DATA, trailers, and reset events. The caller MUST call
//     StreamResponse.Close if it does not drain to EndStream.
//
// All API contracts are described in
// docs/superpowers/specs/2026-05-07-poseidon-client-c1-design.md.
package client
```

- [ ] **Step 2: Create `client/errors.go`**

```go
package client

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Sentinel errors returned (or wrapped via %w) from the client.
var (
	// ErrInvalidRequest indicates a Request failed up-front validation.
	// Wrapped errors carry a human-readable detail.
	ErrInvalidRequest = errors.New("client: invalid request")

	// ErrClosed is returned from Do, DoStream, or transport.acquire
	// after Client.Close has been called.
	ErrClosed = errors.New("client: closed")

	// ErrRedialBackoff is returned when a previous dial attempt
	// failed and the configured DialBackoff window has not elapsed.
	ErrRedialBackoff = errors.New("client: redial in backoff window")

	// ErrEmptyResponse is returned when the response HEADERS frame
	// did not contain a parseable :status pseudo-header.
	ErrEmptyResponse = errors.New("client: response missing :status")
)

// StreamResetError is returned from Do (or surfaced via DoStream's
// EventReset) when the peer sends RST_STREAM mid-response.
type StreamResetError struct {
	Code frame.ErrCode
}

// Error implements the error interface.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("client: stream reset by peer: %s", e.Code)
}

// DialError wraps the underlying dial error and the address that
// failed. Returned from Do/DoStream when the lazy dial fails.
type DialError struct {
	Addr string
	Err  error
}

// Error implements the error interface.
func (e *DialError) Error() string {
	return fmt.Sprintf("client: dial %s: %v", e.Addr, e.Err)
}

// Unwrap exposes the underlying error for errors.Is / errors.As.
func (e *DialError) Unwrap() error { return e.Err }
```

- [ ] **Step 3: Verify it builds**

Run: `go build ./client/...`
Expected: success (no test files yet, just package).

- [ ] **Step 4: Commit**

```bash
git add client/doc.go client/errors.go
git commit -m "feat(client): package skeleton and error types"
```

---

## Task 3: `Request` struct + validation

**Files:**
- Create: `client/request.go`
- Create: `client/client_test.go` (start of file with package declaration and validation tests)

- [ ] **Step 1: Write the failing validation tests**

Create `client/client_test.go`:

```go
package client

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestValidateRequest_OK(t *testing.T) {
	req := &Request{Method: "GET", Path: "/"}
	if err := validateRequest(req); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRequest_NoMethod(t *testing.T) {
	req := &Request{Path: "/"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_NoPath(t *testing.T) {
	req := &Request{Method: "GET"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_PseudoHeaderInRegular(t *testing.T) {
	req := &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte(":authority"), Value: []byte("example.com")},
		},
	}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./client/ -run TestValidateRequest -count=1 -race -timeout 30s`
Expected: FAIL — `Request`, `validateRequest`, `ErrInvalidRequest` undefined or build break.

- [ ] **Step 3: Create `client/request.go`**

```go
package client

import (
	"fmt"
	"io"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Request describes one HTTP/2 request. Required fields: Method, Path.
// All other fields are optional and have safe zero-value behavior.
type Request struct {
	// Pseudo-headers (RFC 7540 §8.1.2.3). Method and Path are required;
	// Scheme and Authority default from the Client when empty.
	Method    string
	Scheme    string
	Authority string
	Path      string

	// Regular headers. The slice is read once during request build;
	// the caller retains ownership. MUST NOT include any name starting
	// with ':' (validated up front).
	Headers []hpack.HeaderField

	// Request body. At most one of Body / BodyReader is honored;
	// BodyReader takes precedence when non-nil.
	Body       []byte
	BodyReader io.Reader

	// Response shaping. When false, the corresponding response data is
	// consumed from the wire (so flow control still runs) but discarded
	// before Do returns.
	WantBody     bool
	WantTrailers bool
}

// validateRequest enforces the up-front rules documented on Request.
// Returns an error wrapping ErrInvalidRequest with a human-readable
// detail.
func validateRequest(r *Request) error {
	if r == nil {
		return fmt.Errorf("%w: nil request", ErrInvalidRequest)
	}
	if r.Method == "" {
		return fmt.Errorf("%w: method is required", ErrInvalidRequest)
	}
	if r.Path == "" {
		return fmt.Errorf("%w: path is required", ErrInvalidRequest)
	}
	for i := range r.Headers {
		if len(r.Headers[i].Name) > 0 && r.Headers[i].Name[0] == ':' {
			return fmt.Errorf("%w: pseudo-header %q in regular Headers slice",
				ErrInvalidRequest, r.Headers[i].Name)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./client/ -run TestValidateRequest -count=1 -race -timeout 30s`
Expected: PASS for all four.

- [ ] **Step 5: Commit**

```bash
git add client/request.go client/client_test.go
git commit -m "feat(client): Request struct and up-front validation"
```

---

## Task 4: `Response`, `StreamResponse`, `StreamEvent`, `EventType`

**Files:**
- Create: `client/response.go`

- [ ] **Step 1: Create `client/response.go` with full type definitions and parseStatus helper**

```go
package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Response is the synchronous result of Client.Do. Body and Trailers
// are nil when the corresponding Request.WantBody / WantTrailers was
// false. Headers, Body, and Trailers are deep copies and are safe to
// retain after Do returns.
type Response struct {
	Status        int
	Headers       []hpack.HeaderField
	Body          []byte
	Trailers      []hpack.HeaderField
	BytesReceived int64
}

// EventType discriminates StreamEvent variants returned from
// StreamResponse.Recv.
type EventType uint8

// EventType values.
const (
	EventData EventType = iota + 1
	EventTrailers
	EventReset
)

// String returns the lowercase name.
func (t EventType) String() string {
	switch t {
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

// StreamEvent is one chunk of a streaming response. Data slice aliases
// internal scratch buffers and is valid only until the next Recv on
// the same StreamResponse — copy if retained.
type StreamEvent struct {
	Type      EventType
	Data      []byte
	Trailers  []hpack.HeaderField
	ResetCode frame.ErrCode
	EndStream bool
}

// StreamResponse is returned by Client.DoStream after the initial
// HEADERS frame arrives. The caller pumps Recv for subsequent events.
// Close MUST be called if the caller does not drain to EndStream;
// it is idempotent and sends RST_STREAM(CANCEL) when needed.
type StreamResponse struct {
	Status  int
	Headers []hpack.HeaderField

	stream    *conn.Stream
	release   func()
	closeOnce sync.Once
	drained   bool
}

// Recv blocks until the next event is available, the stream
// terminates, or ctx is cancelled. After the event whose EndStream
// is true, subsequent calls return io.EOF-style: a wrapped
// ErrStreamEnded so callers can distinguish.
func (sr *StreamResponse) Recv(ctx context.Context) (StreamEvent, error) {
	if sr.drained {
		return StreamEvent{}, errStreamEnded
	}
	for {
		ev, err := sr.stream.Recv(ctx)
		if err != nil {
			return StreamEvent{}, err
		}
		switch ev.Type {
		case conn.EventHeaders:
			// Spurious second HEADERS — protocol-level oddity. Ignore
			// and keep pumping. (Trailers are EventTrailers.)
			continue
		case conn.EventData:
			out := StreamEvent{
				Type:      EventData,
				Data:      ev.Data,
				EndStream: ev.EndStream,
			}
			if ev.EndStream {
				sr.drained = true
			}
			return out, nil
		case conn.EventTrailers:
			out := StreamEvent{
				Type:      EventTrailers,
				Trailers:  ev.Headers,
				EndStream: ev.EndStream,
			}
			if ev.EndStream {
				sr.drained = true
			}
			return out, nil
		case conn.EventReset:
			sr.drained = true
			return StreamEvent{
				Type:      EventReset,
				ResetCode: ev.RSTCode,
				EndStream: true,
			}, nil
		}
	}
}

// Close releases the stream. If neither side reached END_STREAM, the
// underlying conn.Stream sends RST_STREAM(CANCEL). Idempotent.
func (sr *StreamResponse) Close() error {
	var closeErr error
	sr.closeOnce.Do(func() {
		closeErr = sr.stream.Close()
		if sr.release != nil {
			sr.release()
		}
	})
	return closeErr
}

// errStreamEnded is the sentinel returned from Recv after the stream
// has fully drained. Internal — callers should compare via
// errors.Is(err, ErrStreamEnded) only if they care.
var errStreamEnded = errors.New("client: stream ended")

// ErrStreamEnded is returned from StreamResponse.Recv after the final
// event with EndStream=true has been delivered.
var ErrStreamEnded = errStreamEnded

// parseStatus extracts the integer value of the :status pseudo-header
// from a HEADERS payload. Returns ErrEmptyResponse if absent or
// unparseable. Also returns the slice with :status removed (so the
// caller's response.Headers is purely regular).
func parseStatus(in []hpack.HeaderField) (status int, regular []hpack.HeaderField, err error) {
	for i := range in {
		if string(in[i].Name) == ":status" {
			n, perr := strconv.Atoi(string(in[i].Value))
			if perr != nil {
				return 0, nil, fmt.Errorf("%w: :status %q not numeric",
					ErrEmptyResponse, in[i].Value)
			}
			// Remove this entry to produce regular-only slice. Keep
			// allocation here; this runs once per response.
			regular = make([]hpack.HeaderField, 0, len(in)-1)
			regular = append(regular, in[:i]...)
			regular = append(regular, in[i+1:]...)
			return n, regular, nil
		}
	}
	return 0, nil, ErrEmptyResponse
}
```

- [ ] **Step 2: Add a unit test for `parseStatus`**

Append to `client/client_test.go`:

```go
func TestParseStatus_Found(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	st, rest, err := parseStatus(in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if st != 200 {
		t.Fatalf("status = %d, want 200", st)
	}
	if len(rest) != 1 || string(rest[0].Name) != "content-type" {
		t.Fatalf("regular headers wrong: %+v", rest)
	}
}

func TestParseStatus_Missing(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}

func TestParseStatus_NotNumeric(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("OK")},
	}
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./client/ -run "TestParseStatus|TestValidateRequest" -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add client/response.go client/client_test.go
git commit -m "feat(client): Response, StreamResponse, EventType, parseStatus"
```

---

## Task 5: `transport` interface

**Files:**
- Create: `client/transport.go`

- [ ] **Step 1: Create `client/transport.go`**

```go
package client

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// transport is the seam between Client and the underlying connection
// supply. C.1 ships exactly one impl (singleConn); a future C.2
// connection pool will implement the same interface.
type transport interface {
	// acquire returns a healthy *conn.Conn together with a release
	// function that the caller MUST call exactly once when the
	// associated request is fully drained or has errored. release is
	// safe to call from any goroutine. Errors include ErrClosed,
	// ErrRedialBackoff, *DialError, and ctx errors.
	acquire(ctx context.Context) (c *conn.Conn, release func(), err error)

	// close prevents further acquires and closes any underlying conn.
	// Idempotent.
	close() error
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./client/...`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add client/transport.go
git commit -m "feat(client): transport interface for pluggable conn supply"
```

---

## Task 6: `singleConn` — first dial path

**Files:**
- Create: `client/single_conn.go`
- Modify: `client/client_test.go` (append singleConn tests)

- [ ] **Step 1: Write failing test for first-dial path**

Append to `client/client_test.go`:

```go
import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// fakeDialer returns the client end of an in-memory pipe. The server
// goroutine drives the HTTP/2 handshake using the conn package's
// pipeServer-equivalent, exposed as a small inline copy for tests.
type fakeDialer struct {
	dialCount atomic.Int32
	srvAfter  func(srvFr *frame.Framer)
}

func (d *fakeDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.dialCount.Add(1)
	cli, srv := net.Pipe()
	go runFakeH2Server(srv, d.srvAfter)
	return cli, nil
}

// runFakeH2Server is a stripped-down handshake driver that mirrors the
// conn-test pipeServer helper. Lives in client/client_test.go because
// the conn test helper is internal to that package.
func runFakeH2Server(srv net.Conn, after func(srvFr *frame.Framer)) {
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readFull(srv, preface); err != nil {
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(frame.SettingsParams{}) }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	go func() { writeDone <- srvFr.WriteSettingsAck() }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	if after != nil {
		after(srvFr)
	}
}

// readFull reads len(buf) bytes from r, retrying on short reads.
func readFull(r net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		x, err := r.Read(buf[n:])
		if x > 0 {
			n += x
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// nopHandler implements frame.Handler with all-no-op methods.
type nopHandler struct{}

func (nopHandler) OnData(frame.DataFrame) error                 { return nil }
func (nopHandler) OnHeaders(frame.HeadersFrame) error           { return nil }
func (nopHandler) OnPriority(frame.PriorityFrame) error         { return nil }
func (nopHandler) OnRSTStream(frame.RSTStreamFrame) error       { return nil }
func (nopHandler) OnSettings(frame.SettingsFrame) error         { return nil }
func (nopHandler) OnPushPromise(frame.PushPromiseFrame) error   { return nil }
func (nopHandler) OnPing(frame.PingFrame) error                 { return nil }
func (nopHandler) OnGoAway(frame.GoAwayFrame) error             { return nil }
func (nopHandler) OnWindowUpdate(frame.WindowUpdateFrame) error { return nil }
func (nopHandler) OnContinuation(frame.ContinuationFrame) error { return nil }

func TestSingleConn_Acquire_LazyDial(t *testing.T) {
	d := &fakeDialer{}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
	if d.dialCount.Load() != 0 {
		t.Fatalf("dial happened in constructor; count=%d", d.dialCount.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, release, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer release()
	defer c.Close()

	if d.dialCount.Load() != 1 {
		t.Fatalf("dial count = %d, want 1", d.dialCount.Load())
	}
	if !c.IsAlive() {
		t.Fatal("acquired conn must be alive")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./client/ -run TestSingleConn_Acquire_LazyDial -count=1 -race -timeout 30s`
Expected: FAIL — `singleConn` undefined.

- [ ] **Step 3: Create `client/single_conn.go`**

```go
package client

import (
	"context"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// singleConn is the C.1 transport: at most one *conn.Conn per Client.
// Lazy dial on first acquire, conn-only auto-redial when the cached
// conn is no longer alive, optional backoff between failed dials.
type singleConn struct {
	addr     string
	connOpts conn.ConnOptions
	backoff  time.Duration

	mu         sync.Mutex
	cur        *conn.Conn
	dialErr    error
	lastDialAt time.Time
	closed     bool
}

// acquire implements transport.acquire.
func (s *singleConn) acquire(ctx context.Context) (*conn.Conn, func(), error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, ErrClosed
	}
	if s.cur != nil && s.cur.IsAlive() {
		c := s.cur
		s.mu.Unlock()
		return c, func() {}, nil
	}
	// Discard stale.
	s.cur = nil
	if s.backoff > 0 && s.dialErr != nil &&
		time.Since(s.lastDialAt) < s.backoff {
		err := s.dialErr
		s.mu.Unlock()
		return nil, nil, &DialError{Addr: s.addr, Err: err}
	}
	// Release lock for the long dial.
	s.mu.Unlock()

	dialed, dialErr := conn.Dial(ctx, s.addr, s.connOpts)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDialAt = time.Now()
	if s.closed {
		if dialed != nil {
			_ = dialed.Close()
		}
		if dialErr != nil {
			return nil, nil, ErrClosed
		}
		return nil, nil, ErrClosed
	}
	if dialErr != nil {
		s.dialErr = dialErr
		return nil, nil, &DialError{Addr: s.addr, Err: dialErr}
	}
	// Race-loser: another goroutine already populated cur with a live
	// conn. Discard ours and use theirs.
	if s.cur != nil && s.cur.IsAlive() {
		_ = dialed.Close()
		return s.cur, func() {}, nil
	}
	s.cur = dialed
	s.dialErr = nil
	return s.cur, func() {}, nil
}

// close implements transport.close. Idempotent.
func (s *singleConn) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.cur != nil {
		err := s.cur.Close()
		s.cur = nil
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./client/ -run TestSingleConn_Acquire_LazyDial -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/single_conn.go client/client_test.go
git commit -m "feat(client): singleConn lazy-dial transport"
```

---

## Task 7: `singleConn` — cached-conn reuse, GOAWAY-driven redial

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Add tests**

```go
func TestSingleConn_Acquire_ReusesAliveConn(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// hold the connection alive
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
	defer sc.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c1, rel1, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	rel1()
	c2, rel2, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer rel2()
	defer c2.Close()

	if c1 != c2 {
		t.Fatal("expected reuse of the same conn")
	}
	if d.dialCount.Load() != 1 {
		t.Fatalf("dial count = %d, want 1", d.dialCount.Load())
	}
}

func TestSingleConn_Acquire_GoAwayTriggersRedial(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// First handshake's server immediately sends GOAWAY.
		_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
	defer sc.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c1, rel1, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	rel1()
	// Wait for reader to mark goAwayReceived.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !c1.IsAlive() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	c2, rel2, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer rel2()
	defer c2.Close()
	if c1 == c2 {
		t.Fatal("expected a fresh conn after GOAWAY")
	}
	if d.dialCount.Load() != 2 {
		t.Fatalf("dial count = %d, want 2", d.dialCount.Load())
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./client/ -run TestSingleConn_Acquire -count=1 -race -timeout 30s`
Expected: PASS for all three (lazy + reuse + goaway).

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): singleConn reuses alive conn and redials on GOAWAY"
```

---

## Task 8: `singleConn` — backoff and concurrent dial

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Add tests**

```go
// failingDialer always errors.
type failingDialer struct {
	err       error
	dialCount atomic.Int32
}

func (d *failingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d.dialCount.Add(1)
	return nil, d.err
}

func TestSingleConn_Backoff_RefusesWithinWindow(t *testing.T) {
	d := &failingDialer{err: errors.New("boom")}
	sc := &singleConn{
		addr:     "fake:0",
		connOpts: conn.ConnOptions{Dialer: d},
		backoff:  500 * time.Millisecond,
	}
	defer sc.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := sc.acquire(ctx); err == nil {
		t.Fatal("first acquire must fail")
	}
	// Second call within the backoff window must NOT redial.
	if _, _, err := sc.acquire(ctx); err == nil {
		t.Fatal("second acquire must fail")
	}
	if got := d.dialCount.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1 (backoff suppressed second)", got)
	}
}

func TestSingleConn_Acquire_ConcurrentDial_OnlyOneDials(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
	defer sc.close()

	const N = 16
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := make(chan *conn.Conn, N)
	for i := 0; i < N; i++ {
		go func() {
			c, _, err := sc.acquire(ctx)
			if err != nil {
				results <- nil
				return
			}
			results <- c
		}()
	}
	first := <-results
	for i := 1; i < N; i++ {
		got := <-results
		if got != first {
			t.Fatalf("goroutine %d got different conn", i)
		}
	}
	// Allow up to 2 concurrent dials in the race-loser path; the test
	// exists primarily to verify the same conn is observed by all.
	if got := d.dialCount.Load(); got > 2 {
		t.Fatalf("dial count = %d, want 1 or 2 (race-loser permitted)", got)
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./client/ -run TestSingleConn -count=1 -race -timeout 30s`
Expected: PASS for all five so far.

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): singleConn backoff + concurrent dial guarantees"
```

---

## Task 9: `singleConn` — Close blocks new acquires

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Add test**

```go
func TestSingleConn_Close_BlocksNewAcquires(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = sc.close()
	if c.IsAlive() {
		t.Fatal("close must close underlying conn")
	}
	if _, _, err := sc.acquire(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./client/ -run TestSingleConn_Close -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): singleConn close blocks subsequent acquires"
```

---

## Task 10: `Client` — `NewClient`, `Close` (no Do/DoStream yet)

**Files:**
- Create: `client/client.go`
- Modify: `client/client_test.go`

- [ ] **Step 1: Add failing test**

```go
func TestNewClient_RejectsEmptyAddr(t *testing.T) {
	_, err := NewClient(ClientOptions{ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}}})
	if err == nil {
		t.Fatal("expected error on empty addr")
	}
}

func TestNewClient_RejectsNilDialer(t *testing.T) {
	_, err := NewClient(ClientOptions{Addr: "fake:0"})
	if err == nil {
		t.Fatal("expected error on nil dialer")
	}
}

func TestClient_Close_Idempotent(t *testing.T) {
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./client/ -run "TestNewClient|TestClient_Close" -count=1 -race -timeout 30s`
Expected: FAIL — `NewClient`, `Client`, `ClientOptions` undefined.

- [ ] **Step 3: Create `client/client.go`**

```go
package client

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ClientOptions tunes a Client. Addr and ConnOpts.Dialer are required.
type ClientOptions struct {
	// Addr is the "host:port" target used both as the dial target and
	// as the default :authority for requests that don't set one.
	Addr string

	// ConnOpts is forwarded verbatim to conn.Dial. ConnOpts.Dialer
	// must be non-nil.
	ConnOpts conn.ConnOptions

	// DialBackoff suppresses repeated dial attempts within this window
	// after a failed dial. Zero disables suppression (immediate retry).
	DialBackoff time.Duration
}

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr        transport
	authority string
}

// NewClient validates opts and constructs a Client. It does NOT dial;
// the first Do or DoStream call triggers a lazy connection establish.
func NewClient(opts ClientOptions) (*Client, error) {
	if strings.TrimSpace(opts.Addr) == "" {
		return nil, fmt.Errorf("client: ClientOptions.Addr is required")
	}
	if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	}
	tr := &singleConn{
		addr:     opts.Addr,
		connOpts: opts.ConnOpts,
		backoff:  opts.DialBackoff,
	}
	return &Client{tr: tr, authority: deriveAuthority(opts.Addr)}, nil
}

// Close releases the underlying transport. Subsequent Do/DoStream
// calls return ErrClosed. Idempotent.
func (c *Client) Close() error {
	return c.tr.close()
}

// deriveAuthority strips the port if it equals 80 (http) or 443 (https).
func deriveAuthority(addr string) string {
	host, port, ok := strings.Cut(addr, ":")
	if !ok {
		return addr
	}
	if port == "80" || port == "443" {
		return host
	}
	return addr
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./client/ -run "TestNewClient|TestClient_Close" -count=1 -race -timeout 30s`
Expected: PASS for all three.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/client_test.go
git commit -m "feat(client): Client struct, NewClient, Close"
```

Address subtle import lint: the unused `errors` import in `client.go` would block the build. If `goimports` removes it on save, fine; otherwise drop it from the import block before commit. Verify with `go build ./client/...` before staging.

---

## Task 11: `Client.Do` — minimal GET (no body, no opt-ins)

**Files:**
- Modify: `client/client.go`
- Modify: `client/client_test.go`

This task sets up the Do skeleton end-to-end against a fake server that replies with HEADERS(:status=200) + END_STREAM. Body and trailers handling come in subsequent tasks.

- [ ] **Step 1: Write failing integration-shaped unit test**

Append to `client/client_test.go`:

```go
// minimalGETServer responds to one GET request with :status=200 and
// END_STREAM on HEADERS.
func minimalGETServer(t *testing.T) func(srvFr *frame.Framer) {
	return func(srvFr *frame.Framer) {
		// Handshake already done by runFakeH2Server.
		// Read frames until we see HEADERS, then reply.
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				// Decode for sanity.
				_, _ = dec.DecodeBlock(nil, nil)
				// Build :status=200 block.
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      h.StreamID,
					BlockFragment: block,
					EndHeaders:    true,
					EndStream:     true,
				})
				return
			}
		}
	}
}

func TestClient_Do_GET_NoBody_ReturnsStatus200(t *testing.T) {
	d := &fakeDialer{srvAfter: minimalGETServer(t)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("Status = %d, want 200", res.Status)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./client/ -run TestClient_Do_GET_NoBody -count=1 -race -timeout 30s`
Expected: FAIL — `Do` undefined.

- [ ] **Step 3: Append the `Do` method to `client/client.go`**

```go
import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)
```

(Append to existing import block; merge with the existing imports.)

```go
// Do issues a synchronous request and returns a fully-buffered Response.
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	s, err := cn.NewStream(ctx)
	if err != nil {
		return nil, err
	}

	hdrs := buildHeaders(req, c.authority)
	endStream := len(req.Body) == 0 && req.BodyReader == nil
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		_ = s.Close()
		return nil, err
	}

	if !endStream {
		if err := writeRequestBody(ctx, s, req); err != nil {
			_ = s.Close()
			return nil, err
		}
	}

	return drainResponse(ctx, s, req)
}

// buildHeaders assembles the on-wire HEADERS slice with pseudo-headers
// first. The returned slice is a fresh allocation — caller-supplied
// req.Headers entries are referenced by value.
func buildHeaders(req *Request, defaultAuthority string) []hpack.HeaderField {
	scheme := req.Scheme
	if scheme == "" {
		scheme = "https"
	}
	authority := req.Authority
	if authority == "" {
		authority = defaultAuthority
	}
	out := make([]hpack.HeaderField, 0, 4+len(req.Headers))
	out = append(out,
		hpack.HeaderField{Name: []byte(":method"), Value: []byte(req.Method)},
		hpack.HeaderField{Name: []byte(":scheme"), Value: []byte(scheme)},
		hpack.HeaderField{Name: []byte(":authority"), Value: []byte(authority)},
		hpack.HeaderField{Name: []byte(":path"), Value: []byte(req.Path)},
	)
	out = append(out, req.Headers...)
	return out
}

// writeRequestBody is filled in by Task 12; the GET-only test in this
// task takes the endStream=true path.
func writeRequestBody(ctx context.Context, s *conn.Stream, req *Request) error {
	return nil
}

// drainResponse pumps stream events until the response side ends or
// the stream resets.
func drainResponse(ctx context.Context, s *conn.Stream, req *Request) (*Response, error) {
	var (
		gotHeaders bool
		resp       Response
	)
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return nil, err
		}
		switch ev.Type {
		case conn.EventHeaders:
			if !gotHeaders {
				status, regular, perr := parseStatus(ev.Headers)
				if perr != nil {
					return nil, perr
				}
				resp.Status = status
				resp.Headers = copyHeaderFields(regular)
				gotHeaders = true
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventData:
			resp.BytesReceived += int64(len(ev.Data))
			if req.WantBody && len(ev.Data) > 0 {
				resp.Body = append(resp.Body, ev.Data...)
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventTrailers:
			if req.WantTrailers {
				resp.Trailers = copyHeaderFields(ev.Headers)
			}
			if ev.EndStream {
				return &resp, nil
			}
		case conn.EventReset:
			return nil, &StreamResetError{Code: ev.RSTCode}
		}
	}
}

// copyHeaderFields deep-copies a header slice (including its byte
// slices) so the result is safe to retain across Recv / Close.
func copyHeaderFields(in []hpack.HeaderField) []hpack.HeaderField {
	if len(in) == 0 {
		return nil
	}
	out := make([]hpack.HeaderField, len(in))
	for i := range in {
		nm := make([]byte, len(in[i].Name))
		copy(nm, in[i].Name)
		vl := make([]byte, len(in[i].Value))
		copy(vl, in[i].Value)
		out[i] = hpack.HeaderField{Name: nm, Value: vl}
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./client/ -run TestClient_Do_GET_NoBody -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/client_test.go
git commit -m "feat(client): minimal Do for GET (no body, no opt-ins)"
```

---

## Task 12: `Client.Do` — POST with `Body []byte`

**Files:**
- Modify: `client/client.go` (replace `writeRequestBody` stub)
- Modify: `client/client_test.go`

- [ ] **Step 1: Write failing test**

Append to `client/client_test.go`:

```go
// echoPOSTServer reads HEADERS + a single DATA frame and replies with
// HEADERS(:status=200) + DATA(echo) + END_STREAM.
func echoPOSTServer(t *testing.T, captured *[]byte) func(srvFr *frame.Framer) {
	return func(srvFr *frame.Framer) {
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		var streamID uint32
		var body bytes.Buffer
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			switch h.Type {
			case frame.FrameTypeHeaders:
				streamID = h.StreamID
				_, _ = dec.DecodeBlock(nil, nil)
			case frame.FrameTypeData:
				body.Write(h.Payload)
				if h.Flags&frame.FlagDataEndStream != 0 {
					if captured != nil {
						*captured = append((*captured)[:0], body.Bytes()...)
					}
					block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
						{Name: []byte(":status"), Value: []byte("200")},
					})
					_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
						StreamID:      streamID,
						BlockFragment: block,
						EndHeaders:    true,
					})
					_ = srvFr.WriteData(streamID, true, body.Bytes())
					return
				}
			}
		}
	}
}

func TestClient_Do_POST_BodyBytes(t *testing.T) {
	var captured []byte
	d := &fakeDialer{srvAfter: echoPOSTServer(t, &captured)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body := []byte("hello world")
	res, err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		Body:     body,
		WantBody: true,
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if !bytes.Equal(res.Body, body) {
		t.Fatalf("echoed body = %q, want %q", res.Body, body)
	}
	if !bytes.Equal(captured, body) {
		t.Fatalf("server saw %q, want %q", captured, body)
	}
}
```

(Note: `bytes` import.)

Add the `bytes` import at the top of `client/client_test.go`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./client/ -run TestClient_Do_POST_BodyBytes -count=1 -race -timeout 30s`
Expected: FAIL — `writeRequestBody` is a stub; the server times out reading DATA.

- [ ] **Step 3: Replace `writeRequestBody` in `client/client.go`**

Replace the stub:

```go
// writeRequestBody writes Body or BodyReader as DATA frames, ending
// the request side on the final write. The caller has already
// SendHeaders'd with endStream=false.
func writeRequestBody(ctx context.Context, s *conn.Stream, req *Request) error {
	if req.BodyReader != nil {
		return writeBodyReader(ctx, s, req.BodyReader)
	}
	return s.SendData(ctx, req.Body, true)
}

// writeBodyReader is filled in by Task 13.
func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader) error {
	return errors.New("client: BodyReader not yet supported")
}
```

(Add `errors` and `io` imports.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./client/ -run TestClient_Do_POST_BodyBytes -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/client_test.go
git commit -m "feat(client): Do supports POST with Body []byte"
```

---

## Task 13: `Client.Do` — POST with `BodyReader`

**Files:**
- Modify: `client/client.go` (fill in `writeBodyReader`)
- Modify: `client/client_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestClient_Do_POST_BodyReader_ChunkedDATA(t *testing.T) {
	want := bytes.Repeat([]byte("ab"), 10000) // 20 KiB > 16 KiB chunk
	var captured []byte
	d := &fakeDialer{srvAfter: echoPOSTServer(t, &captured)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		WantBody:   true,
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if !bytes.Equal(captured, want) {
		t.Fatalf("server captured %d bytes, want %d", len(captured), len(want))
	}
	if !bytes.Equal(res.Body, want) {
		t.Fatalf("echoed body length %d, want %d", len(res.Body), len(want))
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./client/ -run TestClient_Do_POST_BodyReader -count=1 -race -timeout 30s`
Expected: FAIL — `writeBodyReader` returns "not yet supported".

- [ ] **Step 3: Replace `writeBodyReader`**

```go
// readChunkSize is the per-Read buffer for streaming uploads. The
// underlying conn layer further chunks at the peer's MAX_FRAME_SIZE
// and respects flow control.
const readChunkSize = 16 * 1024

func writeBodyReader(ctx context.Context, s *conn.Stream, r io.Reader) error {
	buf := make([]byte, readChunkSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			final := rerr == io.EOF
			if werr := s.SendData(ctx, buf[:n], final); werr != nil {
				return werr
			}
			if final {
				return nil
			}
		}
		if rerr == io.EOF {
			// EOF with n==0: send empty final DATA to half-close.
			return s.SendData(ctx, nil, true)
		}
		if rerr != nil {
			return fmt.Errorf("client: read request body: %w", rerr)
		}
	}
}
```

Add `fmt` to imports if not present.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./client/ -run TestClient_Do_POST -count=1 -race -timeout 30s`
Expected: PASS for both BodyBytes and BodyReader tests.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/client_test.go
git commit -m "feat(client): Do supports BodyReader chunked upload"
```

---

## Task 14: `Client.Do` — `WantBody=false` discards but counts

**Files:**
- Modify: `client/client_test.go`

The `drainResponse` already implements counting + discarding logic in Task 11. This task just verifies it.

- [ ] **Step 1: Write test**

```go
func TestClient_Do_WantBody_False_DiscardsButCounts(t *testing.T) {
	want := []byte("0123456789abcdef")
	var captured []byte
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// Reply directly with HEADERS+DATA+END_STREAM (no body upload).
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		var streamID uint32
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				streamID = h.StreamID
				_, _ = dec.DecodeBlock(nil, nil)
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      streamID,
					BlockFragment: block,
					EndHeaders:    true,
				})
				_ = srvFr.WriteData(streamID, true, want)
				return
			}
		}
	}}
	_ = captured
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Body != nil {
		t.Fatalf("Body should be nil with WantBody=false, got %v", res.Body)
	}
	if res.BytesReceived != int64(len(want)) {
		t.Fatalf("BytesReceived = %d, want %d", res.BytesReceived, len(want))
	}
}
```

- [ ] **Step 2: Run test**

Run: `go test ./client/ -run TestClient_Do_WantBody_False -count=1 -race -timeout 30s`
Expected: PASS (no impl changes — drainResponse already does this).

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): WantBody=false drops body but counts BytesReceived"
```

---

## Task 15: `Client.Do` — `WantTrailers` capture

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Write test**

```go
func TestClient_Do_WantTrailers_CapturesTrailers(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		var streamID uint32
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				streamID = h.StreamID
				_, _ = dec.DecodeBlock(nil, nil)
				// Initial response HEADERS, no end-stream.
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      streamID,
					BlockFragment: block,
					EndHeaders:    true,
				})
				// One DATA frame.
				_ = srvFr.WriteData(streamID, false, []byte("body"))
				// Trailers: HEADERS with end_stream.
				tblock, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte("grpc-status"), Value: []byte("0")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      streamID,
					BlockFragment: tblock,
					EndHeaders:    true,
					EndStream:     true,
				})
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		WantTrailers: true,
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if len(res.Trailers) != 1 || string(res.Trailers[0].Name) != "grpc-status" {
		t.Fatalf("trailers = %+v", res.Trailers)
	}
}
```

- [ ] **Step 2: Run test**

Run: `go test ./client/ -run TestClient_Do_WantTrailers -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): WantTrailers captures HTTP/2 trailers"
```

---

## Task 16: `Client.Do` — RST_STREAM returns `*StreamResetError`

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Write test**

```go
func TestClient_Do_StreamReset_ReturnsTypedError(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				_ = srvFr.WriteRSTStream(h.StreamID, frame.ErrCodeRefusedStream)
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = c.Do(ctx, &Request{Method: "GET", Path: "/"})
	var rs *StreamResetError
	if !errors.As(err, &rs) {
		t.Fatalf("expected *StreamResetError, got %v", err)
	}
	if rs.Code != frame.ErrCodeRefusedStream {
		t.Fatalf("code = %v, want REFUSED_STREAM", rs.Code)
	}
}
```

- [ ] **Step 2: Run test**

Run: `go test ./client/ -run TestClient_Do_StreamReset -count=1 -race -timeout 30s`
Expected: PASS (drainResponse already returns `*StreamResetError`).

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): RST_STREAM surfaces as *StreamResetError"
```

---

## Task 17: `Client.DoStream` — initial HEADERS + Recv

**Files:**
- Modify: `client/client.go` (add `DoStream`)
- Modify: `client/client_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestClient_DoStream_RecvDataChunks(t *testing.T) {
	chunks := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		var streamID uint32
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				streamID = h.StreamID
				_, _ = dec.DecodeBlock(nil, nil)
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      streamID,
					BlockFragment: block,
					EndHeaders:    true,
				})
				for i, ck := range chunks {
					_ = srvFr.WriteData(streamID, i == len(chunks)-1, ck)
				}
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sr, err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	if sr.Status != 200 {
		t.Fatalf("status = %d", sr.Status)
	}
	var got [][]byte
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventData {
			cp := make([]byte, len(ev.Data))
			copy(cp, ev.Data)
			got = append(got, cp)
		}
		if ev.EndStream {
			break
		}
	}
	if len(got) != 3 {
		t.Fatalf("got %d chunks, want 3", len(got))
	}
	for i, want := range chunks {
		if !bytes.Equal(got[i], want) {
			t.Fatalf("chunk %d = %q, want %q", i, got[i], want)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./client/ -run TestClient_DoStream_Recv -count=1 -race -timeout 30s`
Expected: FAIL — `DoStream` undefined.

- [ ] **Step 3: Add `DoStream` to `client/client.go`**

```go
// DoStream issues a request and returns once the initial HEADERS frame
// has arrived. The caller pumps StreamResponse.Recv for subsequent
// DATA / trailers / reset events. The caller MUST call
// StreamResponse.Close if it does not drain the stream.
func (c *Client) DoStream(ctx context.Context, req *Request) (*StreamResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	cn, release, err := c.tr.acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := cn.NewStream(ctx)
	if err != nil {
		release()
		return nil, err
	}

	hdrs := buildHeaders(req, c.authority)
	endStream := len(req.Body) == 0 && req.BodyReader == nil
	if err := s.SendHeaders(ctx, hdrs, endStream); err != nil {
		_ = s.Close()
		release()
		return nil, err
	}
	if !endStream {
		if err := writeRequestBody(ctx, s, req); err != nil {
			_ = s.Close()
			release()
			return nil, err
		}
	}

	// Read the initial HEADERS event.
	ev, err := s.Recv(ctx)
	if err != nil {
		_ = s.Close()
		release()
		return nil, err
	}
	if ev.Type != conn.EventHeaders {
		_ = s.Close()
		release()
		return nil, fmt.Errorf("client: expected initial HEADERS, got %s", ev.Type)
	}
	status, regular, perr := parseStatus(ev.Headers)
	if perr != nil {
		_ = s.Close()
		release()
		return nil, perr
	}
	sr := &StreamResponse{
		Status:  status,
		Headers: copyHeaderFields(regular),
		stream:  s,
		release: release,
	}
	if ev.EndStream {
		sr.drained = true
	}
	return sr, nil
}
```

- [ ] **Step 4: Run test**

Run: `go test ./client/ -run TestClient_DoStream_Recv -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add client/client.go client/client_test.go
git commit -m "feat(client): DoStream returns after initial HEADERS"
```

---

## Task 18: `Client.DoStream` — `Close` before end sends RST(CANCEL)

**Files:**
- Modify: `client/client_test.go`

- [ ] **Step 1: Write test**

```go
func TestClient_DoStream_CloseBeforeEnd_SendsRSTCancel(t *testing.T) {
	gotRST := make(chan frame.ErrCode, 1)
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		var streamID uint32
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			switch h.Type {
			case frame.FrameTypeHeaders:
				streamID = h.StreamID
				_, _ = dec.DecodeBlock(nil, nil)
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      streamID,
					BlockFragment: block,
					EndHeaders:    true,
				})
				// Send one DATA chunk, then wait for client RST.
				_ = srvFr.WriteData(streamID, false, []byte("partial"))
			case frame.FrameTypeRSTStream:
				rst, ok := h.Decoded.(frame.RSTStreamFrame)
				if ok {
					gotRST <- rst.ErrorCode
				}
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sr, err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	// Receive one chunk, then Close before draining.
	if _, err := sr.Recv(ctx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case code := <-gotRST:
		if code != frame.ErrCodeCancel {
			t.Fatalf("RST code = %v, want CANCEL", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not see RST_STREAM(CANCEL)")
	}
}
```

If `frame.RSTStreamFrame` decoded type / `h.Decoded` access shape differs, replace with:

```go
// Decode RST payload manually if h.Decoded is not exposed.
if h.Type == frame.FrameTypeRSTStream && len(h.Payload) >= 4 {
    code := frame.ErrCode(binary.BigEndian.Uint32(h.Payload[:4]))
    gotRST <- code
    return
}
```

(Verify against `frame/` API before editing the test.)

- [ ] **Step 2: Run test**

Run: `go test ./client/ -run TestClient_DoStream_Close -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add client/client_test.go
git commit -m "test(client): DoStream Close mid-stream sends RST_STREAM(CANCEL)"
```

---

## Task 19: Conformance — pseudo-header ordering (RFC 7540 §8.1.2.1)

**Files:**
- Modify: `client/client_test.go`
- Modify: `docs/RFC_COVERAGE.md`

- [ ] **Step 1: Write conformance test**

```go
func TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst(t *testing.T) {
	// Server captures the decoded request HEADERS block and asserts
	// pseudo-headers (names starting with ':') appear before any
	// regular header.
	captured := make(chan []hpack.HeaderField, 1)
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		dec := hpack.NewDecoder()
		enc := hpack.NewEncoder()
		for {
			h, err := srvFr.ReadFrame(context.Background(), nopHandler{})
			if err != nil {
				return
			}
			if h.Type == frame.FrameTypeHeaders {
				fields, derr := dec.DecodeBlock(nil, h.Payload)
				if derr == nil {
					out := make([]hpack.HeaderField, len(fields))
					for i, f := range fields {
						nm := make([]byte, len(f.Name))
						copy(nm, f.Name)
						vl := make([]byte, len(f.Value))
						copy(vl, f.Value)
						out[i] = hpack.HeaderField{Name: nm, Value: vl}
					}
					captured <- out
				}
				block, _ := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.HeadersFrameParams{
					StreamID:      h.StreamID,
					BlockFragment: block,
					EndHeaders:    true,
					EndStream:     true,
				})
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("x-trace-id"), Value: []byte("abc")},
		},
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	fields := <-captured
	seenRegular := false
	for _, f := range fields {
		isPseudo := len(f.Name) > 0 && f.Name[0] == ':'
		if isPseudo && seenRegular {
			t.Fatalf("pseudo-header %q after regular: %+v",
				f.Name, fields)
		}
		if !isPseudo {
			seenRegular = true
		}
	}
}
```

(If the framer decoder consumes the HPACK block via the handler rather than exposing `h.Payload`, replace with a small `*Conn`-backed handler that captures fields. The conn-test pattern shows how.)

- [ ] **Step 2: Run test**

Run: `go test ./client/ -run TestConformance_RFC7540_Sec8_1_2_1 -count=1 -race -timeout 30s`
Expected: PASS.

- [ ] **Step 3: Add row to `docs/RFC_COVERAGE.md`**

Find the existing matrix (it has a row format like `| §8.1.2 | ... | TestConformance_RFC7540_Sec8_1_2_* |`). Add a row identifying:

```
| 8.1.2.1 | Pseudo-header fields appear before regular header fields | TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst | client/client_test.go |
```

(Match the existing column headers and ordering in the file. If the existing matrix uses different separators, follow that convention.)

- [ ] **Step 4: Commit**

```bash
git add client/client_test.go docs/RFC_COVERAGE.md
git commit -m "test(client): conformance for RFC7540 §8.1.2.1 pseudo-header ordering"
```

---

## Task 20: Integration — GET 200 against real `net/http2.Server`

**Files:**
- Create: `client/integration_test.go`

- [ ] **Step 1: Create the integration test file**

```go
package client_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newTLSH2Server(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.StartTLS()
	t.Cleanup(s.Close)
	addr := strings.TrimPrefix(s.URL, "https://")
	return s, addr
}

func clientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestIntegration_Client_GET_Status200(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
}

func TestIntegration_Client_POST_EchoBody(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	c := clientFor(t, addr)

	want := []byte("hello integration")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Do(ctx, &client.Request{
		Method: "POST", Path: "/echo",
		Body:     want,
		WantBody: true,
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if string(res.Body) != string(want) {
		t.Fatalf("body = %q, want %q", res.Body, want)
	}
}

func TestIntegration_Client_ConcurrentRequests_OneClient(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	const N = 32
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
			if err != nil {
				errCh <- err
				return
			}
			if res.Status != 200 {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Do failed: %v", err)
		}
	}
}

func TestIntegration_Client_DoStream_LargeResponse(t *testing.T) {
	const total = 1 << 20 // 1 MiB
	chunk := []byte(strings.Repeat("x", 4096))
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		for written := 0; written < total; written += len(chunk) {
			_, _ = w.Write(chunk)
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sr, err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	if sr.Status != 200 {
		t.Fatalf("status = %d", sr.Status)
	}
	var got int
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == client.EventData {
			got += len(ev.Data)
		}
		if ev.EndStream {
			break
		}
	}
	if got != total {
		t.Fatalf("read %d, want %d", got, total)
	}
}
```

- [ ] **Step 2: Run integration tests**

Run: `go test ./client/ -run TestIntegration_Client -count=1 -race -timeout 120s`
Expected: PASS for all four.

- [ ] **Step 3: Commit**

```bash
git add client/integration_test.go
git commit -m "test(client): integration GET, POST echo, concurrent, large stream"
```

---

## Task 21: Lint, full test run, README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run lint**

Run: `make lint`
Expected: clean. If `revive` flags missing doc comments on exported `client` symbols, add them inline (e.g. `// EventData ...`).

- [ ] **Step 2: Run full test suite with race**

Run: `make test-race`
Expected: clean (every existing test plus the new client tests pass).

- [ ] **Step 3: Update `README.md` "Quick start" section**

Replace the existing `package main` snippet (currently uses `conn` directly) with:

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func get(ctx context.Context, addr string) error {
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{ServerName: "example.com"}},
		},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	res, err := c.Do(ctx, &client.Request{
		Method: "GET", Path: "/",
		WantBody: true,
	})
	if err != nil {
		return err
	}
	fmt.Println("status:", res.Status, "bytes:", res.BytesReceived)
	return nil
}
```

Also update the **Status:** line at the top of README.md from "Phase B.2.6 (B.2 complete)" to:

```
**Status:** Phase C.1 — public `client` package ships sync `Do` and
streaming `DoStream` over a single-connection lazy-dial transport,
with conn-only auto-redial and `(*conn.Conn).IsAlive` for transport
reuse decisions.
```

And update the Phase list — change C from `*(planned)*` to:

```
- **C.1 — Public client API** *(this release)*: `client.Client`,
  `Request`, `Response`, sync `Do` and streaming `DoStream`,
  single-connection transport with conn-level auto-redial,
  `(*conn.Conn).IsAlive` helper for transport reuse decisions.
- **C.2 / C.3 / C.4 — pool, discovery, stats** *(planned)*.
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README quick start to use client package"
```

---

## Task 22: Final verification

- [ ] **Step 1: Re-run everything**

Run sequentially:

```bash
make tidy
make lint
make test-race
```

All three expected: clean.

- [ ] **Step 2: Self-check phase status doc strings**

Open `conn/doc.go`. If it carries a "current phase" note (it does — see `conn/doc.go`), append a one-line note that the public client now lives in `client/`. Avoid duplicating the README content; one sentence is enough.

If you make this edit, commit:

```bash
git add conn/doc.go
git commit -m "docs(conn): note client package availability"
```

(Skip if `conn/doc.go` already has no phase status mention; verify before editing.)

- [ ] **Step 3: Push branch**

```bash
git push -u origin claude/practical-ritchie-94f8ed
```

(User confirms remote branch name; this is the worktree branch.)

---

## Self-review checklist

1. **Spec coverage:** every public type and method described in the spec maps to a task —
   `Client` (T10), `ClientOptions` (T10), `NewClient` (T10), `Do` (T11–T16), `DoStream` (T17–T18),
   `Close` (T10), `Request` (T3), `Response` (T4), `StreamResponse` (T4 + T17), `StreamEvent` /
   `EventType` (T4), all error types (T2), `transport` iface (T5), `singleConn` (T6–T9),
   `(*conn.Conn).IsAlive` (T1). ✓

2. **Spec coverage — non-goal Path templating:** explicitly not in any task (correctly absent). ✓

3. **Type consistency check:**
   - `client.EventType` and `client.StreamEvent.Type` use the same constants throughout.
   - `client.StreamEvent.Data` aliases scratch (documented in T4 doc-comment).
   - `singleConn.acquire` / `transport.acquire` / `singleConn.close` / `transport.close` signatures match.
   - `Request.Body []byte` vs `Request.BodyReader io.Reader` precedence consistent (T12 vs T13).

4. **No placeholders:** every step contains either explicit code, an explicit command with expected output, or an explicit text edit. The Task 21 README update spells out the exact text replacement.

5. **Spec corrections recorded:** API drift between spec and conn source (`SendData` not `WriteData`; `Stream.Close` not `Stream.Reset(code)`; `ConnOptions` already has `Dialer`) is documented up front so the engineer is not blindsided.
