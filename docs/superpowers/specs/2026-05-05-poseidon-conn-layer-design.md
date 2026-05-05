# Poseidon HTTP/2 Client — Phase B: Connection Layer

**Status:** Design (proposed)
**Date:** 2026-05-05
**Module:** `github.com/lodgvideon/poseidon-http-client`
**Builds on:** Phase A (`v0.1.0-rc1` — frame layer + HPACK)
**Go version:** 1.24
**License:** MIT

---

## 1. Context and Goals

Phase B layers a single-connection HTTP/2 client transport over Phase A's
frame codec. One `*Conn` wraps one `net.Conn` (TLS or cleartext) and runs
the full HTTP/2 lifecycle: handshake (preface + SETTINGS exchange), stream
state machine (RFC 7540 §5.1), flow-control accounting (RFC §5.2 / §6.9),
PING/GOAWAY, and frame→stream event routing.

**Fixed decisions (input from owner):**

| Question | Choice |
|---|---|
| TLS stack | `crypto/tls` (Go stdlib) |
| h2c (cleartext HTTP/2) | Supported (separate Dialer) |
| Goroutine model | Caller drives via `Conn.Pump(ctx)` |
| Flow control default | Adaptive (window grows on demand) |

The connection layer does NOT manage its own goroutine pool: caller runs
exactly one `Pump(ctx)` goroutine. This keeps lifecycle explicit and
suitable for embedding in a load generator that orchestrates many
connections without surprise goroutines.

### Non-goals for Phase B

- Per-host connection pool, retry, balancing, discovery — Phase C.
- Public `RequestStats` with full timing — Phase C (Phase B exposes
  `ConnStats` minimal counters).
- Server push acceptance — peer's PUSH_PROMISE is RST'd
  (`SETTINGS_ENABLE_PUSH=0` at handshake).

---

## 2. Architecture Overview

### 2.1 Layers (built bottom-up over Phase A)

```
frame, hpack            ─ Phase A foundation (unchanged)
   │
internal/flowctl        ─ flow-control window arithmetic, adaptive sizing
   │
internal/streamstate    ─ stream state machine (RFC §5.1: idle→open→half→closed)
   │
conn                    ─ Conn, Dialer (TLS / h2c), Stream, Pump, SETTINGS sync
```

### 2.2 SOLID alignment

| Principle | Application |
|---|---|
| **S** | `flowctl` does only window math; `streamstate` only transitions; `conn` orchestrates. |
| **O** | `Dialer` is an interface — TLS, h2c, custom (mTLS, SNI override) plug in without touching core. |
| **L** | `Stream` interface is direction-symmetric; same API works for any state. |
| **I** | Narrow interfaces: `Dialer.Dial`, `Conn.NewStream`, `Stream.Send/Recv`. |
| **D** | `Conn` depends on `Dialer` and `frame.Handler` interfaces — concrete TLS or in-memory transports interchangeable. |

### 2.3 Concurrency model

- Exactly **one** `Conn`, one `Pump(ctx)` goroutine (caller-supplied).
- Many concurrent `*Stream` operations (typical: one goroutine per request).
- Single `sync.Mutex` on `Conn` serializes the writer side (frame
  encoding + write to `net.Conn`).
- Reader is lock-free (single-goroutine Pump owns read state).
- Per-stream `sync.Cond` wakes blocked senders on `WINDOW_UPDATE`.

---

## 3. Package Layout

```
poseidon-http-client/
├── ... (Phase A files unchanged)
├── conn/                                   # PUBLIC: HTTP/2 connection layer
│   ├── doc.go                              # package overview, contracts
│   ├── errors.go                           # sentinels
│   ├── conn.go                             # Conn struct, Pump, Close, write-side mutex
│   ├── dialer.go                           # Dialer interface
│   ├── tls_dialer.go                       # TLSDialer (crypto/tls + ALPN h2)
│   ├── h2c_dialer.go                       # H2CDialer (raw TCP)
│   ├── settings.go                         # SETTINGS exchange + ack tracking
│   ├── stream.go                           # Stream + StreamEvent
│   ├── handshake.go                        # connection preface + initial SETTINGS exchange
│   ├── handler.go                          # frame.Handler routing frames to Streams
│   ├── ping.go                             # RTT measurement via PING/PING-ACK
│   ├── *_test.go
│   ├── conformance_test.go                 # h2spec subset via in-process server
│   └── bench_test.go
├── internal/flowctl/
│   ├── window.go
│   ├── window_test.go
│   ├── adaptive.go
│   └── adaptive_test.go
├── internal/streamstate/
│   ├── state.go
│   ├── state_test.go
│   └── transitions.go
└── testdata/
    └── h2spec/                             # cached h2spec config / fixtures
```

Phase A files (`frame/`, `hpack/`, `internal/bytesx/`) are not modified.

---

## 4. Public API (`conn` package)

### 4.1 Sentinels

```go
var (
    ErrConnClosed       = errors.New("poseidon/conn: connection closed")
    ErrGoAwayReceived   = errors.New("poseidon/conn: peer sent GOAWAY")
    ErrStreamReset      = errors.New("poseidon/conn: stream reset by peer")
    ErrStreamMaxStreams = errors.New("poseidon/conn: peer SETTINGS_MAX_CONCURRENT_STREAMS reached")
    ErrInvalidState     = errors.New("poseidon/conn: operation invalid in current stream state")
    ErrPushUnsupported  = errors.New("poseidon/conn: server push disabled")
    ErrTLSNoH2          = errors.New("poseidon/conn: TLS ALPN did not negotiate h2")
)
```

### 4.2 Dialer

```go
type Dialer interface {
    Dial(ctx context.Context, addr string) (net.Conn, error)
}

type TLSDialer struct {
    Config    *tls.Config // ALPN must include "h2"; nil → sane default
    NetDialer *net.Dialer
}
func (*TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error)

type H2CDialer struct {
    NetDialer *net.Dialer
}
func (*H2CDialer) Dial(ctx context.Context, addr string) (net.Conn, error)
```

`TLSDialer.Config == nil` defaults to `&tls.Config{MinVersion: TLS12, NextProtos: []string{"h2"}}`. After `Dial`, the `*tls.Conn` is verified to have negotiated `h2`; otherwise `ErrTLSNoH2`.

### 4.3 Conn

```go
type ConnOptions struct {
    InitialWindowSize    uint32 // default 65535 (RFC initial)
    MaxConcurrentStreams uint32 // default 100
    MaxFrameSize         uint32 // default 16384
    MaxHeaderListSize    uint32 // 0 = unbounded
    HeaderTableSize      uint32 // default 4096
    EnablePush           bool   // default false (clients refuse push)

    // Adaptive flow control: 0 disables, otherwise upper bound for
    // recv window on each stream.
    AdaptiveMaxWindow uint32 // default 4 << 20

    PingInterval time.Duration // 0 = disabled
    PingTimeout  time.Duration // default 30s

    OnFrameError func(err error) // optional logger hook
}

type Conn struct { /* unexported */ }

func NewConn(ctx context.Context, transport net.Conn, opts ConnOptions) (*Conn, error)
func Dial(ctx context.Context, dialer Dialer, addr string, opts ConnOptions) (*Conn, error)

func (*Conn) Pump(ctx context.Context) error
func (*Conn) Close(drainTimeout time.Duration) error
func (*Conn) NewStream(ctx context.Context) (*Stream, error)
func (*Conn) Stats() ConnStats
```

```go
type ConnStats struct {
    StreamsOpened    uint64
    StreamsClosed    uint64
    BytesSent        uint64
    BytesReceived    uint64
    PingsRTTNanos    int64        // last successful PING RTT
    PeerGoAway       bool
    PeerLastStreamID uint32
    PeerErrCode      frame.ErrCode
}
```

### 4.4 Stream

```go
type Stream struct { /* unexported */ }

func (*Stream) ID() uint32
func (*Stream) State() StreamState

func (*Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error
func (*Stream) SendData(ctx context.Context, data []byte, endStream bool) error
func (*Stream) SendTrailers(ctx context.Context, fields []hpack.HeaderField) error
func (*Stream) Recv(ctx context.Context) (StreamEvent, error)
func (*Stream) Cancel(code frame.ErrCode) error

type StreamEvent struct {
    Kind      StreamEventKind
    Headers   []hpack.HeaderField
    Data      []byte
    EndStream bool
    ErrCode   frame.ErrCode
}

type StreamEventKind uint8
const (
    StreamHeaders StreamEventKind = 1 + iota
    StreamData
    StreamTrailers
    StreamClosed
)

type StreamState uint8
const (
    StateIdle StreamState = iota
    StateReservedLocal
    StateReservedRemote
    StateOpen
    StateHalfClosedLocal
    StateHalfClosedRemote
    StateClosed
)
```

**Lifetime contracts:**

- `StreamEvent.Headers` and `StreamEvent.Data` are caller-owned: copies of decoder/peer data, safe to retain. (This is a deliberate departure from Phase A's "no copy" hot-path contract — events cross goroutine boundaries.)
- `Cancel` is idempotent.
- `Pump` is the single reader goroutine; `Send*` and `Recv` may run from any goroutine.
- After `Close` initiates shutdown, `NewStream` returns `ErrConnClosed`; existing streams complete or get RST.

---

## 5. Internal Building Blocks

### 5.1 `internal/flowctl`

```go
type FlowWindow struct { available atomic.Int64 }

func New(initial int32) *FlowWindow
func (*FlowWindow) TryConsume(n int32) bool        // sender quota
func (*FlowWindow) Replenish(n int32) int64        // peer WINDOW_UPDATE / local drain
func (*FlowWindow) Available() int64
func (*FlowWindow) AdjustInitial(delta int32)      // SETTINGS_INITIAL_WINDOW_SIZE delta
```

Adaptive sizer (`adaptive.go`):

```go
type AdaptiveSizer struct {
    current  uint32
    min, max uint32
}

// Decide returns a new window cap based on consumption pattern:
//   - if the consumer drains fast (window shrinks below 50% of cap):
//     newCap = min(currentCap * 1.5, max)
//   - if slow (window stays above 75%):
//     newCap = max(currentCap * 0.75, min)
//   - otherwise unchanged.
func (s *AdaptiveSizer) Decide(consumedSinceLast, capacity uint32) uint32
```

### 5.2 `internal/streamstate` — RFC 7540 §5.1

```go
type State uint8
const (
    Idle State = iota
    ReservedLocal
    ReservedRemote
    Open
    HalfClosedLocal
    HalfClosedRemote
    Closed
)

type Event uint8
const (
    EventSendHeaders Event = iota
    EventRecvHeaders
    EventSendEndStream
    EventRecvEndStream
    EventSendRSTStream
    EventRecvRSTStream
    EventRecvPushPromise // client receives → must RST
)

func Transition(s State, e Event) (State, error) // returns ErrInvalidTransition for protocol error
```

Implemented as a switch table. Every (state, event) pair is enumerated; invalid combinations return a typed error mapped to `ErrCodeProtocolError` or `ErrCodeStreamClosed` per RFC §5.1.

### 5.3 `conn/handler.go` — frame routing

`connHandler` implements `frame.Handler`:

- DATA → `c.streamByID(fh.StreamID).pushEvent(StreamEvent{Kind: Data, ...})` after copying payload, then update flow-control accounting.
- HEADERS / CONTINUATION → buffer into stream's pending block; on END_HEADERS, decode HPACK and push `StreamHeaders` (or `StreamTrailers` if state is HalfClosedLocal).
- PUSH_PROMISE → immediately send RST_STREAM (`ErrCodeRefusedStream`) and skip.
- PRIORITY → log; ignore (load generator does not honour priority).
- RST_STREAM → set stream's `ErrCode` and push `StreamClosed`.
- SETTINGS → apply parameters atomically; send ACK if not already an ACK.
- SETTINGS ACK → mark our last sent SETTINGS as applied.
- PING → if ACK, measure RTT; otherwise echo back as ACK.
- GOAWAY → mark conn closing; refuse new `NewStream`; existing streams up to `lastStreamID` continue.
- WINDOW_UPDATE → wake blocked senders on stream (or connection).

### 5.4 Send loop

`Stream.SendHeaders/SendData/SendTrailers` flow:

```go
1. Acquire stream lock (state check)
2. State transition pre-check (e.g. SendHeaders only valid in Idle/Open)
3. If SendData and len > 0:
   - Wait until stream-send-window AND connection-send-window have capacity
     (or ctx cancels)
4. Acquire conn.writeMu
5. Encode via hpack/frame
6. Write to net.Conn through *frame.Framer
7. Release conn.writeMu
8. Update local state (HalfClosedLocal if endStream, etc.)
```

Write mutex guarantees frame interleaving correctness (HPACK requires HEADERS+CONTINUATION to be contiguous on the wire).

---

## 6. Handshake + Lifecycle

### 6.1 Handshake (RFC §3.5 + §6.5)

`NewConn(ctx, transport, opts)`:

1. Construct `*frame.Framer(transport, transport)`, `hpack.Encoder/Decoder`.
2. If `transport` is `*tls.Conn`, verify `ConnectionState().NegotiatedProtocol == "h2"` → else `ErrTLSNoH2`.
3. Send: client preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`) + initial `SETTINGS` (from `ConnOptions`).
4. Read peer's first frame: must be `SETTINGS`. Apply parameters.
5. Send `SETTINGS_ACK` for peer's settings.
6. Wait for peer's `SETTINGS_ACK` of our settings (with `SettingsTimeout`).
7. Return `*Conn`. Caller now runs `Pump(ctx)`.

### 6.2 Pump loop

```go
func (c *Conn) Pump(ctx context.Context) error {
    h := &connHandler{c: c}
    for {
        select {
        case <-ctx.Done():
            return c.shutdown(ctx.Err())
        default:
        }
        fh, err := c.framer.ReadFrame(ctx, h)
        if err != nil {
            if err == io.EOF {
                return c.peerClosed()
            }
            return c.fatal(err)
        }
        c.stats.bytesRecv.Add(uint64(fh.Length + frame.FrameHeaderSize))
    }
}
```

### 6.3 Stream lifecycle (RFC §5.1)

```
NewStream()              → Idle, allocate next odd ID
SendHeaders(ES=false)    → Open
SendHeaders(ES=true)     → HalfClosedLocal
SendData(ES=true)        → HalfClosedLocal/Closed
Recv HEADERS             → Open / HalfClosedRemote
Recv DATA + END_STREAM   → HalfClosedRemote
Both ES                  → Closed
Cancel(code)             → SendRSTStream → Closed (idempotent)
Receive RST_STREAM       → Closed
```

Closed streams stay in the streams map for ~1s grace period (RFC §5.1.2 allows late frames). A Pump-driven ticker GCs them.

### 6.4 PING / RTT

If `PingInterval > 0`, Pump installs a ticker. On tick: send PING with 8-byte timestamp payload. On PING-ACK, measure RTT and update `Stats().PingsRTTNanos`. If `PingTimeout` elapses without ACK, `Conn.fatal(ErrCodeProtocolError)`.

### 6.5 GOAWAY

- Receive: mark `Conn` closing; `NewStream` returns `ErrGoAwayReceived`; existing streams (id ≤ peer's `lastStreamID`) continue.
- Send (during `Close`): `lastStreamID = 0`, `code = NoError`, refuse new streams.

### 6.6 Close

```go
func (c *Conn) Close(drainTimeout time.Duration) error {
    // 1. Send GOAWAY(0, NoError).
    // 2. Wait up to drainTimeout for live streams.
    // 3. Force-close transport.
    // 4. Pump returns ErrConnClosed.
}
```

---

## 7. Testing Strategy

### 7.1 Levels

| Level | Coverage |
|---|---|
| Unit (`flowctl`, `streamstate`) | Pure logic, table-driven. Like Phase A. |
| Unit (`conn` internals: settings, handshake state machine) | Mock `net.Conn` (`io.Pipe` pair). |
| Integration (h2c) | Loopback TCP + Go stdlib `net/http2.Server` as peer. |
| Integration (TLS) | `httptest.NewTLSServer` + ALPN h2. |
| h2spec subset | Optional: spawn `h2spec` against an in-process echo server. |
| Fuzz | Pump against random byte streams via `io.Pipe`. Invariants: no panic, no goroutine leak. |
| Race | All tests under `-race`. |

**Why stdlib `net/http2.Server` is OK in tests:** the runtime ban is on shipping `net/http`/`x/net/http2` in product code. As a test peer, stdlib is the natural reference implementation for cross-checking our behaviour.

### 7.2 Naming convention

`TestConformance_RFC7540_§6_5_Settings*`, `*§6_9_FlowControl*`, `*§5_1_StateMachine*` — keeps the matrix script in CI working.

### 7.3 Bench targets

| Bench | Target | Notes |
|---|---|---|
| `BenchmarkConn_Handshake_h2c` | ≤ 50 µs | inc preface + SETTINGS exchange |
| `BenchmarkConn_Handshake_TLS` | informational | TLS-bound, baseline only |
| `BenchmarkConn_RequestRoundTrip_h2c_GET` | ≤ 5 µs/req on warm conn | full HEADERS→200→END |
| `BenchmarkStream_SendData_64KB_chunked` | ≤ 1 µs/op | window-bound write loop |

Hot-path allocation budget: `Stream.Recv` allocates ≤ 2 allocs/event (data copy + headers slice). `Stream.Send*` should remain `0 allocs/op` after warmup.

### 7.4 Goroutine leak detection

All integration tests register a goroutine count check via `runtime.NumGoroutine()` before/after, with a small grace for finalizers. Phase B explicitly does NOT spawn goroutines from `Conn` methods other than what the caller drives.

---

## 8. CI / QA Gateway Pipelines (delta over Phase A)

`.github/workflows/ci.yml` — adds an `integration` job:

```yaml
integration:
  runs-on: ubuntu-24.04
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version: '1.24' }
    - run: go test -race -count=1 -tags=integration ./conn/...
```

`.github/workflows/h2spec.yml` (new) — installs `h2spec` and runs a subset (frame validation, stream state, flow control) against an in-process Poseidon server (build tag `h2spec`).

Bench-gate covers Phase B benches with the same 0-alloc enforcement on Phase A benches and the documented ≤2 allocs/op budget on Phase B `Stream.Recv` events.

---

## 9. Forward Compatibility (Phase C preview)

Phase C builds on the contracts established here:

- `Dialer` is the extension point for discovery (DNS, Consul, k8s, custom corp). Phase C will add `DiscoveryDialer` wrapping any base Dialer.
- `Conn.Stats()` provides the seed for Phase C's per-request `RequestStats` (Phase C adds Stream-level timing on top).
- `Conn.NewStream` errors (`ErrStreamMaxStreams`, `ErrConnClosed`) drive Phase C retry/balancing decisions.
- `Stream.Cancel`/`Stream.Recv` are the boundaries Phase C wires into a request/response API.

Frozen contracts:

| Decision | Rationale |
|---|---|
| `Conn.Pump` is caller-driven | Phase C's pool spawns its own pump goroutine per conn — explicit control. |
| `Stream.Recv` returns owned data copies | Phase C bridges to user-facing Response without further copies. |
| `Dialer` is an interface | Phase C adds `DiscoveryDialer` without touching Phase B internals. |

---

## 10. Open Questions / Future Work

1. **Server push behaviour:** spec says we RST on PUSH_PROMISE. Confirm load test scenarios don't need anything else (e.g. stats counter on push count).
2. **HPACK dynamic table sizing:** Phase B leaves the 4096 default. For high-RPS scenarios with repetitive headers, raising to 16-64 KiB may pay off — revisit during bench tuning.
3. **Per-stream ctx cancel propagation:** when caller's request `ctx` is cancelled, Stream.Cancel should fire RST. Implementation detail; covered in tests.
4. **h2spec coverage matrix:** which subset is "minimal viable" for CI? Pre-flight phase B implementation will choose tests that pass on day 1.

---

## 11. Acceptance Criteria for Phase B

Phase B ships as `v0.2.0-rc1` when:

- [ ] `Conn.Dial` + `Pump` + `NewStream` + `Stream.SendHeaders/SendData/Recv` all functional against Go stdlib `net/http2.Server` (h2c and TLS).
- [ ] Stream state machine transitions match RFC §5.1 exactly (table-driven test green).
- [ ] Flow control: peer-mandated window updates honoured; adaptive sizer expands/shrinks windows correctly.
- [ ] PING-RTT measurement works.
- [ ] GOAWAY received → no new streams; existing streams complete or RST.
- [ ] `Close(drainTimeout)` drains gracefully.
- [ ] `go test -race ./...` green.
- [ ] `golangci-lint` green.
- [ ] Bench gate: connection benchmarks meet allocation budget.
- [ ] Integration tests against stdlib `net/http2.Server` green for h2c and TLS.
- [ ] No goroutine leaks under integration tests.
- [ ] README updated with Phase B usage example.
