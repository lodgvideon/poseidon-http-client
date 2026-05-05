# Poseidon Phase B.1 — Connection Layer (single-stream MVP) — Design

**Status:** draft
**Date:** 2026-05-05
**Depends on:** Phase A (`frame/`, `hpack/`, `internal/bytesx/`) — released as `v0.1.0-rc1`.
**Successor:** Phase B.2 — multiplex + flow control + state machine.

---

## 1. Context and Goals

Phase A delivered a self-contained codec — frames in/out, HPACK encode/decode,
zero allocations on the hot path, but no networking. Phase B.1 lands the
**minimum viable HTTP/2 connection** on top of it: dial a real h2 server over
TLS, exchange settings, run **exactly one** request/response stream end to end,
and shut down cleanly. It is the smallest unit that proves the Phase A API
survives contact with a real peer.

Out of scope for B.1 (deferred to B.2):
- Concurrent streams (multiplex). B.1 enforces `max in-flight streams = 1`.
- Real flow-control management. B.1 honors peer `SETTINGS_INITIAL_WINDOW_SIZE`
  on receive sizing but does not dynamically grow/shrink windows; the local
  receive window stays at the default 65535 octets and the peer's send window
  bound is enforced by short-circuiting on overflow rather than by stalling.
- Stream state machine (`open / half-closed / closed`) as a first-class FSM.
  B.1 tracks a binary `active / done` per the single live stream; B.2 adds
  the full RFC 7540 §5.1 FSM and uses it for multiplexed peers.
- Server push (`PUSH_PROMISE`). We advertise `SETTINGS_ENABLE_PUSH = 0` and
  treat any `PUSH_PROMISE` from the peer as a connection error.
- `h2c` (cleartext) — not a load-generator target; could land trivially in
  B.2 by swapping the dialer.

### 1.1 Why a B.1 split exists at all

The full B surface (TLS, ALPN, SETTINGS, flow control, multiplex, FSM,
GOAWAY, ping-keepalive) is too much to land in one plan without integration
risk piling up at the end. B.1 carves out the smallest end-to-end slice that
proves: the Phase A frame and HPACK APIs do not need breaking changes, the
goroutine model holds, and our codec interoperates with a reference peer
(`net/http2.Server`). With that locked, B.2 is "expand the slice", not
"discover an architectural mistake".

---

## 2. Architecture

```
                        +----------------------------+
                        |        client.Caller       |     (Phase C — out of scope)
                        +-------------+--------------+
                                      |
                                      v
                        +----------------------------+
                        |          conn.Conn         |
                        |  (this spec, package conn) |
                        +-------------+--------------+
                                      |
              +-----------------------+-----------------------+
              |                       |                       |
              v                       v                       v
     +-----------------+      +---------------+      +-----------------+
     |   Phase A       |      |  Phase A      |      | crypto/tls      |
     |   frame.Framer  |      |  hpack.Enc    |      | (NextProtos=h2) |
     +-----------------+      |  hpack.Dec    |      +-----------------+
                              +---------------+
```

- `conn.Conn` is the **only** new public type at this stage. It owns one
  `*frame.Framer`, one `*hpack.Encoder`, one `*hpack.Decoder`, and one
  `net.Conn` (typically `*tls.Conn`).
- A single internal **reader goroutine** owns `Framer.ReadFrame` for the
  lifetime of the connection. It dispatches frame events through the
  Phase A `frame.Handler` interface to the per-connection state machine,
  which in turn forwards them to the active `Stream`.
- All writes go through `Conn.write*` methods guarded by `conn.wmu`
  (`sync.Mutex`). Callers (`Stream.SendHeaders`, `Stream.SendData`, internal
  control writes) acquire it for the duration of a single frame. **No
  separate writer goroutine yet** — Phase C will add an async write queue
  for high-throughput multiplex; the public API does not change when it
  lands (see §10).

### 2.1 Information flow for one request

```
caller -> Conn.NewStream(ctx)            -> *Stream (id=1)
caller -> Stream.SendHeaders(...)        -> wmu.Lock; Framer.WriteHeaders;  wmu.Unlock
caller -> Stream.SendData(...,end=true)  -> wmu.Lock; Framer.WriteData;     wmu.Unlock
                                            (request side closed)

reader goroutine: Framer.ReadFrame(ctx, &connHandler)
  connHandler.OnHeaders(fh, hb, ...)     -> hpack.Decoder.DecodeBlock(hb, visit)
                                            -> stream.events <- HeadersEvent{...}
  connHandler.OnData(fh, payload, pad)   -> stream.events <- DataEvent{...}
                                            (END_STREAM observed -> stream.events <- EndEvent)
caller <- Stream.Recv(ctx)               -> reads from stream.events channel
caller -> Stream.Close()                 -> if not yet closed, RST_STREAM(NO_ERROR or CANCEL)
```

### 2.2 SOLID notes

- **Single Responsibility:** dialing (`conn.Dial*`), framing (`frame.Framer`,
  Phase A), header coding (`hpack`, Phase A), connection lifecycle (`conn.Conn`),
  stream lifecycle (`conn.stream`) are separate types/files.
- **Open/Closed:** `conn.Dialer` interface lets B.2 add `h2c` and Phase C add
  custom transports (e.g. UDS, mTLS-bound sockets) without touching `Conn`.
- **Liskov:** the `frame.Handler` impl in `conn` is the same shape Phase A
  uses internally — so we can re-target it at testServer/testFramer in
  unit tests without subclassing.
- **Interface segregation:** `Stream` exposes only `SendHeaders / SendData /
  Recv / Close`. No "advanced" knobs leak from `Conn` through `Stream`.
- **Dependency inversion:** `Conn` depends on `*frame.Framer` and
  `hpack.Encoder/Decoder` concretely (they are package-private value-style
  types, not interfaces) but on `net.Conn` and `Dialer` abstractly.

---

## 3. Package Layout

```
conn/
  doc.go                 // package overview
  conn.go                // *Conn, lifecycle, reader goroutine
  dial.go                // Dial, DialTLS, Dialer interface
  stream.go              // *Stream + StreamEvent variants
  options.go             // ConnOptions, defaults
  errors.go              // ConnError, StreamError, sentinels
  settings.go            // exchangeSettings (preface + initial SETTINGS)
  handler.go             // internal frame.Handler implementation
  conn_test.go           // unit tests against in-memory net.Pipe peer
  integration_test.go    // tests against net/http2.Server
  bench_test.go          // hot-path benches (request RTT, throughput)
  fuzz_test.go           // fuzz over framing of arbitrary peer responses
```

No new internal packages. `internal/bytesx` already provides the pool and
slice helpers that B.1 reuses.

---

## 4. Public API

```go
package conn

import (
    "context"
    "crypto/tls"
    "net"
    "time"

    "github.com/lodgvideon/poseidon-http-client/hpack"
)

// Dialer abstracts how the underlying transport is established.
type Dialer interface {
    Dial(ctx context.Context, addr string) (net.Conn, error)
}

// TLSDialer dials addr and runs a TLS handshake with NextProtos=["h2"].
// If the peer does not negotiate "h2" via ALPN, returns ErrALPNFailed.
type TLSDialer struct {
    Config *tls.Config // optional; if nil, a Default-with-h2 config is used
}

func (d *TLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error)

// ConnOptions tunes a connection. Zero value is a sane default.
type ConnOptions struct {
    // Dialer used to obtain the underlying net.Conn. Default = TLSDialer{}.
    Dialer Dialer

    // SETTINGS we advertise to the peer. Zero values become RFC defaults
    // EXCEPT EnablePush which we always set to 0 (we never accept push).
    Settings AdvertisedSettings

    // Hard ceiling on a single Stream lifetime. 0 = no timeout.
    StreamDeadline time.Duration

    // Bound on Stream.events buffer. Default 8.
    StreamEventBuffer int
}

type AdvertisedSettings struct {
    HeaderTableSize      uint32 // default 4096
    MaxConcurrentStreams uint32 // B.1 sets this to 1 regardless
    InitialWindowSize    uint32 // default 65535
    MaxFrameSize         uint32 // default 16384
    MaxHeaderListSize    uint32 // 0 = unset (peer chooses)
}

// Conn is one HTTP/2 connection. Methods are goroutine-safe (writes are
// serialized internally; the reader goroutine is owned).
type Conn struct { /* unexported */ }

// NewClientConn wraps an already-handshaken transport. The caller is
// responsible for ALPN negotiation if applicable.
func NewClientConn(ctx context.Context, transport net.Conn, opts ConnOptions) (*Conn, error)

// Dial dials addr (host:port), runs TLS+ALPN, exchanges SETTINGS, and
// returns a ready-to-use connection.
func Dial(ctx context.Context, addr string, opts ConnOptions) (*Conn, error)

// NewStream allocates a new stream. B.1 enforces at most ONE in-flight
// stream per Conn — returns ErrTooManyStreams otherwise.
func (c *Conn) NewStream(ctx context.Context) (*Stream, error)

// Close sends GOAWAY(NO_ERROR), drains, and closes the transport.
func (c *Conn) Close() error

// Stats returns a point-in-time snapshot of connection counters.
func (c *Conn) Stats() ConnStats

type ConnStats struct {
    BytesSent     int64
    BytesReceived int64
    FramesSent    int64
    FramesReceived int64
    StreamsOpened uint32
}

// Stream is a single in-flight HTTP/2 stream.
type Stream struct { /* unexported */ }

func (s *Stream) ID() uint32

// SendHeaders sends a HEADERS frame (always EndHeaders=true in B.1; we do
// not split into CONTINUATION at this stage). If endStream is true, we
// also half-close the request side.
func (s *Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error

// SendData sends a single DATA frame. Caller is responsible for chunking
// to fit MaxFrameSize. If endStream is true, the request side closes.
// Returns ErrFlowControlExhausted if peer's send window cannot accommodate
// the payload (B.1 does not stall — caller chunks smaller).
func (s *Stream) SendData(ctx context.Context, p []byte, endStream bool) error

// Recv blocks until the next event for this stream is ready, the stream
// terminates, or ctx is cancelled.
func (s *Stream) Recv(ctx context.Context) (StreamEvent, error)

// Close cancels the stream. If the response side has not yet ended, sends
// RST_STREAM(CANCEL). Idempotent.
func (s *Stream) Close() error

// StreamEvent is the discriminated union of things the peer sends for a
// stream. The Type field tells the caller which Variant to read.
type StreamEvent struct {
    Type StreamEventType

    // Headers populated for EventHeaders / EventTrailers. Slices alias the
    // hpack decoder's scratch arena and are valid only until the next Recv.
    Headers []hpack.HeaderField

    // Data populated for EventData. Slice aliases an internal pool buffer
    // and is valid only until the next Recv.
    Data []byte

    // EndStream is true when this event closes the response side.
    EndStream bool

    // RSTCode populated for EventReset.
    RSTCode frame.ErrCode
}

type StreamEventType uint8
const (
    EventHeaders  StreamEventType = iota + 1
    EventData
    EventTrailers
    EventReset
)
```

### 4.1 Slice ownership

The Phase A contract — "decoder slice fields alias scratch and are valid
only during the visit call" — is preserved at the connection layer with a
narrow, documented extension:

> A `StreamEvent.Headers` / `StreamEvent.Data` slice is valid until the
> caller's next call to `Stream.Recv` or `Stream.Close` on the same stream.
> Callers who need to retain the bytes MUST copy them.

Why the looser bound vs. Phase A: the reader goroutine cannot hold the
hpack scratch arena indefinitely (it would block subsequent header blocks),
so before pushing a `StreamEvent` to the channel we **swap arenas**. Each
`Stream` owns a small ring of two arenas; the reader fills one while the
caller reads from the other. This keeps zero-alloc steady state and makes
the lifetime contract explicit.

### 4.2 Errors

```go
var (
    ErrALPNFailed         = errors.New("conn: ALPN did not negotiate h2")
    ErrTooManyStreams     = errors.New("conn: B.1 supports one in-flight stream")
    ErrConnClosed         = errors.New("conn: connection closed")
    ErrStreamClosed       = errors.New("conn: stream already closed")
    ErrFlowControlExhausted = errors.New("conn: send window too small for payload")
    ErrUnexpectedPushPromise = errors.New("conn: peer sent PUSH_PROMISE while ENABLE_PUSH=0")
)

// ConnError is a connection-fatal error. Always implies the Conn is dead.
type ConnError struct {
    Code   frame.ErrCode  // RFC 7540 §7
    Reason string         // human-readable detail; never includes secrets
    Last   uint32         // last-stream-id from GOAWAY (0 if originated locally)
}
func (e *ConnError) Error() string

// StreamError is non-fatal — RST_STREAM either way.
type StreamError struct {
    StreamID uint32
    Code     frame.ErrCode
}
func (e *StreamError) Error() string
```

After a `*ConnError`, all `Stream` methods on streams created by the dead
connection return `ErrConnClosed`. After a `*StreamError`, `Conn` itself
remains usable for a NEW stream (in B.2 sense; in B.1 the caller must
`Close()` and `Dial` again or simply spin up another Conn).

---

## 5. Wire Lifecycle

### 5.1 Handshake

1. `Dial(ctx, addr, opts)`:
   1. `opts.Dialer.Dial(ctx, addr)` → raw `net.Conn`. Default dialer wraps
      stdlib `tls.Dialer` with `NextProtos=["h2"]`. After handshake, assert
      `tls.Conn.ConnectionState().NegotiatedProtocol == "h2"`; otherwise
      `ErrALPNFailed` + close transport.
   2. `Framer.WriteClientPreface()` (Phase A).
   3. `Framer.WriteSettings(advertised)`.
   4. Spawn reader goroutine.
   5. Wait (with `ctx`) for the peer's first SETTINGS frame; record peer
      values. Then write `WriteSettingsAck()`. The protocol allows
      interleaving acks with frames — we keep it strictly serialized in
      B.1 to make the handshake easy to reason about.
   6. Wait for our own SETTINGS to be ACKed (peer SETTINGS frame with
      ACK flag). After that, the connection is "ready" and `Dial` returns.
2. Steady state: reader goroutine pumps `ReadFrame` indefinitely; caller
   uses `NewStream/Stream.SendX/Recv`.

### 5.2 Single stream

Stream IDs are odd-incrementing (`1, 3, 5, ...`). B.1 only ever assigns 1.
The stream is `open` from `SendHeaders` until either both directions reach
`END_STREAM` or one side resets.

### 5.3 Shutdown

`Conn.Close` sends `GOAWAY(LastStreamID=lastClient, NO_ERROR, "")`, waits
up to a short grace timeout for in-flight frames to drain, then closes
the transport. Concurrent `Conn.Close` calls are safe (idempotent).

If the peer sends GOAWAY first, the reader goroutine surfaces it as a
`*ConnError{Code: GoAwayCode}` and triggers Conn close.

### 5.4 Frames B.1 understands

| Frame          | Direction | B.1 behavior |
|----------------|-----------|--------------|
| HEADERS        | both      | client writes / server reads via hpack |
| DATA           | both      | round-trip into `StreamEvent` |
| SETTINGS       | both      | handshake + ack only; mid-stream changes are **honored** but no client reaction (B.2 will rewindow) |
| PING           | both      | reply ACK immediately; do not surface to caller |
| GOAWAY         | recv      | trigger ConnError, close |
| RST_STREAM     | recv      | surface as `EventReset`; stream becomes done |
| WINDOW_UPDATE  | recv      | accept, but no flow-control machinery in B.1 |
| WINDOW_UPDATE  | send      | client always advertises max window via SETTINGS — never sends WINDOW_UPDATE in B.1 |
| PRIORITY       | both      | no-op; deprecated by RFC 9113, not load-bearing |
| PUSH_PROMISE   | recv      | always a connection error (we set ENABLE_PUSH=0) |
| CONTINUATION   | recv      | join into the in-flight HEADERS' header block fragment until END_HEADERS |
| CONTINUATION   | send      | not used (we never split HEADERS in B.1; rejected with ErrFrameTooLarge if a single header block exceeds peer MAX_FRAME_SIZE) |

---

## 6. Goroutine Model and Synchronization

```
+----------------+        +-----------------+
|  caller goroutine(s)    |    reader goroutine
+----------------+        +-----------------+
        |                          |
   Stream.SendX                Framer.ReadFrame
        |                          |
   acquire wmu                 dispatch via frame.Handler
        |                          |
   Framer.WriteX               update streamSet under smu
        |                          |
   release wmu                 push StreamEvent on stream.events
                                   (non-blocking; bounded buffer)
```

**Locks:**

- `wmu sync.Mutex` — guards every write to the underlying `net.Conn`.
  Acquired by all `Stream.Send*` methods and by internal control writes
  (PING ACK, SETTINGS ACK, GOAWAY). Held for the duration of a single
  frame, never across blocking I/O on the user's side.
- `smu sync.Mutex` — guards the `Conn.streams` map and last-stream-id
  counter. Held briefly during open/close.
- Each `Stream` has its own `events chan StreamEvent` of bounded size
  (default 8). The reader goroutine does a non-blocking send: if the
  buffer is full it falls back to a `select { case events <-...: case <-ctx:
  default: drop+RST_STREAM(REFUSED_STREAM) }`. This protects the reader
  from a slow consumer and is documented as part of the API contract.

**Cancellation:**

Every blocking method takes `ctx`. Cancellation tears down the **stream**
(RST_STREAM); it never tears down the connection unless the caller passes
the cancellation through `Conn.Close`.

**Reader goroutine lifetime:**

The reader exits on (a) GOAWAY received or originated, (b) transport
read error, (c) `Conn.Close`. On exit it flushes terminal events to all
live streams (`EventReset` with appropriate code) and closes their channels.

---

## 7. Allocation Strategy

Phase A's hot path is 0 allocs/op for the codec inner loop. B.1 inherits
that and extends it:

- `Stream.SendHeaders` reuses the caller's `[]hpack.HeaderField` slice and
  a per-Conn pooled scratch buffer for the encoded header block (taken
  from `internal/bytesx.GetReadBuf` / `PutReadBuf`).
- `Stream.SendData` writes the caller's buffer directly through `Framer`.
  No copy.
- Reader-side header-block buffer: per-stream `headerScratch [2][]byte`
  ring (see §4.1) reused across requests on the same stream id (B.2 ring
  becomes per-stream pool).
- Reader-side DATA: each read into a pooled buffer obtained from the same
  bytesx pool; surfaced to the caller as a slice into that buffer; returned
  to pool on the **next** Recv/Close.
- `StreamEvent` itself is a value type (not a pointer-in-interface). The
  channel buffers carry the value directly.

**Bench gates** (added on top of Phase A's):

| Bench                                        | Target |
|----------------------------------------------|-------:|
| `BenchmarkConn_Roundtrip_Empty`              | 0 alloc steady state (after handshake). ~5–15 µs/op. |
| `BenchmarkConn_Roundtrip_DATA_1KB`           | 0 alloc steady state. ≤ 30 µs/op. |
| `BenchmarkConn_Recv_HEADERS_min`             | 0 alloc; covers reader→channel→caller path. |

Handshake itself is allowed to allocate (TLS handshake dominates the time
budget anyway). The gate covers steady-state request RTT only.

---

## 8. Testing Strategy

### 8.1 Unit (in-package, no network)

`net.Pipe`-backed tests for individual control paths: SETTINGS exchange,
PING reply, GOAWAY handling, oversized-frame rejection, header-block
rebuild via CONTINUATION. These are deterministic and fast.

### 8.2 Integration

`integration_test.go` spins up `*httptest.Server` configured with
`http2.ConfigureServer` (`net/http2`) and dials it through our `conn.Dial`.
Tests:

- Empty GET round-trip — verify status, end-stream timing, byte counts.
- POST with 1 KB body — verify echo / status.
- Server resets stream — verify `EventReset` propagation.
- Server closes connection mid-stream — verify `*ConnError` surfaces.
- Concurrent two `NewStream` calls on B.1 → second returns
  `ErrTooManyStreams`.
- Cancel `ctx` during `Recv` → stream torn down with `RST_STREAM(CANCEL)`,
  Conn remains usable on a fresh stream.

### 8.3 Conformance

Re-use Phase A's conformance discipline:
`integration_test.go` includes `TestConformance_RFC7540_Sec3_ClientPreface_OnTheWire`
that asserts our preface bytes appear verbatim on the wire when dialing a
mocked peer. The conformance gate (Phase A) already enforces presence of
`TestConformance_RFC7540_*` PASS lines; B.1's tests slot into the same
gate without script changes.

### 8.4 Fuzz

`FuzzConnReader` — feed arbitrary byte streams as the "server side" of
`net.Pipe`, run our reader goroutine over them, assert no panic and
either a clean error or a finite number of frames consumed.

### 8.5 Bench

See §7.

---

## 9. CI Pipelines

No new workflows. Existing jobs pick up B.1 transparently:

- `ci/lint` — vet, golangci-lint.
- `ci/test` — `go test -race ./...` includes `conn_test.go` and
  `integration_test.go`.
- `ci/fuzz-corpus-replay` — picks up `FuzzConnReader` corpus.
- `ci/coverage` — gate at the existing 70% floor (we target ≥80% in `conn`
  out of the gate, since the surface is small and unit-testable).
- `bench-gate/bench` — picks up new benches; the absolute zero-alloc gate
  applies to them.
- `conformance-gate/rfc` — picks up new `TestConformance_RFC7540_*` tests.
- `nightly/fuzz` — extend with `FuzzConnReader`.

---

## 10. Forward Compatibility

### 10.1 → B.2 (multiplex + flow control + state machine)

API changes from B.1 → B.2: **none expected**. Internal changes:

- Drop the `max in-flight streams = 1` runtime guard; let stream id
  allocation walk odd numbers freely.
- Replace the binary `active/done` per-stream tracking with a real
  RFC 7540 §5.1 FSM (`reservedLocal/reservedRemote/open/halfClosedLocal/
  halfClosedRemote/closed`). The FSM lives in a separate file and is
  goroutine-private to the reader.
- Add real flow control: per-stream and per-connection send/recv windows;
  send-side stalling (`SendData` blocks on a `windowAvailable` chan when
  the peer hasn't given us enough credit); recv-side `WINDOW_UPDATE`
  emitter that opens the window after the caller drains a `DataEvent`.
- Honor `SETTINGS_MAX_CONCURRENT_STREAMS` from peer, return `ErrTooManyStreams`
  when our open count hits it.
- Implement keep-alive ping policy.

### 10.2 → Phase C (load-generator)

Goroutine model evolves from "writer mutex" to "writer goroutine + write
queue" — this is what was sketched as variant C in brainstorming. Public
`Conn`/`Stream` API is unchanged; the change is internal:

- `Stream.SendData` no longer acquires `wmu`. It enqueues a `writeOp` on
  a per-Conn channel (bounded, configurable).
- A dedicated writer goroutine drains the channel under flow control
  (per-stream and per-conn windows), splits oversized payloads to
  `MaxFrameSize`, and serializes onto the transport.
- This unlocks high-rate concurrent `SendData` from many goroutines on
  many streams without serializing on `wmu` — the load-generator target.

The B.1 spec deliberately keeps `wmu` so this evolution is mechanical, not
architectural: the channel + writer is a drop-in replacement for "acquire
wmu, write, release" and obeys the same observable contract.

### 10.3 What B.1 freezes

| Decision | Frozen because |
|---|---|
| `Stream` interface (SendHeaders/SendData/Recv/Close) | B.2 and Phase C both need exactly this surface |
| `StreamEvent` slice-aliasing rule (valid until next Recv/Close) | reader goroutine must not retain arenas across requests |
| `ConnOptions` zero value = sensible defaults | every load test we run will use defaults plus 1–2 knobs; we don't want a builder pattern |
| `Dialer` interface (`Dial(ctx, addr) net.Conn`) | h2c, mTLS, UDS all fit |
| Errors as `*ConnError` / `*StreamError` value types + sentinels | callers do `errors.As` in B.2 + Phase C; sentinels for the common cases |
| Reader goroutine ownership of `Framer.ReadFrame` | non-negotiable to keep `Framer` not-goroutine-safe contract from Phase A |

---

## 11. Open Questions

1. **`MaxFrameSize` default.** Spec default is 16384. Servers like nginx
   commonly advertise 16384 too. We'd advertise 16384 and rely on splitting
   in B.2. Confirm before tag.
2. **Should `Conn.Close` block on GOAWAY ACK?** RFC 7540 says GOAWAY is
   advisory; we currently plan a 1-second drain timeout then hard close.
   Worth measuring against `net/http2.Server` to see if it ever flushes
   late.
3. **Single-shot Conn lifetime in B.1.** B.1 enforces 1 stream concurrent,
   not 1 stream lifetime. Is "one Conn → many sequential streams" the
   right loosening for B.1, or do we restrict to a single stream ever
   (forcing redial)? Recommended: allow sequential, since it's cheap and
   mirrors what B.2 will do anyway.

---

## 12. Acceptance Criteria for B.1

B.1 is ready to merge when all are true:

- [ ] `conn.Dial` against a `net/http2`-backed `httptest.Server` runs an
      empty GET round-trip and a 1 KB POST round-trip without error.
- [ ] Reader goroutine handles GOAWAY, RST_STREAM, PING, SETTINGS-mid-stream,
      and PUSH_PROMISE correctly per §5.4.
- [ ] `ctx` cancellation on `Stream.Recv` produces `RST_STREAM(CANCEL)` on
      the wire (verified against `net/http2.Server` logs in test).
- [ ] Steady-state benches `BenchmarkConn_Roundtrip_*` are 0 allocs/op.
- [ ] `go test -race ./...` — green.
- [ ] `golangci-lint` — green.
- [ ] Coverage gate green at the 70% floor (target ≥80% in `conn`).
- [ ] Conformance gate still green (no Phase A regressions).
- [ ] README updated with B.1 quick-start (`conn.Dial → NewStream →
      SendHeaders → Recv`).
- [ ] `docs/RFC_COVERAGE.md` extended with new conformance rows for B.1.
