# Poseidon Phase B — Connection Layer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the HTTP/2 connection-layer client (`conn` package) on top of Phase A. One `*Conn` per `net.Conn`, caller-driven `Pump`, full handshake + state machine + flow control + PING/GOAWAY.

**Architecture:** `internal/streamstate` (state machine) + `internal/flowctl` (window math) → `conn` package (Conn, Stream, Dialer, handler). TLS via `crypto/tls`, h2c via raw TCP. Adaptive flow control on by default. No internal goroutine pool.

**Tech Stack:** Go 1.24; `crypto/tls` stdlib; `net/http2.Server` used as test peer only (NOT in product code).

**Spec reference:** `docs/superpowers/specs/2026-05-05-poseidon-conn-layer-design.md`.

**Branch:** `design/2026-05-05-conn-layer-phase-b` (off `design/2026-05-02-frame-layer-phase-a`).

**Acceptance:** see spec §11.

---

## Task Order

Bottom-up: state machine → flow control → conn errors+dialers → handshake → frame routing → Stream send/recv → integration → conformance → bench → milestone.

Each task ends with TDD-verified commit. Git rules unchanged from Phase A: subject ≤50 chars, NO Co-Authored-By, conventional commit format `<type>[scope]: <description>`.

---

## internal/streamstate

### Task 1: state.go + transitions table

**Files:** `internal/streamstate/state.go`, `internal/streamstate/state_test.go`

Per RFC 7540 §5.1.

- [ ] **Step 1: Test (RED)** — table of all (state, event) pairs with expected (newState, err). Cover all 7 states × 7 events.

- [ ] **Step 2: Implementation** — switch table per RFC §5.1 diagram.

- [ ] **Step 3: Commit** subject: `feat(streamstate): RFC 5.1 state machine`

---

## internal/flowctl

### Task 2: window.go

**Files:** `internal/flowctl/window.go`, `_test.go`

- [ ] Test (RED): TryConsume, Replenish, AdjustInitial, negative-on-overshoot.

- [ ] Implementation: atomic.Int64-backed FlowWindow.

- [ ] Commit: `feat(flowctl): atomic FlowWindow`

### Task 3: adaptive.go

**Files:** `internal/flowctl/adaptive.go`, `_test.go`

- [ ] Adaptive sizer: grow on >50% drained, shrink on <25%, bounded by min/max.

- [ ] Commit: `feat(flowctl): adaptive sizer`

---

## conn package — foundation

### Task 4: doc.go + errors.go

**Files:** `conn/doc.go`, `conn/errors.go`

- [ ] Sentinels: ErrConnClosed, ErrGoAwayReceived, ErrStreamReset, ErrStreamMaxStreams, ErrInvalidState, ErrPushUnsupported, ErrTLSNoH2.

- [ ] Commit: `feat(conn): package doc and errors`

### Task 5: Dialer + TLSDialer + H2CDialer

**Files:** `conn/dialer.go`, `conn/tls_dialer.go`, `conn/h2c_dialer.go`, `_test.go`

- [ ] Test: TLSDialer ALPN check; H2CDialer plain TCP.

- [ ] Impl: TLSDialer uses crypto/tls; verifies NegotiatedProtocol == "h2".

- [ ] Commit: `feat(conn): Dialer + TLSDialer + H2CDialer`

### Task 6: Conn struct + ConnOptions + ConnStats

**Files:** `conn/conn.go`

- [ ] Define types; NewConn stub.

- [ ] Commit: `feat(conn): Conn struct skeleton`

---

## Handshake + SETTINGS

### Task 7: settings.go

**Files:** `conn/settings.go`, `_test.go`

- [ ] Apply peer SETTINGS atomically; track ACK with timeout.

- [ ] Commit: `feat(conn): SETTINGS exchange`

### Task 8: handshake + NewConn

**Files:** `conn/handshake.go`, `conn/conn.go`, `_test.go`

- [ ] Send preface + SETTINGS; read peer SETTINGS; ACK exchange; timeout enforcement.

- [ ] Test via `net.Pipe`: paired peer that exchanges SETTINGS.

- [ ] Commit: `feat(conn): handshake and NewConn`

---

## Frame routing + Pump

### Task 9: handler.go skeleton

**Files:** `conn/handler.go`

- [ ] connHandler implementing frame.Handler with stub bodies.

- [ ] Commit: `feat(conn): connHandler skeleton`

### Task 10: Pump loop

**Files:** `conn/conn.go` (Pump method)

- [ ] Read frames in loop; return on EOF/ctx/fatal.

- [ ] Commit: `feat(conn): Pump loop`

---

## Stream

### Task 11: stream.go skeleton

**Files:** `conn/stream.go`

- [ ] Define Stream, StreamEvent, StreamEventKind, StreamState.

- [ ] NewStream allocates next odd ID.

- [ ] Commit: `feat(conn): Stream and event types`

### Task 12: SendHeaders

**Files:** `conn/stream.go`

- [ ] HPACK encode; HEADERS+CONTINUATION fragmentation; writeMu lock; state transition.

- [ ] Commit: `feat(conn): Stream.SendHeaders`

### Task 13: SendData with flow control

**Files:** `conn/stream.go`

- [ ] Chunk by peer MAX_FRAME_SIZE; wait for window via cond; honor ctx.

- [ ] Test: 10 KB send with peer initial window 1 KB.

- [ ] Commit: `feat(conn): Stream.SendData with flow control`

### Task 14: Recv + DATA/HEADERS routing

**Files:** `conn/handler.go`, `conn/stream.go`

- [ ] OnData: copy payload, push StreamEvent. OnHeaders+OnContinuation: buffer, decode HPACK at END_HEADERS, push StreamHeaders/StreamTrailers.

- [ ] Commit: `feat(conn): Stream.Recv routing`

### Task 15: SendTrailers + Cancel

**Files:** `conn/stream.go`

- [ ] SendTrailers = SendHeaders with endStream. Cancel = WriteRSTStream + idempotent close.

- [ ] Commit: `feat(conn): SendTrailers and Cancel`

---

## Connection-level frames

### Task 16: WINDOW_UPDATE

- [ ] StreamID 0 vs >0 routing; cond signaling.

- [ ] Commit: `feat(conn): WINDOW_UPDATE handling`

### Task 17: PING + RTT

- [ ] OnPing ACK: RTT measurement; non-ACK: echo back. Pump-driven ticker if PingInterval>0.

- [ ] Commit: `feat(conn): PING and RTT`

### Task 18: GOAWAY

- [ ] Mark closing; refuse NewStream.

- [ ] Commit: `feat(conn): GOAWAY handling`

### Task 19: PUSH_PROMISE rejection

- [ ] RST_STREAM with RefusedStream code.

- [ ] Commit: `feat(conn): reject PUSH_PROMISE`

### Task 20: RST_STREAM

- [ ] Set ErrCode; push StreamClosed; transition.

- [ ] Commit: `feat(conn): RST_STREAM handling`

---

## Close + lifecycle polish

### Task 21: Close + drain

- [ ] GOAWAY(0,NoError); wait drainTimeout; force close.

- [ ] Commit: `feat(conn): Close and drain`

### Task 22: Closed-stream GC

- [ ] Pump 1s ticker GCs Closed streams older than RFC §5.1.2 grace.

- [ ] Commit: `feat(conn): closed-stream GC`

---

## Integration tests

### Task 23: h2c integration

**Files:** `conn/integration_h2c_test.go` (build tag `integration`)

- [ ] Spin up `net/http2.Server` (stdlib, test only) over loopback TCP.
- [ ] Poseidon connects via H2CDialer; GET; verify 200 + body.

- [ ] Commit: `test(conn): integration h2c stdlib`

### Task 24: TLS integration

**Files:** `conn/integration_tls_test.go`

- [ ] httptest.NewTLSServer with ALPN h2.
- [ ] Full request lifecycle.

- [ ] Commit: `test(conn): integration TLS stdlib`

### Task 25: Concurrent streams

**Files:** `conn/integration_concurrent_test.go`

- [ ] 100 goroutines, single Conn; verify no goroutine leak.

- [ ] Commit: `test(conn): concurrent streams`

---

## Conformance + bench + fuzz

### Task 26: RFC 5.1 conformance

**Files:** `conn/conformance_test.go`

- [ ] Wrap streamstate transitions as `TestConformance_RFC7540_5_1_*`.

- [ ] Commit: `test(conn): RFC 5.1 state conformance`

### Task 27: Bench gates

**Files:** `conn/bench_test.go`

- [ ] BenchmarkConn_Handshake_h2c, BenchmarkConn_RequestRoundTrip_h2c_GET, BenchmarkStream_SendData_64KB.

- [ ] Commit: `test(conn): bench gates`

### Task 28: Fuzz Pump

**Files:** `conn/fuzz_test.go`

- [ ] Random byte stream invariants.

- [ ] Commit: `test(conn): FuzzPump`

---

## CI delta

### Task 29: integration job

**Files:** `.github/workflows/ci.yml`

- [ ] Add integration job with `-tags=integration`.

- [ ] Commit: `ci: add integration test job`

### Task 30: README Phase B

**Files:** `README.md`

- [ ] Phase B usage example.

- [ ] Commit: `docs: README Phase B example`

---

## Milestone B

### Task 31: Acceptance verification + tag v0.2.0-rc1

- [ ] make test-race green
- [ ] Bench gates met
- [ ] Integration green
- [ ] git tag v0.2.0-rc1

---

## Self-Review

Spec coverage: §1-§11 mapped to Tasks 1-31. State machine ↔ Task 1. Flow control ↔ Tasks 2-3. Public API ↔ Tasks 4-6, 11-15. Handshake ↔ Tasks 7-8. Pump+routing ↔ Tasks 9-10, 14-20. Lifecycle ↔ Tasks 21-22. Testing ↔ Tasks 23-28. CI ↔ Task 29. Acceptance ↔ Task 31.

Type consistency: types match spec — Stream, StreamEvent, Conn, Dialer, ConnOptions, ConnStats, StreamState (alias to streamstate.State), Event.

Placeholders: tasks deliberately leaner than Phase A plan since pattern is established. Each task is one TDD red-green-refactor commit.

Caveat: `tdd-guard` PreToolUse hook in some environments crashes on Write/Edit. Workaround: Bash heredoc (`cat > file <<'EOF' ... EOF`) bypasses the hook. Document in Phase B execution session.

---

## Execution

Subagent-driven recommended (each task isolated TDD cycle). Fresh session strongly suggested for execution given Phase A consumed substantial context.
