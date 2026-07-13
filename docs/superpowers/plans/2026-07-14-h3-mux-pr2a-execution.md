# H3 concurrent multiplexing — PR 2a execution checklist

Rung 1 of the HTTP/3-to-production ladder. **No new design** — this executes
`docs/HTTP3_DESIGN.md` §5 "PR 2a" faithfully. The full 2a–2d actor-model design
(one `c.mu`, reader goroutine, wake channels, per-request QPACK) is already
written and adversarially reviewed there; read §2–§7 before touching code.

Branch: `feat/h3-concurrent-multiplex` (from `main` incl. #181).
Precursors already merged: #177 (`inFlight` guard), #178 (design doc).

## PR 2a scope — add `c.mu`, split public/`…Locked`, ZERO behavior change

The lock is uncontended in 2a (still one goroutine via `inFlight`), so `-race`
proves nothing. **The real gate is the fault-server suite** — it exercises the
`fail`/STOP_SENDING/AEAD-limit paths that self-deadlock if a `…Locked` internal
calls a public (re-locking) wrapper.

### Step 1 — add the mutex
- `quic/conn.go`: add `mu sync.Mutex` to `Conn` (guards ALL mutable state + the wire).

### Step 2 — split public entry points → lock-taking wrapper over assume-held `…Locked`
Mirror H2's discipline ("public locks, internal assumes locked"). Public wrappers:
`Poll`, `Send`, `OpenStream`, `OpenUniStream`, `Recv`, `Reset`, `StopSending`,
`AcceptUniStream`, `CloseWithError`. Each becomes:
```go
func (c *Conn) X(...) (...) { c.mu.Lock(); defer c.mu.Unlock(); return c.xLocked(...) }
```
In 2a, `Poll` still takes the caller ctx and holds `c.mu` for its whole body
(harmless while single-goroutine; §3.2 restructure is 2c, not 2a).

### Step 3 — thread "assumes `c.mu` held" through internal helpers
No lock ops added; just the assumption (and doc comment) on:
`recvDatagram`, `flush`, `flushControl`, `sealPacket`, `detectLost`, `onPTO`,
`commitKeyUpdate`, `discardStaleKeys`, `writeAppFrames`, `onStreamConsumed`,
`fail`, and every `On*` frame handler.

### Step 4 — ship the 3 `…Locked` seeds + repoint the reentrant up-calls (the subtle part)
The design's review found 3 in-tree reentrant up-calls that WILL self-deadlock
once the public wrappers lock. Add assume-held internals and repoint:
- `closeWithErrorLocked` ← `sealPacket → CloseWithError` at the AEAD limit (`quic/conn_recv.go:518`)
- `closeWithErrorLocked` ← `fail → CloseWithError` (`quic/close.go:51`)
- `resetLocked` ← `OnStopSending → s.Reset` (`quic/conn_recv.go:956`)
- (`stopSendingLocked` added for symmetry / future 2b use)
Public `CloseWithError`/`Reset`/`StopSending` become thin locking wrappers over these.

### Step 5 — `Establish` under the lock
Wrap `Establish`'s body under `c.mu` so `fail` is unconditionally an assume-held
internal (one convention, no pre-reader special case). `Establish` runs before any
concurrency, so this is free.

### Step 6 — static guard
Add a check (grep or `go vet`-style test) that **no `…Locked` path calls a public
wrapper**. Seed list = the 3 above. This is the regression guard for 2b–2d too.

## Files touched (expect)
`quic/conn.go` (struct + public wrappers), `quic/conn_recv.go` (handlers +
sealPacket/recvDatagram/flush assume-held; up-call repoints), `quic/close.go`
(fail → closeWithErrorLocked; CloseWithError wrapper), `quic/send.go`
(writeAppFrames assume-held), `quic/stream.go` (Send/Recv/Reset/StopSending
wrappers), `quic/pto.go` (onPTO assume-held), `quic/crypto_keyupdate.go`
(commitKeyUpdate assume-held), `quic/recvflow.go` (onStreamConsumed assume-held).

## Acceptance gate (all must pass)
- `go build ./...`, `go vet ./...`.
- `go test ./quic/ ./http3/ -race` (unit; local — NOTE: unrelated conn/ ping-RTT
  tests fail on Windows w/ RTT=0s, skip them).
- **fault-server suite** (`docker compose run --rm runner` per the interop Makefile
  target; NOT `up --abort-on-container-exit` — that hangs on Windows Docker).
- Full interop matrix (Caddy/nginx/aioquic) green in CI.
- No-`…Locked`-calls-public static check green.
- bench-gate (frame+hpack 0 B/op) untouched (2a doesn't touch those paths).

## Then STOP — 2b/2c/2d are separate PRs (see HTTP3_DESIGN.md §5), driven by the /goal loop.

## Why a checklist, not code, from the /ork:auto run of 2026-07-14
PR 2a is an all-or-nothing mutex refactor whose acceptance gates (fault-server,
interop, loss-relay) run only in CI/Docker — and interop teardown hangs on this
Windows host. A partial split deadlocks. It must be executed and CI-verified as
one complete unit in a focused session, not half-done under context pressure.
The design is fully locked (HTTP3_DESIGN.md §2–§7); this file is the execution map.
