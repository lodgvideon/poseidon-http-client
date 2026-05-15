# D.4 — Active PING / RTT Keepalive Design

**Date:** 2026-05-15
**Status:** Approved
**Goal:** Add `conn.Conn.Ping(ctx)` for on-demand RTT measurement and `ConnOptions.KeepaliveInterval` for automatic dead-connection detection via periodic PINGs.

---

## Background

`NewClientConn` already echoes inbound non-ACK PING frames (RFC 7540 §6.7), but drops inbound ACKs silently because no outbound PINGs are sent. Load generators running long tests need to detect dead connections before sending a large burst; the keepalive loop also lets the pool evict stale conns proactively rather than discovering failure mid-request.

---

## Architecture

```
caller
  └─ conn.Conn.Ping(ctx)
       │  pack pingCounter → [8]byte payload
       │  register chan struct{} in pingWaiters[payload]
       │  WritePing(ack=false, payload) under wmu
       │
       │  select { <-ch | <-ctx.Done() | <-readerDone }
       │
       ▼  (ACK arrives via readerLoop → OnPing → close(ch))
     time.Duration, error

ConnOptions.KeepaliveInterval != 0
  └─ NewClientConn spawns keepaliveLoop(interval)
       every interval: Ping with timeout=interval
       on error: c.Close()
       on readerDone: return
```

---

## Section 1: `conn.Conn.Ping`

**File:** `conn/conn.go`

New fields on `Conn`:

```go
pingMu      sync.Mutex
pingWaiters map[[8]byte]chan struct{}
pingCounter atomic.Uint64
```

Initialised in `NewClientConn`:

```go
pingWaiters: make(map[[8]byte]chan struct{}),
```

Public method:

```go
// Ping sends a PING frame and blocks until the peer's ACK arrives.
// Returns the round-trip time on success.
// Returns ErrConnClosed if the connection is already closed or closes
// before the ACK arrives. Returns ctx.Err() if the context is
// cancelled or times out first.
func (c *Conn) Ping(ctx context.Context) (time.Duration, error) {
    if c.closed.Load() {
        return 0, ErrConnClosed
    }
    // Pack a monotonically increasing counter into 8 bytes.
    n := c.pingCounter.Add(1)
    var payload [8]byte
    binary.BigEndian.PutUint64(payload[:], n)

    ch := make(chan struct{})
    c.pingMu.Lock()
    c.pingWaiters[payload] = ch
    c.pingMu.Unlock()

    start := time.Now()
    c.wmu.Lock()
    err := c.fr.WritePing(false, payload)
    if err == nil {
        c.bumpFramesSent()
    }
    c.wmu.Unlock()

    if err != nil {
        c.pingMu.Lock()
        delete(c.pingWaiters, payload)
        c.pingMu.Unlock()
        return 0, err
    }

    select {
    case <-ch:
        return time.Since(start), nil
    case <-ctx.Done():
        c.pingMu.Lock()
        delete(c.pingWaiters, payload)
        c.pingMu.Unlock()
        return 0, ctx.Err()
    case <-c.readerDone:
        return 0, ErrConnClosed
    }
}
```

Import added to `conn/conn.go`: `"encoding/binary"`.

---

## Section 2: ACK routing in `OnPing`

**File:** `conn/handler.go`

Replace the existing ACK branch (currently `return nil`):

```go
func (h *connHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
    if fh.Flags&frame.FlagPingAck != 0 {
        h.streams.deliverPingAck(payload)
        return nil
    }
    return h.streams.writePingAck(payload)
}
```

`deliverPingAck` is a new method on `*Conn` (referenced via the `connStreams` interface):

```go
// deliverPingAck signals any Ping call waiting for payload.
// Unrecognised payloads (unsolicited ACK from buggy peer) are silently ignored.
func (c *Conn) deliverPingAck(payload [8]byte) {
    c.pingMu.Lock()
    ch, ok := c.pingWaiters[payload]
    if ok {
        delete(c.pingWaiters, payload)
    }
    c.pingMu.Unlock()
    if ok {
        close(ch)
    }
}
```

`connOps` interface in `conn/handler.go` gains:

```go
deliverPingAck(payload [8]byte)
```

---

## Section 3: Keepalive loop

**File:** `conn/conn.go`

```go
// keepaliveLoop sends a PING every interval. If the ACK does not arrive
// within the same interval the connection is considered dead and closed.
// The loop exits when the connection closes (readerDone closed).
func (c *Conn) keepaliveLoop(interval time.Duration) {
    t := time.NewTicker(interval)
    defer t.Stop()
    for {
        select {
        case <-t.C:
            ctx, cancel := context.WithTimeout(context.Background(), interval)
            _, err := c.Ping(ctx)
            cancel()
            if err != nil {
                c.Close()
                return
            }
        case <-c.readerDone:
            return
        }
    }
}
```

Started in `NewClientConn` after the SETTINGS handshake:

```go
if opts.KeepaliveInterval > 0 {
    go c.keepaliveLoop(opts.KeepaliveInterval)
}
```

---

## Section 4: `ConnOptions.KeepaliveInterval`

**File:** `conn/options.go`

Add to `ConnOptions`:

```go
// KeepaliveInterval, when non-zero, enables a background keepalive
// loop. The loop sends a PING every interval; if no ACK arrives within
// the same interval the connection is closed. Zero disables keepalive.
KeepaliveInterval time.Duration
```

No change to `ConnOptions.defaulted()` — zero is the correct default (opt-in).

---

## Section 5: Error handling

- `Ping` on already-closed conn → `ErrConnClosed` (fast path via `c.closed.Load()`)
- Conn closes while Ping waiting → `ErrConnClosed` (via `readerDone` case)
- ctx cancelled/expired → `ctx.Err()`; waiter cleaned up from map immediately
- Write failure (rare — transport gone) → error returned from `WritePing`; waiter cleaned up
- Unsolicited PING ACK from peer → `deliverPingAck` no-ops (no waiter for payload)
- Concurrent `Ping` calls with same payload: impossible — `pingCounter` is monotonically incrementing, each call gets a unique uint64

---

## Section 6: Tests

**File:** `conn/ping_test.go` (new)

Uses `httptest.NewUnstartedServer` with `EnableHTTP2 = true` throughout (real TCP — avoids `net.Pipe` deadlock on concurrent reads/writes).

| Test | What it pins |
|---|---|
| `TestConn_Ping_RTT` | Ping against live h2 server; RTT > 0 and < 1s; no error |
| `TestConn_Ping_ConcurrentSafe` | 20 concurrent `Ping` calls; race detector clean; all return nil |
| `TestConn_Ping_CtxCancelledBeforeACK` | ctx cancelled while waiting → `context.Canceled` |
| `TestConn_Ping_AfterClose` | Ping on closed conn → `ErrConnClosed` |
| `TestConn_Keepalive_HealthyConn` | `KeepaliveInterval=50ms`; conn stays alive; no spurious close after 200ms |
| `TestConn_Keepalive_ClosesDeadConn` | `KeepaliveInterval=50ms`; server TCP listener closed after handshake; conn is closed within 200ms |

---

## Section 7: File changes

```
Modified:
  conn/conn.go      — pingMu/pingWaiters/pingCounter fields,
                      Ping method, deliverPingAck method,
                      keepaliveLoop, start keepalive in NewClientConn
  conn/handler.go   — OnPing routes ACK to deliverPingAck;
                      connStreams interface gains deliverPingAck
  conn/options.go   — KeepaliveInterval field on ConnOptions

Created:
  conn/ping_test.go — 6 tests listed above
```

---

## Non-goals

- Active PING initiation from `client.Client` — caller uses `conn.Conn` directly for RTT probes; pool keepalive is set via `ConnOptions` passed through `ClientOptions.ConnOpts`
- PING rate limiting — caller controls interval; no built-in throttle
- Exposing RTT history or statistics — out of scope for D.4
