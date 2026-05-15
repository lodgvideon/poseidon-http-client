# D.4 — Active PING / RTT Keepalive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `conn.Conn.Ping(ctx)` for on-demand RTT measurement and `ConnOptions.KeepaliveInterval` for automatic dead-connection detection via periodic background PINGs.

**Architecture:** `Conn` gains three new fields (`pingMu`, `pingWaiters`, `pingCounter`) and a `Ping` method that packs a monotonic counter into the 8-byte PING payload, registers a waiter channel, sends the frame under `wmu`, then selects on the channel / ctx / `readerDone`. `OnPing` in `handler.go` is updated to route inbound ACK frames to waiting callers via a new `deliverPingAck` method (added to the `connOps` interface). `keepaliveLoop` is a goroutine started by `NewClientConn` when `ConnOptions.KeepaliveInterval > 0`; it ticks at the interval, calls `Ping` with a matching timeout, and closes the conn on failure.

**Tech Stack:** Go 1.24, `encoding/binary` (new import to `conn/conn.go`), standard `net/http/httptest` for integration tests.

---

## Background: what already exists (do NOT re-implement)

- `conn.Conn.writePingAck` (line ~713 in `conn/conn.go`) — sends PING with ACK=1 under `wmu`; used by inbound echo path
- `connOps` interface (`conn/handler.go` lines 24-33) — the contract `connHandler` calls into `*Conn` through; add `deliverPingAck` here
- `OnPing` (`conn/handler.go` lines 224-229) — currently echoes non-ACK PINGs and silently drops ACKs; change the ACK branch
- `startH2TestServer` + `dialServer` helpers in `conn/integration_test.go` — copy pattern for new tests
- `conn.IsAlive()` (`conn/conn.go` line ~259) — used in keepalive test to detect closed conn

---

## File Structure

```
Modified:
  conn/conn.go       — new Conn fields, Ping method, deliverPingAck,
                       keepaliveLoop, init in NewClientConn,
                       encoding/binary import
  conn/handler.go    — deliverPingAck added to connOps interface;
                       OnPing routes ACK to deliverPingAck
  conn/options.go    — KeepaliveInterval field on ConnOptions

Created:
  conn/ping_test.go  — 6 integration tests
```

---

## Task 1: `conn.Conn.Ping` + ACK routing

**Files:**
- Modify: `conn/conn.go`
- Modify: `conn/handler.go`
- Create: `conn/ping_test.go`

- [ ] **Step 1: Create `conn/ping_test.go` with the four failing Ping tests**

Create `conn/ping_test.go` with this exact content:

```go
package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// startPingServer is identical to startH2TestServer in integration_test.go.
// Duplicated here to keep this file self-contained.
func startPingServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialPingServer(t *testing.T, srv *httptest.Server, cfg *tls.Config, opts ConnOptions) *Conn {
	t.Helper()
	opts.Dialer = &TLSDialer{Config: cfg}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestConn_Ping_RTT(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rtt, err := c.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if rtt <= 0 {
		t.Errorf("RTT = %v, want > 0", rtt)
	}
	if rtt >= time.Second {
		t.Errorf("RTT = %v, want < 1s (loopback server)", rtt)
	}
}

func TestConn_Ping_ConcurrentSafe(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 20
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = c.Ping(ctx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Ping = %v", i, err)
		}
	}
}

func TestConn_Ping_CtxCancelledBeforeACK(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})

	// context.WithTimeout(..., 0) creates an already-expired context.
	// ctx.Done() is closed before Ping enters the select, so only that
	// branch fires. The ACK arrives later (after network round-trip).
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	_, err := c.Ping(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ping with expired ctx = %v, want context.DeadlineExceeded", err)
	}
}

func TestConn_Ping_AfterClose(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	c := dialPingServer(t, srv, cfg, ConnOptions{})
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Ping(ctx)
	if !errors.Is(err, ErrConnClosed) {
		t.Fatalf("Ping after Close = %v, want ErrConnClosed", err)
	}
}
```

- [ ] **Step 2: Run the failing tests**

```bash
go test ./conn/ -run "TestConn_Ping" -v -count=1
```

Expected: FAIL — `c.Ping undefined`.

- [ ] **Step 3: Add new fields to `Conn` struct in `conn/conn.go`**

Add `encoding/binary` to the import block (after `"context"`):

```go
import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)
```

Add three fields to the `Conn` struct, after the `readerDone chan struct{}` line (~line 71):

```go
	readerDone chan struct{}

	// pingMu guards pingWaiters. pingCounter produces unique payloads.
	pingMu      sync.Mutex
	pingWaiters map[[8]byte]chan struct{}
	pingCounter atomic.Uint64
```

In `NewClientConn`, add `pingWaiters` to the `Conn` literal (after `readerDone: make(chan struct{})`):

```go
	c := &Conn{
		transport:          transport,
		fr:                 frame.NewFramer(transport, transport),
		enc:                hpack.NewEncoder(),
		dec:                hpack.NewDecoder(),
		opts:               opts,
		nextID:             1,
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		pingWaiters:        make(map[[8]byte]chan struct{}),
		connRecvWindow:     int32(connInitialRecvWindow),
		peerConnSendWindow: int32(connInitialRecvWindow),
	}
```

- [ ] **Step 4: Add `deliverPingAck` to `connOps` interface and `*Conn` in `conn/handler.go` and `conn/conn.go`**

In `conn/handler.go`, add `deliverPingAck` to the `connOps` interface (after `writePingAck`):

```go
type connOps interface {
	lookupStream(id uint32) *Stream
	onDataReceived(s *Stream, length uint32) error
	markStreamDone(id uint32)
	onWindowUpdate(streamID, increment uint32) error
	applyPeerSettings(s frame.SettingsParams) error
	writeSettingsAck() error
	writePingAck(payload [8]byte) error
	deliverPingAck(payload [8]byte)
	onGoAwayReceived(lastStreamID uint32, code frame.ErrCode)
}
```

Update `OnPing` in `conn/handler.go` to route ACKs instead of dropping them:

```go
// OnPing implements frame.Handler. Non-ACK PING frames are echoed
// back with ACK=1 and the same opaque 8-byte payload (RFC 7540 §6.7).
// ACK frames are delivered to any Ping call waiting for that payload.
func (h *connHandler) OnPing(fh frame.FrameHeader, payload [8]byte) error {
	if fh.Flags&frame.FlagPingAck != 0 {
		h.streams.deliverPingAck(payload)
		return nil
	}
	return h.streams.writePingAck(payload)
}
```

Add `deliverPingAck` method to `*Conn` in `conn/conn.go` (place it after `writePingAck`):

```go
// deliverPingAck signals any Ping call waiting for payload.
// Unsolicited ACKs (no matching waiter) are silently ignored.
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

- [ ] **Step 5: Add `Ping` method to `*Conn` in `conn/conn.go`**

Add after `deliverPingAck`:

```go
// Ping sends a PING frame and blocks until the peer's ACK arrives,
// returning the round-trip time. Returns ErrConnClosed if the
// connection is closed before the ACK arrives. Returns ctx.Err() if
// the context expires or is cancelled first.
func (c *Conn) Ping(ctx context.Context) (time.Duration, error) {
	if c.closed.Load() {
		return 0, ErrConnClosed
	}

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

- [ ] **Step 6: Run Ping tests — verify they pass**

```bash
go test ./conn/ -run "TestConn_Ping" -v -count=1 -race
```

Expected: all 4 PASS.

- [ ] **Step 7: Run full test suite**

```bash
go test -race -count=1 ./...
```

Expected: all green.

- [ ] **Step 8: Run lint**

```bash
make lint
```

Expected: `0 issues.`

- [ ] **Step 9: Commit**

```bash
git add conn/conn.go conn/handler.go conn/ping_test.go
git commit -m "feat(conn): D.4 Conn.Ping — on-demand RTT probe via PING ACK routing"
```

---

## Task 2: `ConnOptions.KeepaliveInterval` + keepalive loop

**Files:**
- Modify: `conn/options.go`
- Modify: `conn/conn.go`
- Modify: `conn/ping_test.go`

- [ ] **Step 1: Add two keepalive tests to `conn/ping_test.go`**

Append to the end of `conn/ping_test.go`:

```go
func TestConn_Keepalive_HealthyConn(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// KeepaliveInterval set; server responds to PINGs normally.
	c := dialPingServer(t, srv, cfg, ConnOptions{KeepaliveInterval: 30 * time.Millisecond})

	// Wait 3 keepalive intervals; conn must remain alive.
	time.Sleep(100 * time.Millisecond)
	if !c.IsAlive() {
		t.Fatal("keepalive closed a healthy connection")
	}
}

func TestConn_Keepalive_ClosesDeadConn(t *testing.T) {
	srv, cfg := startPingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	c := dialPingServer(t, srv, cfg, ConnOptions{KeepaliveInterval: 50 * time.Millisecond})

	// Close server: kills the TCP connection. The keepalive loop's next
	// Ping will fail (ErrConnClosed from readerDone or write error) and
	// c.Close() will be called. Allow 3× interval.
	srv.Close()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !c.IsAlive() {
			return // test passes
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("conn still alive 200ms after server closed — keepalive did not detect dead conn")
}
```

- [ ] **Step 2: Run the failing tests**

```bash
go test ./conn/ -run "TestConn_Keepalive" -v -count=1
```

Expected: FAIL — `unknown field KeepaliveInterval in struct literal of type ConnOptions`.

- [ ] **Step 3: Add `KeepaliveInterval` to `ConnOptions` in `conn/options.go`**

Replace the `ConnOptions` struct:

```go
// ConnOptions tunes a Conn. The zero value is sensible.
type ConnOptions struct {
	Dialer            Dialer
	Settings          AdvertisedSettings
	StreamEventBuffer int
	// KeepaliveInterval, when non-zero, enables a background keepalive
	// loop. The loop sends a PING every interval; if no ACK arrives
	// within the same interval the connection is closed. Zero disables
	// keepalive.
	KeepaliveInterval time.Duration
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

Add `"time"` to `conn/options.go` imports (it has none currently — add the import block):

```go
package conn

import "time"
```

- [ ] **Step 4: Add `keepaliveLoop` and start it in `NewClientConn` in `conn/conn.go`**

Add `keepaliveLoop` after `deliverPingAck` (before `Ping`):

```go
// keepaliveLoop sends a PING every interval. If the ACK does not
// arrive within the same interval the connection is closed.
// The loop exits when the connection closes (readerDone is closed).
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

In `NewClientConn`, after `go c.readerLoop()`, add:

```go
	go c.readerLoop()
	if opts.KeepaliveInterval > 0 {
		go c.keepaliveLoop(opts.KeepaliveInterval)
	}
	return c, nil
```

- [ ] **Step 5: Run keepalive tests — verify they pass**

```bash
go test ./conn/ -run "TestConn_Keepalive" -v -count=1 -race -timeout 30s
```

Expected: both PASS.

- [ ] **Step 6: Run all Ping + Keepalive tests**

```bash
go test ./conn/ -run "TestConn_Ping|TestConn_Keepalive" -v -count=1 -race
```

Expected: all 6 PASS.

- [ ] **Step 7: Run full test suite**

```bash
go test -race -count=1 ./...
```

Expected: all green.

- [ ] **Step 8: Run lint**

```bash
make lint
```

Expected: `0 issues.`

- [ ] **Step 9: Commit**

```bash
git add conn/options.go conn/conn.go conn/ping_test.go
git commit -m "feat(conn): D.4 KeepaliveInterval — background PING loop for dead-conn detection"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `pingMu`, `pingWaiters`, `pingCounter` fields on `Conn` | Task 1 Step 3 |
| `pingWaiters` initialised in `NewClientConn` | Task 1 Step 3 |
| `deliverPingAck` on `connOps` interface | Task 1 Step 4 |
| `deliverPingAck` method on `*Conn` | Task 1 Step 4 |
| `OnPing` routes ACK to `deliverPingAck` | Task 1 Step 4 |
| `Conn.Ping` method — sends PING, waits ACK, returns RTT | Task 1 Step 5 |
| `Ping` returns `ErrConnClosed` on closed conn | Task 1 Step 5 |
| `Ping` returns `ctx.Err()` on ctx cancel | Task 1 Step 5 |
| `encoding/binary` import added | Task 1 Step 3 |
| `ConnOptions.KeepaliveInterval time.Duration` | Task 2 Step 3 |
| `keepaliveLoop` goroutine | Task 2 Step 4 |
| `NewClientConn` starts keepalive when interval > 0 | Task 2 Step 4 |
| `TestConn_Ping_RTT` | Task 1 Step 1 |
| `TestConn_Ping_ConcurrentSafe` | Task 1 Step 1 |
| `TestConn_Ping_CtxCancelledBeforeACK` | Task 1 Step 1 |
| `TestConn_Ping_AfterClose` | Task 1 Step 1 |
| `TestConn_Keepalive_HealthyConn` | Task 2 Step 1 |
| `TestConn_Keepalive_ClosesDeadConn` | Task 2 Step 1 |

**Placeholder scan:** No TBD, no TODO. All steps have complete code.

**Type consistency:**
- `deliverPingAck(payload [8]byte)` — matches interface declaration and `*Conn` method signature ✓
- `pingWaiters map[[8]byte]chan struct{}` — matches all access sites ✓
- `KeepaliveInterval time.Duration` — `options.go` type matches `keepaliveLoop(interval time.Duration)` param ✓
- `Ping(ctx context.Context) (time.Duration, error)` — called in tests and in `keepaliveLoop` ✓
