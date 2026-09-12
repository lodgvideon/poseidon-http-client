# AddressDialer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a *managed* transport's dialer receive the full resolver `Address` (host, port, and `Attributes`) instead of a flattened `"host:port"` string, via an optional `AddressDialer` capability interface that falls back to the plain `Dialer` when a dialer does not implement it. Closes [#943](https://github.com/lodgvideon/poseidon-http-client/issues/943).

**Architecture:** Add `pool.AddressDialer` — mirroring `conn.ALPNAsserter`'s capability-check idiom — plus a small `pool.Dial` helper that prefers it. Thread the resolver's `Address` from `internal/poolcore.ManagedCore.GetOrCreateSubPool` (the one place in the whole call graph where it is alive today before being flattened to a string key) down into the two sub-pool types that actually hold a `conn.Dialer`: `client.h1Pool` (under `TransportH1Managed`) and `internal/poolcore.Pool` (under the HTTP/2 `TransportManaged`). Reuse `conn.DialerFunc` (landed on this branch moments ago, #942) to bridge the H2 case, where the dial happens inside `conn.Dial` rather than at a bare `dialer.Dial(...)` call site.

**Tech Stack:** Go, testify (`require`/`assert`), the repo's existing `net.Pipe`-based dialer fakes (`h1FakeDialer`, `fakeDialer`).

---

## Corrections to the issue text (read before starting)

- The issue cites `client.Address` at `client/resolver.go:13-25`. **That file does not exist.** The real type is `pool.Address` (`pool/resolver.go:12-25`); `client.Address` is a type *alias* of it (`client/pool_vocab.go:15`: `type Address = pool.Address`). They are one type — a `Resolver`/`Dialer` written against either name satisfies both. Every reference below uses whichever name is idiomatic for the file being edited (bare `Address` inside `pool`/`client`/`poolcore`, since all three have it in scope via alias).
- `Address` has exactly three fields today — `Host string`, `Port int`, `Attributes map[string]string` — no network-family field. Adding one is a separate, unrelated change; not attempted here.

## Explicitly out of scope

- **Non-managed transports** (`TransportH1SingleConn`, `TransportH1Pool`, `TransportSingleConn`, `TransportPool`, `TransportALPN`). None of them ever construct a resolver `Address` — their address is a caller-supplied string from `ClientOptions.Addr` with no `Resolver`/`Selector` in the picture at all. Zero files in this group are touched; a custom dialer implementing `AddressDialer` simply never gets `DialAddress` called on these transports, because there is nothing genuine to offer it.
- **HTTP/3** (`TransportH3`, `TransportH3Pool`, `TransportH3Managed`). All three dial through a distinct `func(ctx, string, *tls.Config) (h3Client, error)` shape, never through `conn.Dialer` — there is no seam to check `AddressDialer` against. `client/h3_managed_pool.go`'s `NewSub` closure gets a mechanical signature update in Task 3 (to keep the shared generic core compiling) but no new behavior.
- **Attempt-number / retry-count reaching the dialer**, and any `context.WithValue`-based propagation — the issue's own hedged, secondary ask ("would cover the retry case too"). Two independent reasons to defer it rather than bolt it on here: (1) the attempt counter that already exists (`retryDoer.doAttempt(ctx, req, resp, attempt int)`, `client/retry.go:154-166`) is used only for observability and stops at `Client.doAttempt`/`doStreamAttempt` — it never reaches `sendRequest`, `openExchange`, or any dial call, so wiring it through means widening `openExchange`'s signature on every transport, a materially larger and separate change; (2) there is currently **zero** use of `context.WithValue` anywhere in this codebase — introducing it here would be a first-of-its-kind pattern against the codebase's own explicit-parameter style (e.g. `PickContext` is a plain struct, not a context value), which deserves its own design discussion, not a rider on this issue. Recommend filing a follow-up issue.
- A pre-existing documentation drift noticed during research — `docs/CLIENT_GUIDE.md`'s Selectors section (~line 1543-1553) still shows `PickContext{Request *Request}`, but `pool/selector.go` has since made `PickContext` an empty struct. Unrelated to #943; flagged to the user separately rather than fixed here.

---

### Task 1: `pool.AddressDialer` interface and `pool.Dial` helper

**Files:**
- Create: `pool/dialer.go`
- Test: `pool/dialer_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package pool

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDialer implements only the plain Dial method.
type fakeDialer struct {
	conn net.Conn
	err  error
}

func (d *fakeDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	return d.conn, d.err
}

// fakeAddressDialer implements both Dial and DialAddress, recording every
// DialAddress call so a test can assert what it received.
type fakeAddressDialer struct {
	fakeDialer
	addrCalls []Address
	addrConn  net.Conn
	addrErr   error
}

func (d *fakeAddressDialer) DialAddress(_ context.Context, addr Address) (net.Conn, error) {
	d.addrCalls = append(d.addrCalls, addr)
	return d.addrConn, d.addrErr
}

var _ AddressDialer = (*fakeAddressDialer)(nil)

func TestDial_PrefersAddressDialerWhenResolvedGiven(t *testing.T) {
	t.Parallel()
	pipeClient, pipeServer := net.Pipe()
	defer func() { _ = pipeClient.Close(); _ = pipeServer.Close() }()
	resolved := Address{Host: "10.0.0.5", Port: 8443, Attributes: map[string]string{"zone": "us-west-2a"}}
	d := &fakeAddressDialer{addrConn: pipeClient}

	got, err := Dial(context.Background(), d, &resolved, "10.0.0.5:8443")

	require.NoError(t, err, "Dial")
	require.Lenf(t, d.addrCalls, 1, "DialAddress call count = %d, want 1", len(d.addrCalls))
	assert.Equalf(t, resolved, d.addrCalls[0],
		"DialAddress got Address = %+v, want %+v — Attributes must pass through unchanged", d.addrCalls[0], resolved)
	assert.Samef(t, pipeClient, got, "Dial = %v, want the AddressDialer path's conn unchanged", got)
}

func TestDial_FallsBackToPlainDialWhenResolvedIsNil(t *testing.T) {
	t.Parallel()
	pipeClient, pipeServer := net.Pipe()
	defer func() { _ = pipeClient.Close(); _ = pipeServer.Close() }()
	d := &fakeAddressDialer{fakeDialer: fakeDialer{conn: pipeClient}}

	got, err := Dial(context.Background(), d, nil, "10.0.0.5:8443")

	require.NoError(t, err, "Dial")
	assert.Emptyf(t, d.addrCalls, "Dial called DialAddress with a nil resolved Address")
	assert.Samef(t, pipeClient, got, "Dial = %v, want the plain Dial path's conn unchanged", got)
}

func TestDial_FallsBackToPlainDialWhenDialerLacksAddressDialer(t *testing.T) {
	t.Parallel()
	pipeClient, pipeServer := net.Pipe()
	defer func() { _ = pipeClient.Close(); _ = pipeServer.Close() }()
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{conn: pipeClient} // implements Dial only

	got, err := Dial(context.Background(), d, &resolved, "10.0.0.5:8443")

	require.NoError(t, err, "Dial")
	assert.Samef(t, pipeClient, got, "Dial = %v, want the plain Dial path's conn unchanged", got)
}

func TestDial_PropagatesAddressDialerError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeAddressDialer{addrErr: wantErr}

	got, err := Dial(context.Background(), d, &resolved, "10.0.0.5:8443")

	assert.Samef(t, wantErr, err, "Dial error = %v, want the AddressDialer path's error unchanged", err)
	assert.Nilf(t, got, "Dial conn = %v, want nil on error", got)
}

func TestDial_PropagatesPlainDialError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	d := &fakeDialer{err: wantErr}

	got, err := Dial(context.Background(), d, nil, "10.0.0.5:8443")

	assert.Samef(t, wantErr, err, "Dial error = %v, want the plain Dial path's error unchanged", err)
	assert.Nilf(t, got, "Dial conn = %v, want nil on error", got)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pool/... -run TestDial -v`
Expected: FAIL to compile — `undefined: AddressDialer`, `undefined: Dial`.

- [ ] **Step 3: Implement `pool/dialer.go`**

```go
package pool

import (
	"context"
	"net"
)

// AddressDialer is implemented by a Dialer that wants the resolver's full
// Address — host, port, and any Selector-visible Attributes — instead of a
// flattened "host:port" string. Managed transports check for this the same
// way a conn.Dialer is checked for conn.ALPNAsserter, falling back to the
// plain Dial(ctx, addr string) when a dialer does not implement it.
//
// Only consulted on managed transports, where a Selector actually picks an
// Address (client.h1Pool under TransportH1Managed, internal/poolcore.Pool
// under the HTTP/2 TransportManaged). Non-managed transports dial a
// caller-supplied address with no Resolver involved and never construct an
// Address to offer, so DialAddress is never called there. HTTP/3 transports
// do not use this Dialer abstraction at all.
//
// A caller implementing DialAddress must treat addr.Attributes as read-only:
// it is the same map instance the Resolver/Selector hold, not a copy.
type AddressDialer interface {
	DialAddress(ctx context.Context, addr Address) (net.Conn, error)
}

// baseDialer is the plain-string Dial method every conn.Dialer satisfies.
// Declared locally, structurally, so this package does not need to import
// conn to type-check against it.
type baseDialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// Dial dials addr using d, preferring d's AddressDialer capability when
// resolved is non-nil and d implements it, so a caller-configured dialer
// receives the resolver's Address (Attributes included) instead of a
// flattened string. Falls back to d.Dial(ctx, addr) when resolved is nil (no
// resolver in play) or d does not implement AddressDialer.
func Dial(ctx context.Context, d baseDialer, resolved *Address, addr string) (net.Conn, error) {
	if resolved != nil {
		if ad, ok := d.(AddressDialer); ok {
			return ad.DialAddress(ctx, *resolved)
		}
	}
	return d.Dial(ctx, addr)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pool/... -run TestDial -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add pool/dialer.go pool/dialer_test.go
git commit -m "feat(pool): add AddressDialer capability + Dial helper"
```

---

### Task 2: Expose `client.AddressDialer`

**Files:**
- Modify: `client/pool_vocab.go`

`pool` is not in the documented public-package list (`client`, `conn`, `frame`, `grpc`, `hpack`, `http1`, `http3`, `quic`, `qpack`, `trace`); `client` re-exports the resolver vocabulary via type aliases instead. `AddressDialer` needs the same treatment so a caller programs against `client.AddressDialer`, not `pool.AddressDialer`.

- [ ] **Step 1: Add the alias**

In `client/pool_vocab.go`, insert immediately after the existing `Address` alias:

```go
// Address is one resolved backend endpoint.
type Address = pool.Address

// AddressDialer is implemented by a Dialer that wants the resolver's full
// Address instead of a flattened "host:port" string.
type AddressDialer = pool.AddressDialer

// Resolver discovers backend addresses for a logical service.
type Resolver = pool.Resolver
```

No test step: this is a plain `type X = Y` alias with no behavior of its own — the same convention `Address`, `Resolver`, and `Selector` already follow with no dedicated pin test. It is exercised for real by Task 6's integration test, which implements `client.AddressDialer` from a `client_test`-style caller's perspective.

- [ ] **Step 2: Verify it compiles**

Run: `go build ./client/...`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add client/pool_vocab.go
git commit -m "feat(client): expose AddressDialer as public API"
```

---

### Task 3: Widen `NewSub` to carry the resolved `Address` (mechanical, no new behavior)

**Files:**
- Modify: `internal/poolcore/managed_core.go`
- Modify: `internal/poolcore/managed_pool.go`
- Modify: `client/h1_managed_pool.go`
- Modify: `client/h3_managed_pool.go`
- Modify: `internal/poolcore/actor_acquire_test.go`

`ManagedCore.GetOrCreateSubPool(addr Address)` (`internal/poolcore/managed_core.go:117-149`) is the one place the resolver's `Address` is alive before being reduced to a string key — but it currently hands only the string to `mp.newSub`. This task widens `newSub`'s signature so the `Address` survives one hop further; it adds no new behavior yet; the two closures that will actually use it start out ignoring the new parameter (`_ Address`) so this step compiles cleanly and every existing test keeps passing unchanged. All five call sites below share one generic type parameter, so all five must move together or the module fails to build.

- [ ] **Step 1: Widen the generic core**

In `internal/poolcore/managed_core.go`, change the `ManagedCore` struct field (around line 86):

```go
	// The three measured differences, injected rather than branched on.
	newSub    func(key string, addr Address) P
	connOf    func(MC) C
	mkRelease func(P, MC) R
```

Change the `CoreConfig` struct field and its doc comment (around line 480-487):

```go
	// NewSub builds the per-address sub-pool for key. addr is the same
	// Address key was derived from (key == addr.String()), still carrying
	// Attributes — passed through so a sub-pool that holds a conn.Dialer can
	// offer it to a pool.AddressDialer at dial time (#943). A sub-pool with
	// no conn.Dialer of its own (h3Pool) ignores it.
	//
	// Its closure must capture the SAME Recorder this config carries. Each
	// pool constructor substitutes a fresh recorder for a nil one, so letting
	// every sub-pool default independently would under-count the caller's
	// metrics with the whole suite green.
	NewSub func(key string, addr Address) P
```

Change the call site inside `GetOrCreateSubPool` (around line 143-146):

```go
	s = &CoreSubPool[P, MC]{
		p:    mp.newSub(key, addr),
		addr: addr,
	}
```

- [ ] **Step 2: Update the H2 managed-pool closure**

In `internal/poolcore/managed_pool.go`, change:

```go
		NewSub: func(key string, _ Address) *Pool { return New(key, co, po, obs, rec, nil) },
```

- [ ] **Step 3: Update the H1 managed-pool closure**

In `client/h1_managed_pool.go`, change:

```go
		NewSub: func(key string, _ Address) *h1Pool {
			return newH1Pool(key, dialer, po, hooksRef, metrics)
		},
```

- [ ] **Step 4: Update the H3 managed-pool closure (permanently unused parameter)**

In `client/h3_managed_pool.go`, change:

```go
			// addr is ignored: h3Pool dials through dialFn (a plain
			// func(ctx, string, *tls.Config)), not conn.Dialer, so it has no
			// AddressDialer seam to offer it to (#943 scoped AddressDialer to
			// the H1/H2 managed pools only).
			NewSub: func(key string, _ Address) *h3Pool {
				return newH3Pool(key, tlsConfig, po, dialFn, hooksRef, metrics)
			},
```

- [ ] **Step 5: Update the direct `CoreConfig` construction in tests**

In `internal/poolcore/actor_acquire_test.go`, change (around line 228):

```go
		NewSub:   func(key string, _ Address) *Pool { return New(key, co, po, nil, nil, nil) },
```

- [ ] **Step 6: Verify the whole module still builds and every existing test still passes**

Run: `go build ./...`
Expected: clean (a mismatched `NewSub` signature anywhere would fail generic type inference at compile time).

Run: `go test ./client/... ./internal/poolcore/... -count=1 -race`
Expected: PASS, identical to before this task — this step is a pure signature refactor, not new behavior, so there is no new red-then-green cycle; the existing suite passing unchanged **is** the verification.

- [ ] **Step 7: Commit**

```bash
git add internal/poolcore/managed_core.go internal/poolcore/managed_pool.go \
  client/h1_managed_pool.go client/h3_managed_pool.go internal/poolcore/actor_acquire_test.go
git commit -m "refactor(poolcore): thread resolved Address into NewSub"
```

---

### Task 4: Wire `AddressDialer` into the H1 managed pool

**Files:**
- Modify: `client/h1_pool.go`
- Modify: `client/h1_managed_pool.go`
- Test: `client/h1_pool_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `client/h1_pool_test.go` (after the existing `h1FakeDialer` definitions):

```go
// h1FakeAddressDialer wraps h1FakeDialer to additionally implement
// pool.AddressDialer, recording the Address each DialAddress call received so
// a test can assert what the resolver's Attributes looked like by the time
// they reached the dialer.
type h1FakeAddressDialer struct {
	*h1FakeDialer
	mu       sync.Mutex
	gotAddrs []Address
}

func newH1FakeAddressDialer() *h1FakeAddressDialer {
	return &h1FakeAddressDialer{h1FakeDialer: newH1FakeDialer()}
}

// DialAddress implements pool.AddressDialer, recording addr and then dialing
// addr.String() through the embedded fake exactly as Dial would.
func (d *h1FakeAddressDialer) DialAddress(ctx context.Context, addr Address) (net.Conn, error) {
	d.mu.Lock()
	d.gotAddrs = append(d.gotAddrs, addr)
	d.mu.Unlock()
	return d.h1FakeDialer.Dial(ctx, addr.String())
}

// addresses returns a copy of the Addresses seen by DialAddress so far.
func (d *h1FakeAddressDialer) addresses() []Address {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Address(nil), d.gotAddrs...)
}

func TestH1Pool_Acquire_PrefersAddressDialerWhenManaged(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.9", Port: 9443, Attributes: map[string]string{"zone": "us-east-1a"}}
	d := newH1FakeAddressDialer()
	p := newH1Pool(addr.String(), d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	p.address = &addr
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)

	require.NoError(t, err, "Acquire")
	p.release(mc, true)
	got := d.addresses()
	require.Lenf(t, got, 1, "DialAddress call count = %d, want 1", len(got))
	assert.Equalf(t, addr, got[0],
		"DialAddress got Address = %+v, want %+v — Attributes must survive the managed dial path", got[0], addr)
}

func TestH1Pool_Acquire_NonManagedNeverCallsDialAddress(t *testing.T) {
	t.Parallel()
	d := newH1FakeAddressDialer()
	p := newH1Pool("10.0.0.9:9443", d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	// p.address is left nil, exactly as newH1Pool leaves it for the
	// non-managed construction path (NewH1PoolClient / TransportH1Pool).
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)

	require.NoError(t, err, "Acquire")
	p.release(mc, true)
	assert.Emptyf(t, d.addresses(),
		"DialAddress was called on a non-managed pool with no resolved Address — p.address must stay nil outside the managed path")
	assert.Equalf(t, int32(1), d.h1FakeDialer.dials.Load(),
		"plain Dial call count = %d, want exactly 1 — the non-managed path must keep using the string form", d.h1FakeDialer.dials.Load())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./client/... -run TestH1Pool_Acquire_.*AddressDialer -v`
Expected: FAIL to compile — `p.address undefined (type *h1Pool has no field or method address)`.

- [ ] **Step 3: Implement**

In `client/h1_pool.go`, add the `pool` import:

```go
import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
	"github.com/lodgvideon/poseidon-http-client/pool"
)
```

Add the `address` field to the `h1Pool` struct:

```go
type h1Pool struct {
	opts PoolOptions
	addr string
	// address is the resolver Address addr was derived from (addr ==
	// address.String()), set only when this pool is a managed sub-pool (see
	// h1_managed_pool.go's NewSub). nil for a pool built directly on a
	// caller-supplied address (NewH1PoolClient / TransportH1Pool), which has
	// no Resolver in play and nothing but the string to offer.
	address *Address

	// dialer establishes the underlying transport (TCP, or TLS whose ALPN does
	// not assert "h2"). It is the test seam: tests pass a fake conn.Dialer, so no
	// live server is required.
	dialer conn.Dialer
```

Change `dialOne`'s dial call:

```go
func (p *h1Pool) dialOne() {
	nc, err := dialAttempt(p.dialEnv(), func(ctx context.Context) (net.Conn, error) {
		nc, derr := pool.Dial(ctx, p.dialer, p.address, p.addr)
		if derr != nil {
			return nil, derr
		}
		if aerr := assertH1Conn(nc); aerr != nil {
			_ = nc.Close()
			return nil, aerr
		}
		return nc, nil
	})
```

In `client/h1_managed_pool.go`, populate `address` on the managed path:

```go
		NewSub: func(key string, addr Address) *h1Pool {
			p := newH1Pool(key, dialer, po, hooksRef, metrics)
			p.address = &addr
			return p
		},
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./client/... -run TestH1Pool_Acquire_.*AddressDialer -v`
Expected: PASS.

Run: `go test ./client/... -count=1 -race`
Expected: PASS (full package regression, including the pre-existing `TestH1ManagedPool_*` suite).

- [ ] **Step 5: Commit**

```bash
git add client/h1_pool.go client/h1_managed_pool.go client/h1_pool_test.go
git commit -m "feat(client): prefer AddressDialer on the H1 managed pool"
```

---

### Task 5: Wire `AddressDialer` into the H2 managed pool

**Files:**
- Modify: `internal/poolcore/pool.go`
- Modify: `internal/poolcore/managed_pool.go`
- Test: `internal/poolcore/pool_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/poolcore/pool_test.go`:

```go
// fakeAddressDialer wraps fakeDialer (helpers_test.go) to additionally
// implement pool.AddressDialer, recording the Address each DialAddress call
// received.
type fakeAddressDialer struct {
	*fakeDialer
	mu       sync.Mutex
	gotAddrs []Address
}

func newFakeAddressDialer(t *testing.T) *fakeAddressDialer {
	t.Helper()
	return &fakeAddressDialer{fakeDialer: liveDialer(t)}
}

// DialAddress implements pool.AddressDialer, recording addr and then dialing
// addr.String() through the embedded fake exactly as Dial would.
func (d *fakeAddressDialer) DialAddress(ctx context.Context, addr Address) (net.Conn, error) {
	d.mu.Lock()
	d.gotAddrs = append(d.gotAddrs, addr)
	d.mu.Unlock()
	return d.fakeDialer.Dial(ctx, addr.String())
}

// addresses returns a copy of the Addresses seen by DialAddress so far.
func (d *fakeAddressDialer) addresses() []Address {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Address(nil), d.gotAddrs...)
}

func TestPool_DialOne_PrefersAddressDialerWhenManaged(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.9", Port: 9443, Attributes: map[string]string{"zone": "us-east-1a"}}
	d := newFakeAddressDialer(t)
	p := New(addr.String(), conn.ConnOptions{Dialer: d},
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil, nil)
	p.address = &addr
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)

	require.NoError(t, err, "Acquire")
	p.Release(mc)
	got := d.addresses()
	require.Lenf(t, got, 1, "DialAddress call count = %d, want 1", len(got))
	assert.Equalf(t, addr, got[0],
		"DialAddress got Address = %+v, want %+v — Attributes must survive the managed dial path", got[0], addr)
}

func TestPool_DialOne_NonManagedNeverCallsDialAddress(t *testing.T) {
	t.Parallel()
	d := newFakeAddressDialer(t)
	p := New("10.0.0.9:9443", conn.ConnOptions{Dialer: d},
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil, nil)
	// p.address is left nil, exactly as New leaves it for a pool built
	// outside the managed path.
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)

	require.NoError(t, err, "Acquire")
	p.Release(mc)
	assert.Emptyf(t, d.addresses(),
		"DialAddress was called on a non-managed pool with no resolved Address — p.address must stay nil outside the managed path")
	assert.Equalf(t, int32(1), d.fakeDialer.dialCount.Load(),
		"plain Dial call count = %d, want exactly 1 — the non-managed path must keep using the string form", d.fakeDialer.dialCount.Load())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/poolcore/... -run TestPool_DialOne_.*AddressDialer -v`
Expected: FAIL to compile — `p.address undefined (type *Pool has no field or method address)`.

- [ ] **Step 3: Implement**

In `internal/poolcore/pool.go`, add the `net` import:

```go
import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/pool"
)
```

Add the `address` field to the `Pool` struct (immediately after the existing `Addr` field, around line 113):

```go
type Pool struct {
	opts     PoolOptions
	connOpts conn.ConnOptions
	Addr     string
	// address is the resolver Address Addr was derived from (Addr ==
	// address.String()), set only when this pool is a managed sub-pool (see
	// managed_pool.go's NewSub). nil for a pool built directly on a
	// caller-supplied address, which has no Resolver in play and nothing but
	// the string to offer.
	address *Address
```

Change `dialOne` (lines 664-673) to prefer `AddressDialer` only when a resolved `Address` is present — the `if p.address != nil` guard means a non-managed pool's `conn.ConnOptions` (and its own nil-`Dialer`-defaults-to-`TLSDialer` behavior inside `conn.Dial`) is left completely untouched:

```go
func (p *Pool) dialOne() {
	c, err := DialAttempt(p.DialEnv(), func(ctx context.Context) (*conn.Conn, error) {
		opts := p.connOpts
		if p.address != nil {
			opts.Dialer = conn.DialerFunc(func(dctx context.Context, addr string) (net.Conn, error) {
				return pool.Dial(dctx, p.connOpts.Dialer, p.address, addr)
			})
		}
		return conn.Dial(ctx, p.Addr, opts)
	})
	if err != nil {
		p.dialDoneCh <- DialResult{Err: &DialError{Addr: p.Addr, Err: err}}
		return
	}
	var typed any
	if p.wrap != nil {
		typed, err = p.wrap(c)
		if err != nil {
			p.dialDoneCh <- DialResult{Err: &DialError{Addr: p.Addr, Err: err}}
			return
		}
	}
	p.dialDoneCh <- DialResult{Mc: &ManagedConn{C: c, Typed: typed, LastUsed: time.Now(), p: p}}
}
```

In `internal/poolcore/managed_pool.go`, populate `address` on the managed path:

```go
		NewSub: func(key string, addr Address) *Pool {
			p := New(key, co, po, obs, rec, nil)
			p.address = &addr
			return p
		},
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/poolcore/... -run TestPool_DialOne_.*AddressDialer -v`
Expected: PASS.

Run: `go test ./internal/poolcore/... -count=1 -race`
Expected: PASS (full package regression).

- [ ] **Step 5: Commit**

```bash
git add internal/poolcore/pool.go internal/poolcore/managed_pool.go internal/poolcore/pool_test.go
git commit -m "feat(poolcore): prefer AddressDialer on the H2 managed pool"
```

---

### Task 6: End-to-end integration test

**Files:**
- Test: `client/h1_managed_pool_test.go`

Tasks 4 and 5 each unit-test one protocol's `dialOne` in isolation. This task proves the whole chain a real caller depends on: a `Resolver` that attaches `Attributes`, through `Selector.Pick`, through the generic `ManagedCore`, into a custom `client.AddressDialer`. One protocol (H1) is enough — both protocols share the *same* `ManagedCore.GetOrCreateSubPool`/`Acquire` and the *same* `pool.Dial` helper (already exhaustively unit-tested in Task 1), so re-proving that shared machinery a second time end-to-end for H2 would be duplicate coverage of code this plan does not fork per protocol.

- [ ] **Step 1: Write the failing test**

Append to `client/h1_managed_pool_test.go`:

```go
func TestH1ManagedPool_Acquire_DialerReceivesResolverAttributes(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.7", Port: 8080, Attributes: map[string]string{"zone": "us-west-2a", "weight": "10"}}
	d := newH1FakeAddressDialer()
	mp, err := newH1ManagedPool(StaticResolver(addr), RoundRobin(), DrainGraceful,
		d, h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, release, gotAddr, aerr := mp.Acquire(ctx)

	require.NoError(t, aerr, "Acquire")
	require.True(t, c.IsAlive(), "Acquire handed back a dead conn")
	release(true)
	assert.Equalf(t, addr, gotAddr, "Acquire's own returned Address = %+v, want %+v", gotAddr, addr)
	got := d.addresses()
	require.Lenf(t, got, 1, "DialAddress call count = %d, want 1", len(got))
	assert.Equalf(t, addr, got[0],
		"DialAddress got Address = %+v, want %+v — a managed client's custom AddressDialer must see the resolver's Attributes, not just host:port", got[0], addr)
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `go test ./client/... -run TestH1ManagedPool_Acquire_DialerReceivesResolverAttributes -v`
Expected: PASS immediately — Tasks 1-5 already implement the behavior this test exercises, so there is no red phase here. That is intentional: Tasks 4 and 5 each pin one protocol's `dialOne` in isolation with a hand-built `Address`; this test is the end-to-end proof that the real path a caller depends on — `Resolver` → `Selector.Pick` → `ManagedCore` → sub-pool → dialer — carries `Attributes` all the way through, which no unit test in Tasks 4-5 exercises by itself. If it fails here, a wiring step in Task 3 or 4 was missed.

- [ ] **Step 3: Commit**

```bash
git add client/h1_managed_pool_test.go
git commit -m "test(client): pin resolver Attributes reaching a custom AddressDialer"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docs/CLIENT_GUIDE.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update `docs/CLIENT_GUIDE.md`**

In the `### Dialers (`ConnOpts.Dialer`)` section, immediately after the existing `**Dialer/transport pairing is checked.**` paragraph (the one ending "...instead of `http1: read status line: EOF` on every exchange."), add:

```markdown
**Managed transports offer the resolved Address, if your dialer wants it.** A dialer may additionally implement `client.AddressDialer` (`DialAddress(ctx, addr client.Address) (net.Conn, error)`) to receive the `Address` a `Selector` picked — host, port, and any `Attributes` the `Resolver` attached (availability zone, weight class, …) — instead of a flattened `"host:port"` string. `TransportH1Managed` and the HTTP/2 `TransportManaged` check for it on every dial and prefer `DialAddress` over `Dial` when it is implemented; `Dial` is still called for a dialer that does not implement `AddressDialer`, so existing dialers keep working unchanged. `Attributes` is the same map instance the `Resolver`/`Selector` hold — treat it as read-only.

Non-managed transports (`TransportH1SingleConn`, `TransportH1Pool`, `TransportSingleConn`, `TransportPool`, `TransportALPN`) have no `Resolver` in the picture and never construct an `Address` to offer, so `DialAddress` is never called on them even if the configured dialer implements it — they always call `Dial`. HTTP/3 transports do not use `conn.Dialer` at all and are unaffected.
```

- [ ] **Step 2: Update `CHANGELOG.md`**

In the `## [Unreleased]` → `### Added` section, insert as the new first bullet (above the `conn.DialerFunc` entry):

```markdown
- **A managed transport's dialer can receive the resolver's Address, Attributes included.** `client.AddressDialer` (`DialAddress(ctx, addr client.Address) (net.Conn, error)`) is an optional capability a `Dialer` implements to receive the `Selector`-picked `Address` — host, port, and `Attributes` — instead of a flattened `"host:port"` string. Checked the same way `conn.ALPNAsserter` is, on `TransportH1Managed` and the HTTP/2 `TransportManaged`; falls back to `Dial` when a dialer does not implement it, so nothing changes for existing dialers. Non-managed transports and HTTP/3 have no resolved `Address` to offer and are unaffected (#943).

```

- [ ] **Step 3: Commit**

```bash
git add docs/CLIENT_GUIDE.md CHANGELOG.md
git commit -m "docs: document AddressDialer"
```

---

## Verification checklist (run once, after Task 7)

```bash
go build ./...
go vet ./...
make lint
go test ./... -count=1 -race
make mutation
```

Expected: all clean. This plan does not touch any RFC-conformance-tagged test (`TestConformance_*`) or wire-protocol byte encoding, so `scripts/rfc-coverage-gate.sh` needs no new row and `make bench` / `alloc-gate` needs no attention — nothing here runs on a request hot path (dialing happens once per new connection, not per request), and no new file falls under the seven `bench-gate` packages. `make mutation` scopes to the diff against `origin/main`, so run it after all seven tasks are committed, not per-task.
