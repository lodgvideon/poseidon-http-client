# AddressDialer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a *managed* transport's dialer receive the full resolver `Address` (host, port, and `Attributes`) instead of a flattened `"host:port"` string, via an optional `AddressDialer` capability interface that falls back to the plain `Dialer` when a dialer does not implement it. Closes [#943](https://github.com/lodgvideon/poseidon-http-client/issues/943).

**Architecture:** Add `pool.AddressDialer` — mirroring `conn.ALPNAsserter`'s capability-check idiom — plus a small `pool.DialResolved` helper that prefers it. Wrap the resolver's `Address` into the dialer **once, at sub-pool construction time**, for the two sub-pool types that actually hold a `conn.Dialer`: `client.h1Pool` (via a new `newH1SubPool` constructor, under `TransportH1Managed`) and `internal/poolcore.Pool` (via a new `newSubPool` constructor, under the HTTP/2 `TransportManaged`). Building the wrapper with `conn.DialerFunc` (landed on this branch moments ago, #942) means neither sub-pool's own dial method (`dialOne`) changes at all — whether a dial offers `AddressDialer` is decided once, when the sub-pool is built for a specific address, not re-decided on every dial.

**Tech Stack:** Go, testify (`require`/`assert`), the repo's existing `net.Pipe`-based dialer fakes (`h1FakeDialer`, `fakeDialer`).

---

## Corrections to the issue text (read before starting)

- The issue cites `client.Address` at `client/resolver.go:13-25`. **That file does not exist.** The real type is `pool.Address` (`pool/resolver.go:12-25`); `client.Address` is a type *alias* of it (`client/pool_vocab.go:15`: `type Address = pool.Address`). They are one type — a `Resolver`/`Dialer` written against either name satisfies both. Every reference below uses whichever name is idiomatic for the file being edited (bare `Address` inside `pool`/`client`/`poolcore`, since all three have it in scope via alias).
- `Address` has exactly three fields today — `Host string`, `Port int`, `Attributes map[string]string` — no network-family field. Adding one is a separate, unrelated change; not attempted here.

## Explicitly out of scope

- **Non-managed transports** (`TransportH1SingleConn`, `TransportH1Pool`, `TransportSingleConn`, `TransportPool`, `TransportALPN`). None of them ever construct a resolver `Address` — their address is a caller-supplied string from `ClientOptions.Addr` with no `Resolver`/`Selector` in the picture at all. A plain `newH1Pool`/`New` pool is never routed through the new `newH1SubPool`/`newSubPool` constructors, so a custom dialer implementing `AddressDialer` is simply never wrapped, and `DialAddress` is never called on these transports — not because of a runtime check, but because the wrapping code never runs on that path at all.
- **HTTP/3** (`TransportH3`, `TransportH3Pool`, `TransportH3Managed`). All three dial through a distinct `func(ctx, string, *tls.Config) (h3Client, error)` shape, never through `conn.Dialer` — there is no seam to check `AddressDialer` against. `client/h3_managed_pool.go`'s `NewSub` closure gets a mechanical signature update in Task 3 (to keep the shared generic core compiling) but no new behavior.
- **Attempt-number / retry-count reaching the dialer**, and any `context.WithValue`-based propagation — the issue's own hedged, secondary ask ("would cover the retry case too"). Two independent reasons to defer it rather than bolt it on here: (1) the attempt counter that already exists (`retryDoer.doAttempt(ctx, req, resp, attempt int)`, `client/retry.go:154-166`) is used only for observability and stops at `Client.doAttempt`/`doStreamAttempt` — it never reaches `sendRequest`, `openExchange`, or any dial call, so wiring it through means widening `openExchange`'s signature on every transport, a materially larger and separate change; (2) there is currently **zero** use of `context.WithValue` anywhere in this codebase — introducing it here would be a first-of-its-kind pattern against the codebase's own explicit-parameter style (e.g. `PickContext` is a plain struct, not a context value), which deserves its own design discussion, not a rider on this issue. Recommend filing a follow-up issue.
- A pre-existing documentation drift noticed during research — `docs/CLIENT_GUIDE.md`'s Selectors section (~line 1543-1553) still shows `PickContext{Request *Request}`, but `pool/selector.go` has since made `PickContext` an empty struct. Unrelated to #943; flagged to the user separately rather than fixed here.

---

### Task 1: `pool.Dialer`, `pool.AddressDialer`, `pool.DialResolved`

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

// stubConn returns a net.Conn that is never read from or written to. These
// tests assert only WHICH conn value came back from DialResolved, so the pipe
// exists to produce a distinguishable, closeable net.Conn and nothing else.
func stubConn(t *testing.T) net.Conn {
	t.Helper()
	c, peer := net.Pipe()
	t.Cleanup(func() { _ = c.Close(); _ = peer.Close() })
	return c
}

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
	gotAddrs    []Address
	addressConn net.Conn
	addressErr  error
}

func (d *fakeAddressDialer) DialAddress(_ context.Context, addr Address) (net.Conn, error) {
	d.gotAddrs = append(d.gotAddrs, addr)
	return d.addressConn, d.addressErr
}

var _ AddressDialer = (*fakeAddressDialer)(nil)

func TestDialResolved_PrefersAddressDialerWhenImplemented(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443, Attributes: map[string]string{"zone": "us-west-2a"}}
	d := &fakeAddressDialer{addressConn: want}

	got, err := DialResolved(context.Background(), d, resolved)

	require.NoError(t, err, "DialResolved")
	require.Lenf(t, d.gotAddrs, 1, "DialAddress call count = %d, want 1", len(d.gotAddrs))
	assert.Equalf(t, resolved, d.gotAddrs[0],
		"DialAddress got Address = %+v, want %+v — Attributes must pass through unchanged", d.gotAddrs[0], resolved)
	assert.Samef(t, want, got, "DialResolved = %v, want the AddressDialer path's conn unchanged", got)
}

func TestDialResolved_FallsBackWhenDialerLacksCapability(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{conn: want} // implements Dial only

	got, err := DialResolved(context.Background(), d, resolved)

	require.NoError(t, err, "DialResolved")
	assert.Samef(t, want, got, "DialResolved = %v, want the plain Dial path's conn unchanged", got)
}

func TestDialResolved_PropagatesAddressDialerError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeAddressDialer{addressErr: wantErr}

	got, err := DialResolved(context.Background(), d, resolved)

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the AddressDialer path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}

func TestDialResolved_PropagatesPlainDialError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	d := &fakeDialer{err: wantErr}

	got, err := DialResolved(context.Background(), d, Address{Host: "10.0.0.5", Port: 8443})

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the plain Dial path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pool/... -run TestDialResolved -v`
Expected: FAIL to compile — `undefined: AddressDialer`, `undefined: DialResolved`.

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

// Dialer is the plain-string dial contract every conn.Dialer satisfies. It is
// declared here structurally, rather than imported, so this package does not
// depend on conn; conn.Dialer and any custom dialer satisfy it without
// naming it.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// DialResolved dials resolved using d, preferring d's AddressDialer
// capability so a caller-configured dialer receives the resolver's Address
// (Attributes included) instead of a flattened string. Falls back to
// d.Dial(ctx, resolved.String()) when d does not implement AddressDialer.
func DialResolved(ctx context.Context, d Dialer, resolved Address) (net.Conn, error) {
	if ad, ok := d.(AddressDialer); ok {
		return ad.DialAddress(ctx, resolved)
	}
	return d.Dial(ctx, resolved.String())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pool/... -run TestDialResolved -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Review the new tests**

Run the `reviewing-tests` skill over the four tests added in this task (mutate each assertion and confirm it goes red; check the case set against equivalence classes — capability present/absent crossed with success/error — rather than hand-picked; confirm AAA + require/assert placement).

- [ ] **Step 6: Commit**

```bash
git add pool/dialer.go pool/dialer_test.go
git commit -m "feat(pool): add AddressDialer capability + DialResolved helper"
```

---

### Task 2: Expose `client.AddressDialer`

**Files:**
- Modify: `client/pool_vocab.go`

`pool` is not in the documented public-package list (`client`, `conn`, `frame`, `grpc`, `hpack`, `http1`, `http3`, `quic`, `qpack`, `trace`); `client` re-exports the resolver vocabulary via type aliases instead. `AddressDialer` needs the same treatment so a caller programs against `client.AddressDialer`, not `pool.AddressDialer`.

`pool.Dialer` (Task 1) is deliberately **not** aliased here: `client`'s documented convention (`docs/CLIENT_GUIDE.md`) already points callers at `conn.Dialer` for anything dialer-shaped, and a second `client.Dialer` name for a structurally-identical-but-different type would be a pun a reader has to untangle, not a convenience.

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

`ManagedCore.GetOrCreateSubPool(addr Address)` (`internal/poolcore/managed_core.go:117-149`) is the one place the resolver's `Address` is alive before being reduced to a string key — but it currently hands only the string to `mp.newSub`. This task changes `newSub` to take the `Address` **instead of** the string (not in addition to it — the string is always `addr.String()`, so keeping both would just be the same value spelled two ways). Each closure now derives its own key string inline. This is a pure signature change: every closure below produces byte-identical output to before, so there is no new behavior and no red-then-green cycle — the existing suite passing unchanged **is** the verification. All five call sites share one generic type parameter, so all five must move together or the module fails to build.

- [ ] **Step 1: Widen the generic core**

In `internal/poolcore/managed_core.go`, change the `ManagedCore` struct field (around line 86):

```go
	// The three measured differences, injected rather than branched on.
	newSub    func(addr Address) P
	connOf    func(MC) C
	mkRelease func(P, MC) R
```

Change the `CoreConfig` struct field and its doc comment (around line 480-487):

```go
	// NewSub builds the sub-pool for one resolved address. addr still
	// carries Attributes, so a sub-pool holding a conn.Dialer can offer them
	// to a pool.AddressDialer at dial time (#943); a sub-pool with no
	// conn.Dialer of its own (h3Pool) uses only addr.String().
	//
	// Its closure must capture the SAME Recorder this config carries. Each
	// pool constructor substitutes a fresh recorder for a nil one, so letting
	// every sub-pool default independently would under-count the caller's
	// metrics with the whole suite green.
	NewSub func(addr Address) P
```

Change the call site inside `GetOrCreateSubPool` (around line 143-146):

```go
	s = &CoreSubPool[P, MC]{
		p:    mp.newSub(addr),
		addr: addr,
	}
```

(`key` is still computed just above, for the map lookup and insert — untouched.)

- [ ] **Step 2: Update the H2 managed-pool closure**

In `internal/poolcore/managed_pool.go`, change:

```go
		NewSub: func(addr Address) *Pool { return New(addr.String(), co, po, obs, rec, nil) },
```

- [ ] **Step 3: Update the H1 managed-pool closure**

In `client/h1_managed_pool.go`, change:

```go
		NewSub: func(addr Address) *h1Pool {
			return newH1Pool(addr.String(), dialer, po, hooksRef, metrics)
		},
```

(Task 4 changes this again, to call a new `newH1SubPool` instead.)

- [ ] **Step 4: Update the H3 managed-pool closure**

In `client/h3_managed_pool.go`, change:

```go
			// Only addr.String() is used: h3Pool dials through dialFn (a
			// plain func(ctx, string, *tls.Config)), not conn.Dialer, so
			// there is no AddressDialer seam to offer the Attributes to
			// (#943 scoped AddressDialer to the H1/H2 managed pools only).
			NewSub: func(addr Address) *h3Pool {
				return newH3Pool(addr.String(), tlsConfig, po, dialFn, hooksRef, metrics)
			},
```

- [ ] **Step 5: Update the direct `CoreConfig` construction in tests**

In `internal/poolcore/actor_acquire_test.go`, change (around line 228):

```go
		NewSub:   func(addr Address) *Pool { return New(addr.String(), co, po, nil, nil, nil) },
```

- [ ] **Step 6: Verify the whole module still builds and every existing test still passes**

Run: `go build ./...`
Expected: clean (a mismatched `NewSub` signature anywhere would fail generic type inference at compile time).

Run: `go test ./client/... ./internal/poolcore/... -count=1 -race`
Expected: PASS, identical to before this task.

- [ ] **Step 7: Commit**

```bash
git add internal/poolcore/managed_core.go internal/poolcore/managed_pool.go \
  client/h1_managed_pool.go client/h3_managed_pool.go internal/poolcore/actor_acquire_test.go
git commit -m "refactor(poolcore): pass the resolved Address itself to NewSub"
```

---

### Task 4: Wire `AddressDialer` into the H1 managed pool

**Files:**
- Modify: `client/h1_managed_pool.go`
- Test: `client/h1_pool_test.go`

The capability is wired **once, when a sub-pool is constructed for a specific address** — not on every dial. `client/h1_pool.go` (in particular `h1Pool.dialOne`) is not touched by this task at all: a managed sub-pool's `dialer` field already *is* the wrapped dialer by the time `dialOne` ever runs.

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

var _ AddressDialer = (*h1FakeAddressDialer)(nil)

func TestNewH1SubPool_PrefersAddressDialer(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.9", Port: 9443, Attributes: map[string]string{"zone": "us-east-1a"}}
	d := newH1FakeAddressDialer()
	p := newH1SubPool(addr, d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
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

func TestNewH1Pool_NeverWrapsForAddressDialer(t *testing.T) {
	t.Parallel()
	d := newH1FakeAddressDialer()
	p := newH1Pool("10.0.0.9:9443", d, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	defer func() { _ = p.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)
	require.NoError(t, err, "Acquire")
	p.release(mc, true)

	assert.Emptyf(t, d.addresses(),
		"DialAddress was called on a pool built by the plain newH1Pool constructor — only newH1SubPool may wrap a dialer for AddressDialer")
	assert.Equalf(t, int32(1), d.h1FakeDialer.dials.Load(),
		"plain Dial call count = %d, want exactly 1 — the non-managed constructor must never wrap the dialer", d.h1FakeDialer.dials.Load())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./client/... -run 'TestNewH1SubPool_PrefersAddressDialer|TestNewH1Pool_NeverWrapsForAddressDialer' -v`
Expected: FAIL to compile — `undefined: newH1SubPool`.

- [ ] **Step 3: Implement**

In `client/h1_managed_pool.go`, add the `context`, `net`, and `pool` imports:

```go
import (
	"context"
	"net"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http1"
	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
	"github.com/lodgvideon/poseidon-http-client/pool"
)
```

Add `newH1SubPool`, and switch `NewSub` to call it instead of `newH1Pool` directly:

```go
// newH1SubPool builds the managed sub-pool for resolved: a sub-pool whose
// dial offers the resolver's Address (Attributes included) to a
// pool.AddressDialer, falling back to the plain string Dial otherwise
// (#943). A nil dialer is left alone — it cannot implement the capability.
//
// The wrap hides any capability interface the caller's dialer implements
// (conn.ALPNAsserter today) behind a plain DialerFunc. Safe because the only
// consumer, client.validateDialerALPN, runs at NewClient time on the
// original dialer — a future capability checked at dial time would need to
// be re-exposed here too.
func newH1SubPool(resolved Address, dialer conn.Dialer, po PoolOptions,
	hooksRef *atomic.Pointer[Hooks], metrics *Metrics,
) *h1Pool {
	if base := dialer; base != nil {
		dialer = conn.DialerFunc(func(ctx context.Context, addr string) (net.Conn, error) {
			return pool.DialResolved(ctx, base, resolved)
		})
	}
	return newH1Pool(resolved.String(), dialer, po, hooksRef, metrics)
}
```

```go
		NewSub: func(addr Address) *h1Pool {
			return newH1SubPool(addr, dialer, po, hooksRef, metrics)
		},
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./client/... -run 'TestNewH1SubPool_PrefersAddressDialer|TestNewH1Pool_NeverWrapsForAddressDialer' -v`
Expected: PASS.

Run: `go test ./client/... -count=1 -race`
Expected: PASS (full package regression, including the pre-existing `TestH1ManagedPool_*` suite).

- [ ] **Step 5: Review the new tests**

Run the `reviewing-tests` skill over the two tests added in this task.

- [ ] **Step 6: Commit**

```bash
git add client/h1_managed_pool.go client/h1_pool_test.go
git commit -m "feat(client): prefer AddressDialer on the H1 managed pool"
```

---

### Task 5: Wire `AddressDialer` into the H2 managed pool

**Files:**
- Modify: `internal/poolcore/managed_pool.go`
- Test: `internal/poolcore/pool_test.go`

Same shape as Task 4: the capability is wired once, at sub-pool construction. `internal/poolcore/pool.go` (`Pool.dialOne`) is not touched at all.

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

var _ pool.AddressDialer = (*fakeAddressDialer)(nil)

func TestNewSubPool_PrefersAddressDialer(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.9", Port: 9443, Attributes: map[string]string{"zone": "us-east-1a"}}
	d := newFakeAddressDialer(t)
	p := newSubPool(addr, conn.ConnOptions{Dialer: d},
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil)
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

func TestNewPool_NeverWrapsForAddressDialer(t *testing.T) {
	t.Parallel()
	d := newFakeAddressDialer(t)
	p := New("10.0.0.9:9443", conn.ConnOptions{Dialer: d},
		PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc, err := p.Acquire(ctx)
	require.NoError(t, err, "Acquire")
	p.Release(mc)

	assert.Emptyf(t, d.addresses(),
		"DialAddress was called on a pool built by the plain New constructor — only newSubPool may wrap a dialer for AddressDialer")
	assert.Equalf(t, int32(1), d.fakeDialer.dialCount.Load(),
		"plain Dial call count = %d, want exactly 1 — the non-managed constructor must never wrap the dialer", d.fakeDialer.dialCount.Load())
}
```

This test file is in package `poolcore`, which has `Address` bare via its own `internal/poolcore/vocab.go` alias but does not currently import `pool` in this file — add it:

```go
import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/pool"
)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/poolcore/... -run 'TestNewSubPool_PrefersAddressDialer|TestNewPool_NeverWrapsForAddressDialer' -v`
Expected: FAIL to compile — `undefined: newSubPool`.

- [ ] **Step 3: Implement**

In `internal/poolcore/managed_pool.go`, add the `context` and `net` imports:

```go
import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/pool"
)
```

Add `newSubPool`, and switch `NewSub` to call it instead of `New` directly:

```go
// newSubPool builds the managed sub-pool for resolved: a sub-pool whose
// dial offers the resolver's Address (Attributes included) to a
// pool.AddressDialer, falling back to the plain string Dial otherwise
// (#943). A nil Dialer is left alone — it cannot implement the capability,
// and conn.Dial applies its own default in that case.
//
// The wrap hides any capability interface the caller's dialer implements
// (conn.ALPNAsserter today) behind a plain DialerFunc. Safe because the only
// consumer, client.validateDialerALPN, runs at NewClient time on the
// original dialer — a future capability checked at dial time would need to
// be re-exposed here too.
func newSubPool(resolved Address, co conn.ConnOptions, po PoolOptions,
	obs pool.Observer, rec pool.Recorder,
) *Pool {
	if base := co.Dialer; base != nil {
		co.Dialer = conn.DialerFunc(func(ctx context.Context, addr string) (net.Conn, error) {
			return pool.DialResolved(ctx, base, resolved)
		})
	}
	return New(resolved.String(), co, po, obs, rec, nil)
}
```

```go
		NewSub: func(addr Address) *Pool { return newSubPool(addr, co, po, obs, rec) },
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/poolcore/... -run 'TestNewSubPool_PrefersAddressDialer|TestNewPool_NeverWrapsForAddressDialer' -v`
Expected: PASS.

Run: `go test ./internal/poolcore/... -count=1 -race`
Expected: PASS (full package regression).

- [ ] **Step 5: Review the new tests**

Run the `reviewing-tests` skill over the two tests added in this task.

- [ ] **Step 6: Commit**

```bash
git add internal/poolcore/managed_pool.go internal/poolcore/pool_test.go
git commit -m "feat(poolcore): prefer AddressDialer on the H2 managed pool"
```

---

### Task 6: End-to-end integration test

**Files:**
- Test: `client/h1_managed_pool_test.go`
- Test: `internal/poolcore/managed_pool_internal_test.go`

> **Scope note (revised during Task 5's code-quality review):** originally scoped to H1 only, on the theory that both protocols share the same `ManagedCore`/`pool.DialResolved` machinery so one end-to-end proof would do. That reasoning had a gap: Tasks 4 and 5 each unit-test one protocol's *sub-pool constructor* (`newH1SubPool`/`newSubPool`) directly, called with a hand-built `Address` — neither exercises the one-line `NewSub` closure inside `newH1ManagedPool`/`BuildManagedPool` that wires that constructor up for real. Reverting just that closure back to calling `newH1Pool`/`New` directly (dropping the #943 wiring) would leave every existing test, Tasks 4-5's included, green. The shared machinery isn't what was at risk — each protocol's *own* `NewSub` closure is, and that's exactly what Tasks 4-5's unit tests bypass. Widened to cover both protocols.

This task proves the whole chain a real caller depends on: a `Resolver` that attaches `Attributes`, through `Selector.Pick`, through the generic `ManagedCore`, into a custom `AddressDialer` — for both H1 (`client.AddressDialer` via `newH1ManagedPool`) and H2 (`pool.AddressDialer` via `BuildManagedPool`/`NewManagedPool`).

- [ ] **Step 1: Write the failing H1 test**

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

- [ ] **Step 2: Write the failing H2 test**

Append to `internal/poolcore/managed_pool_internal_test.go`:

```go
func TestManagedPool_Acquire_DialerReceivesResolverAttributes(t *testing.T) {
	t.Parallel()
	addr := Address{Host: "10.0.0.7", Port: 8080, Attributes: map[string]string{"zone": "us-west-2a", "weight": "10"}}
	d := newFakeAddressDialer(t)
	mp, err := NewManagedPool(StaticResolver(addr), RoundRobin(), DrainGraceful,
		conn.ConnOptions{Dialer: d}, PoolOptions{MaxConnsPerHost: 1, HealthCheckPeriod: time.Hour}, nil, nil)
	require.NoError(t, err, "NewManagedPool")
	defer mp.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c, release, gotAddr, aerr := mp.Acquire(ctx)
	require.NoError(t, aerr, "Acquire")
	require.True(t, c.IsAlive(), "Acquire handed back a dead conn")
	release()

	assert.Equalf(t, addr, gotAddr, "Acquire's own returned Address = %+v, want %+v", gotAddr, addr)
	got := d.addresses()
	require.Lenf(t, got, 1, "DialAddress call count = %d, want 1", len(got))
	assert.Equalf(t, addr, got[0],
		"DialAddress got Address = %+v, want %+v — a managed client's custom AddressDialer must see the resolver's Attributes, not just host:port", got[0], addr)
}
```

`fakeAddressDialer` / `newFakeAddressDialer` (`internal/poolcore/pool_test.go`) and `StaticResolver` (`internal/poolcore/vocab.go`, forwarding `pool.StaticResolver`) already exist in this package — Task 5 added the fake, so no new fixture is needed here. `newFakeAddressDialer(t)` wraps `liveDialer(t)`, which completes a real (faked-over-`net.Pipe`) H2 handshake via `runFakeH2Server`, so `Acquire` genuinely dials rather than stubbing the conn away.

- [ ] **Step 3: Run both tests to verify they pass**

Run: `go test ./client/... -run TestH1ManagedPool_Acquire_DialerReceivesResolverAttributes -v`
Run: `go test ./internal/poolcore/... -run TestManagedPool_Acquire_DialerReceivesResolverAttributes -v`
Expected: PASS immediately for both — Tasks 1-5 already implement the behavior these tests exercise, so there is no red phase here. That is intentional: Tasks 4 and 5 each pin one protocol's sub-pool constructor in isolation, bypassing the managed-pool's own `NewSub` closure; these are the end-to-end proof that the real path a caller depends on — `Resolver` → `Selector.Pick` → `ManagedCore` → the protocol's `NewSub` closure → dialer — carries `Attributes` all the way through for both protocols. If either fails, a wiring step in Task 4 (H1) or Task 5 (H2) was missed.

- [ ] **Step 4: Review the new tests**

Run the `reviewing-tests` skill over both tests added in this task.

- [ ] **Step 5: Commit**

```bash
git add client/h1_managed_pool_test.go internal/poolcore/managed_pool_internal_test.go
git commit -m "test(client,poolcore): pin resolver Attributes reaching a custom AddressDialer"
```

---

### Task 7: Documentation

**Files:**
- Modify: `docs/CLIENT_GUIDE.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update `docs/CLIENT_GUIDE.md`**

In the `### Dialers (`ConnOpts.Dialer`)` section, immediately after the existing `**Dialer/transport pairing is checked.**` paragraph (the one ending "...instead of `http1: read status line: EOF` on every exchange."), add:

```markdown
**Managed transports offer the resolved Address, if your dialer wants it.** A dialer may additionally implement `client.AddressDialer` (`DialAddress(ctx, addr client.Address) (net.Conn, error)`) to receive the `Address` a `Selector` picked — host, port, and any `Attributes` the `Resolver` attached (availability zone, weight class, …) — instead of a flattened `"host:port"` string. `TransportH1Managed` and the HTTP/2 `TransportManaged` check for it once, when a sub-pool is built for a resolved address, and prefer `DialAddress` over `Dial` for every dial that sub-pool makes when it is implemented; `Dial` is still called for a dialer that does not implement `AddressDialer`, so existing dialers keep working unchanged. `Attributes` is the same map instance the `Resolver`/`Selector` hold — treat it as read-only.

Non-managed transports (`TransportH1SingleConn`, `TransportH1Pool`, `TransportSingleConn`, `TransportPool`, `TransportALPN`) have no `Resolver` in the picture and never construct an `Address` to offer, so `DialAddress` is never called on them even if the configured dialer implements it — they always call `Dial`. HTTP/3 transports do not use `conn.Dialer` at all and are unaffected.
```

- [ ] **Step 2: Update `CHANGELOG.md`**

In the `## [Unreleased]` → `### Added` section, insert as the new first bullet (above the `conn.DialerFunc` entry), wrapped to match the surrounding entries:

```markdown
- **A managed transport's dialer can receive the resolver's Address,
  Attributes included.** `client.AddressDialer` (`DialAddress(ctx, addr
  client.Address) (net.Conn, error)`) is an optional capability a `Dialer`
  implements to receive the `Selector`-picked `Address` — host, port, and
  `Attributes` — instead of a flattened `"host:port"` string. Checked the
  same way `conn.ALPNAsserter` is, on `TransportH1Managed` and the HTTP/2
  `TransportManaged`; falls back to `Dial` when a dialer does not implement
  it, so nothing changes for existing dialers. Non-managed transports and
  HTTP/3 have no resolved `Address` to offer and are unaffected (#943).

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
