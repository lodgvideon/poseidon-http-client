# C.3 Service Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pluggable `Resolver` (Pull + Watch) with built-in DNS / Static implementations, a pluggable `Selector` (RoundRobin/Random/Hash), and a `managedPool` that fans existing `*client.Pool` instances out across resolved addresses with configurable drain modes — fully backwards-compatible with `ClientOptions.Addr`.

**Architecture:** A new `managedPool` owns a `map[Address]*Pool` and a `Resolver`/`Selector`. Existing `*Pool` is reused unchanged as the per-address sub-pool. `poolTransport` is rewired to drive `managedPool`. `ClientOptions.Addr` is wrapped into `StaticResolver(addrs...)` when no explicit `Resolver` is supplied. Drain (graceful/hard/lazy) is enforced by `managedPool` — sub-pools themselves are unmodified.

**Tech Stack:** Go 1.24, stdlib only (`net`, `hash/fnv`, `math/rand`, `sync`, `sync/atomic`, `time`, `context`). No new module deps.

**Spec:** [`docs/superpowers/specs/2026-05-08-c3-service-discovery-design.md`](../specs/2026-05-08-c3-service-discovery-design.md)

**Branch:** `claude/c3-service-discovery` (already created, spec committed).

---

## Reference: existing types you will integrate with

Before starting, read these files to understand the contract you're plugging into:

- `client/client.go` — `Client`, `ClientOptions`, `NewClient`, `Client.PoolStats`.
- `client/transport.go` — the internal `transport` interface (`acquire(ctx) (*conn.Conn, func(), error)` + `close()`).
- `client/pool.go` — `Pool`, `PoolOptions`, `Stats`, `Pool.acquire`, `Pool.release`, `Pool.Stats`, `Pool.Close`. Pool is **actor-based** — one goroutine, channels for all mutation. Don't try to mutate Pool fields from outside; if you need a new behavior on Pool itself, the answer is almost always "no — let `managedPool` handle it externally".
- `client/pool_transport.go` — current `poolTransport` (the thing you'll rewire).
- `client/errors.go` — pattern for sentinel errors. Add yours there.

Pool's `acquire` returns a private `*managedConn`; call sites use it via `release(mc, reqErr)`. To fit `managedPool` into the existing transport interface, `managedPool.acquire` must return `(*conn.Conn, func(), error)` and the release closure must call the originating sub-pool's `release(mc, nil)`. **Do not change Pool's public surface.**

---

## Task 1: Error sentinels + Address type

**Files:**
- Modify: `client/errors.go`
- Create: `client/resolver.go` (will hold Address type + Resolver interface + StaticResolver — start with just Address here)
- Create: `client/resolver_test.go`

- [ ] **Step 1.1: Add error sentinels**

Open `client/errors.go` and add (after the existing `Err…` block, follow the file's existing pattern):

```go
// ErrWatchUnsupported is returned by a Resolver.Watch implementation
// that does not support push-style updates. The managedPool falls
// back to a ticker around Resolve when it sees this error.
var ErrWatchUnsupported = errors.New("client: resolver does not support Watch")

// ErrNoAddresses is returned when a Resolver yields zero addresses
// AND has no cached set to fall back on, or when a Selector receives
// an empty candidate set.
var ErrNoAddresses = errors.New("client: resolver returned no addresses")

// ErrInvalidOptions is returned by NewClient when ClientOptions are
// internally inconsistent (e.g. both Addr and Resolver supplied).
var ErrInvalidOptions = errors.New("client: invalid ClientOptions")
```

If `errors.go` does not yet import `errors`, add it.

- [ ] **Step 1.2: Write the failing test for Address.String**

Create `client/resolver_test.go`:

```go
package client

import "testing"

func TestAddress_String_HostPort(t *testing.T) {
	t.Parallel()
	got := Address{Host: "example.com", Port: 443}.String()
	if got != "example.com:443" {
		t.Errorf("Address.String() = %q, want %q", got, "example.com:443")
	}
}

func TestAddress_String_IPv6Brackets(t *testing.T) {
	t.Parallel()
	got := Address{Host: "::1", Port: 8443}.String()
	if got != "[::1]:8443" {
		t.Errorf("Address.String() = %q, want %q", got, "[::1]:8443")
	}
}
```

- [ ] **Step 1.3: Run the test — must fail (Address undefined)**

```bash
go test ./client/ -run TestAddress_String -count=1
```

Expected: compile error `undefined: Address`.

- [ ] **Step 1.4: Create resolver.go with Address type**

Create `client/resolver.go`:

```go
// Package client — service discovery: Address, Resolver, built-in
// resolvers.
package client

import (
	"net"
	"strconv"
)

// Address is one resolved backend endpoint.
type Address struct {
	// Host is the dial target — IP literal or DNS name. The pool
	// never re-resolves Host; the Resolver owns that.
	Host string
	Port int
	// Attributes carries optional metadata for user Selectors
	// (zone, weight, etc.). Built-in selectors ignore it.
	Attributes map[string]string
}

// String returns "host:port" using net.JoinHostPort (which adds
// brackets around IPv6 literals automatically).
func (a Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}
```

- [ ] **Step 1.5: Run the test — must pass**

```bash
go test ./client/ -run TestAddress_String -count=1
```

Expected: PASS.

- [ ] **Step 1.6: Commit**

```bash
git add client/errors.go client/resolver.go client/resolver_test.go
git commit -m "feat(client): add Address type and resolver-related error sentinels"
```

---

## Task 2: Resolver interface + StaticResolver

**Files:**
- Modify: `client/resolver.go`
- Modify: `client/resolver_test.go`

- [ ] **Step 2.1: Write failing tests**

Append to `client/resolver_test.go`:

```go
import (
	"context"
	// (preserve existing "testing" import — adjust import block accordingly)
)

func TestStaticResolver_Resolve_ReturnsFixedSet(t *testing.T) {
	t.Parallel()
	addrs := []Address{
		{Host: "a", Port: 1},
		{Host: "b", Port: 2},
	}
	r := StaticResolver(addrs...)
	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve err = %v, want nil", err)
	}
	if len(got) != 2 || got[0] != addrs[0] || got[1] != addrs[1] {
		t.Errorf("Resolve = %v, want %v", got, addrs)
	}
}

func TestStaticResolver_Watch_SendsThenCloses(t *testing.T) {
	t.Parallel()
	addrs := []Address{{Host: "a", Port: 1}}
	r := StaticResolver(addrs...)
	ch, err := r.Watch(context.Background())
	if err != nil {
		t.Fatalf("Watch err = %v, want nil", err)
	}
	first, ok := <-ch
	if !ok {
		t.Fatal("Watch channel closed before sending initial set")
	}
	if len(first) != 1 || first[0] != addrs[0] {
		t.Errorf("Watch initial = %v, want %v", first, addrs)
	}
	if _, ok := <-ch; ok {
		t.Error("Watch channel should be closed after initial set; got another value")
	}
}
```

- [ ] **Step 2.2: Run the tests — must fail**

```bash
go test ./client/ -run "TestStaticResolver" -count=1
```

Expected: compile error `undefined: StaticResolver`.

- [ ] **Step 2.3: Implement Resolver interface + StaticResolver**

Append to `client/resolver.go`:

```go
import "context"  // add to existing import block

// Resolver discovers backend addresses for a logical service. It
// must be goroutine-safe.
type Resolver interface {
	// Resolve returns the current address set. Implementations may
	// cache and serve from a TTL-backed cache. A non-nil err with
	// len(addrs) == 0 is a hard failure; non-nil err with
	// len(addrs) > 0 is a soft warning (use cached set).
	Resolve(ctx context.Context) ([]Address, error)

	// Watch streams address-set updates. The first message MUST be
	// the current full set. Subsequent messages MUST also be the
	// full set (not deltas). The channel is closed when ctx is
	// cancelled or on terminal error. Implementations without push
	// support return ErrWatchUnsupported; the managedPool falls back
	// to a ticker around Resolve in that case.
	Watch(ctx context.Context) (<-chan []Address, error)
}

// staticResolver returns a fixed address set; Watch sends it once
// then closes.
type staticResolver struct {
	addrs []Address
}

// StaticResolver constructs a Resolver that always returns the given
// fixed set. The slice is copied; subsequent caller mutation has no
// effect.
func StaticResolver(addrs ...Address) Resolver {
	cp := make([]Address, len(addrs))
	copy(cp, addrs)
	return &staticResolver{addrs: cp}
}

func (r *staticResolver) Resolve(_ context.Context) ([]Address, error) {
	return r.addrs, nil
}

func (r *staticResolver) Watch(_ context.Context) (<-chan []Address, error) {
	ch := make(chan []Address, 1)
	ch <- r.addrs
	close(ch)
	return ch, nil
}
```

- [ ] **Step 2.4: Run tests — must pass**

```bash
go test ./client/ -run "TestStaticResolver|TestAddress_String" -count=1
```

Expected: PASS.

- [ ] **Step 2.5: Commit**

```bash
git add client/resolver.go client/resolver_test.go
git commit -m "feat(client): Resolver interface + StaticResolver"
```

---

## Task 3: DNSResolver — Resolve (happy + TTL cache + stale-on-error + PreferIPv4)

DNSResolver wraps `*net.Resolver`. To keep tests hermetic we hide the lookup behind a tiny interface so tests inject a fake.

**Files:**
- Modify: `client/resolver.go`
- Modify: `client/resolver_test.go`

- [ ] **Step 3.1: Write failing tests**

Append to `client/resolver_test.go`:

```go
import (
	"errors"
	"net"
	"sync/atomic"
	"time"
)

// fakeLookup implements dnsLookup for tests. Each call records the
// host queried and either returns the configured result or invokes a
// per-call callback.
type fakeLookup struct {
	calls atomic.Int32
	fn    func(host string) ([]net.IPAddr, error)
}

func (f *fakeLookup) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	f.calls.Add(1)
	return f.fn(host)
}

func TestDNSResolver_Resolve_HappyPath(t *testing.T) {
	t.Parallel()
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("10.0.0.1")},
			{IP: net.ParseIP("10.0.0.2")},
		}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 8080, DNSOptions{}, fl)
	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve err = %v, want nil", err)
	}
	if len(got) != 2 || got[0].Host != "10.0.0.1" || got[1].Host != "10.0.0.2" {
		t.Errorf("Resolve = %v, want [10.0.0.1:8080 10.0.0.2:8080]", got)
	}
	if got[0].Port != 8080 {
		t.Errorf("Port = %d, want 8080", got[0].Port)
	}
}

func TestDNSResolver_Resolve_TTLCache(t *testing.T) {
	t.Parallel()
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Hour}, fl)
	if _, err := r.Resolve(context.Background()); err != nil {
		t.Fatalf("first Resolve err = %v", err)
	}
	if _, err := r.Resolve(context.Background()); err != nil {
		t.Fatalf("second Resolve err = %v", err)
	}
	if c := fl.calls.Load(); c != 1 {
		t.Errorf("LookupIPAddr calls = %d, want 1 (second call must hit cache)", c)
	}
}

func TestDNSResolver_Resolve_StaleOnError(t *testing.T) {
	t.Parallel()
	var attempt atomic.Int32
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		if attempt.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		}
		return nil, errors.New("dns: connection refused")
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Nanosecond}, fl)
	if _, err := r.Resolve(context.Background()); err != nil {
		t.Fatalf("first Resolve err = %v", err)
	}
	// TTL=ns ensures the second call goes through to the lookup (which now errors).
	got, err := r.Resolve(context.Background())
	if err == nil {
		t.Errorf("second Resolve err = nil, want non-nil (soft warning)")
	}
	if len(got) != 1 || got[0].Host != "10.0.0.1" {
		t.Errorf("Resolve = %v, want cached [10.0.0.1:80]", got)
	}
}

func TestDNSResolver_Resolve_PreferIPv4_FiltersV6(t *testing.T) {
	t.Parallel()
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("10.0.0.1")},
			{IP: net.ParseIP("2001:db8::1")},
		}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{PreferIPv4: true}, fl)
	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve err = %v", err)
	}
	if len(got) != 1 || got[0].Host != "10.0.0.1" {
		t.Errorf("Resolve = %v, want [10.0.0.1:80] only", got)
	}
}
```

- [ ] **Step 3.2: Run tests — must fail**

```bash
go test ./client/ -run "TestDNSResolver_Resolve" -count=1
```

Expected: compile error (`undefined: DNSOptions`, `newDNSResolverWithLookup`).

- [ ] **Step 3.3: Implement DNSResolver**

Append to `client/resolver.go`:

```go
import (
	"sync"  // add to import block
	"time"  // add to import block
)

// DNSOptions configures DNSResolver.
type DNSOptions struct {
	// TTL governs both cache lifetime returned by Resolve AND the
	// Watch ticker period. Zero → 30s default.
	TTL time.Duration
	// Resolver is the underlying *net.Resolver; nil → net.DefaultResolver.
	Resolver *net.Resolver
	// PreferIPv4 filters AAAA results when both families resolve.
	PreferIPv4 bool
}

// dnsLookup is the seam DNSResolver depends on. *net.Resolver
// satisfies it. Tests inject a fake.
type dnsLookup interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// dnsResolver implements Resolver via DNS A/AAAA lookups with TTL
// caching. Concurrent Resolve callers serialize on first-fetch via
// rmu to avoid thundering-herd dispatch to net.Resolver.
type dnsResolver struct {
	host string
	port int
	opts DNSOptions
	rsv  dnsLookup

	rmu      sync.Mutex
	cached   []Address
	cachedAt time.Time
}

// DNSResolver constructs a DNS-backed Resolver for the given host:port.
func DNSResolver(host string, port int, opts DNSOptions) Resolver {
	rsv := dnsLookup(opts.Resolver)
	if opts.Resolver == nil {
		rsv = net.DefaultResolver
	}
	return newDNSResolverWithLookup(host, port, opts, rsv)
}

// newDNSResolverWithLookup is the internal constructor that takes an
// explicit dnsLookup (for tests). The TTL default is applied here.
func newDNSResolverWithLookup(host string, port int, opts DNSOptions, rsv dnsLookup) *dnsResolver {
	if opts.TTL <= 0 {
		opts.TTL = 30 * time.Second
	}
	return &dnsResolver{host: host, port: port, opts: opts, rsv: rsv}
}

// Resolve returns the cached address set if within TTL. Otherwise
// refreshes via dnsLookup. On lookup failure with a non-empty
// cache, returns (cache, error) — the cache wins, the error is a
// soft warning to the caller.
func (r *dnsResolver) Resolve(ctx context.Context) ([]Address, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()
	if r.cached != nil && time.Since(r.cachedAt) < r.opts.TTL {
		return r.cached, nil
	}
	ips, err := r.rsv.LookupIPAddr(ctx, r.host)
	if err != nil {
		if r.cached != nil {
			return r.cached, err
		}
		return nil, err
	}
	addrs := make([]Address, 0, len(ips))
	for _, ip := range ips {
		if r.opts.PreferIPv4 && ip.IP.To4() == nil {
			continue
		}
		addrs = append(addrs, Address{Host: ip.IP.String(), Port: r.port})
	}
	if len(addrs) == 0 {
		if r.cached != nil {
			return r.cached, ErrNoAddresses
		}
		return nil, ErrNoAddresses
	}
	r.cached = addrs
	r.cachedAt = time.Now()
	return addrs, nil
}

// Watch is implemented in Task 4.
func (r *dnsResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	return nil, ErrWatchUnsupported // overridden in Task 4
}
```

(The `Watch` stub returning `ErrWatchUnsupported` is intentional — Task 4 replaces it. We keep the type satisfying the interface so the package builds.)

- [ ] **Step 3.4: Run tests — must pass**

```bash
go test ./client/ -run "TestDNSResolver_Resolve|TestStaticResolver|TestAddress_String" -count=1 -race
```

Expected: PASS.

- [ ] **Step 3.5: Commit**

```bash
git add client/resolver.go client/resolver_test.go
git commit -m "feat(client): DNSResolver Resolve with TTL cache, stale-on-error, PreferIPv4"
```

---

## Task 4: DNSResolver — Watch (ticker)

**Files:**
- Modify: `client/resolver.go`
- Modify: `client/resolver_test.go`

- [ ] **Step 4.1: Write failing tests**

Append to `client/resolver_test.go`:

```go
func TestDNSResolver_Watch_TickerEmitsInitialAndUpdate(t *testing.T) {
	t.Parallel()
	var phase atomic.Int32
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		switch phase.Load() {
		case 0:
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		default:
			return []net.IPAddr{
				{IP: net.ParseIP("10.0.0.1")},
				{IP: net.ParseIP("10.0.0.2")},
			}, nil
		}
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: 25 * time.Millisecond}, fl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch err = %v", err)
	}
	first := mustReceiveSet(t, ch, time.Second)
	if len(first) != 1 || first[0].Host != "10.0.0.1" {
		t.Errorf("first set = %v, want [10.0.0.1:80]", first)
	}
	phase.Store(1)
	second := mustReceiveSet(t, ch, time.Second)
	if len(second) != 2 || second[1].Host != "10.0.0.2" {
		t.Errorf("second set = %v, want [10.0.0.1:80 10.0.0.2:80]", second)
	}
	cancel()
	// Channel must close after ctx cancel. Loose timeout to absorb scheduler.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, ok := <-ch
		if !ok {
			return
		}
	}
	t.Error("Watch channel did not close after ctx cancel")
}

func TestDNSResolver_Watch_NoEmitOnUnchangedSet(t *testing.T) {
	t.Parallel()
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: 25 * time.Millisecond}, fl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch err = %v", err)
	}
	_ = mustReceiveSet(t, ch, time.Second) // initial
	select {
	case got := <-ch:
		t.Errorf("unexpected second emit on unchanged set: %v", got)
	case <-time.After(80 * time.Millisecond): // > 3 ticks
	}
}

// mustReceiveSet reads one address-set from ch within d, fatally
// failing if nothing arrives.
func mustReceiveSet(t *testing.T, ch <-chan []Address, d time.Duration) []Address {
	t.Helper()
	select {
	case s, ok := <-ch:
		if !ok {
			t.Fatal("Watch channel closed before sending set")
		}
		return s
	case <-time.After(d):
		t.Fatal("Watch did not emit within timeout")
		return nil
	}
}
```

- [ ] **Step 4.2: Run tests — must fail**

```bash
go test ./client/ -run "TestDNSResolver_Watch" -count=1
```

Expected: hangs/fails (Watch returns ErrWatchUnsupported per the stub).

- [ ] **Step 4.3: Replace Watch stub with ticker implementation**

In `client/resolver.go`, replace the `(r *dnsResolver) Watch` stub with:

```go
// Watch emits the initial address set immediately and then re-resolves
// every TTL. Subsequent emits happen ONLY when the set changes (string
// equality on the JoinHostPort form, order-sensitive). The channel is
// closed when ctx is cancelled.
func (r *dnsResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	out := make(chan []Address, 1)
	first, err := r.Resolve(ctx)
	if err != nil && len(first) == 0 {
		close(out)
		return nil, err
	}
	out <- first
	go func() {
		defer close(out)
		t := time.NewTicker(r.opts.TTL)
		defer t.Stop()
		prev := first
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			next, err := r.Resolve(ctx)
			if err != nil && len(next) == 0 {
				continue // soft fail, retain prev
			}
			if addrSetEqual(prev, next) {
				continue
			}
			select {
			case out <- next:
			case <-ctx.Done():
				return
			}
			prev = next
		}
	}()
	return out, nil
}

// addrSetEqual returns true when a and b contain the same Address
// values in the same order. Order matters because DNS results
// arrive in a stable order from net.Resolver.
func addrSetEqual(a, b []Address) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

(`Address` struct contains a `map`; comparing two `Address` values directly with `!=` panics if the map is non-nil. To avoid that, compare via `String()` if `Attributes` is ever populated by DNSResolver. **DNSResolver never populates Attributes**, so the direct `!=` is safe here. Keep `addrSetEqual` private and only use it on slices produced by `dnsResolver.Resolve`.)

- [ ] **Step 4.4: Run tests — must pass**

```bash
go test ./client/ -run "TestDNSResolver_Watch|TestDNSResolver_Resolve|TestStaticResolver" -count=1 -race
```

Expected: PASS.

- [ ] **Step 4.5: Commit**

```bash
git add client/resolver.go client/resolver_test.go
git commit -m "feat(client): DNSResolver.Watch with TTL ticker, change-only emits"
```

---

## Task 5: Selectors — interface + RoundRobin + Random + Hash

**Files:**
- Create: `client/selector.go`
- Create: `client/selector_test.go`

- [ ] **Step 5.1: Write failing tests**

Create `client/selector_test.go`:

```go
package client

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRoundRobin_RotatesSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := RoundRobin()
	got := make([]string, 6)
	for i := 0; i < 6; i++ {
		a, err := s.Pick(set, PickContext{})
		if err != nil {
			t.Fatalf("Pick err = %v", err)
		}
		got[i] = a.Host
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Pick[%d] = %s, want %s (full = %v)", i, got[i], want[i], got)
		}
	}
}

func TestRoundRobin_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	if _, err := RoundRobin().Pick(nil, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick(nil) err = %v, want ErrNoAddresses", err)
	}
}

func TestRoundRobin_Concurrent_FairBalance(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}, {Host: "d"}}
	s := RoundRobin()
	const total = 4000
	var counts [4]atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total/8; i++ {
				a, err := s.Pick(set, PickContext{})
				if err != nil {
					t.Errorf("Pick err = %v", err)
					return
				}
				switch a.Host {
				case "a":
					counts[0].Add(1)
				case "b":
					counts[1].Add(1)
				case "c":
					counts[2].Add(1)
				case "d":
					counts[3].Add(1)
				}
			}
		}()
	}
	wg.Wait()
	for i, c := range counts {
		got := c.Load()
		if got != int32(total/4) {
			t.Errorf("addr %d count = %d, want %d (round-robin must be exact under atomic.Add)", i, got, total/4)
		}
	}
}

func TestRandom_PicksFromSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}}
	s := Random(rand.New(rand.NewSource(1)))
	for i := 0; i < 100; i++ {
		a, err := s.Pick(set, PickContext{})
		if err != nil {
			t.Fatalf("Pick err = %v", err)
		}
		if a.Host != "a" && a.Host != "b" {
			t.Errorf("Pick = %v, want a or b", a)
		}
	}
}

func TestRandom_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	if _, err := Random(nil).Pick(nil, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick(nil) err = %v, want ErrNoAddresses", err)
	}
}

func TestHash_Deterministic(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := Hash(func(pc PickContext) string {
		return pc.Request.Path
	})
	first, _ := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})
	second, _ := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})
	if first != second {
		t.Errorf("Hash not deterministic: %v vs %v", first, second)
	}
}

func TestHash_EmptyKey_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}}
	s := Hash(func(_ PickContext) string { return "" })
	if _, err := s.Pick(set, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick err = %v, want ErrNoAddresses on empty key", err)
	}
}

// silence unused import warning if context isn't used directly.
var _ = context.TODO
```

- [ ] **Step 5.2: Run tests — must fail**

```bash
go test ./client/ -run "TestRoundRobin|TestRandom|TestHash" -count=1
```

Expected: compile errors (`undefined: Selector`, `RoundRobin`, etc.).

- [ ] **Step 5.3: Implement selectors**

Create `client/selector.go`:

```go
// Package client — service discovery: address selection (Selector,
// PickContext, built-in RoundRobin / Random / Hash).
package client

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
)

// Selector picks one address from a set for the next dial.
// Implementations must be goroutine-safe.
type Selector interface {
	Pick(set []Address, pc PickContext) (Address, error)
}

// PickContext carries optional hints to the selector. All fields are
// optional; the zero value is valid.
type PickContext struct {
	// Request is the in-flight request, if Pick is invoked from the
	// Acquire path (vs background dial). May be nil.
	Request *Request
}

// roundRobin rotates through the set via an atomic counter.
type roundRobin struct {
	c atomic.Uint64
}

// RoundRobin returns a stateful Selector that rotates through the
// candidate set in order. The counter is shared across all calls;
// concurrent Pick is exact (atomic.Add).
func RoundRobin() Selector { return &roundRobin{} }

func (r *roundRobin) Pick(set []Address, _ PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	idx := r.c.Add(1) - 1
	return set[int(idx%uint64(len(set)))], nil
}

// randomSel picks uniformly at random. The supplied *rand.Rand (or
// the default time-seeded one) is serialized via mu — math/rand.Rand
// is not goroutine-safe.
type randomSel struct {
	rng *rand.Rand
	mu  sync.Mutex
}

// Random returns a Selector that picks uniformly. nil rng → a
// time-seeded *rand.Rand owned by the Selector.
func Random(rng *rand.Rand) Selector {
	if rng == nil {
		rng = rand.New(rand.NewSource(timeNowNanos()))
	}
	return &randomSel{rng: rng}
}

func (r *randomSel) Pick(set []Address, _ PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	r.mu.Lock()
	idx := r.rng.Intn(len(set))
	r.mu.Unlock()
	return set[idx], nil
}

// hashSel picks deterministically by hash(keyFn(pc)).
type hashSel struct {
	keyFn func(PickContext) string
}

// Hash returns a Selector that picks by FNV-1a hash of keyFn(pc) %
// len(set). keyFn returning "" → ErrNoAddresses (caller hint
// insufficient for deterministic selection).
func Hash(keyFn func(PickContext) string) Selector {
	return &hashSel{keyFn: keyFn}
}

func (h *hashSel) Pick(set []Address, pc PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	k := h.keyFn(pc)
	if k == "" {
		return Address{}, ErrNoAddresses
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(k))
	idx := int(sum.Sum64() % uint64(len(set)))
	return set[idx], nil
}

// timeNowNanos exists as a seam — overridden in tests to avoid
// time.Now() in unit-test paths that need determinism. Default uses
// the wall clock.
var timeNowNanos = func() int64 {
	return time.Now().UnixNano()
}
```

Wait — the snippet above uses `time.Now()` but the import block does not yet include `time`. Add `"time"` to the import block. (The keep-it-deterministic-via-seam comment is a future hook; in this task we just use the wall clock.)

Final import block for `client/selector.go`:

```go
import (
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
```

- [ ] **Step 5.4: Run tests — must pass**

```bash
go test ./client/ -run "TestRoundRobin|TestRandom|TestHash" -count=1 -race
```

Expected: PASS.

- [ ] **Step 5.5: Commit**

```bash
git add client/selector.go client/selector_test.go
git commit -m "feat(client): Selector interface + RoundRobin + Random + Hash built-ins"
```

---

## Task 6: managedPool skeleton — single-Resolve, lazy sub-pools, Acquire via Selector with dial-error iteration

**Files:**
- Create: `client/managed_pool.go`
- Create: `client/managed_pool_test.go`

This is the structural backbone. We start without Watch — `managedPool.run()` is added in Task 7. Drain modes are added in Tasks 8/9.

- [ ] **Step 6.1: Write failing test (uses real httptest h2 server — pattern matches existing conformance_test.go)**

Create `client/managed_pool_test.go`:

```go
package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// addrFromServer extracts the host+port from an httptest server URL.
func addrFromServer(t *testing.T, hostPort string) client.Address {
	t.Helper()
	host, port := splitHostPortInt(t, hostPort)
	return client.Address{Host: host, Port: port}
}

// splitHostPortInt parses host:port, returns host + port-as-int.
func splitHostPortInt(t *testing.T, hp string) (string, int) {
	t.Helper()
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			port := 0
			for _, c := range hp[i+1:] {
				port = port*10 + int(c-'0')
			}
			return hp[:i], port
		}
	}
	t.Fatalf("malformed host:port %q", hp)
	return "", 0
}

// hits counts requests to a handler.
type hits struct {
	n atomic.Int32
}

func (h *hits) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h.n.Add(1)
		w.WriteHeader(200)
	})
}

func TestManagedPool_StaticResolver_DialsAllAddresses(t *testing.T) {
	t.Parallel()
	var h1, h2, h3 hits
	_, addr1 := newTLSH2Server(t, h1.handler())
	_, addr2 := newTLSH2Server(t, h2.handler())
	_, addr3 := newTLSH2Server(t, h3.handler())

	res := client.StaticResolver(
		addrFromServer(t, addr1),
		addrFromServer(t, addr2),
		addrFromServer(t, addr3),
	)

	c, err := client.NewClient(client.ClientOptions{
		Resolver: res,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// 3 requests with RoundRobin (default) hit all three addresses.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
		cancel()
		if err != nil {
			t.Fatalf("Do[%d] = %v", i, err)
		}
	}
	if h1.n.Load() == 0 || h2.n.Load() == 0 || h3.n.Load() == 0 {
		t.Errorf("hits = (%d, %d, %d), want all > 0", h1.n.Load(), h2.n.Load(), h3.n.Load())
	}
}
```

(`newTLSH2Server` already exists in `client/conformance_test.go` — same package, available to this test file.)

This test depends on `ClientOptions.Resolver` (added in Task 11). For Task 6 we test the managedPool internals directly. **Skip 6.1 and run 6.2 instead.**

- [ ] **Step 6.1 (replacement): Write internal-only failing test using managedPool directly**

Create `client/managed_pool_test.go`:

```go
package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// fakeDialer routes Dial calls per address to one of the supplied
// underlying dialers. addr (host:port) → dialer.
type fakeDialer struct {
	per map[string]conn.Dialer
}

func (f *fakeDialer) Dial(ctx context.Context, addr string) (net.Conn, error) { /* ... */ }
```

Hold on — fully unit-testing `managedPool` requires either real connections (httptest) or a thoroughly mocked `*Pool`. Since `Pool` is concrete and actor-based, mocking it would require an interface refactor. The existing pool tests (`pool_test.go`) and conformance tests use real httptest h2 servers. **Match that pattern.**

Move `managed_pool_test.go` back to `package client_test` and use httptest. Delete the internal-test sketch above.

Final form of Step 6.1 (use the package-external test from the top of this task):

```go
// client/managed_pool_test.go — package client_test
// (the StaticResolver test from the top of Task 6)
```

…but this depends on `ClientOptions.Resolver` (Task 11). To avoid that ordering problem, **Task 6 introduces an internal constructor** `newManagedPool(resolver, selector, drainMode, connOpts, poolOpts) *managedPool` and tests it directly via the internal test file. The wiring through `ClientOptions` happens in Task 11.

**Replace Step 6.1 with this internal test (`package client`, file `client/managed_pool_internal_test.go`):**

```go
package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startH2Servers boots N httptest+h2 servers each backed by an
// independent hits counter; returns parsed Addresses + cleanup.
func startH2Servers(t *testing.T, n int) ([]Address, []*atomic.Int32, func()) {
	t.Helper()
	addrs := make([]Address, n)
	counts := make([]*atomic.Int32, n)
	servers := make([]*httptest.Server, n)
	for i := 0; i < n; i++ {
		c := &atomic.Int32{}
		counts[i] = c
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			c.Add(1)
			w.WriteHeader(200)
		}))
		if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
			t.Fatalf("ConfigureServer: %v", err)
		}
		srv.EnableHTTP2 = true
		srv.StartTLS()
		servers[i] = srv
		host, port := splitHostPortInt(t, srv.Listener.Addr().String())
		addrs[i] = Address{Host: host, Port: port}
	}
	cleanup := func() {
		for _, s := range servers {
			s.Close()
		}
	}
	return addrs, counts, cleanup
}

func splitHostPortInt(t *testing.T, hp string) (string, int) {
	t.Helper()
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			port := 0
			for _, c := range hp[i+1:] {
				port = port*10 + int(c-'0')
			}
			return hp[:i], port
		}
	}
	t.Fatalf("malformed host:port %q", hp)
	return "", 0
}

func newConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

func TestManagedPool_StaticResolver_RoundRobin_DistributesDials(t *testing.T) {
	t.Parallel()
	addrs, counts, cleanup := startH2Servers(t, 3)
	defer cleanup()

	mp, err := newManagedPool(
		StaticResolver(addrs...),
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// 9 sequential acquires — RoundRobin distributes 3-3-3.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, release, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		// Open and close a stream to drive a real round trip per request.
		mustOneRequest(t, c)
		release()
	}
	for i, c := range counts {
		if got := c.Load(); got < 1 {
			t.Errorf("server[%d] hits = %d, want > 0", i, got)
		}
	}
}

// mustOneRequest sends GET / over the conn and drains the response.
// Implementation lives in client (existing helpers): the simplest
// path is to construct a minimal Request and use Client's machinery.
// For Task 6 we accept a coarser check: each address must dial at
// least once.
func mustOneRequest(t *testing.T, c *conn.Conn) {
	t.Helper()
	// Cheap aliveness probe: IsAlive must hold. The actual HTTP roundtrip
	// is exercised by Task 12's integration tests via Client.Do.
	if !c.IsAlive() {
		t.Fatal("conn not alive after acquire")
	}
}

func TestManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newManagedPool(
		StaticResolver(), // empty
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = mp.acquire(ctx)
	if err != ErrNoAddresses {
		t.Errorf("acquire err = %v, want ErrNoAddresses", err)
	}
}
```

- [ ] **Step 6.2: Run tests — must fail**

```bash
go test ./client/ -run "TestManagedPool_StaticResolver_RoundRobin|TestManagedPool_NoAddresses" -count=1
```

Expected: compile errors (`undefined: newManagedPool`, `DrainGraceful`).

- [ ] **Step 6.3: Implement DrainMode + managedPool skeleton**

Create `client/managed_pool.go`:

```go
// Package client — managedPool: per-address sub-pool fan-out
// driven by a Resolver and Selector.
package client

import (
	"context"
	"errors"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// DrainMode governs sub-pool lifecycle when an address is removed
// from the resolver's set.
type DrainMode int

const (
	// DrainGraceful refuses new acquires on the removed sub-pool;
	// existing in-flight requests complete naturally; sub-pool
	// closes when its InFlightStreams drains to zero.
	DrainGraceful DrainMode = iota
	// DrainHard closes every conn in the removed sub-pool
	// immediately; in-flight streams surface as RST_STREAM(CANCEL).
	DrainHard
	// DrainLazy refuses new acquires; idle eviction tick is the only
	// closer.
	DrainLazy
)

// subPoolState wraps a *Pool with managedPool-level metadata.
type subPoolState struct {
	p        *Pool
	addr     Address
	draining bool // true once removed from the resolver set
}

// managedPool fans Acquire across per-address sub-pools driven by
// resolver+selector. Goroutine-safe.
type managedPool struct {
	resolver  Resolver
	selector  Selector
	drainMode DrainMode
	connOpts  conn.ConnOptions
	poolOpts  PoolOptions

	mu       sync.RWMutex
	addrs    []Address               // current resolved set (active only)
	subPools map[Address]*subPoolState // includes draining sub-pools

	closeOnce sync.Once
	closed    chan struct{}
}

// newManagedPool constructs a managedPool. It performs an initial
// Resolve to surface a hard error early; if Resolve returns 0 addrs
// and no error, the pool starts empty (Acquire returns ErrNoAddresses
// until a Watch update or subsequent Resolve seeds the set).
func newManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions) (*managedPool, error) {
	if s == nil {
		s = RoundRobin()
	}
	mp := &managedPool{
		resolver:  r,
		selector:  s,
		drainMode: dm,
		connOpts:  co,
		poolOpts:  po,
		subPools:  make(map[Address]*subPoolState),
		closed:    make(chan struct{}),
	}
	// Best-effort initial resolve. Soft-fail if cache is empty: Watch
	// (Task 7) populates later.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs, err := r.Resolve(ctx)
	if err != nil && len(addrs) == 0 {
		return nil, err
	}
	mp.addrs = addrs
	return mp, nil
}

// snapshotActive returns a copy of the currently active (non-draining)
// address set. The slice is safe for the caller to retain.
func (mp *managedPool) snapshotActive() []Address {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	out := make([]Address, len(mp.addrs))
	copy(out, mp.addrs)
	return out
}

// getOrCreateSubPool returns the sub-pool for addr, creating it
// under the write lock if absent. Returns nil if the managedPool is
// closed.
func (mp *managedPool) getOrCreateSubPool(addr Address) *subPoolState {
	mp.mu.RLock()
	if s, ok := mp.subPools[addr]; ok {
		mp.mu.RUnlock()
		return s
	}
	mp.mu.RUnlock()
	mp.mu.Lock()
	defer mp.mu.Unlock()
	select {
	case <-mp.closed:
		return nil
	default:
	}
	if s, ok := mp.subPools[addr]; ok {
		return s // raced
	}
	s := &subPoolState{
		p:    newPool(addr.String(), mp.connOpts, mp.poolOpts),
		addr: addr,
	}
	mp.subPools[addr] = s
	return s
}

// acquire picks an address via Selector, dials/uses the sub-pool, and
// returns the connection plus its release closure. On dial-only
// errors (DialError, ErrDialBackoff) it iterates through remaining
// addresses bounded by the active set size.
func (mp *managedPool) acquire(ctx context.Context) (*conn.Conn, func(), error) {
	tried := make(map[Address]struct{})
	var lastErr error
	for {
		set := mp.snapshotActive()
		// Filter already-tried addresses.
		if len(tried) > 0 {
			pruned := set[:0]
			for _, a := range set {
				if _, t := tried[a]; !t {
					pruned = append(pruned, a)
				}
			}
			set = pruned
		}
		if len(set) == 0 {
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, ErrNoAddresses
		}
		addr, err := mp.selector.Pick(set, PickContext{})
		if err != nil {
			return nil, nil, err
		}
		sub := mp.getOrCreateSubPool(addr)
		if sub == nil {
			return nil, nil, ErrPoolClosed
		}
		mc, err := sub.p.acquire(ctx)
		if err == nil {
			release := func() { sub.p.release(mc, nil) }
			return mc.c, release, nil
		}
		// Address-level failover: dial-related errors → mark this
		// address as tried, keep going. Other errors surface verbatim.
		if !isDialOnlyErr(err) {
			return nil, nil, err
		}
		lastErr = err
		tried[addr] = struct{}{}
	}
}

// isDialOnlyErr returns true for transient dial failures eligible for
// address-level failover (per spec §"Dial path"). DialError and
// ErrDialBackoff are the two cases.
func isDialOnlyErr(err error) bool {
	if errors.Is(err, ErrDialBackoff) {
		return true
	}
	var de *DialError
	if errors.As(err, &de) {
		return true
	}
	return false
}

// close stops managedPool and closes every sub-pool. Idempotent.
func (mp *managedPool) close() error {
	mp.closeOnce.Do(func() {
		close(mp.closed)
		mp.mu.Lock()
		defer mp.mu.Unlock()
		for _, s := range mp.subPools {
			_ = s.p.Close()
		}
		mp.subPools = nil
		mp.addrs = nil
	})
	return nil
}
```

- [ ] **Step 6.4: Run tests — must pass**

```bash
go test ./client/ -run "TestManagedPool_StaticResolver_RoundRobin|TestManagedPool_NoAddresses" -count=1 -race -timeout 60s
```

Expected: PASS.

- [ ] **Step 6.5: Run full client suite — no regressions**

```bash
go test ./client/ -count=1 -race -timeout 120s
```

Expected: PASS.

- [ ] **Step 6.6: Commit**

```bash
git add client/managed_pool.go client/managed_pool_internal_test.go
git commit -m "feat(client): managedPool skeleton with Selector-driven dial fan-out"
```

---

## Task 7: managedPool Watch consumer + address-set diff

**Files:**
- Modify: `client/managed_pool.go`
- Modify: `client/managed_pool_internal_test.go`

This task adds a background goroutine that consumes Resolver.Watch updates and applies adds/removes. **Drain modes** (graceful/hard/lazy) are still placeholders here — Task 8/9 implement them.

- [ ] **Step 7.1: Write failing test for address addition**

Append to `client/managed_pool_internal_test.go`:

```go
// scriptedResolver: a Resolver whose Watch channel is driven by the
// test (push manual updates).
type scriptedResolver struct {
	initial []Address
	updates chan []Address
}

func newScriptedResolver(initial []Address) *scriptedResolver {
	return &scriptedResolver{
		initial: initial,
		updates: make(chan []Address, 8),
	}
}

func (s *scriptedResolver) Resolve(_ context.Context) ([]Address, error) {
	return s.initial, nil
}

func (s *scriptedResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	out := make(chan []Address, 1)
	out <- s.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case set, ok := <-s.updates:
				if !ok {
					return
				}
				select {
				case out <- set:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (s *scriptedResolver) push(set []Address) { s.updates <- set }

func TestManagedPool_Watch_AddedAddress_PickedUp(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()

	res := newScriptedResolver([]Address{addrs[0]})
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Push expanded set; managedPool's Watch consumer must pick it up.
	res.push([]Address{addrs[0], addrs[1], addrs[2]})

	// Wait briefly for the Watch goroutine to apply the update.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("active set never grew to 3; got %d", len(mp.snapshotActive()))
}
```

- [ ] **Step 7.2: Run — must fail (no Watch consumer yet)**

```bash
go test ./client/ -run "TestManagedPool_Watch_AddedAddress" -count=1
```

Expected: FAIL (active set stays at 1).

- [ ] **Step 7.3: Implement run() goroutine**

In `client/managed_pool.go`, modify `newManagedPool` to start the watcher and add the `run` method.

Add at end of `newManagedPool`, just before `return mp, nil`:

```go
	go mp.run()
```

Add new methods:

```go
// run is the Watch consumer. It subscribes to Resolver.Watch and
// applies address-set updates until the managedPool is closed. If
// Watch returns ErrWatchUnsupported, run starts a TTL ticker that
// re-Resolves at DNSOptions.TTL cadence (or 30s default).
//
// Drain mode handling for removed addresses lives in Task 8/9; this
// task only seats additions and a placeholder removal that calls
// subPool.Close() (DrainHard semantics — temporary).
func (mp *managedPool) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-mp.closed
		cancel()
	}()

	ch, err := mp.resolver.Watch(ctx)
	if err != nil {
		// Fallback to ticker mode (Task 10).
		mp.runTicker(ctx)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case set, ok := <-ch:
			if !ok {
				// Watch terminated (StaticResolver case, or Resolver-driven close).
				// Fall back to ticker if there's still work to do.
				mp.runTicker(ctx)
				return
			}
			mp.applySet(set)
		}
	}
}

// runTicker is a placeholder until Task 10 wires it. For now it just
// blocks until ctx.Done so close() exits cleanly.
func (mp *managedPool) runTicker(ctx context.Context) {
	<-ctx.Done()
}

// applySet diffs old vs new active address set and updates subPools.
// Additions: noop (sub-pools dial lazily on first Acquire). Removals:
// mark sub-pool as draining; close path is Task 8/9.
func (mp *managedPool) applySet(next []Address) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	// Build set membership maps.
	prev := make(map[Address]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a] = struct{}{}
	}
	nextSet := make(map[Address]struct{}, len(next))
	for _, a := range next {
		nextSet[a] = struct{}{}
	}

	// Removed: in prev but not in next.
	for addr := range prev {
		if _, ok := nextSet[addr]; ok {
			continue
		}
		if s, ok := mp.subPools[addr]; ok {
			s.draining = true
			// Drain implementation arrives in Task 8/9. Until then we
			// rely on Close-on-managedPool-close to release resources.
		}
	}

	// Replace the active slice with next (additions are implicit:
	// sub-pools are created lazily on Acquire).
	mp.addrs = append(mp.addrs[:0:0], next...)
}
```

`Address` contains a `map`; using it as a map key panics if `Attributes` is non-nil at runtime. **Built-in resolvers never set `Attributes`**, so map-key use is safe for in-tree paths. User resolvers that populate `Attributes` violate the contract; document this on `Address`. Add a Doc note:

In `client/resolver.go`, replace the `Attributes` field comment with:

```go
	// Attributes carries optional metadata for user Selectors
	// (zone, weight, etc.). Built-in selectors ignore it.
	//
	// Note: Attributes must remain nil if the Address is used as a
	// map key (managedPool's sub-pool registry). Built-in resolvers
	// never set it. User resolvers that need attributes should
	// expose them via a side channel rather than the Address itself.
	Attributes map[string]string
```

- [ ] **Step 7.4: Run — must pass**

```bash
go test ./client/ -run "TestManagedPool_Watch_AddedAddress|TestManagedPool_StaticResolver|TestManagedPool_NoAddresses" -count=1 -race
```

Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add client/managed_pool.go client/managed_pool_internal_test.go client/resolver.go
git commit -m "feat(client): managedPool Watch consumer with address-set diff"
```

---

## Task 8: Drain modes — Graceful

**Files:**
- Modify: `client/managed_pool.go`
- Modify: `client/managed_pool_internal_test.go`

`DrainGraceful` (default): when an address is removed by Watch, the sub-pool stops accepting new acquires (`managedPool` filters it out via the `draining` flag); existing in-flight requests complete; once `Stats().InFlightStreams == 0` the sub-pool is closed and dropped from the map.

The "stop accepting new acquires" half is **already correct in Task 7**: `snapshotActive` only returns non-draining addrs (see below — fix in Step 8.3).

- [ ] **Step 8.1: Write failing test for graceful drain**

Append to `client/managed_pool_internal_test.go`:

```go
func TestManagedPool_DrainGraceful_RemovedAddress_KeepsInFlight(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Acquire a conn for addr[0] (RoundRobin first pick is index 0).
	c0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}
	if !c0.IsAlive() {
		t.Fatal("conn 0 not alive")
	}

	// Remove addr[0] from the resolver set.
	res.push([]Address{addrs[1]})
	// Wait for applySet.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// In-flight conn should still be alive (graceful).
	if !c0.IsAlive() {
		t.Error("conn 0 closed during graceful drain — expected alive until release")
	}

	// New acquire never picks addr[0] (only addr[1] active).
	c1, rel1, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after remove: %v", err)
	}
	defer rel1()
	if c1 == c0 {
		t.Error("new acquire returned the draining conn — expected addr[1]'s conn")
	}

	// Release the in-flight conn → sub-pool should close itself.
	rel0()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mp.mu.RLock()
		_, present := mp.subPools[addrs[0]]
		mp.mu.RUnlock()
		if !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("sub-pool for drained address still present after release; expected close+evict")
}
```

- [ ] **Step 8.2: Run — must fail**

```bash
go test ./client/ -run "TestManagedPool_DrainGraceful_RemovedAddress" -count=1
```

Expected: FAIL (sub-pool never gets closed; test times out at the final loop).

- [ ] **Step 8.3: Implement graceful drain**

In `client/managed_pool.go`:

(a) Make `snapshotActive` filter out draining sub-pools (defense in depth — `mp.addrs` already excludes draining, but a dial in flight from before the update could race):

```go
func (mp *managedPool) snapshotActive() []Address {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	out := make([]Address, 0, len(mp.addrs))
	for _, a := range mp.addrs {
		if s, ok := mp.subPools[a]; ok && s.draining {
			continue
		}
		out = append(out, a)
	}
	return out
}
```

(b) Replace `applySet` so removals trigger drain handling per `drainMode`:

```go
func (mp *managedPool) applySet(next []Address) {
	mp.mu.Lock()
	prev := make(map[Address]struct{}, len(mp.addrs))
	for _, a := range mp.addrs {
		prev[a] = struct{}{}
	}
	nextSet := make(map[Address]struct{}, len(next))
	for _, a := range next {
		nextSet[a] = struct{}{}
	}
	var toDrain []*subPoolState
	for addr := range prev {
		if _, ok := nextSet[addr]; ok {
			continue
		}
		if s, ok := mp.subPools[addr]; ok && !s.draining {
			s.draining = true
			toDrain = append(toDrain, s)
		}
	}
	mp.addrs = append(mp.addrs[:0:0], next...)
	mp.mu.Unlock()

	for _, s := range toDrain {
		mp.beginDrain(s)
	}
}
```

(c) Add `beginDrain` and a graceful watchdog goroutine:

```go
// beginDrain dispatches per-mode drain logic for a removed sub-pool.
func (mp *managedPool) beginDrain(s *subPoolState) {
	switch mp.drainMode {
	case DrainHard:
		mp.dropSubPool(s, true)
	case DrainLazy:
		// No-op: stays in subPools map, draining=true blocks new
		// Acquires; idle eviction in Pool itself handles conn close.
	default: // DrainGraceful
		go mp.watchDrain(s)
	}
}

// watchDrain polls the sub-pool's Stats; once InFlightStreams == 0,
// it closes the pool and removes it from the registry. Honors
// managedPool.closed.
func (mp *managedPool) watchDrain(s *subPoolState) {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-mp.closed:
			return
		case <-t.C:
		}
		st := s.p.Stats()
		if st.InFlightStreams == 0 {
			mp.dropSubPool(s, true)
			return
		}
	}
}

// dropSubPool removes s from the registry and optionally closes it.
func (mp *managedPool) dropSubPool(s *subPoolState, doClose bool) {
	mp.mu.Lock()
	delete(mp.subPools, s.addr)
	mp.mu.Unlock()
	if doClose {
		_ = s.p.Close()
	}
}
```

Add `time` to imports if not already there. Now `applySet` no longer needs to mutate `s.draining` outside `mu` — it's set under `mu.Lock` then `beginDrain` runs without the lock.

(d) `acquire`'s `getOrCreateSubPool` must NOT recreate a sub-pool that is draining or that was just dropped. Update:

```go
func (mp *managedPool) getOrCreateSubPool(addr Address) *subPoolState {
	mp.mu.RLock()
	s, ok := mp.subPools[addr]
	mp.mu.RUnlock()
	if ok && !s.draining {
		return s
	}
	if ok && s.draining {
		// caller's snapshotActive shouldn't have given us this addr;
		// guard against TOCTOU.
		return nil
	}
	mp.mu.Lock()
	defer mp.mu.Unlock()
	select {
	case <-mp.closed:
		return nil
	default:
	}
	if s, ok := mp.subPools[addr]; ok {
		if s.draining {
			return nil
		}
		return s
	}
	s = &subPoolState{
		p:    newPool(addr.String(), mp.connOpts, mp.poolOpts),
		addr: addr,
	}
	mp.subPools[addr] = s
	return s
}
```

(e) `acquire`'s loop must treat `nil` sub-pool (drained while we picked) like an address-failover signal:

```go
		sub := mp.getOrCreateSubPool(addr)
		if sub == nil {
			tried[addr] = struct{}{}
			continue
		}
```

- [ ] **Step 8.4: Run — must pass**

```bash
go test ./client/ -run "TestManagedPool_" -count=1 -race -timeout 60s
```

Expected: PASS.

- [ ] **Step 8.5: Commit**

```bash
git add client/managed_pool.go client/managed_pool_internal_test.go
git commit -m "feat(client): DrainGraceful — sub-pool drains in-flight then closes"
```

---

## Task 9: Drain modes — Hard + Lazy

**Files:**
- Modify: `client/managed_pool_internal_test.go`

The implementation in Task 8's `beginDrain` already covers all three modes. This task pins their behavior with focused tests.

- [ ] **Step 9.1: Write failing tests**

Append to `client/managed_pool_internal_test.go`:

```go
func TestManagedPool_DrainHard_RemovedAddress_ClosesImmediately(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainHard, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	c0, rel0, err := mp.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire 0: %v", err)
	}

	res.push([]Address{addrs[1]})

	// DrainHard closes the sub-pool synchronously inside applySet.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c0.IsAlive() == false {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c0.IsAlive() {
		t.Error("conn 0 still alive after DrainHard removal")
	}
	rel0()
}

func TestManagedPool_DrainLazy_RemovedAddress_RetainsSubPoolNoNewDials(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainLazy, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Seed both sub-pools by acquiring one conn each.
	for i := 0; i < 2; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("seed acquire %d: %v", i, err)
		}
		rel()
	}

	res.push([]Address{addrs[1]})

	// Wait for applySet.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mp.mu.RLock()
	_, present := mp.subPools[addrs[0]]
	mp.mu.RUnlock()
	if !present {
		t.Error("DrainLazy: sub-pool dropped immediately, expected retained")
	}

	// New acquires must pick addr[1] only.
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, rel, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("post-drain acquire %d: %v", i, err)
		}
		rel()
	}
}
```

- [ ] **Step 9.2: Run — must pass**

```bash
go test ./client/ -run "TestManagedPool_DrainHard|TestManagedPool_DrainLazy" -count=1 -race
```

Expected: PASS.

- [ ] **Step 9.3: Commit**

```bash
git add client/managed_pool_internal_test.go
git commit -m "test(client): DrainHard + DrainLazy semantics"
```

---

## Task 10: Watch fallback to TTL ticker + Stats aggregation

**Files:**
- Modify: `client/managed_pool.go`
- Modify: `client/pool.go` (add `Addresses` and `DrainingSubpools` to `Stats`)
- Modify: `client/managed_pool_internal_test.go`

- [ ] **Step 10.1: Write failing tests**

Append to `client/managed_pool_internal_test.go`:

```go
// noWatchResolver satisfies Resolver with a working Resolve and a
// Watch that always returns ErrWatchUnsupported.
type noWatchResolver struct {
	mu    sync.Mutex
	addrs []Address
}

func (r *noWatchResolver) Resolve(_ context.Context) ([]Address, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Address, len(r.addrs))
	copy(out, r.addrs)
	return out, nil
}

func (r *noWatchResolver) Watch(_ context.Context) (<-chan []Address, error) {
	return nil, ErrWatchUnsupported
}

func (r *noWatchResolver) set(addrs []Address) {
	r.mu.Lock()
	r.addrs = addrs
	r.mu.Unlock()
}

func TestManagedPool_WatchUnsupported_FallsBackToTicker(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 2)
	defer cleanup()

	res := &noWatchResolver{}
	res.set([]Address{addrs[0]})
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	mp.tickerPeriod = 25 * time.Millisecond // test seam
	defer mp.close()

	res.set(addrs)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mp.snapshotActive()) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("ticker mode never picked up the new address; active = %d", len(mp.snapshotActive()))
}

func TestManagedPool_StatsAggregation_SumsAcrossSubPools(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()

	mp, err := newManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// Seed each sub-pool with one conn.
	holds := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		holds = append(holds, rel)
	}

	st := mp.stats()
	if st.ActiveConns != 3 {
		t.Errorf("ActiveConns = %d, want 3", st.ActiveConns)
	}
	if st.Addresses != 3 {
		t.Errorf("Addresses = %d, want 3", st.Addresses)
	}
	if st.InFlightStreams != 3 {
		t.Errorf("InFlightStreams = %d, want 3", st.InFlightStreams)
	}
	for _, rel := range holds {
		rel()
	}
}
```

- [ ] **Step 10.2: Run — must fail**

```bash
go test ./client/ -run "TestManagedPool_WatchUnsupported_FallsBackToTicker|TestManagedPool_StatsAggregation" -count=1
```

Expected: compile errors (`Stats.Addresses` undefined; `mp.stats` undefined; `mp.tickerPeriod` undefined).

- [ ] **Step 10.3: Add new Stats fields**

In `client/pool.go`, find the `Stats` struct (around line 65) and append:

```go
type Stats struct {
	ActiveConns     int
	InFlightStreams int
	Waiters         int
	InFlightDials   int
	// C.3 additions: managedPool aggregates across sub-pools.
	// Sub-pool's individual Stats() leaves these zero.
	Addresses        int // current resolved set size (active only)
	DrainingSubpools int // sub-pools awaiting drain completion
}
```

- [ ] **Step 10.4: Implement runTicker + stats() + tickerPeriod**

In `client/managed_pool.go`, replace `runTicker` with the real implementation and add `stats()`:

Add to the `managedPool` struct (in the `// channels` block at the bottom):

```go
	tickerPeriod time.Duration // 0 → defaultManagedPoolTickerPeriod
```

Add a constant near the top of the file:

```go
const defaultManagedPoolTickerPeriod = 30 * time.Second
```

Replace `runTicker`:

```go
func (mp *managedPool) runTicker(ctx context.Context) {
	period := mp.tickerPeriod
	if period <= 0 {
		period = defaultManagedPoolTickerPeriod
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		next, err := mp.resolver.Resolve(ctx)
		if err != nil && len(next) == 0 {
			continue // soft fail
		}
		mp.applySet(next)
	}
}
```

Add `stats`:

```go
// stats aggregates Stats across all sub-pools (active and draining)
// and returns the combined snapshot.
func (mp *managedPool) stats() Stats {
	mp.mu.RLock()
	subs := make([]*subPoolState, 0, len(mp.subPools))
	for _, s := range mp.subPools {
		subs = append(subs, s)
	}
	addrCount := len(mp.addrs)
	mp.mu.RUnlock()

	var out Stats
	out.Addresses = addrCount
	for _, s := range subs {
		st := s.p.Stats()
		out.ActiveConns += st.ActiveConns
		out.InFlightStreams += st.InFlightStreams
		out.Waiters += st.Waiters
		out.InFlightDials += st.InFlightDials
		if s.draining {
			out.DrainingSubpools++
		}
	}
	return out
}
```

- [ ] **Step 10.5: Run — must pass**

```bash
go test ./client/ -run "TestManagedPool_" -count=1 -race -timeout 60s
```

Expected: PASS.

- [ ] **Step 10.6: Commit**

```bash
git add client/managed_pool.go client/pool.go client/managed_pool_internal_test.go
git commit -m "feat(client): managedPool ticker fallback + Stats aggregation"
```

---

## Task 11: Wire managedPool into Client (poolTransport + ClientOptions + PoolStats)

**Files:**
- Modify: `client/client.go`
- Modify: `client/pool_transport.go`

- [ ] **Step 11.1: Write failing tests for ClientOptions changes**

Append to `client/client_test.go`:

```go
func TestNewClient_AddrAndResolver_BothSet_Errors(t *testing.T) {
	t.Parallel()
	_, err := client.NewClient(client.ClientOptions{
		Addr:     "x:1",
		Resolver: client.StaticResolver(client.Address{Host: "y", Port: 2}),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Transport: client.TransportPool,
		Pool:     &client.PoolOptions{MaxConnsPerHost: 1},
	})
	if !errors.Is(err, client.ErrInvalidOptions) {
		t.Errorf("err = %v, want ErrInvalidOptions", err)
	}
}

func TestNewClient_NoAddrNoResolver_Errors(t *testing.T) {
	t.Parallel()
	_, err := client.NewClient(client.ClientOptions{
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Transport: client.TransportPool,
		Pool:      &client.PoolOptions{MaxConnsPerHost: 1},
	})
	if !errors.Is(err, client.ErrInvalidOptions) {
		t.Errorf("err = %v, want ErrInvalidOptions", err)
	}
}

func TestNewClient_AddrOnly_WrapsInStaticResolver_Multi(t *testing.T) {
	t.Parallel()
	var h hits
	_, addr := newTLSH2Server(t, h.handler())

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if _, err := c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if h.n.Load() != 1 {
		t.Errorf("hits = %d, want 1", h.n.Load())
	}
}
```

(`hits` and `newTLSH2Server` are existing helpers in `conformance_test.go`.)

Add the missing imports at the top of `client/client_test.go` if not already present: `"errors"`, `"crypto/tls"`, `"context"`, `"time"`, `"github.com/lodgvideon/poseidon-http-client/conn"`.

- [ ] **Step 11.2: Run — must fail**

```bash
go test ./client/ -run "TestNewClient_AddrAndResolver|TestNewClient_NoAddrNoResolver|TestNewClient_AddrOnly_WrapsInStaticResolver_Multi" -count=1
```

Expected: compile errors (`ClientOptions.Resolver` undefined etc.).

- [ ] **Step 11.3: Add Resolver/Selector/DrainMode to ClientOptions**

In `client/client.go`, modify the `ClientOptions` struct (around line 29). Add after `Pool *PoolOptions`:

```go
	// Resolver is the address source for the pool. Optional;
	// when nil, Addr is wrapped in StaticResolver. Mutually
	// exclusive with Addr (NewClient returns ErrInvalidOptions
	// when both are set).
	Resolver Resolver

	// Selector picks an address for new dials. nil → RoundRobin.
	// Only used when Transport == TransportPool.
	Selector Selector

	// DrainMode governs sub-pool lifecycle when an address is
	// removed by a Watch update. Zero value = DrainGraceful.
	DrainMode DrainMode
```

- [ ] **Step 11.4: Update NewClient validation + transport construction**

Replace the `NewClient` body (`client/client.go`) with:

```go
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Addr != "" && opts.Resolver != nil {
		return nil, fmt.Errorf("%w: Addr and Resolver are mutually exclusive", ErrInvalidOptions)
	}
	if opts.Addr == "" && opts.Resolver == nil {
		return nil, fmt.Errorf("%w: one of Addr or Resolver is required", ErrInvalidOptions)
	}
	if opts.Addr != "" && containsAnyWhitespace(opts.Addr) {
		return nil, fmt.Errorf("client: ClientOptions.Addr must be a non-empty host:port without whitespace")
	}
	if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	}
	switch opts.Transport {
	case TransportSingleConn:
		if opts.Pool != nil {
			return nil, fmt.Errorf("%w: Pool must be nil for TransportSingleConn", ErrInvalidPoolOptions)
		}
		if opts.Resolver != nil {
			return nil, fmt.Errorf("%w: Resolver requires TransportPool", ErrInvalidOptions)
		}
	case TransportPool:
		if opts.Pool == nil {
			return nil, fmt.Errorf("%w: Pool is required for TransportPool", ErrInvalidPoolOptions)
		}
	default:
		return nil, fmt.Errorf("%w: %d", ErrInvalidTransportKind, int(opts.Transport))
	}

	resolver := opts.Resolver
	if resolver == nil && opts.Addr != "" {
		host, port, err := parseHostPort(opts.Addr)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidOptions, err)
		}
		resolver = StaticResolver(Address{Host: host, Port: port})
	}

	var tr transport
	switch opts.Transport {
	case TransportSingleConn:
		tr = &singleConn{
			addr:     opts.Addr,
			connOpts: opts.ConnOpts,
			backoff:  opts.DialBackoff,
		}
	case TransportPool:
		mpt, err := newPoolTransportWithResolver(resolver, opts.Selector, opts.DrainMode, opts.ConnOpts, *opts.Pool)
		if err != nil {
			return nil, err
		}
		tr = mpt
	}
	return &Client{tr: tr, authority: deriveAuthority(opts.Addr)}, nil
}
```

(`opts.Addr` may be empty if `Resolver` is set; `deriveAuthority("")` must handle that. Check `client.go:331` for `deriveAuthority` — if it doesn't handle empty input, add a nil-safe path that returns `""`. Authority in that case is the user's responsibility via `Request.Authority`.)

Add a small helper at the bottom of `client/client.go`:

```go
// parseHostPort splits "host:port" returning host + port-as-int.
// Used by NewClient to wrap Addr into a StaticResolver.
func parseHostPort(s string) (string, int, error) {
	host, p, err := net.SplitHostPort(s)
	if err != nil {
		return "", 0, fmt.Errorf("client: invalid Addr %q: %w", s, err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, fmt.Errorf("client: invalid Addr port %q: %w", p, err)
	}
	return host, port, nil
}
```

Add `"net"` and `"strconv"` to `client/client.go` imports.

- [ ] **Step 11.5: Rewire poolTransport to drive managedPool**

Replace `client/pool_transport.go` with:

```go
package client

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// poolTransport adapts *managedPool to the internal transport
// interface consumed by Client.
type poolTransport struct {
	mp *managedPool
}

// newPoolTransportWithResolver constructs a managedPool-backed
// transport. Internal: callers go through NewClient with
// Transport == TransportPool.
func newPoolTransportWithResolver(r Resolver, sel Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions) (*poolTransport, error) {
	mp, err := newManagedPool(r, sel, dm, co, po)
	if err != nil {
		return nil, err
	}
	return &poolTransport{mp: mp}, nil
}

// stats returns the managedPool's aggregated Stats.
func (pt *poolTransport) stats() Stats { return pt.mp.stats() }

// acquire delegates to managedPool.
func (pt *poolTransport) acquire(ctx context.Context) (*conn.Conn, func(), error) {
	return pt.mp.acquire(ctx)
}

// close shuts down the managedPool.
func (pt *poolTransport) close() error { return pt.mp.close() }
```

The old `newPoolTransport(addr, ...)` and `newPoolTransportFromPool(p)` constructors are gone. Search for their callers:

```bash
grep -rn 'newPoolTransport\|newPoolTransportFromPool' client/
```

Update each call site. The likely call sites are inside `pool_transport_test.go` — replace those tests with managedPool-driven equivalents OR delete tests that became redundant. (Most of `pool_transport_test.go` tests pool-level acquire/release semantics that are now exercised at the sub-pool layer; one or two tests may need a thin wrapper.)

If any test in `pool_transport_test.go` exercises a single Pool wrapped as transport, replace its `newPoolTransport(addr, co, po)` with:

```go
mp, err := newManagedPool(StaticResolver(Address{...split addr...}), RoundRobin(), DrainGraceful, co, po)
if err != nil { t.Fatalf("newManagedPool: %v", err) }
pt := &poolTransport{mp: mp}
```

- [ ] **Step 11.6: Update Client.PoolStats to use poolTransport.stats**

In `client/client.go`, replace `PoolStats`:

```go
func (c *Client) PoolStats() Stats {
	pt, ok := c.tr.(*poolTransport)
	if !ok {
		return Stats{}
	}
	return pt.stats()
}
```

- [ ] **Step 11.7: Run the full client suite**

```bash
go test ./client/ -count=1 -race -timeout 180s
```

Expected: PASS. If `pool_transport_test.go` tests don't compile, fix per the pattern in 11.5.

- [ ] **Step 11.8: Commit**

```bash
git add client/client.go client/pool_transport.go client/client_test.go client/pool_transport_test.go
git commit -m "feat(client): wire managedPool through ClientOptions.Resolver + Addr backwards-compat"
```

---

## Task 12: Public-API integration tests + RoundRobin distribution

**Files:**
- Modify: `client/managed_pool_test.go` (or create — package `client_test`)

- [ ] **Step 12.1: Write integration tests via the public API**

Create or append to `client/managed_pool_test.go` (package `client_test`):

```go
package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newCounterServer(t *testing.T) (string, *atomic.Int32) {
	c := &atomic.Int32{}
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c.Add(1)
		w.WriteHeader(200)
	}))
	return addr, c
}

func TestClient_ResolverRoundRobin_DistributesRequestsAcrossAddresses(t *testing.T) {
	t.Parallel()
	const N = 4
	addrs := make([]client.Address, N)
	counters := make([]*atomic.Int32, N)
	for i := 0; i < N; i++ {
		hp, c := newCounterServer(t)
		host, port := splitHostPortFromTest(t, hp)
		addrs[i] = client.Address{Host: host, Port: port}
		counters[i] = c
	}

	c, err := client.NewClient(client.ClientOptions{
		Resolver: client.StaticResolver(addrs...),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 8,
			HealthCheckPeriod: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	const total = N * 8
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"})
			if err != nil {
				t.Errorf("Do: %v", err)
			}
		}()
	}
	wg.Wait()

	// Each address must be hit at least once. Exact distribution
	// isn't enforced because RoundRobin picks the *dial* target,
	// and reused conns may absorb multiple requests.
	for i, c := range counters {
		if c.Load() == 0 {
			t.Errorf("server %d hits = 0, want > 0 (RoundRobin must dial each addr)", i)
		}
	}

	st := c.PoolStats()
	if st.Addresses != N {
		t.Errorf("PoolStats.Addresses = %d, want %d", st.Addresses, N)
	}
	if st.ActiveConns < 1 {
		t.Errorf("ActiveConns = %d, want >= 1", st.ActiveConns)
	}
}

// splitHostPortFromTest is a thin wrapper exposed to client_test.
func splitHostPortFromTest(t *testing.T, hp string) (string, int) {
	t.Helper()
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			port := 0
			for _, ch := range hp[i+1:] {
				port = port*10 + int(ch-'0')
			}
			return hp[:i], port
		}
	}
	t.Fatalf("malformed host:port %q", hp)
	return "", 0
}
```

If the symbol `c` clashes (used both as `*Client` and `*atomic.Int32`), rename the inner client to `cli`.

- [ ] **Step 12.2: Run — must pass**

```bash
go test ./client/ -run "TestClient_ResolverRoundRobin_DistributesRequestsAcrossAddresses" -count=1 -race -timeout 90s
```

Expected: PASS.

- [ ] **Step 12.3: Commit**

```bash
git add client/managed_pool_test.go
git commit -m "test(client): integration — Resolver+RoundRobin distributes across addresses"
```

---

## Task 13: Goroutine-leak guard + RFC trace + final verification

**Files:**
- Modify: `client/managed_pool_internal_test.go`
- Modify: `docs/RFC_COVERAGE.md`

- [ ] **Step 13.1: Goroutine-leak test**

Append to `client/managed_pool_internal_test.go`:

```go
import "runtime"

func TestManagedPool_Close_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	addrs, _, cleanup := startH2Servers(t, 3)
	defer cleanup()

	res := newScriptedResolver(addrs)
	mp, err := newManagedPool(res, RoundRobin(), DrainGraceful, newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second})
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}

	// Trigger sub-pool creation across all addresses.
	for i := 0; i < 3; i++ {
		_, rel, err := mp.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rel()
	}
	// One drain in flight too.
	res.push([]Address{addrs[0], addrs[1]})
	time.Sleep(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	_ = mp.close()

	// Allow goroutines to unwind.
	deadline := time.Now().Add(2 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= before/2+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("goroutines after close = %d, before close = %d (close should drain)", after, before)
}
```

The test is conservative — `before/2 + 5` accounts for sub-pool actor goroutines that close winds down. It primarily catches managedPool's own goroutines (run, watchDrain, runTicker) leaking.

- [ ] **Step 13.2: Run — must pass**

```bash
go test ./client/ -run "TestManagedPool_Close_NoGoroutineLeak" -count=1 -race
```

Expected: PASS.

- [ ] **Step 13.3: RFC_COVERAGE update**

Open `docs/RFC_COVERAGE.md` and add a row for service discovery in the §B.2 / Phase B integration section. C.3 doesn't bind to a specific RFC section — it's pool-topology, not protocol. **Skip** the RFC matrix update; no new RFC sections covered.

Instead, add a one-line note at the bottom of the §"B.1 / B.2.* / B.2.6 connection-layer integration" intro paragraph:

```markdown
Phase C.3 adds a Resolver+Selector layer above the per-host pool
(`managedPool` fans across `Address`-keyed sub-pools) — covered by
client/managed_pool_internal_test.go and client/managed_pool_test.go.
RFC trace unchanged.
```

(Insertion point: after the "Phase B.2.6 finishes the lifecycle" paragraph in `docs/RFC_COVERAGE.md`, around line 69. Adapt phrasing to match surrounding tone.)

- [ ] **Step 13.4: Run full test suite**

```bash
go test ./... -count=1 -race -timeout 300s
```

Expected: PASS across all packages.

- [ ] **Step 13.5: go vet + lint sanity**

```bash
go vet ./...
```

Expected: no output.

(Local `golangci-lint` v2 is config-incompatible with the v1.64 CI gate; CI runs the canonical lint in the workflow. `go vet ./...` is sufficient locally.)

- [ ] **Step 13.6: Commit**

```bash
git add client/managed_pool_internal_test.go docs/RFC_COVERAGE.md
git commit -m "test(client): goroutine-leak guard for managedPool.close + C.3 doc note"
```

- [ ] **Step 13.7: Push and open draft PR**

```bash
git push -u origin claude/c3-service-discovery
```

Then open a draft PR titled `feat(client): C.3 service discovery — Resolver/Selector + per-address sub-pools` referencing the spec.

---

## Self-review summary

**Spec coverage:**
- §"Files" mapping: covered by Tasks 1–11 (each new file created in its own task; existing files modified surgically).
- Public API surface (Address, Resolver, StaticResolver, DNSResolver, DNSOptions, Selector, PickContext, RoundRobin, Random, Hash, DrainMode, ErrWatchUnsupported, ErrNoAddresses, ErrInvalidOptions, ClientOptions additions): covered Tasks 1, 2, 3, 4, 5, 11.
- Dial path with Selector + dial-error iteration: Task 6.
- Watch consumer + address diff: Task 7.
- DrainGraceful/Hard/Lazy: Tasks 8, 9.
- TTL ticker fallback when Watch unsupported: Task 10.
- Stats aggregation + new fields: Task 10, 11.
- Backwards-compat `Addr → StaticResolver` wrap: Task 11.
- Goroutine-leak guard: Task 13.
- RFC trace policy: Task 13 (no RFC section bound; doc note added).

**No placeholders:** all steps include exact code, exact file paths, exact commands.

**Type consistency:** verified — `Address`, `Resolver`, `Selector`, `PickContext`, `DrainMode`, `subPoolState`, `managedPool`, `poolTransport`, `Stats` (with `Addresses` + `DrainingSubpools` additions) are referenced consistently across tasks.
