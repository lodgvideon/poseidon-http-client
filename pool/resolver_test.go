package pool

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddress_String_HostPort(t *testing.T) {
	t.Parallel()

	got := Address{Host: "example.com", Port: 443}.String()

	assert.Equalf(t, "example.com:443", got, "Address.String() = %q, want %q", got, "example.com:443")
}

func TestAddress_String_IPv6Brackets(t *testing.T) {
	t.Parallel()

	got := Address{Host: "::1", Port: 8443}.String()

	assert.Equalf(t, "[::1]:8443", got,
		"Address.String() = %q, want %q — an unbracketed IPv6 literal is not a dialable "+
			"host:port and net.Dial rejects it", got, "[::1]:8443")
}

func TestStaticResolver_Resolve_ReturnsFixedSet(t *testing.T) {
	t.Parallel()
	addrs := []Address{
		{Host: "a", Port: 1},
		{Host: "b", Port: 2},
	}
	r := StaticResolver(addrs...)

	got, err := r.Resolve(context.Background())

	require.NoError(t, err, "Resolve on a static set must not fail")
	require.Lenf(t, got, 2, "Resolve len = %d, want 2", len(got))
	assert.Equal(t, addrs[0], got[0], "Resolve[0] = %+v, want %+v", got[0], addrs[0])
	assert.Equalf(t, addrs[1], got[1],
		"Resolve[1] = %+v, want %+v — order is load-bearing: selectors index into this slice",
		got[1], addrs[1])
}

func TestStaticResolver_Watch_SendsThenCloses(t *testing.T) {
	t.Parallel()
	addrs := []Address{{Host: "a", Port: 1}}
	r := StaticResolver(addrs...)

	ch, err := r.Watch(context.Background())

	require.NoError(t, err, "Watch on a static set must not fail")
	first, ok := <-ch
	require.True(t, ok, "Watch channel closed before sending the initial set")
	require.Lenf(t, first, 1, "Watch initial len = %d, want 1", len(first))
	assert.Equal(t, addrs[0], first[0], "Watch initial[0] = %+v, want %+v", first[0], addrs[0])
	_, ok = <-ch
	assert.Falsef(t, ok,
		"Watch channel stayed open after the initial set; a static set never changes, so a "+
			"consumer ranging over it would block forever")
}

// fakeLookup implements dnsLookup for tests.
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

	require.NoError(t, err, "Resolve with a healthy lookup")
	require.Lenf(t, got, 2, "Resolve = %v, want two addresses", got)
	assert.Equal(t, "10.0.0.1", got[0].Host, "Resolve[0].Host = %q", got[0].Host)
	assert.Equal(t, "10.0.0.2", got[1].Host, "Resolve[1].Host = %q", got[1].Host)
	assert.Equalf(t, 8080, got[0].Port,
		"Port = %d, want 8080 — the configured port must be carried onto every resolved IP",
		got[0].Port)
}

func TestDNSResolver_Resolve_TTLCache(t *testing.T) {
	t.Parallel()
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Hour}, fl)

	_, err1 := r.Resolve(context.Background())
	_, err2 := r.Resolve(context.Background())

	require.NoError(t, err1, "first Resolve")
	require.NoError(t, err2, "second Resolve")
	assert.Equalf(t, int32(1), fl.calls.Load(),
		"LookupIPAddr calls = %d, want 1 — the second call must hit the cache, or every "+
			"request pays a DNS round trip", fl.calls.Load())
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
	// Pin the clock so the TTL boundary is deterministic on every host. Without
	// this, time.Now() resolution varies by OS and kernel and the second Resolve
	// may incorrectly hit the cache (see golang/go#50929). The fake clock
	// advances by 2 ns per call, exceeding the 1 ns TTL.
	tick := 0
	clock := time.Unix(1_700_000_000, 0)
	r.setNow(func() time.Time {
		tick++
		return clock.Add(time.Duration(tick) * 2 * time.Nanosecond)
	})
	_, err := r.Resolve(context.Background())
	require.NoError(t, err, "first Resolve must succeed to populate the cache")

	// Second call: cache is now stale (clock advanced past the 1 ns TTL), the
	// lookup fails, but the cache is non-empty so we get (cache, err).
	got, err := r.Resolve(context.Background())

	assert.Errorf(t, err,
		"second Resolve err = nil, want a soft warning — a caller that logs on error would "+
			"never learn its DNS is down while it serves a stale set")
	require.Lenf(t, got, 1, "Resolve = %v, want the cached [10.0.0.1:80]", got)
	assert.Equalf(t, "10.0.0.1", got[0].Host,
		"Resolve = %v, want the cached set: a lookup ERROR must not drain live backends", got)
}

// TestDNSResolver_Resolve_StaleOnError_AlsoPassesWithFreshCache confirms
// the negative branch: when the cache is still fresh, lookup is NOT
// called and no error is returned. The test pins the clock so TTL
// never elapses, even across two Resolve calls.
func TestDNSResolver_Resolve_StaleOnError_AlsoPassesWithFreshCache(t *testing.T) {
	t.Parallel()
	var attempt atomic.Int32
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		attempt.Add(1)
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Hour}, fl)
	// Frozen clock: TTL never elapses.
	clock := time.Unix(1_700_000_000, 0)
	r.setNow(func() time.Time { return clock })
	_, err := r.Resolve(context.Background())
	require.NoError(t, err, "first Resolve must succeed to populate the cache")

	got, err := r.Resolve(context.Background())

	require.NoError(t, err, "second Resolve err = %v, want nil (cache hit, no lookup)", err)
	require.Lenf(t, got, 1, "Resolve = %v, want [10.0.0.1:80]", got)
	assert.Equal(t, "10.0.0.1", got[0].Host, "Resolve = %v, want [10.0.0.1:80]", got)
	assert.Equalf(t, int32(1), attempt.Load(),
		"LookupIPAddr calls = %d, want 1 (the second call must hit the cache)", attempt.Load())
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

	require.NoError(t, err, "Resolve with PreferIPv4 against a dual-stack answer")
	require.Lenf(t, got, 1,
		"Resolve = %v, want [10.0.0.1:80] only — the AAAA answer survived PreferIPv4, so a "+
			"v4-only host would be handed an unreachable address", got)
	assert.Equal(t, "10.0.0.1", got[0].Host, "Resolve = %v, want [10.0.0.1:80] only", got)
}

// mustReceiveSet reads one address-set from ch within d, fatally
// failing if nothing arrives.
func mustReceiveSet(t *testing.T, ch <-chan []Address, d time.Duration) []Address {
	t.Helper()
	select {
	case s, ok := <-ch:
		require.True(t, ok, "Watch channel closed before sending set")
		return s
	case <-time.After(d):
		require.Fail(t, "Watch did not emit within timeout", "waited %v", d)
		return nil
	}
}

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

	require.NoError(t, err, "Watch on a DNS resolver")
	first := mustReceiveSet(t, ch, time.Second)
	require.Lenf(t, first, 1, "first set = %v, want [10.0.0.1:80]", first)
	assert.Equal(t, "10.0.0.1", first[0].Host, "first set = %v, want [10.0.0.1:80]", first)
	phase.Store(1)
	second := mustReceiveSet(t, ch, time.Second)
	require.Lenf(t, second, 2,
		"second set = %v, want both addresses — a set that grew between lookups must reach "+
			"the watcher or new backends never take traffic", second)
	assert.Equal(t, "10.0.0.2", second[1].Host, "second set = %v, want [10.0.0.1:80 10.0.0.2:80]", second)
	cancel()
	// Channel must close after ctx cancel.
	closed := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := <-ch; !ok {
			closed = true
			break
		}
	}
	assert.True(t, closed,
		"Watch channel did not close after ctx cancel; the watch goroutine outlives its context")
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
	require.NoError(t, err, "Watch on a DNS resolver")
	_ = mustReceiveSet(t, ch, time.Second) // consume initial

	var extra []Address
	var gotExtra bool
	select {
	case extra, gotExtra = <-ch:
	case <-time.After(80 * time.Millisecond): // > 3 ticks at 25ms
	}

	assert.Falsef(t, gotExtra,
		"unexpected second emit on an unchanged set: %v — every consumer would rebuild its "+
			"connection set on each tick", extra)
}

// TestResolve_SuccessfulEmpty_DoesNotServeStale is a regression test for
// stale-masking: a SUCCESSFUL DNS lookup returning zero addresses used to be
// reported as the stale cached set, so a service scaled to zero kept receiving
// traffic to dead backends forever. The authoritative empty result must now
// propagate (cache cleared, ErrNoAddresses), not the stale set.
func TestResolve_SuccessfulEmpty_DoesNotServeStale(t *testing.T) {
	var attempt atomic.Int32
	fl := &fakeLookup{fn: func(_ string) ([]net.IPAddr, error) {
		if attempt.Add(1) == 1 {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		}
		return []net.IPAddr{}, nil // success, but empty
	}}
	r := newDNSResolverWithLookup("svc.local", 80, DNSOptions{TTL: time.Nanosecond}, fl)
	tick := 0
	clock := time.Unix(1_700_000_000, 0)
	r.setNow(func() time.Time {
		tick++
		return clock.Add(time.Duration(tick) * 2 * time.Nanosecond)
	})
	first, err := r.Resolve(context.Background())
	require.NoError(t, err, "first Resolve must succeed to populate the cache")
	require.Lenf(t, first, 1, "first Resolve = %v, want one address", first)

	got, err := r.Resolve(context.Background())

	assert.Emptyf(t, got,
		"second Resolve returned the stale set %v; a service scaled to zero would keep "+
			"receiving traffic to dead backends forever", got)
	assert.ErrorIsf(t, err, ErrNoAddresses,
		"second Resolve err = %v, want ErrNoAddresses — an authoritative empty answer is "+
			"different from a lookup FAILURE, which does keep the stale set", err)
}
