package client

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"
)

// Address is one resolved backend endpoint.
type Address struct {
	// Host is the dial target — IP literal or DNS name. The pool
	// never re-resolves Host; the Resolver owns that.
	Host string
	Port int
	// Attributes carries optional metadata for user Selectors
	// (zone, weight, etc.). Built-in selectors ignore it.
	//
	// Note: Attributes is not comparable (contains a map); Address
	// values with non-nil Attributes cannot be used as Go map keys
	// or compared with ==.
	Attributes map[string]string
}

// String returns "host:port" using net.JoinHostPort (adds brackets
// around IPv6 literals automatically).
func (a Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}

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

	// now is overridable for tests so the TTL boundary can be
	// pinned without relying on the host's time.Now() resolution
	// (which is not guaranteed to advance monotonically by even
	// one nanosecond between two calls in the same goroutine —
	// see https://github.com/golang/go/issues/50929).
	now func() time.Time
}

// DNSResolver constructs a DNS-backed Resolver for the given host:port.
func DNSResolver(host string, port int, opts DNSOptions) Resolver {
	var rsv dnsLookup = net.DefaultResolver
	if opts.Resolver != nil {
		rsv = opts.Resolver
	}
	return newDNSResolverWithLookup(host, port, opts, rsv)
}

// newDNSResolverWithLookup is the internal constructor that accepts an
// explicit dnsLookup seam (for tests). TTL default is applied here.
func newDNSResolverWithLookup(host string, port int, opts DNSOptions, rsv dnsLookup) *dnsResolver {
	if opts.TTL <= 0 {
		opts.TTL = 30 * time.Second
	}
	return &dnsResolver{
		host: host, port: port, opts: opts, rsv: rsv,
		now: time.Now,
	}
}

// setNow overrides the clock used by the cache-TTL check. Tests use
// this to pin the TTL boundary without relying on host time.Now()
// resolution; nil restores time.Now. Not safe to call concurrently
// with Resolve — tests must serialize.
func (r *dnsResolver) setNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	r.now = now
}

// Resolve returns the cached address set if within TTL. Otherwise
// refreshes via dnsLookup. On lookup failure with a non-empty cache,
// returns (cache, error) — the cache wins, the error is a soft warning.
func (r *dnsResolver) Resolve(ctx context.Context) ([]Address, error) {
	now := r.now()
	r.rmu.Lock()
	defer r.rmu.Unlock()
	if r.cached != nil && now.Sub(r.cachedAt) < r.opts.TTL {
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
		// A SUCCESSFUL lookup returning zero addresses is authoritative: every
		// endpoint was deregistered (or all were filtered out). Clear the cache
		// so the now-empty result propagates and dead backends are drained —
		// unlike a lookup ERROR (handled above), which keeps the stale set as a
		// soft warning. Serving the stale set here would route to dead backends
		// forever with no recovery path. (cachedAt is irrelevant while cached
		// is nil: the TTL check short-circuits and the next Resolve re-looks-up.)
		r.cached = nil
		return nil, ErrNoAddresses
	}
	r.cached = addrs
	r.cachedAt = r.now()
	return addrs, nil
}

// Watch emits the initial address set immediately, then re-resolves
// every TTL. Emits only when the set changes (order-sensitive string
// comparison). The channel is closed when ctx is cancelled.
func (r *dnsResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	out := make(chan []Address, 1)
	first, err := r.Resolve(ctx)
	// Only a transient lookup failure aborts the watch; an authoritative-empty
	// first result (ErrNoAddresses) emits the empty set and keeps Watch mode,
	// consistent with the re-resolve loop below.
	if err != nil && !errors.Is(err, ErrNoAddresses) {
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
			if err != nil && !errors.Is(err, ErrNoAddresses) {
				continue // transient soft fail — retain prev set
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
// values in the same order. Compares Host and Port only; DNSResolver
// never sets Attributes, so ignoring maps is safe.
func addrSetEqual(a, b []Address) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Host != b[i].Host || a[i].Port != b[i].Port {
			return false
		}
	}
	return true
}
