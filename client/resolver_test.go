package client

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

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
	if len(got) != 2 {
		t.Fatalf("Resolve len = %d, want 2", len(got))
	}
	if got[0].Host != addrs[0].Host || got[0].Port != addrs[0].Port {
		t.Errorf("Resolve[0] = {%s, %d}, want {%s, %d}", got[0].Host, got[0].Port, addrs[0].Host, addrs[0].Port)
	}
	if got[1].Host != addrs[1].Host || got[1].Port != addrs[1].Port {
		t.Errorf("Resolve[1] = {%s, %d}, want {%s, %d}", got[1].Host, got[1].Port, addrs[1].Host, addrs[1].Port)
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
	if len(first) != 1 {
		t.Fatalf("Watch initial len = %d, want 1", len(first))
	}
	if first[0].Host != addrs[0].Host || first[0].Port != addrs[0].Port {
		t.Errorf("Watch initial[0] = {%s, %d}, want {%s, %d}", first[0].Host, first[0].Port, addrs[0].Host, addrs[0].Port)
	}
	if _, ok := <-ch; ok {
		t.Error("Watch channel should be closed after initial set; got another value")
	}
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
	// Channel must close after ctx cancel.
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
	_ = mustReceiveSet(t, ch, time.Second) // consume initial
	select {
	case got := <-ch:
		t.Errorf("unexpected second emit on unchanged set: %v", got)
	case <-time.After(80 * time.Millisecond): // > 3 ticks at 25ms
	}
}
