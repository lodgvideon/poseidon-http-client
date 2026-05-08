package client

import (
	"context"
	"testing"
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
