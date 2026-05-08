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
