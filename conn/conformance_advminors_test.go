package conn

import (
	"sync"
	"testing"
)

// Regression guards for two adversarial-round minors on the push accept path.

// TestIsKnownOrigin_StripsScheme pins that the ORIGIN authoritative check
// compares a promised :authority (no scheme) against the host[:port] of each
// ORIGIN entry (RFC 6454 serialized origin, "scheme://host[:port]"). A regression
// compared the whole serialized origin, so the ORIGIN allowance never matched.
func TestIsKnownOrigin_StripsScheme(t *testing.T) {
	c := &Conn{origins: []string{"https://example.com", "https://cdn.example.com:8443"}}
	for _, tc := range []struct {
		auth string
		want bool
	}{
		{"example.com", true},
		{"cdn.example.com:8443", true},
		{"evil.com", false},
		{"https://example.com", false}, // a :authority never carries a scheme
	} {
		if got := c.isKnownOrigin([]byte(tc.auth)); got != tc.want {
			t.Errorf("isKnownOrigin(%q) = %v, want %v", tc.auth, got, tc.want)
		}
	}
}

// TestRecycleStream_ResetsReqAuthority pins the pooled-stream-reset invariant for
// the per-request reqAuthority field: a recycled Stream must not carry a previous
// request's :authority into the next request on a reused connection.
func TestRecycleStream_ResetsReqAuthority(t *testing.T) {
	var pool sync.Pool
	s := newStream(1, 8, nil, 65535)
	s.reqAuthority = "example.com"
	s.localEnded, s.remoteEnded = true, true
	recycleStream(&pool, s)
	// recycleStream clears the fields in place before pooling s, so assert on s
	// directly. Going back through pool.Get() is flaky: sync.Pool may return nil
	// (the GC can drop a pooled item), which is not what this test is checking.
	if s.reqAuthority != "" {
		t.Errorf("recycled Stream.reqAuthority = %q, want empty", s.reqAuthority)
	}
}
