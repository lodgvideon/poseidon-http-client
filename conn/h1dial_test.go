package conn

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// startDualProtoServer starts a TLS server that offers BOTH "h2" and
// "http/1.1" over ALPN — the ordinary HTTPS deployment, and the shape that
// makes a silently-rewritten ALPN offer negotiate h2. Returns the address and
// the client TLS config trusting its certificate.
func startDualProtoServer(t *testing.T) (addr string, clientCfg *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-proto", r.Proto)
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	// httptest offers ONLY "h2" when EnableHTTP2 is set; pin both tokens so the
	// server behaves like a real dual-protocol HTTPS origin, where the ALPN offer
	// the client makes is what decides the protocol.
	srv.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.NextProtos = nil // let each dialer set its own offer
	return srv.Listener.Addr().String(), cfg
}

// TestH1TLSDialer_NegotiatesHTTP11 is the regression test for the bug: against a
// server offering h2 AND http/1.1, H1TLSDialer must come back with http/1.1.
func TestH1TLSDialer_NegotiatesHTTP11(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc, err := (&H1TLSDialer{Config: cfg}).Dial(ctx, addr)
	if err != nil {
		t.Fatalf("H1TLSDialer.Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()

	if got := NegotiatedProtocol(nc); got != "http/1.1" {
		t.Fatalf("NegotiatedProtocol = %q, want %q", got, "http/1.1")
	}
}

// TestH1TLSDialer_KeepsExplicitNextProtos verifies a caller-supplied
// NextProtos that already pins http/1.1 is used as-is.
func TestH1TLSDialer_KeepsExplicitNextProtos(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoServer(t)
	cfg.NextProtos = []string{"http/1.1"}
	cfg.MinVersion = 0 // exercise the MinVersion default branch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc, err := (&H1TLSDialer{Config: cfg}).Dial(ctx, addr)
	if err != nil {
		t.Fatalf("H1TLSDialer.Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()
	if got := NegotiatedProtocol(nc); got != "http/1.1" {
		t.Fatalf("NegotiatedProtocol = %q, want %q", got, "http/1.1")
	}
}

// TestH1TLSDialer_RejectsH2InConfig verifies the dialer refuses a config whose
// NextProtos contradicts its assertion, before any network I/O.
func TestH1TLSDialer_RejectsH2InConfig(t *testing.T) {
	t.Parallel()
	d := &H1TLSDialer{Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}}
	// 203.0.113.0/24 is TEST-NET-3: a dial that reached the network would hang,
	// so returning promptly is itself part of the assertion.
	_, err := d.Dial(context.Background(), "203.0.113.1:443")
	if !errors.Is(err, ErrALPNConflict) {
		t.Fatalf("Dial error = %v, want ErrALPNConflict", err)
	}
}

// TestH1TLSDialer_NilConfig verifies the nil-Config path offers http/1.1 and
// applies the TLS 1.2 floor. The handshake fails on certificate verification
// (httptest's cert is not in the system roots) — which is proof enough that the
// nil branch built a usable config and dialed.
func TestH1TLSDialer_NilConfig(t *testing.T) {
	t.Parallel()
	addr, _ := startDualProtoServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc, err := (&H1TLSDialer{}).Dial(ctx, addr)
	if err == nil {
		_ = nc.Close()
		t.Fatal("Dial succeeded against an untrusted cert, want verification failure")
	}
	if errors.Is(err, ErrALPNConflict) || errors.Is(err, ErrALPNNotHTTP11) {
		t.Fatalf("Dial error = %v, want a certificate error", err)
	}
}

// TestH1TLSDialer_RejectsNonHTTP11Peer verifies a peer that selects a protocol
// other than http/1.1 is refused with ErrALPNNotHTTP11 rather than handed back.
// ALPN selection follows the SERVER's preference order, so a server preferring
// a non-HTTP token picks it even though the client listed http/1.1 first.
func TestH1TLSDialer_RejectsNonHTTP11Peer(t *testing.T) {
	t.Parallel()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.TLS = &tls.Config{NextProtos: []string{"spdy/3", "http/1.1"}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.NextProtos = []string{"http/1.1", "spdy/3"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc, err := (&H1TLSDialer{Config: cfg}).Dial(ctx, srv.Listener.Addr().String())
	if err == nil {
		proto := NegotiatedProtocol(nc)
		_ = nc.Close()
		t.Fatalf("Dial succeeded with negotiated protocol %q, want ErrALPNNotHTTP11", proto)
	}
	if !errors.Is(err, ErrALPNNotHTTP11) {
		t.Fatalf("Dial error = %v, want ErrALPNNotHTTP11", err)
	}
}

// TestTLSDialer_RejectsALPNWithoutH2 is the other half of the fix: TLSDialer
// must refuse an explicit NextProtos that excludes h2 instead of silently
// prepending h2 and asserting the server picked it.
func TestTLSDialer_RejectsALPNWithoutH2(t *testing.T) {
	t.Parallel()
	d := &TLSDialer{Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"http/1.1"},
	}}
	_, err := d.Dial(context.Background(), "203.0.113.1:443")
	if !errors.Is(err, ErrALPNConflict) {
		t.Fatalf("Dial error = %v, want ErrALPNConflict", err)
	}
}

// TestTLSDialer_AllowsALPNListContainingH2 verifies the guard only fires on a
// list that excludes h2 — a caller listing h2 alongside other tokens is fine.
func TestTLSDialer_AllowsALPNListContainingH2(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoServer(t)
	cfg.NextProtos = []string{"http/1.1", "h2"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc, err := (&TLSDialer{Config: cfg}).Dial(ctx, addr)
	if err != nil {
		t.Fatalf("TLSDialer.Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()
	if got := NegotiatedProtocol(nc); got != "h2" {
		t.Fatalf("NegotiatedProtocol = %q, want %q", got, "h2")
	}
}

// TestProxyTLSDialer_RejectsALPNWithoutH2 verifies the proxy dialer carries the
// same guard — it too asserts h2 after the CONNECT tunnel.
func TestProxyTLSDialer_RejectsALPNWithoutH2(t *testing.T) {
	t.Parallel()
	u, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	d := &ProxyTLSDialer{
		ProxyURL:  u,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
	}
	_, err = d.Dial(context.Background(), "example.com:443")
	if !errors.Is(err, ErrALPNConflict) {
		t.Fatalf("Dial error = %v, want ErrALPNConflict", err)
	}
}

// TestAssertsALPN_Values pins the ALPNAsserter answers the client's
// construction-time pairing check depends on.
func TestAssertsALPN_Values(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		d    Dialer
		want string
	}{
		{"TLSDialer", &TLSDialer{}, "h2"},
		{"H1TLSDialer", &H1TLSDialer{}, "http/1.1"},
		{"FlexDialer", &FlexDialer{}, ""},
		{"ProxyTLSDialer", &ProxyTLSDialer{}, "h2"},
	}
	for _, tc := range cases {
		a, ok := tc.d.(ALPNAsserter)
		if !ok {
			t.Fatalf("%s does not implement ALPNAsserter", tc.name)
		}
		if got := a.AssertsALPN(); got != tc.want {
			t.Errorf("%s.AssertsALPN() = %q, want %q", tc.name, got, tc.want)
		}
	}
	// PlaintextDialer deliberately makes no assertion at all.
	if _, ok := Dialer(&PlaintextDialer{}).(ALPNAsserter); ok {
		t.Error("PlaintextDialer implements ALPNAsserter; it should assert nothing")
	}
}
