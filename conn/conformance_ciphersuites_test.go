package conn

import (
	"crypto/tls"
	"testing"
)

// allowedH2TLS12 is the exact set of TLS 1.2 cipher suites an h2-only dialer may
// offer: the six forward-secret AEAD suites RFC 9113 Appendix A leaves
// unprohibited. Any suite outside this set on the h2-only default path is a
// §9.2.2 SHOULD-NOT violation.
var allowedH2TLS12 = map[uint16]bool{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:       true,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:         true,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:       true,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:         true,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: true,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:   true,
}

// TestConformance_RFC9113_Sec9_2_2_TLS12CipherSuitesProhibited pins RFC 9113
// §9.2.2: "A deployment of HTTP/2 over TLS 1.2 SHOULD NOT use any of the
// prohibited cipher suites listed in Appendix A." The h2-only TLSDialer (no
// http/1.1 fallback, so §9.2.2's advertise-for-fallback note does not apply)
// pins its TLS 1.2 offer to the AEAD allowlist, which offers none of the
// Appendix A suites and includes the mandated
// TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.
func TestConformance_RFC9113_Sec9_2_2_TLS12CipherSuitesProhibited(t *testing.T) {
	// A sample of Appendix A prohibited suites: the TLS 1.2 mandatory suite,
	// a non-forward-secret GCM suite, and two CBC suites.
	prohibited := []uint16{
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	}
	for _, tc := range []struct {
		name string
		in   *tls.Config
	}{
		{"nil config", nil},
		{"config without CipherSuites", &tls.Config{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &TLSDialer{Config: tc.in}
			got := d.tlsClientConfig()
			if len(got.CipherSuites) == 0 {
				t.Fatal("CipherSuites left unset; an h2-only dialer must pin the AEAD allowlist (§9.2.2)")
			}
			mandatoryPresent := false
			for _, cs := range got.CipherSuites {
				if !allowedH2TLS12[cs] {
					t.Errorf("offered prohibited/unknown TLS 1.2 suite 0x%04x (§9.2.2 Appendix A)", cs)
				}
				if cs == tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 {
					mandatoryPresent = true
				}
			}
			if !mandatoryPresent {
				t.Error("missing the §9.2.2-mandated TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
			}
			for _, p := range prohibited {
				for _, cs := range got.CipherSuites {
					if cs == p {
						t.Errorf("prohibited suite 0x%04x is offered (§9.2.2 Appendix A)", p)
					}
				}
			}
		})
	}
}

// TestConformance_RFC9113_Sec9_2_2_ExplicitCipherSuitesRespected is the
// over-rejection guard: §9.2.2 is a deployment SHOULD-NOT, not a library
// mandate, so a caller that pins its own CipherSuites (even a prohibited one) is
// honored, not silently overridden.
func TestConformance_RFC9113_Sec9_2_2_ExplicitCipherSuitesRespected(t *testing.T) {
	caller := []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA}
	d := &TLSDialer{Config: &tls.Config{CipherSuites: caller}} //nolint:gosec // caller's explicit choice is respected by design
	got := d.tlsClientConfig()
	if len(got.CipherSuites) != 1 || got.CipherSuites[0] != tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA {
		t.Errorf("caller CipherSuites overridden: got %#x, want the caller's explicit [0xc013]", got.CipherSuites)
	}
}

// TestH2TLS12CipherSuites_FreshSlicePerCall guards the aliasing note in the
// helper: two default configs must not share a backing array, so mutating one
// cannot corrupt the other.
func TestH2TLS12CipherSuites_FreshSlicePerCall(t *testing.T) {
	a := h2TLS12CipherSuites()
	b := h2TLS12CipherSuites()
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("empty cipher list")
	}
	a[0] = 0xdead
	if b[0] == 0xdead {
		t.Error("h2TLS12CipherSuites returned an aliased backing array; mutating one call corrupted another")
	}
}
