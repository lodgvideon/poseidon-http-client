package conn

import (
	"crypto/tls"
	"testing"
)

// TestConformance_RFC9113_Sec9_2_TLS12Floor pins RFC 9113 §9.2: "Implementations
// of HTTP/2 MUST use TLS version 1.2 ... or higher for HTTP/2 over TLS". The dialer
// raises MinVersion to TLS 1.2 even when a caller pins an explicit lower value,
// and never lowers a higher one, without mutating the caller's config.
func TestConformance_RFC9113_Sec9_2_TLS12Floor(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *tls.Config
		want uint16
	}{
		{"nil config", nil, tls.VersionTLS12},
		{"unset MinVersion", &tls.Config{}, tls.VersionTLS12},
		{"explicit TLS 1.0", &tls.Config{MinVersion: tls.VersionTLS10}, tls.VersionTLS12},
		{"explicit TLS 1.1", &tls.Config{MinVersion: tls.VersionTLS11}, tls.VersionTLS12},
		{"explicit TLS 1.2", &tls.Config{MinVersion: tls.VersionTLS12}, tls.VersionTLS12},
		{"explicit TLS 1.3 kept", &tls.Config{MinVersion: tls.VersionTLS13}, tls.VersionTLS13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var origMin uint16
			if tc.in != nil {
				origMin = tc.in.MinVersion
			}
			d := &TLSDialer{Config: tc.in}
			got := d.tlsClientConfig()
			if got.MinVersion != tc.want {
				t.Errorf("MinVersion = 0x%04x, want 0x%04x (RFC 9113 §9.2 TLS 1.2 floor)", got.MinVersion, tc.want)
			}
			if tc.in != nil && tc.in.MinVersion != origMin {
				t.Error("tlsClientConfig mutated the caller's tls.Config MinVersion")
			}
			h2 := false
			for _, p := range got.NextProtos {
				if p == "h2" {
					h2 = true
				}
			}
			if !h2 {
				t.Errorf("NextProtos = %v, missing h2", got.NextProtos)
			}
		})
	}
}
