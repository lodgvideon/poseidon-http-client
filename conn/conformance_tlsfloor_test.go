package conn

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
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

			assert.Equalf(t, tc.want, got.MinVersion,
				"MinVersion = 0x%04x, want 0x%04x (RFC 9113 §9.2 TLS 1.2 floor)", got.MinVersion, tc.want)
			if tc.in != nil {
				assert.Equal(t, origMin, tc.in.MinVersion,
					"tlsClientConfig mutated the caller's tls.Config MinVersion")
			}
			assert.Containsf(t, got.NextProtos, "h2", "NextProtos = %v, missing h2", got.NextProtos)
		})
	}
}
