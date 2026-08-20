package conn

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versionProbeServer starts a TLS listener that records the protocol versions
// each ClientHello offers and then refuses the handshake.
//
// GetConfigForClient runs after the ClientHello has been parsed and before the
// server needs a certificate, and ClientHelloInfo.SupportedVersions is exactly
// the set of versions the client offered — so a dialer's floor is observable
// with no certificate, no successful handshake, and no dependence on whether
// the Go TLS stack would still agree to complete an obsolete handshake.
func versionProbeServer(t *testing.T) (offered func() []uint16, addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "probe listen")
	t.Cleanup(func() { _ = ln.Close() })

	var mu sync.Mutex
	var seen []uint16
	cfg := &tls.Config{
		// Deliberately permissive: whether this server would AGREE to an
		// obsolete version is not the question, and pinning it at 1.2 would make
		// the probe depend on crypto/tls running GetConfigForClient before
		// version negotiation rather than after.
		MinVersion: tls.VersionTLS10,
		GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			mu.Lock()
			seen = append(seen[:0], hi.SupportedVersions...)
			mu.Unlock()
			return nil, errors.New("probe: refusing once the offer is recorded")
		},
	}
	go func() {
		for {
			raw, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			tc := tls.Server(raw, cfg)
			_ = tc.Handshake()
			_ = tc.Close()
		}
	}()
	return func() []uint16 {
		mu.Lock()
		defer mu.Unlock()
		return append([]uint16(nil), seen...)
	}, ln.Addr().String()
}

// TestConformance_RFC9113_Sec9_2_TLS12FloorInDialers pins the §9.2 floor on the
// two dialers that build their tls.Config inline rather than through
// TLSDialer.tlsClientConfig: "Implementations of HTTP/2 MUST use TLS version
// 1.2 [TLS12] or higher for HTTP/2 over TLS."
//
// Both set MinVersion only when the caller left it at zero, and both already had
// a test that reached the branch — each says so in a comment, "exercise the
// MinVersion default branch" — and then asserted only the negotiated ALPN token.
// The httptest servers they dial negotiate TLS 1.3 whatever the floor is, so the
// floor was executed and never observed: lowering it to TLS 1.0 left the whole
// package green (#810). It is the only thing standing between a caller who
// passes a bare &tls.Config{} and a TLS 1.0 session, and gosec's G402 is tuned
// off here precisely because TLS config is the caller's to choose.
func TestConformance_RFC9113_Sec9_2_TLS12FloorInDialers(t *testing.T) {
	for _, tc := range []struct {
		name string
		dial func(ctx context.Context, addr string) (net.Conn, error)
	}{
		{"H1TLSDialer with no Config at all", (&H1TLSDialer{}).Dial},
		{"H1TLSDialer with MinVersion unset", (&H1TLSDialer{Config: &tls.Config{}}).Dial},
		{"FlexDialer with no Config at all", (&FlexDialer{}).Dial},
		{"FlexDialer with MinVersion unset", (&FlexDialer{Config: &tls.Config{}}).Dial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			offered, addr := versionProbeServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, _ = tc.dial(ctx, addr)

			got := offered()
			require.NotEmptyf(t, got,
				"the probe server never saw a ClientHello, so this test observed nothing "+
					"about the floor — the dial failed before the handshake")
			for _, v := range got {
				assert.GreaterOrEqualf(t, v, uint16(tls.VersionTLS12),
					"the dialer offered TLS 0x%04x (whole offer %#04x); RFC 9113 §9.2 requires "+
						"TLS 1.2 or higher for HTTP/2 over TLS, and a caller who hands this "+
						"dialer a bare &tls.Config{} has nothing else stopping a 1.0 session",
					v, got)
			}
		})
	}
}
