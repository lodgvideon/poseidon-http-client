package quic

import (
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9001_Sec48_TLSAlertToCryptoError checks that a TLS alert maps
// to a CRYPTO_ERROR code of 0x0100 plus the alert description, found even through
// a wrapping error (RFC 9001 §4.8).
func TestConformance_RFC9001_Sec48_TLSAlertToCryptoError(t *testing.T) {
	wrapped := fmt.Errorf("tls handshake: %w", tls.AlertError(48)) // certificate_unknown

	code, ok := closeCodeFor(tls.AlertError(42)) // bad_certificate
	wrappedCode, wrappedOK := closeCodeFor(wrapped)

	require.True(t, ok, "a TLS alert must map to a connection error to signal")
	assert.Equalf(t, ErrCodeCryptoBase+42, code,
		"code = %#x, want %#x (0x0100 + alert 42)", code, ErrCodeCryptoBase+42)
	require.Truef(t, wrappedOK, "wrapped alert: code=%#x ok=%v, want %#x",
		wrappedCode, wrappedOK, ErrCodeCryptoBase+48)
	assert.Equalf(t, ErrCodeCryptoBase+48, wrappedCode,
		"wrapped alert: code=%#x ok=%v, want %#x", wrappedCode, wrappedOK, ErrCodeCryptoBase+48)
}

// TestConn_Fail_TLSAlert_SendsCryptoErrorClose checks that failing on a TLS alert
// sends a transport CONNECTION_CLOSE carrying the CRYPTO_ERROR code, instead of
// tearing the handshake down silently.
func TestConn_Fail_TLSAlert_SendsCryptoErrorClose(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x48)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("crypterr"), oneRTTSealer: sealer}

	err := c.fail(tls.AlertError(42))

	require.Error(t, err, "fail should return the original error")
	require.Lenf(t, pc.writes, 1, "wrote %d packets, want 1 CONNECTION_CLOSE", len(pc.writes))
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	assert.Truef(t, h.got && !h.app,
		"CONNECTION_CLOSE got=%v app=%v, want a transport close", h.got, h.app)
	assert.Equalf(t, ErrCodeCryptoBase+42, h.code,
		"close code = %#x, want CRYPTO_ERROR %#x", h.code, ErrCodeCryptoBase+42)
}
