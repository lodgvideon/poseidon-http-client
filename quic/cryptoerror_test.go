package quic

import (
	"crypto/tls"
	"fmt"
	"testing"
)

// TestConformance_RFC9001_Sec48_TLSAlertToCryptoError checks that a TLS alert maps
// to a CRYPTO_ERROR code of 0x0100 plus the alert description, found even through
// a wrapping error (RFC 9001 §4.8).
func TestConformance_RFC9001_Sec48_TLSAlertToCryptoError(t *testing.T) {
	code, ok := closeCodeFor(tls.AlertError(42)) // bad_certificate
	if !ok {
		t.Fatal("a TLS alert must map to a connection error to signal")
	}
	if code != ErrCodeCryptoBase+42 {
		t.Fatalf("code = %#x, want %#x (0x0100 + alert 42)", code, ErrCodeCryptoBase+42)
	}
	wrapped := fmt.Errorf("tls handshake: %w", tls.AlertError(48)) // certificate_unknown
	if code, ok = closeCodeFor(wrapped); !ok || code != ErrCodeCryptoBase+48 {
		t.Fatalf("wrapped alert: code=%#x ok=%v, want %#x", code, ok, ErrCodeCryptoBase+48)
	}
}

// TestConn_Fail_TLSAlert_SendsCryptoErrorClose checks that failing on a TLS alert
// sends a transport CONNECTION_CLOSE carrying the CRYPTO_ERROR code, instead of
// tearing the handshake down silently.
func TestConn_Fail_TLSAlert_SendsCryptoErrorClose(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x48)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("crypterr"), oneRTTSealer: sealer}
	if err := c.fail(tls.AlertError(42)); err == nil {
		t.Fatal("fail should return the original error")
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 CONNECTION_CLOSE", len(pc.writes))
	}
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	if !h.got || h.app {
		t.Fatalf("CONNECTION_CLOSE got=%v app=%v, want a transport close", h.got, h.app)
	}
	if h.code != ErrCodeCryptoBase+42 {
		t.Fatalf("close code = %#x, want CRYPTO_ERROR %#x", h.code, ErrCodeCryptoBase+42)
	}
}
