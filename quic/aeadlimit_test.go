package quic

import (
	"bytes"
	"crypto/tls"
	"testing"
)

// TestConformance_RFC9001_Sec66_AEADLimitsSuiteAware checks that the §6.6 AEAD
// usage limits are selected per cipher suite: AEAD_CHACHA20_POLY1305 has a larger
// confidentiality limit (2^62) but a SMALLER integrity limit (2^36) than AES-GCM
// (2^23 / 2^52). A single shared integrity limit would let a ChaCha20 connection
// tolerate 2^16x more forged packets than §6.6 permits, so the split is mandatory.
func TestConformance_RFC9001_Sec66_AEADLimitsSuiteAware(t *testing.T) {
	if conf, integ := aeadLimits(tls.TLS_CHACHA20_POLY1305_SHA256); conf != 1<<62 || integ != 1<<36 {
		t.Fatalf("ChaCha20 limits = (%d, %d), want (2^62=%d, 2^36=%d)",
			conf, integ, uint64(1)<<62, uint64(1)<<36)
	}
	for _, s := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384} {
		if conf, integ := aeadLimits(s); conf != aeadConfidentialityLimit || integ != aeadIntegrityLimit {
			t.Fatalf("AES suite %#x limits = (%d, %d), want AES-GCM (2^23, 2^52)", s, conf, integ)
		}
	}
	// A connection with no key-update state falls back to the AES-GCM limits.
	if got := (&Conn{}).integrityLimit(); got != aeadIntegrityLimit {
		t.Fatalf("integrityLimit() with nil ku = %d, want AES 2^52", got)
	}
	// A ChaCha20 connection reports the ChaCha20 limits at the enforcement sites.
	c := &Conn{ku: &keyUpdate{suite: tls.TLS_CHACHA20_POLY1305_SHA256}}
	if got := c.integrityLimit(); got != 1<<36 {
		t.Fatalf("integrityLimit() on a ChaCha20 conn = %d, want 2^36", got)
	}
	if got := c.confidentialityLimit(); got != 1<<62 {
		t.Fatalf("confidentialityLimit() on a ChaCha20 conn = %d, want 2^62", got)
	}
}

// TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses checks that, having
// sealed the confidentiality limit of 1-RTT packets under one key, the client
// refuses to seal another and closes with AEAD_LIMIT_REACHED (RFC 9001 §6.6) —
// it cannot rotate its own keys.
func TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x66)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("aeadtest"), oneRTTSealer: sealer, appSendCount: aeadConfidentialityLimit}

	if _, err := c.sealPacket(spaceApp, []byte{byte(FramePing)}, true, nil, false); err != ErrAEADLimit {
		t.Fatalf("sealPacket at the confidentiality limit = %v, want ErrAEADLimit", err)
	}
	if !c.closed {
		t.Fatal("connection must close at the confidentiality limit")
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 CONNECTION_CLOSE", len(pc.writes))
	}
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	if !h.got || h.code != ErrCodeAEADLimitReached {
		t.Fatalf("close code = %#x, want AEAD_LIMIT_REACHED (%#x)", h.code, ErrCodeAEADLimitReached)
	}
}

// TestConn_AEADConfidentiality_CounterIncrements checks that each 1-RTT packet
// sealed advances the confidentiality counter.
func TestConn_AEADConfidentiality_CounterIncrements(t *testing.T) {
	sealer, _ := closeTestSealerOpener(t, 0x67)
	c := &Conn{pc: &closePC{}, dcid: []byte("aeadtest"), oneRTTSealer: sealer}
	for i := 0; i < 3; i++ {
		if _, err := c.sealPacket(spaceApp, []byte{byte(FramePing)}, true, nil, false); err != nil {
			t.Fatalf("sealPacket %d: %v", i, err)
		}
	}
	if c.appSendCount != 3 {
		t.Fatalf("appSendCount = %d, want 3", c.appSendCount)
	}
}

// TestConn_AEADConfidentiality_ResetOnKeyUpdate checks that a key update restarts
// the confidentiality counter for the new write key (RFC 9001 §6.6): a peer that
// updates regularly keeps the client well under the limit.
func TestConn_AEADConfidentiality_ResetOnKeyUpdate(t *testing.T) {
	c := newKUTestConn(t, bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x02}, 32))
	c.appSendCount = 12345
	c.commitKeyUpdate(7)
	if c.appSendCount != 0 {
		t.Fatalf("appSendCount = %d after a key update, want 0", c.appSendCount)
	}
}

// TestConn_AEADIntegrity_CountsAuthFailures checks that a 1-RTT packet that fails
// authentication advances the integrity counter (RFC 9001 §6.6).
func TestConn_AEADIntegrity_CountsAuthFailures(t *testing.T) {
	_, opener := closeTestSealerOpener(t, 0x68)
	c := &Conn{}
	c.keys.OneRTT = opener
	garbage := make([]byte, 64) // long enough to sample header protection; will not decrypt
	garbage[0] = 0x40           // short-header form
	if _, _, ok := c.openApp(garbage, 1); ok {
		t.Fatal("garbage packet should not authenticate")
	}
	if c.authFailures != 1 {
		t.Fatalf("authFailures = %d, want 1 (a failed authentication is counted)", c.authFailures)
	}
}
