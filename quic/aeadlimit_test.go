package quic

import (
	"bytes"
	"testing"
)

// TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses checks that, having
// sealed the confidentiality limit of 1-RTT packets under one key, the client
// refuses to seal another and closes with AEAD_LIMIT_REACHED (RFC 9001 §6.6) —
// it cannot rotate its own keys.
func TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x66)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("aeadtest"), oneRTTSealer: sealer, appSendCount: aeadConfidentialityLimit}

	if _, err := c.sealPacket(spaceApp, []byte{byte(FramePing)}, true, nil); err != ErrAEADLimit {
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
		if _, err := c.sealPacket(spaceApp, []byte{byte(FramePing)}, true, nil); err != nil {
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
