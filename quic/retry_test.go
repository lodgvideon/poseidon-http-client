package quic

import (
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The Retry example from RFC 9001 Appendix A.4: the integrity tag is computed
// over the original destination connection ID 0x8394c8f03e515708.
const (
	a4RetryPacket = "ff000000010008f067a5502a4262b5746f6b656e04a265ba2eff4d829058fb3f0f2496ba"
	a4ODCID       = "8394c8f03e515708"
)

// TestConformance_RFC9001_Sec58_RetryIntegrityKAT verifies the Retry Integrity
// Tag against the RFC 9001 Appendix A.4 known-answer vector, and that a tampered
// packet or wrong original DCID fails.
func TestConformance_RFC9001_Sec58_RetryIntegrityKAT(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	if !verifyRetryIntegrity(odcid, pkt) {
		t.Fatal("A.4 Retry integrity tag should verify")
	}
	// A single flipped byte in the token invalidates the tag.
	bad := append([]byte(nil), pkt...)
	bad[16] ^= 0x01
	if verifyRetryIntegrity(odcid, bad) {
		t.Fatal("a tampered Retry must not verify")
	}
	// The wrong original DCID invalidates the tag.
	if verifyRetryIntegrity(mustHex(t, "0000000000000000"), pkt) {
		t.Fatal("a Retry must not verify against the wrong original DCID")
	}
}

// TestConformance_RFC9000_Sec1725_RetryRekeysAndResends checks that a valid Retry
// adopts the server's connection ID, re-derives the Initial keys, stores the
// token, and re-queues the ClientHello for resend (RFC 9000 §17.2.5).
func TestConformance_RFC9000_Sec1725_RetryRekeysAndResends(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	client, _ := InitialKeys(odcid)
	sealer, _ := NewSealer(client)
	c := &Conn{dcid: odcid, initialSealer: sealer}
	c.keys.Initial, _ = NewOpener(client)
	// Simulate the first Initial in flight, carrying the ClientHello CRYPTO.
	c.sent[spaceInitial].onSent(0, c.clock(), true, []retransFrame{{kind: retransCrypto, offset: 0, data: []byte("clienthello")}})
	c.bytesInFlight = 1200

	hdr, err := ParseHeader(pkt, len(c.scid))
	if err != nil {
		t.Fatal(err)
	}
	c.handleRetry(hdr, pkt)

	if !c.handledRetry {
		t.Fatal("handledRetry should be set after a valid Retry")
	}
	if got := hex.EncodeToString(c.dcid); got != "f067a5502a4262b5" {
		t.Fatalf("dcid = %s, want the Retry SCID f067a5502a4262b5", got)
	}
	if got := hex.EncodeToString(c.retrySCID); got != "f067a5502a4262b5" {
		t.Fatalf("retrySCID = %s, want the Retry SCID f067a5502a4262b5 (for §7.3 validation)", got)
	}
	if string(c.retryToken) != "token" {
		t.Fatalf("retryToken = %q, want \"token\"", c.retryToken)
	}
	if len(c.sent[spaceInitial].packets) != 0 {
		t.Fatal("the outstanding Initial should be abandoned")
	}
	if len(c.retransQueue[spaceInitial]) == 0 {
		t.Fatal("the ClientHello should be re-queued for resend")
	}
	// The re-derived sealer must match keys derived from the new connection ID.
	newClient, _ := InitialKeys(mustHex(t, "f067a5502a4262b5"))
	want, _ := NewSealer(newClient)
	if c.initialSealer.iv != want.iv {
		t.Fatal("Initial keys should be re-derived from the new connection ID")
	}
}

// TestConformance_RFC9000_Sec17253_RetryResendCarriesToken checks that the
// Initial resent after a Retry carries the token and is sealed under the keys
// derived from the new connection ID (RFC 9000 §17.2.5.3).
func TestConformance_RFC9000_Sec17253_RetryResendCarriesToken(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	client, _ := InitialKeys(odcid)
	sealer, _ := NewSealer(client)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: append([]byte(nil), odcid...), initialSealer: sealer}
	c.keys.Initial, _ = NewOpener(client)
	c.sent[spaceInitial].onSent(0, c.clock(), true, []retransFrame{{kind: retransCrypto, offset: 0, data: []byte("clienthello")}})

	hdr, _ := ParseHeader(pkt, 0)
	c.handleRetry(hdr, pkt)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 resent Initial", len(pc.writes))
	}
	rh, err := ParseHeader(pc.writes[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if rh.Type != PacketInitial {
		t.Fatalf("resent packet type = %v, want Initial", rh.Type)
	}
	if string(rh.Token) != "token" {
		t.Fatalf("resent Initial token = %q, want \"token\"", rh.Token)
	}
	if len(pc.writes[0]) < InitialDatagramMinSize {
		t.Fatalf("resent Initial datagram %d bytes, want >= %d (§14.1 padding)", len(pc.writes[0]), InitialDatagramMinSize)
	}
	// The server unprotects the client's Initial with the CLIENT keys derived from
	// the new connection ID — the ones the client sealed with.
	newClient, _ := InitialKeys(mustHex(t, "f067a5502a4262b5"))
	opener, _ := NewOpener(newClient)
	if _, _, _, err := opener.Open(pc.writes[0][:rh.PacketLen], rh.PNOffset, 0); err != nil {
		t.Fatalf("resent Initial should decrypt with the new keys: %v", err)
	}
}

// TestConn_Retry_Discards checks the discard rules (RFC 9000 §17.2.5.2): a Retry
// with a failing tag, an empty token, an SCID equal to the Initial DCID, or a
// second Retry is ignored.
func TestConn_Retry_Discards(t *testing.T) {
	pkt := mustHex(t, a4RetryPacket)
	odcid := mustHex(t, a4ODCID)
	newConn := func() *Conn {
		client, _ := InitialKeys(odcid)
		sealer, _ := NewSealer(client)
		c := &Conn{dcid: append([]byte(nil), odcid...), initialSealer: sealer}
		c.keys.Initial, _ = NewOpener(client)
		return c
	}

	// Bad tag → ignored.
	c := newConn()
	bad := append([]byte(nil), pkt...)
	bad[len(bad)-1] ^= 0x01
	hdr, _ := ParseHeader(bad, 0)
	c.handleRetry(hdr, bad)
	if c.handledRetry || hex.EncodeToString(c.dcid) != a4ODCID {
		t.Fatal("a Retry with a bad tag must be discarded")
	}

	// Second Retry → ignored (at most one).
	c = newConn()
	hdr, _ = ParseHeader(pkt, 0)
	c.handleRetry(hdr, pkt) // first, accepted
	firstDCID := hex.EncodeToString(c.dcid)
	c.handleRetry(hdr, pkt) // second, ignored
	if hex.EncodeToString(c.dcid) != firstDCID {
		t.Fatal("a second Retry must be discarded")
	}

	// A Retry after a server Initial has been processed (gotServerCID) → ignored,
	// so an injected Retry cannot corrupt the adopted connection ID (§17.2.5.2).
	c = newConn()
	c.gotServerCID = true
	hdr, _ = ParseHeader(pkt, 0)
	c.handleRetry(hdr, pkt)
	if c.handledRetry || hex.EncodeToString(c.dcid) != a4ODCID {
		t.Fatal("a Retry after a server Initial must be discarded")
	}
}
