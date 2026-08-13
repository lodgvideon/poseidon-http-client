package quic

import "testing"

// TestConformance_RFC9000_Sec62_DiscardingASpaceKeepsTheVNGuard pins that
// discarding a packet-number space does not re-arm the Version Negotiation
// abandon path.
//
// shouldAbandonOnVN treats "a server packet was already processed" as the signal
// that a later VN is stale or spoofed (§6.2, §17.2.5.2), and it reads that signal
// from haveRecv. discardSpace deliberately leaves haveRecv alone; every other
// per-space field it touches is zeroed.
//
// The distinction matters because a VN packet is UNAUTHENTICATED — no header
// protection, no AEAD tag, nothing an off-path attacker cannot produce — and
// prefilterPacket evaluates it before decryption, on every packet, for the whole
// life of the connection. If a discard cleared haveRecv there would be a window
// between discarding the Handshake space and receiving the first 1-RTT packet in
// which all three haveRecv entries are false, and a forged VN would tear down a
// fully established connection. An off-path denial of service out of one spoofed
// datagram.
//
// This is a guard for a change nobody has made yet: #508 proposes moving the nine
// per-space arrays into a pnSpace struct with its own discard(), described as
// "mechanical, no behavior change". A discard() that zeroes its whole struct —
// the obvious implementation — is exactly the regression above. So the test is
// here first.
func TestConformance_RFC9000_Sec62_DiscardingASpaceKeepsTheVNGuard(t *testing.T) {
	dcid, scid := []byte("clientci"), []byte("serverci")
	vn := makeVN(dcid, scid, 0x6b3343cf) // no v1 offered: the abandon case
	hdr, err := ParseHeader(vn, 0)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}

	for _, tc := range []struct {
		name    string
		receive int
		discard []int
	}{
		{"initial received, initial discarded", spaceInitial, []int{spaceInitial}},
		{"handshake received, handshake discarded", spaceHandshake, []int{spaceHandshake}},
		{
			"both received, both discarded — the window before the first 1-RTT packet",
			spaceHandshake,
			[]int{spaceInitial, spaceHandshake},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			c.haveRecv[spaceInitial] = true
			c.haveRecv[tc.receive] = true

			// Sanity: the guard is closed before the discard, so a failure below is
			// about the discard rather than about the fixture.
			if c.shouldAbandonOnVN(vn, hdr) {
				t.Fatal("the VN guard was already open before any space was discarded, " +
					"so this test would not be measuring the discard")
			}

			for _, sp := range tc.discard {
				c.discardSpace(sp)
			}

			if c.shouldAbandonOnVN(vn, hdr) {
				t.Error("discarding a packet-number space re-opened the Version Negotiation " +
					"abandon path.\nA VN carries no authentication of any kind and is " +
					"evaluated before decryption, so this lets one spoofed datagram tear " +
					"down an established connection (RFC 9000 §6.2).")
			}
		})
	}
}

// TestDiscardSpace_ClearsWhatItShould is the other half: the fields discardSpace
// does zero must stay zeroed, so a future pnSpace.discard() cannot quietly drop
// one while keeping haveRecv.
//
// RFC 9002 §6.4 requires the sent-packet state to go — loss detection must not
// keep timing packets in a space that can no longer be acknowledged — and §4.9
// requires the keys to go with it.
func TestDiscardSpace_ClearsWhatItShould(t *testing.T) {
	c := &Conn{}
	c.sent[spaceInitial].onSent(1, c.clock(), true, nil)
	c.acks[spaceInitial].receive(1, false)
	c.pendingCrypto[spaceInitial] = []byte("clienthello")
	c.retransQueue[spaceInitial] = []retransFrame{{kind: retransCrypto}}
	c.cryptoRecv[spaceInitial] = recvStream{}

	c.discardSpace(spaceInitial)

	if n := len(c.sent[spaceInitial].packets); n != 0 {
		t.Errorf("sent packets = %d after discard, want 0: loss detection would keep "+
			"timing packets that can never be acknowledged (RFC 9002 §6.4)", n)
	}
	if c.pendingCrypto[spaceInitial] != nil {
		t.Error("pendingCrypto survived the discard: bytes queued for a space with no keys")
	}
	if c.retransQueue[spaceInitial] != nil {
		t.Error("retransQueue survived the discard: frames queued for a space with no keys")
	}
	if c.keys.Initial != nil || c.initialSealer != nil {
		t.Error("Initial keys survived the discard (RFC 9000 §4.9)")
	}
	if c.ptoCount != 0 {
		t.Error("ptoCount survived the discard: a Handshake-space backoff would inflate " +
			"the Application space's first probe timeout (RFC 9002 §6.2.2)")
	}
}
