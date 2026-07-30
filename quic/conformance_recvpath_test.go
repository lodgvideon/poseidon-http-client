package quic

import (
	"bytes"
	"errors"
	"testing"
)

// newInitialConn builds a client Conn that can open server Initial packets keyed
// on origDCID, as the tests below receive them.
func newInitialConn(t *testing.T, origDCID []byte) *Conn {
	t.Helper()
	_, serverKeys := InitialKeys(origDCID)
	c := &Conn{pc: &closePC{}, dcid: append([]byte(nil), origDCID...), handshakeComplete: true}
	op, err := NewOpener(serverKeys)
	if err != nil {
		t.Fatal(err)
	}
	c.keys.Initial = op
	return c
}

// TestConformance_RFC9000_Sec52_ForeignVersionDiscarded pins "If a client receives
// a packet that uses a different version than it initially selected, it MUST
// discard that packet." The packet must be dropped BEFORE decryption: Initial keys
// derive from the observable connection ID with a public salt, so an on-path forger
// can seal a perfectly authenticating v1 Initial while stamping another version in
// the header. Discarding is silent — never a connection error, since the header is
// unauthenticated at that point.
func TestConformance_RFC9000_Sec52_ForeignVersionDiscarded(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	forgedSCID := []byte{0x11, 0x22, 0x33, 0x44}

	for _, version := range []uint32{0x00000002, 0x6b3343cf /* a GREASE version */, QUICVersion1 + 1} {
		c := newInitialConn(t, origDCID)
		pkt := craftServerInitialVersion(t, serverKeys, nil, forgedSCID, 0,
			AppendPing(nil), version)
		if err := c.recvDatagram(pkt); err != nil {
			t.Fatalf("version %#x: recvDatagram = %v, want nil (silently discarded)", version, err)
		}
		if c.gotServerCID {
			t.Fatalf("version %#x: adopted the server connection ID from a foreign-version packet", version)
		}
		if !bytes.Equal(c.dcid, origDCID) {
			t.Fatalf("version %#x: DCID poisoned: %x, want %x", version, c.dcid, origDCID)
		}
		if c.haveRecv[spaceInitial] {
			t.Fatalf("version %#x: a foreign-version packet was processed", version)
		}
	}

	// Control: the same packet at version 1 IS processed, so the test is not
	// passing merely because the fixture never decrypts.
	c := newInitialConn(t, origDCID)
	ok := craftServerInitial(t, serverKeys, nil, forgedSCID, 0, AppendPing(nil))
	if err := c.recvDatagram(ok); err != nil {
		t.Fatalf("control: recvDatagram = %v, want nil", err)
	}
	if !c.gotServerCID || !c.haveRecv[spaceInitial] {
		t.Fatal("control: a valid v1 Initial was not processed, so the fixture proves nothing")
	}
}

// TestConformance_RFC9000_Sec122_CoalescedForeignDCIDIgnored pins "Receivers SHOULD
// ignore any subsequent packets with a different Destination Connection ID than the
// first packet in the datagram." A forger can append a packet to a genuine
// datagram; only the first packet's DCID is the one the connection answers to.
func TestConformance_RFC9000_Sec122_CoalescedForeignDCIDIgnored(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	scid := []byte{0xaa, 0xbb, 0xcc, 0xdd}

	// Packet 1 addresses the client's (empty) connection ID; packet 2 carries a
	// different DCID and must be ignored even though it authenticates.
	p1 := craftServerInitial(t, serverKeys, nil, scid, 1, AppendPing(nil))
	p2 := craftServerInitial(t, serverKeys, []byte{0x09, 0x09, 0x09, 0x09}, scid, 2, AppendPing(nil))

	c := newInitialConn(t, origDCID)
	if err := c.recvDatagram(append(append([]byte{}, p1...), p2...)); err != nil {
		t.Fatalf("recvDatagram = %v, want nil", err)
	}
	if !c.haveRecv[spaceInitial] {
		t.Fatal("the first packet was not processed, so the test proves nothing")
	}
	if got := c.largestRecv[spaceInitial]; got != 1 {
		t.Fatalf("largestRecv = %d, want 1 — the foreign-DCID packet was processed", got)
	}
}

// TestConformance_RFC9000_Sec124_EmptyPacketIsProtocolViolation pins "An endpoint
// MUST treat receipt of a packet containing no frames as a connection error of type
// PROTOCOL_VIOLATION." Unlike the discards above this packet authenticated, so it
// is the peer's own violation, not something anyone can inject.
func TestConformance_RFC9000_Sec124_EmptyPacketIsProtocolViolation(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)

	c := newInitialConn(t, origDCID)
	empty := craftServerInitial(t, serverKeys, nil, []byte{0xaa}, 0, nil)
	if err := c.recvDatagram(empty); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("recvDatagram(zero-frame packet) = %v, want ErrProtocolViolation", err)
	}
}

// TestConformance_RFC9001_Sec57_ZeroRTTDiscardedNotDecrypted pins "a client MUST NOT
// attempt to decrypt 0-RTT packets it receives and instead MUST discard them"
// (RFC 9001 §5.7). Without the discard, packetSpace's default arm routes a 0-RTT
// packet into the Initial space and hands it to the Initial Opener — and Initial
// keys derive from the observable connection ID with a public salt, so an on-path
// forger can seal a 0-RTT-typed packet that genuinely authenticates.
func TestConformance_RFC9001_Sec57_ZeroRTTDiscardedNotDecrypted(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	forgedSCID := []byte{0x11, 0x22, 0x33, 0x44}

	sealer, err := NewSealer(serverKeys)
	if err != nil {
		t.Fatal(err)
	}
	payload := AppendPing(make([]byte, 0, 32))
	pnLen := 4
	hdr, pnOff := AppendLongHeader(nil, PacketZeroRTT, QUICVersion1, nil, forgedSCID, nil, pnLen, uint64(pnLen+len(payload)+16))
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, 0)
	}
	pkt, err := sealer.Seal(nil, hdr, pnOff, pnLen, 0, payload)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	c := newInitialConn(t, origDCID)
	if err := c.recvDatagram(pkt); err != nil {
		t.Fatalf("recvDatagram(0-RTT) = %v, want nil (discarded)", err)
	}
	if c.gotServerCID {
		t.Fatal("a 0-RTT packet must not be decrypted, let alone have its SCID adopted")
	}
	if c.haveRecv[spaceInitial] {
		t.Fatal("a 0-RTT packet was processed in the Initial packet-number space")
	}
}
