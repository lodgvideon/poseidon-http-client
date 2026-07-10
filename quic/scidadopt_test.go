package quic

import (
	"bytes"
	"testing"
)

// TestConformance_RFC9000_Sec72_ServerCIDAdoptedOnlyWhenAuthenticated checks the
// RFC 9000 §7.2 rules for the server's connection ID: it is adopted only from a
// packet that authenticates (so a forged or garbage Initial cannot poison our
// Destination Connection ID), and once it is known, a long-header packet bearing a
// different Source Connection ID is discarded.
func TestConformance_RFC9000_Sec72_ServerCIDAdoptedOnlyWhenAuthenticated(t *testing.T) {
	origDCID := []byte("origdcid")        // the client's first, random DCID
	_, serverKeys := InitialKeys(origDCID) // the client opens server packets with these
	realSCID := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	otherSCID := []byte{0x11, 0x22, 0x33, 0x44}

	newConn := func() *Conn {
		c := &Conn{pc: &closePC{}, dcid: append([]byte(nil), origDCID...), handshakeComplete: true}
		c.keys.Initial, _ = NewOpener(serverKeys)
		return c
	}

	// A garbage Initial (sealed with keys the client does not hold, so it fails AEAD)
	// with an attacker-chosen SCID must NOT change our DCID (this is the fix — the
	// old code adopted before decrypting, so one spoofed datagram poisoned it).
	t.Run("unauthenticated-does-not-poison", func(t *testing.T) {
		_, wrongKeys := InitialKeys([]byte("wrongdci")) // keys the client does not hold
		garbage := craftServerInitial(t, wrongKeys, nil, otherSCID, 0, make([]byte, 20))
		c := newConn()
		if err := c.recvDatagram(garbage); err != nil {
			t.Fatalf("recvDatagram(garbage Initial) = %v, want nil (skipped)", err)
		}
		if c.gotServerCID {
			t.Fatal("an unauthenticated Initial must not adopt the server connection ID")
		}
		if !bytes.Equal(c.dcid, origDCID) {
			t.Fatalf("DCID poisoned: got %x, want the original %x", c.dcid, origDCID)
		}
	})

	// A valid server Initial adopts its SCID.
	t.Run("authenticated-adopts", func(t *testing.T) {
		pkt := craftServerInitial(t, serverKeys, nil, realSCID, 0, make([]byte, 20)) // PADDING payload
		c := newConn()
		if err := c.recvDatagram(pkt); err != nil {
			t.Fatalf("recvDatagram(valid Initial) = %v, want nil", err)
		}
		if !c.gotServerCID || !bytes.Equal(c.dcid, realSCID) || !bytes.Equal(c.serverSCID, realSCID) {
			t.Fatalf("adopt: gotServerCID=%v dcid=%x serverSCID=%x, want dcid/serverSCID=%x", c.gotServerCID, c.dcid, c.serverSCID, realSCID)
		}
	})

	// Once the server CID is known, a long-header packet with the SAME SCID is
	// processed (here a STREAM frame in Initial trips the §12.4 space gate), but one
	// with a DIFFERENT SCID is discarded before it is even decrypted (§7.2).
	streamInInitial := AppendStream(nil, 0, 0, false, []byte("x"))
	t.Run("matching-scid-processed", func(t *testing.T) {
		c := newConn()
		c.gotServerCID, c.serverSCID = true, append([]byte(nil), realSCID...)
		c.dcid = append(c.dcid[:0], realSCID...)
		pkt := craftServerInitial(t, serverKeys, nil, realSCID, 1, streamInInitial)
		if err := c.recvDatagram(pkt); err != ErrProtocolViolation {
			t.Fatalf("matching-SCID Initial with a STREAM frame = %v, want ErrProtocolViolation (it was processed)", err)
		}
	})
	t.Run("mismatched-scid-discarded", func(t *testing.T) {
		c := newConn()
		c.gotServerCID, c.serverSCID = true, append([]byte(nil), realSCID...)
		c.dcid = append(c.dcid[:0], realSCID...)
		pkt := craftServerInitial(t, serverKeys, nil, otherSCID, 1, streamInInitial)
		if err := c.recvDatagram(pkt); err != nil {
			t.Fatalf("mismatched-SCID Initial = %v, want nil (discarded before decrypt, §7.2)", err)
		}
	})
}
