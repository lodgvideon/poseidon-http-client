package quic

import (
	"fmt"
	"testing"
)

// TestConformance_RFC9000_Sec1731_ReservedBitsProtocolViolation checks that a
// 1-RTT (short-header) packet whose header reserved bits are non-zero, after
// protection is removed from an authenticated packet, is a PROTOCOL_VIOLATION
// connection error (RFC 9000 §17.3.1).
// Both bits of each mask are exercised. An earlier version set only 0x08, which
// an implementation testing that single bit instead of the whole mask passes
// while accepting a packet with the other one set — a §17.2/§17.3.1 MUST
// violation that would have gone unnoticed.
func TestConformance_RFC9000_Sec1731_ReservedBitsProtocolViolation(t *testing.T) {
	for _, bit := range []byte{0x08, 0x10} { // the 0x18 short-header reserved mask
		t.Run(fmt.Sprintf("bit_%#02x", bit), func(t *testing.T) {
			dcid := []byte("rsvdtest")
			keys, _ := InitialKeys(dcid)
			sealer, _ := NewSealer(keys)
			opener, _ := NewOpener(keys)

			// Sealed so header protection masks the bit — the client reveals it
			// again on decrypt, which is the only point the rule can be applied.
			pnLen := 4
			hdr, pnOff := AppendShortHeader(nil, nil, pnLen, false) // 0-length DCID
			hdr[0] |= bit
			for i := 0; i < pnLen; i++ {
				hdr = append(hdr, 0) // packet number 0
			}
			pkt, err := sealer.Seal(nil, hdr, pnOff, pnLen, 0, make([]byte, 20))
			if err != nil {
				t.Fatal(err)
			}

			c := &Conn{pc: &closePC{}, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
			c.keys.OneRTT = opener

			if err := c.recvDatagram(pkt); err != ErrProtocolViolation {
				t.Fatalf("reserved bit %#02x: recvDatagram = %v, want ErrProtocolViolation", bit, err)
			}
		})
	}
}

// TestConformance_RFC9000_Sec172_LongHeaderReservedBits checks the same rule for
// a long-header (Initial) packet, whose reserved bits are the 0x0c mask.
func TestConformance_RFC9000_Sec172_LongHeaderReservedBits(t *testing.T) {
	for _, bit := range []byte{0x04, 0x08} { // the 0x0c long-header reserved mask
		t.Run(fmt.Sprintf("bit_%#02x", bit), func(t *testing.T) {
			longHeaderReservedBit(t, bit)
		})
	}
}

func longHeaderReservedBit(t *testing.T, bit byte) {
	t.Helper()
	dcid := []byte("rsvdlong")
	_, serverKeys := InitialKeys(dcid) // the client opens server packets with the server keys
	serverSealer, _ := NewSealer(serverKeys)
	clientOpener, _ := NewOpener(serverKeys)

	pnLen := 4
	length := uint64(pnLen + 20 + 16)
	hdr, pnOff := AppendLongHeader(nil, PacketInitial, QUICVersion1, dcid, []byte{0xaa}, nil, pnLen, length)
	hdr[0] |= bit
	for i := 0; i < pnLen; i++ {
		hdr = append(hdr, 0)
	}
	pkt, err := serverSealer.Seal(nil, hdr, pnOff, pnLen, 0, make([]byte, 20))
	if err != nil {
		t.Fatal(err)
	}

	c := &Conn{pc: &closePC{}, dcid: dcid}
	c.keys.Initial = clientOpener

	if err := c.recvDatagram(pkt); err != ErrProtocolViolation {
		t.Fatalf("recvDatagram = %v, want ErrProtocolViolation", err)
	}
}

// TestConn_ReservedBitsZero_Accepted checks that a well-formed packet with zero
// reserved bits is not rejected (the check does not false-fire).
func TestConn_ReservedBitsZero_Accepted(t *testing.T) {
	dcid := []byte("rsvdtes2")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)

	// A normal short-header packet (reserved bits zero) carrying a PING.
	pkt := sealServerPacket(t, sealer, PacketShort, nil, nil, 0, []byte{byte(FramePing)})
	c := &Conn{pc: &closePC{}, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT = opener

	if err := c.recvDatagram(pkt); err != nil {
		t.Fatalf("recvDatagram of a valid packet = %v, want nil", err)
	}
}
