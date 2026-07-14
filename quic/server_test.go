package quic

import (
	"bytes"
	"testing"
)

// TestAcceptInitial_RoundTrip builds a real client Initial with BuildInitialPacket
// (sealed with the client's Initial keys), then checks AcceptInitial derives the
// same keys from the DCID, decrypts it, and recovers the connection IDs and the
// ClientHello CRYPTO bytes through the PADDING that pads the datagram to 1200.
func TestAcceptInitial_RoundTrip(t *testing.T) {
	t.Parallel()
	dcid := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	scid := []byte{0xaa, 0xbb, 0xcc}
	clientHello := []byte("this-stands-in-for-a-TLS-ClientHello-flight")

	clientKeys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(clientKeys)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	pkt, err := BuildInitialPacket(nil, sealer, dcid, scid, nil, 0, 4, 0, clientHello, InitialDatagramMinSize)
	if err != nil {
		t.Fatalf("BuildInitialPacket: %v", err)
	}

	ci, err := AcceptInitial(pkt)
	if err != nil {
		t.Fatalf("AcceptInitial: %v", err)
	}
	if !bytes.Equal(ci.DCID, dcid) {
		t.Errorf("DCID = %x, want %x", ci.DCID, dcid)
	}
	if !bytes.Equal(ci.SCID, scid) {
		t.Errorf("SCID = %x, want %x", ci.SCID, scid)
	}
	if len(ci.Token) != 0 {
		t.Errorf("Token = %x, want empty", ci.Token)
	}
	if !bytes.Equal(ci.CryptoData, clientHello) {
		t.Errorf("CryptoData = %q, want %q", ci.CryptoData, clientHello)
	}
}

// TestAcceptInitial_NotInitial rejects a non-Initial packet (a 1-RTT short header)
// with ErrNotInitial rather than trying to decrypt it.
func TestAcceptInitial_NotInitial(t *testing.T) {
	t.Parallel()
	// Short header: high bit clear (short form), fixed bit set.
	if _, err := AcceptInitial([]byte{0x40, 0x00, 0x00, 0x00, 0x00}); err != ErrNotInitial {
		t.Fatalf("AcceptInitial(short header) = %v, want ErrNotInitial", err)
	}
}

// TestAcceptInitial_Malformed surfaces the decode error for inputs that are not
// parseable packet headers.
func TestAcceptInitial_Malformed(t *testing.T) {
	t.Parallel()
	if _, err := AcceptInitial(nil); err == nil {
		t.Fatal("AcceptInitial(nil) = nil error, want a decode error")
	}
	if _, err := AcceptInitial([]byte{0xc0, 0x00, 0x00}); err == nil {
		t.Fatal("AcceptInitial(truncated long header) = nil error, want a decode error")
	}
}

// TestCryptoReassembler_OutOfOrderAndCap checks the reassembler stitches
// out-of-order CRYPTO frames by offset and rejects an offset past the cap.
func TestCryptoReassembler_OutOfOrderAndCap(t *testing.T) {
	t.Parallel()
	var c cryptoReassembler
	if err := c.OnCrypto(6, []byte("world")); err != nil {
		t.Fatalf("OnCrypto(6): %v", err)
	}
	if err := c.OnCrypto(0, []byte("hello ")); err != nil {
		t.Fatalf("OnCrypto(0): %v", err)
	}
	if got := string(c.assembled()); got != "hello world" {
		t.Errorf("assembled = %q, want %q", got, "hello world")
	}
	if err := c.OnCrypto(maxInitialCrypto, []byte{0x00}); err != ErrCryptoBufferExceeded {
		t.Errorf("OnCrypto past cap = %v, want ErrCryptoBufferExceeded", err)
	}
}

// TestSealPacket_RoundTrip seals a CRYPTO-carrying packet in each of the long
// (Initial, Handshake) and short (1-RTT) forms, then reparses the header and
// AEAD-opens it with the matching keys, recovering the packet number and the
// CRYPTO payload.
func TestSealPacket_RoundTrip(t *testing.T) {
	t.Parallel()
	dcid := []byte{0xaa, 0xbb, 0xcc}       // the client SCID = the server's reply DCID
	scid := []byte{0x01, 0x02, 0x03, 0x04} // the server's chosen SCID
	_, serverKeys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, err := NewSealer(serverKeys)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	opener, err := NewOpener(serverKeys)
	if err != nil {
		t.Fatalf("NewOpener: %v", err)
	}
	crypto := []byte("server-handshake-flight-bytes")
	payload := AppendCrypto(nil, 0, crypto)

	cases := []struct {
		typ     PacketType
		dcidLen int // ParseHeader needs the local DCID length for a short header
	}{
		{PacketInitial, 0},
		{PacketHandshake, 0},
		{PacketShort, len(dcid)},
	}
	for _, tc := range cases {
		pkt, err := SealPacket(nil, sealer, tc.typ, dcid, scid, nil, 7, 4, payload)
		if err != nil {
			t.Fatalf("SealPacket(%v): %v", tc.typ, err)
		}
		hdr, err := ParseHeader(pkt, tc.dcidLen)
		if err != nil {
			t.Fatalf("ParseHeader(%v): %v", tc.typ, err)
		}
		if hdr.Type != tc.typ {
			t.Errorf("type = %v, want %v", hdr.Type, tc.typ)
		}
		pn, _, frames, err := opener.Open(pkt, hdr.PNOffset, 0)
		if err != nil {
			t.Fatalf("Open(%v): %v", tc.typ, err)
		}
		if pn != 7 {
			t.Errorf("%v: pn = %d, want 7", tc.typ, pn)
		}
		var cr cryptoReassembler
		if err := ParseFrames(frames, &cr); err != nil {
			t.Fatalf("ParseFrames(%v): %v", tc.typ, err)
		}
		if !bytes.Equal(cr.assembled(), crypto) {
			t.Errorf("%v: crypto = %q, want %q", tc.typ, cr.assembled(), crypto)
		}
	}
}

// TestSealPacket_PadsShortPayload checks that a payload too small for the header-
// protection sample is padded so the packet still seals and opens.
func TestSealPacket_PadsShortPayload(t *testing.T) {
	t.Parallel()
	_, keys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	// One PING byte with a 1-byte packet number is under the 20-byte floor.
	pkt, err := SealPacket(nil, sealer, PacketHandshake, []byte{9, 9, 9, 9}, []byte{8, 8}, nil, 0, 1, []byte{0x01})
	if err != nil {
		t.Fatalf("SealPacket: %v", err)
	}
	hdr, err := ParseHeader(pkt, 0)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if _, _, _, err := opener.Open(pkt, hdr.PNOffset, 0); err != nil {
		t.Fatalf("Open padded packet: %v", err)
	}
}

// TestSealPacket_BadPNLen rejects an out-of-range packet-number length.
func TestSealPacket_BadPNLen(t *testing.T) {
	t.Parallel()
	_, keys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, _ := NewSealer(keys)
	if _, err := SealPacket(nil, sealer, PacketHandshake, []byte{1}, []byte{2}, nil, 0, 5, []byte{0x01}); err != ErrPacketEncoding {
		t.Fatalf("SealPacket(pnLen=5) = %v, want ErrPacketEncoding", err)
	}
}
