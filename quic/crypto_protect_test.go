package quic

import (
	"bytes"
	"crypto/aes"
	"errors"
	"testing"
)

// TestConformance_RFC9001_Sec53_Nonce checks the AEAD nonce construction (IV XOR
// left-padded packet number) against hand-computed values built from the RFC
// 9001 Appendix A.1 client IV.
func TestConformance_RFC9001_Sec53_Nonce(t *testing.T) {
	var iv [12]byte
	copy(iv[:], unhex(t, "fa044b2f42a3fd3b46fb255c")) // A.1 client iv
	if got := makeNonce(iv, 2); !bytes.Equal(got[:], unhex(t, "fa044b2f42a3fd3b46fb255e")) {
		t.Errorf("nonce(pn=2) = %x, want ...255e", got)
	}
	if got := makeNonce(iv, 0x1234); !bytes.Equal(got[:], unhex(t, "fa044b2f42a3fd3b46fb3768")) {
		t.Errorf("nonce(pn=0x1234) = %x, want ...3768", got)
	}
	if got := makeNonce(iv, 0); !bytes.Equal(got[:], iv[:]) {
		t.Errorf("nonce(pn=0) = %x, want the IV unchanged", got)
	}
}

func sealTestPacket(t *testing.T, s *Sealer, pn uint64, pnLen int, payload []byte) (sealed []byte, pnOff int) {
	t.Helper()
	length := uint64(pnLen + len(payload) + 16) // packet number + ciphertext + tag
	hdr, pnOff := AppendLongHeader(nil, PacketInitial, QUICVersion1, []byte{1, 2, 3, 4}, nil, nil, pnLen, length)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	var err error
	sealed, err = s.Seal(nil, hdr, pnOff, pnLen, pn, payload)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return sealed, pnOff
}

// TestPacketProtection_RoundTrip seals a packet and opens it back, recovering
// the exact packet number and payload — validating nonce, AAD, and the
// header-protection offsets end to end.
func TestPacketProtection_RoundTrip(t *testing.T) {
	client, _ := InitialKeys(unhex(t, "8394c8f03e515708"))
	sealer, err := NewSealer(client)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(client)
	if err != nil {
		t.Fatal(err)
	}

	pn := uint64(2)
	pnLen := 2
	payload := []byte("a QUIC frame payload long enough for the header-protection sample")
	sealed, pnOff := sealTestPacket(t, sealer, pn, pnLen, payload)

	// Header protection must have altered the first byte and/or packet number.
	unprotectedFirst := 0xc0 | byte(pnLen-1)
	if sealed[0] == unprotectedFirst && sealed[pnOff] == 0x00 && sealed[pnOff+1] == 0x02 {
		t.Error("header protection appears to be a no-op")
	}

	gotPN, gotPNLen, gotPayload, err := opener.Open(append([]byte(nil), sealed...), pnOff, pn-1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if gotPN != pn || gotPNLen != pnLen || !bytes.Equal(gotPayload, payload) {
		t.Fatalf("Open = pn=%d pnLen=%d payload=%q; want %d/%d/%q", gotPN, gotPNLen, gotPayload, pn, pnLen, payload)
	}
}

// TestHeaderProtection_Reversible checks that applying then removing header
// protection restores the header exactly and recovers the packet-number length.
func TestHeaderProtection_Reversible(t *testing.T) {
	block, err := aes.NewCipher(unhex(t, "9f50449e04a0e810283a1e9933adedd2")) // A.1 client hp
	if err != nil {
		t.Fatal(err)
	}
	pkt := make([]byte, 25)
	pkt[0] = 0xc3 // long header, pn-len bits = 3 → 4-byte packet number
	for i := 1; i < len(pkt); i++ {
		pkt[i] = byte(i)
	}
	orig := append([]byte(nil), pkt...)

	if err := applyHeaderProtection(block, pkt, 1, 4); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(pkt[:5], orig[:5]) {
		t.Fatal("apply did not change the header")
	}
	pnLen, err := removeHeaderProtection(block, pkt, 1)
	if err != nil {
		t.Fatal(err)
	}
	if pnLen != 4 {
		t.Fatalf("recovered pnLen = %d, want 4", pnLen)
	}
	if !bytes.Equal(pkt, orig) {
		t.Fatalf("not reversible:\n got %x\nwant %x", pkt, orig)
	}
}

// TestPacketProtection_AuthFailure verifies AEAD rejects a tampered packet and a
// wrong key.
func TestPacketProtection_AuthFailure(t *testing.T) {
	dcid := unhex(t, "8394c8f03e515708")
	client, server := InitialKeys(dcid)
	sealer, _ := NewSealer(client)
	clientOpener, _ := NewOpener(client)
	serverOpener, _ := NewOpener(server)

	sealed, pnOff := sealTestPacket(t, sealer, 5, 2, []byte("payload for the sample plus some"))

	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 0xff
	if _, _, _, err := clientOpener.Open(tampered, pnOff, 4); !errors.Is(err, ErrCryptoDecrypt) {
		t.Errorf("tampered packet: err = %v, want ErrCryptoDecrypt", err)
	}

	if _, _, _, err := serverOpener.Open(append([]byte(nil), sealed...), pnOff, 4); err == nil {
		t.Error("opening client packet with server keys should fail")
	}
}

// TestPacketProtection_ShortSample rejects a packet too short to sample header
// protection.
func TestPacketProtection_ShortSample(t *testing.T) {
	client, _ := InitialKeys(unhex(t, "8394c8f03e515708"))
	sealer, _ := NewSealer(client)
	hdr := make([]byte, 5) // pnOffset 3, pnLen 1 → packet ends well before pnOffset+4+16
	if _, err := sealer.Seal(nil, hdr, 3, 1, 0, []byte{0x01}); !errors.Is(err, ErrCryptoSample) {
		t.Fatalf("err = %v, want ErrCryptoSample", err)
	}
}

// TestDecodePacketNumber covers the RFC 9000 §A.3 reconstruction across a window
// boundary.
func TestDecodePacketNumber(t *testing.T) {
	// largestAcked = 0xa82f30ea, 2-byte truncated 0x9b32 → 0xa82f9b32 (§A.3 example).
	if got := decodePacketNumber(0xa82f30ea, 0x9b32, 2); got != 0xa82f9b32 {
		t.Fatalf("decode = %#x, want 0xa82f9b32", got)
	}
	if got := decodePacketNumber(0, 2, 1); got != 2 {
		t.Fatalf("decode small = %d, want 2", got)
	}
}
