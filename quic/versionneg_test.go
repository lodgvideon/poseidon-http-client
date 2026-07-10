package quic

import (
	"encoding/binary"
	"testing"
)

// makeVN builds a Version Negotiation packet (RFC 9000 §17.2.1): long-header form,
// Version 0, then DCID, SCID, and the Supported Versions list.
func makeVN(dcid, scid []byte, versions ...uint32) []byte {
	pkt := []byte{0x80}           // long-header form (the remaining bits are unused in a VN)
	pkt = append(pkt, 0, 0, 0, 0) // Version 0 marks a Version Negotiation packet
	pkt = append(pkt, byte(len(dcid)))
	pkt = append(pkt, dcid...)
	pkt = append(pkt, byte(len(scid)))
	pkt = append(pkt, scid...)
	for _, v := range versions {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], v)
		pkt = append(pkt, b[:]...)
	}
	return pkt
}

// TestConformance_RFC9000_Sec62_VersionNegotiationAbandons checks that a Version
// Negotiation packet offering no version the client supports makes the client
// abandon the connection attempt (RFC 9000 §6.2).
func TestConformance_RFC9000_Sec62_VersionNegotiationAbandons(t *testing.T) {
	c := &Conn{}
	vn := makeVN([]byte("clientci"), []byte("serverci"), 0x6b3343cf, 0xff00001d) // QUIC v2 + a draft, no v1
	if err := c.recvDatagram(vn); err != ErrVersionNegotiation {
		t.Fatalf("recvDatagram(VN without v1) = %v, want ErrVersionNegotiation", err)
	}
}

// TestConformance_RFC9000_Sec62_VersionNegotiationDiscardExceptions checks the two
// §6.2 exceptions: a Version Negotiation packet that lists the client's own
// version, or one received after another server packet was already processed, is
// discarded (shouldAbandonOnVN false) rather than abandoned.
func TestConformance_RFC9000_Sec62_VersionNegotiationDiscardExceptions(t *testing.T) {
	dcid, scid := []byte("clientci"), []byte("serverci")

	// A VN offering no common version is abandoned (the base case).
	c := &Conn{}
	vn := makeVN(dcid, scid, 0x6b3343cf)
	hdr, err := ParseHeader(vn, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !c.shouldAbandonOnVN(vn, hdr) {
		t.Fatal("a VN offering no common version should be abandoned")
	}

	// Exception: it lists the client's own version (v1) → discarded.
	vnV1 := makeVN(dcid, scid, QUICVersion1, 0x6b3343cf)
	hdrV1, _ := ParseHeader(vnV1, 0)
	if (&Conn{}).shouldAbandonOnVN(vnV1, hdrV1) {
		t.Fatal("a VN listing v1 must be discarded, not abandoned")
	}

	// Exception: a server packet was already processed → discarded.
	c2 := &Conn{}
	c2.haveRecv[spaceInitial] = true
	if c2.shouldAbandonOnVN(vn, hdr) {
		t.Fatal("a VN after a processed packet must be discarded, not abandoned")
	}

	// Exception: a Retry was already processed → discarded (RFC 9000 §17.2.5.2).
	c3 := &Conn{handledRetry: true}
	if c3.shouldAbandonOnVN(vn, hdr) {
		t.Fatal("a VN after a processed Retry must be discarded, not abandoned")
	}
}
