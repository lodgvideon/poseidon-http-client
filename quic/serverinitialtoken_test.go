package quic

import "testing"

// TestConformance_RFC9000_Sec1722_ServerInitialWithTokenDiscarded checks that a
// server Initial packet carrying a non-empty Token is discarded (RFC 9000 §17.2.2:
// a server's Initial MUST have a zero-length Token), while an empty-token Initial is
// processed normally.
func TestConformance_RFC9000_Sec1722_ServerInitialWithTokenDiscarded(t *testing.T) {
	dcid := []byte("srvtoken")
	_, serverKeys := InitialKeys(dcid)
	ping := []byte{byte(FramePing)}

	// build seals a server Initial (pn 0) carrying a PING, with the given Token.
	build := func(token []byte) []byte {
		sealer, err := NewSealer(serverKeys)
		if err != nil {
			t.Fatal(err)
		}
		pnLen := 4
		length := uint64(pnLen + len(ping) + 16)
		hdr, pnOff := AppendLongHeader(nil, PacketInitial, QUICVersion1, dcid, []byte{0xaa}, token, pnLen, length)
		for i := 0; i < pnLen; i++ {
			hdr = append(hdr, 0) // packet number 0
		}
		pkt, err := sealer.Seal(nil, hdr, pnOff, pnLen, 0, ping)
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}
	newConn := func() *Conn {
		c := &Conn{pc: &closePC{}, dcid: dcid, handshakeComplete: true}
		c.keys.Initial, _ = NewOpener(serverKeys)
		return c
	}

	// An empty-token server Initial is processed: it records receipt.
	c := newConn()
	if err := c.recvDatagram(build(nil)); err != nil {
		t.Fatal(err)
	}
	if !c.haveRecv[spaceInitial] {
		t.Fatal("an empty-token server Initial should be processed")
	}

	// A server Initial with a non-empty Token is discarded before processing.
	c2 := newConn()
	if err := c2.recvDatagram(build([]byte("tok"))); err != nil {
		t.Fatalf("recvDatagram(Initial with token) = %v, want nil (discarded)", err)
	}
	if c2.haveRecv[spaceInitial] {
		t.Fatal("a server Initial with a non-empty Token must be discarded (§17.2.2)")
	}
}
