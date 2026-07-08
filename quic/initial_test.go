package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"testing"
)

// initialCryptoCollector captures the Initial-level CRYPTO bytes (the
// ClientHello) emitted by a handshake.
type initialCryptoCollector struct {
	initial []byte
}

func (c *initialCryptoCollector) WriteCrypto(l tls.QUICEncryptionLevel, d []byte) error {
	if l == tls.QUICEncryptionLevelInitial {
		c.initial = append(c.initial, d...)
	}
	return nil
}
func (c *initialCryptoCollector) SetReadKeys(tls.QUICEncryptionLevel, uint16, []byte) error {
	return nil
}
func (c *initialCryptoCollector) SetWriteKeys(tls.QUICEncryptionLevel, uint16, []byte) error {
	return nil
}
func (c *initialCryptoCollector) PeerTransportParameters([]byte) error { return nil }
func (c *initialCryptoCollector) HandshakeComplete() error             { return nil }

// cryptoFrameSink captures the reassembled CRYPTO-frame bytes from a parsed
// packet payload.
type cryptoFrameSink struct {
	nopFrameHandler
	data []byte
}

func (s *cryptoFrameSink) OnCrypto(_ uint64, d []byte) error {
	s.data = append(s.data, d...)
	return nil
}

// TestConformance_RFC9000_Sec141_InitialFlight builds a real client Initial
// packet (padded to 1200) carrying a genuine ClientHello, then parses and
// decrypts it with the same Initial keys the server would derive — exercising
// key derivation, packet assembly, CRYPTO framing, AEAD, and header protection
// end to end.
func TestConformance_RFC9000_Sec141_InitialFlight(t *testing.T) {
	dcid := make([]byte, 8)
	if _, err := rand.Read(dcid); err != nil {
		t.Fatal(err)
	}
	scid := []byte(nil) // zero-length client connection ID

	client, _ := InitialKeys(dcid)
	sealer, err := NewSealer(client)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(client) // the server derives these same client Initial keys
	if err != nil {
		t.Fatal(err)
	}

	// Drive a real handshake far enough to produce the ClientHello.
	_, pool := genServerCert(t)
	hs := NewClientHandshake(&tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	if err := hs.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cc := &initialCryptoCollector{}
	if err := hs.Pump(cc); err != nil {
		t.Fatal(err)
	}
	if len(cc.initial) == 0 {
		t.Fatal("handshake produced no ClientHello")
	}

	pkt, err := BuildInitialPacket(nil, sealer, dcid, scid, nil, 0, 4, 0, cc.initial, InitialDatagramMinSize)
	if err != nil {
		t.Fatalf("BuildInitialPacket: %v", err)
	}
	if len(pkt) < InitialDatagramMinSize {
		t.Fatalf("Initial datagram = %d bytes, want >= %d", len(pkt), InitialDatagramMinSize)
	}

	// Parse the header (as the server would) and decrypt.
	h, err := ParseHeader(pkt, 0)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Type != PacketInitial || h.Version != QUICVersion1 || !bytes.Equal(h.DCID, dcid) {
		t.Fatalf("header: type=%d version=%#x dcid=%x", h.Type, h.Version, h.DCID)
	}
	pn, _, payload, err := opener.Open(pkt, h.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if pn != 0 {
		t.Fatalf("packet number = %d, want 0", pn)
	}

	// The payload is the CRYPTO frame (ClientHello) plus PADDING.
	sink := &cryptoFrameSink{}
	if err := ParseFrames(payload, sink); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	if !bytes.Equal(sink.data, cc.initial) {
		t.Fatalf("recovered CRYPTO (%d bytes) != ClientHello (%d bytes)", len(sink.data), len(cc.initial))
	}
}
