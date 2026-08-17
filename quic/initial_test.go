package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	_, err := rand.Read(dcid)
	require.NoError(t, err, "generate the destination connection ID")
	scid := []byte(nil) // zero-length client connection ID
	client, _ := InitialKeys(dcid)
	sealer, err := NewSealer(client)
	require.NoError(t, err, "NewSealer for the client Initial keys")
	opener, err := NewOpener(client) // the server derives these same client Initial keys
	require.NoError(t, err, "NewOpener for the keys the server would derive")
	// Drive a real handshake far enough to produce the ClientHello.
	_, pool := genServerCert(t)
	hs := newClientHandshake(&tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	require.NoError(t, hs.Start(context.Background()), "start the TLS handshake")
	cc := &initialCryptoCollector{}
	require.NoError(t, hs.Pump(cc), "pump the handshake for the ClientHello")
	require.NotEmpty(t, cc.initial, "handshake produced no ClientHello")

	pkt, err := buildInitialPacket(nil, sealer, dcid, scid, nil, 0, 4, 0, cc.initial, InitialDatagramMinSize)

	require.NoError(t, err, "buildInitialPacket")
	assert.GreaterOrEqualf(t, len(pkt), InitialDatagramMinSize,
		"Initial datagram = %d bytes, want >= %d (§14.1)", len(pkt), InitialDatagramMinSize)
	// Parse the header (as the server would) and decrypt.
	h, err := ParseHeader(pkt, 0)
	require.NoError(t, err, "ParseHeader as the server would")
	assert.Truef(t, h.Type == PacketInitial && h.Version == QUICVersion1 && bytes.Equal(h.DCID, dcid),
		"header: type=%d version=%#x dcid=%x", h.Type, h.Version, h.DCID)
	pn, _, payload, err := opener.Open(pkt, h.PNOffset, 0)
	require.NoError(t, err, "Open the Initial packet")
	assert.Zerof(t, pn, "packet number = %d, want 0", pn)
	// The payload is the CRYPTO frame (ClientHello) plus PADDING.
	sink := &cryptoFrameSink{}
	require.NoError(t, ParseFrames(payload, sink), "ParseFrames on the Initial payload")
	assert.Truef(t, bytes.Equal(sink.data, cc.initial),
		"recovered CRYPTO (%d bytes) != ClientHello (%d bytes)", len(sink.data), len(cc.initial))
}
