package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errPipeTimeout = errors.New("pipe read timeout")

// chanPC is an in-memory datagram PacketConn: writes go to tx, reads take from
// rx. Read times out so a stalled handshake fails the test instead of hanging.
type chanPC struct {
	rx <-chan []byte
	tx chan<- []byte
}

func (p *chanPC) Write(b []byte) (int, error) {
	p.tx <- append([]byte(nil), b...)
	return len(b), nil
}
func (p *chanPC) Read(b []byte) (int, error) {
	select {
	case d := <-p.rx:
		return copy(b, d), nil
	case <-time.After(3 * time.Second):
		return 0, errPipeTimeout
	}
}
func (p *chanPC) Close() error { return nil }

// serverSink collects the server handshake's outbound CRYPTO and Sealers.
type serverSink struct {
	crypto          [numSpaces][]byte
	handshakeSealer *Sealer
}

func (s *serverSink) WriteCrypto(l tls.QUICEncryptionLevel, d []byte) error {
	s.crypto[levelSpace(l)] = append(s.crypto[levelSpace(l)], d...)
	return nil
}
func (s *serverSink) SetReadKeys(tls.QUICEncryptionLevel, uint16, []byte) error { return nil }
func (s *serverSink) SetWriteKeys(l tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	if levelSpace(l) == spaceHandshake {
		k, err := KeysFromSecret(suite, secret)
		if err != nil {
			return err
		}
		s.handshakeSealer, err = NewSealer(k)
		return err
	}
	return nil
}
func (s *serverSink) PeerTransportParameters([]byte) error { return nil }
func (s *serverSink) HandshakeComplete() error             { return nil }

func sealServerPacket(t *testing.T, s *Sealer, typ PacketType, dcid, scid []byte, pn uint64, frames []byte) []byte {
	t.Helper()
	const minFrames = 20
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...)
	}
	pnLen := 4
	length := uint64(pnLen + len(frames) + 16)
	var hdr []byte
	var pnOff int
	if typ == PacketShort {
		hdr, pnOff = AppendShortHeader(nil, dcid, pnLen, false)
	} else {
		hdr, pnOff = AppendLongHeader(nil, typ, QUICVersion1, dcid, scid, nil, pnLen, length)
	}
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	pkt, err := s.Seal(nil, hdr, pnOff, pnLen, pn, frames)
	require.NoErrorf(t, err, "sealServerPacket: %v", err)
	return pkt
}

// runServerHandshake reads the client's Initial, drives a TLS server, and sends
// the server's Initial (ServerHello) and Handshake (EncryptedExtensions ..
// Finished) flights in separate datagrams — enough for the client to complete.
func runServerHandshake(t *testing.T, pc PacketConn, cert tls.Certificate, tp, scid []byte, done chan<- struct{}) {
	defer close(done)
	buf := make([]byte, 2048)
	n, err := pc.Read(buf)
	if !assert.NoError(t, err, "server read") {
		return
	}
	dg := buf[:n]
	hdr, err := ParseHeader(dg, 0)
	if !assert.NoError(t, err, "server ParseHeader") {
		return
	}
	clientDCID := append([]byte(nil), hdr.DCID...)
	clientKeys, serverKeys := InitialKeys(clientDCID)
	clientOpener, _ := NewOpener(clientKeys)
	serverInitial, _ := NewSealer(serverKeys)

	_, _, payload, err := clientOpener.Open(dg, hdr.PNOffset, 0)
	if !assert.NoError(t, err, "server open client Initial") {
		return
	}
	var ch cryptoFrameSink
	if !assert.NoError(t, ParseFrames(payload, &ch), "server parse client frames") {
		return
	}

	shs := NewServerHandshake(&tls.Config{Certificates: []tls.Certificate{cert}}, tp)
	ss := &serverSink{}
	if !assert.NoError(t, shs.Start(context.Background()), "server Start") {
		return
	}
	_ = shs.Pump(ss)
	if !assert.NoError(t, shs.HandleCrypto(tls.QUICEncryptionLevelInitial, ch.data), "server HandleCrypto") {
		return
	}
	if !assert.NoError(t, shs.Pump(ss), "server Pump") {
		return
	}

	if len(ss.crypto[spaceInitial]) > 0 {
		f := AppendCrypto(nil, 0, ss.crypto[spaceInitial])
		_, err := pc.Write(sealServerPacket(t, serverInitial, PacketInitial, nil, scid, 0, f))
		if !assert.NoError(t, err, "server write Initial") {
			return
		}
	}
	if ss.handshakeSealer != nil && len(ss.crypto[spaceHandshake]) > 0 {
		f := AppendCrypto(nil, 0, ss.crypto[spaceHandshake])
		_, err := pc.Write(sealServerPacket(t, ss.handshakeSealer, PacketHandshake, nil, scid, 0, f))
		assert.NoError(t, err, "server write Handshake")
	}
}

// TestConn_Establish_InMemory completes a full QUIC v1 + TLS 1.3 handshake
// between the client Conn and an in-memory server, over a channel datagram
// pipe. Success means the client processes the server's flights, installs
// Handshake/1-RTT keys, and its TLS handshake completes.
func TestConn_Establish_InMemory(t *testing.T) {
	cert, pool := genServerCert(t)
	clientTP := concat(
		tpInt(tpInitialMaxData, 1<<20),
		tpInt(tpInitialMaxStreamDataBidiRemote, 1<<20),
		tpInt(tpInitialMaxStreamsBidi, 16),
	)
	// The server seals its packets with this Source Connection ID; the client
	// adopts it as its destination CID and authenticates the server's
	// initial_source_connection_id against it (RFC 9000 §7.3), so the server's
	// transport parameters must carry the same value.
	serverSCID := []byte{0xab, 0xcd, 0xef}
	toServer := make(chan []byte, 16)
	fromServer := make(chan []byte, 16)
	clientPC := &chanPC{rx: fromServer, tx: toServer}
	serverPC := &chanPC{rx: toServer, tx: fromServer}
	client, err := NewConn(clientPC, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP)
	require.NoError(t, err)
	// The server echoes the client's first-Initial Destination Connection ID in
	// original_destination_connection_id, which the client authenticates (§7.3).
	serverTP := concat(clientTP,
		tpBytes(tpInitialSourceConnectionID, serverSCID),
		tpBytes(tpOriginalDestinationConnectionID, client.origDCID))
	done := make(chan struct{})
	go runServerHandshake(t, serverPC, cert, serverTP, serverSCID, done)

	err = client.Establish(context.Background())

	require.NoErrorf(t, err, "client Establish: %v", err)
	assert.True(t, client.handshakeComplete, "client handshake did not complete")
	assert.Truef(t, client.keys.Handshake != nil, "client did not install Handshake keys")
	assert.Truef(t, client.handshakeSealer != nil, "client did not install Handshake keys")
	<-done
}
