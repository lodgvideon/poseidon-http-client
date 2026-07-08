package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"
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
	if err != nil {
		t.Fatalf("sealServerPacket: %v", err)
	}
	return pkt
}

// runServerHandshake reads the client's Initial, drives a TLS server, and sends
// the server's Initial (ServerHello) and Handshake (EncryptedExtensions ..
// Finished) flights in separate datagrams — enough for the client to complete.
func runServerHandshake(t *testing.T, pc PacketConn, cert tls.Certificate, tp []byte, done chan<- struct{}) {
	defer close(done)
	buf := make([]byte, 2048)
	n, err := pc.Read(buf)
	if err != nil {
		t.Errorf("server read: %v", err)
		return
	}
	dg := buf[:n]
	hdr, err := ParseHeader(dg, 0)
	if err != nil {
		t.Errorf("server ParseHeader: %v", err)
		return
	}
	clientDCID := append([]byte(nil), hdr.DCID...)
	clientKeys, serverKeys := InitialKeys(clientDCID)
	clientOpener, _ := NewOpener(clientKeys)
	serverInitial, _ := NewSealer(serverKeys)

	_, _, payload, err := clientOpener.Open(dg, hdr.PNOffset, 0)
	if err != nil {
		t.Errorf("server open client Initial: %v", err)
		return
	}
	var ch cryptoFrameSink
	if err := ParseFrames(payload, &ch); err != nil {
		t.Errorf("server parse client frames: %v", err)
		return
	}

	shs := &TLSHandshake{
		conn: tls.QUICServer(&tls.QUICConfig{TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h3"},
			MinVersion:   tls.VersionTLS13,
		}}),
		tp: tp,
	}
	ss := &serverSink{}
	if err := shs.Start(context.Background()); err != nil {
		t.Errorf("server Start: %v", err)
		return
	}
	_ = shs.Pump(ss)
	if err := shs.HandleCrypto(tls.QUICEncryptionLevelInitial, ch.data); err != nil {
		t.Errorf("server HandleCrypto: %v", err)
		return
	}
	if err := shs.Pump(ss); err != nil {
		t.Errorf("server Pump: %v", err)
		return
	}

	scid := []byte{0xab, 0xcd, 0xef}
	if len(ss.crypto[spaceInitial]) > 0 {
		f := AppendCrypto(nil, 0, ss.crypto[spaceInitial])
		if _, err := pc.Write(sealServerPacket(t, serverInitial, PacketInitial, nil, scid, 0, f)); err != nil {
			t.Errorf("server write Initial: %v", err)
			return
		}
	}
	if ss.handshakeSealer != nil && len(ss.crypto[spaceHandshake]) > 0 {
		f := AppendCrypto(nil, 0, ss.crypto[spaceHandshake])
		if _, err := pc.Write(sealServerPacket(t, ss.handshakeSealer, PacketHandshake, nil, scid, 0, f)); err != nil {
			t.Errorf("server write Handshake: %v", err)
		}
	}
}

// TestConn_Establish_InMemory completes a full QUIC v1 + TLS 1.3 handshake
// between the client Conn and an in-memory server, over a channel datagram
// pipe. Success means the client processes the server's flights, installs
// Handshake/1-RTT keys, and its TLS handshake completes.
func TestConn_Establish_InMemory(t *testing.T) {
	cert, pool := genServerCert(t)
	tp := []byte{0x01, 0x02, 0x03}

	toServer := make(chan []byte, 16)
	fromServer := make(chan []byte, 16)
	clientPC := &chanPC{rx: fromServer, tx: toServer}
	serverPC := &chanPC{rx: toServer, tx: fromServer}

	client, err := NewConn(clientPC, &tls.Config{ServerName: "example.com", RootCAs: pool}, tp)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go runServerHandshake(t, serverPC, cert, tp, done)

	if err := client.Establish(context.Background()); err != nil {
		t.Fatalf("client Establish: %v", err)
	}
	if !client.handshakeComplete {
		t.Fatal("client handshake did not complete")
	}
	if client.keys.Handshake == nil || client.handshakeSealer == nil {
		t.Fatal("client did not install Handshake keys")
	}
	<-done
}
