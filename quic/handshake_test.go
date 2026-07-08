package quic

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"
)

func genServerCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "example.com"},
		DNSNames:              []string{"example.com"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}, pool
}

// memSink records handshake output for one side of an in-memory handshake.
type memSink struct {
	crypto            map[tls.QUICEncryptionLevel][]byte
	appRead, appWrite []byte
	suite             uint16
	done              bool
}

func newMemSink() *memSink {
	return &memSink{crypto: map[tls.QUICEncryptionLevel][]byte{}}
}

func (s *memSink) WriteCrypto(l tls.QUICEncryptionLevel, d []byte) error {
	s.crypto[l] = append(s.crypto[l], d...)
	return nil
}
func (s *memSink) SetReadKeys(l tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	if l == tls.QUICEncryptionLevelApplication {
		s.appRead = append([]byte(nil), secret...)
		s.suite = suite
	}
	return nil
}
func (s *memSink) SetWriteKeys(l tls.QUICEncryptionLevel, suite uint16, secret []byte) error {
	if l == tls.QUICEncryptionLevelApplication {
		s.appWrite = append([]byte(nil), secret...)
	}
	return nil
}
func (s *memSink) PeerTransportParameters([]byte) error { return nil }
func (s *memSink) HandshakeComplete() error             { s.done = true; return nil }

func deliverCrypto(t *testing.T, src *memSink, dst *TLSHandshake) {
	t.Helper()
	for _, lvl := range []tls.QUICEncryptionLevel{
		tls.QUICEncryptionLevelInitial,
		tls.QUICEncryptionLevelHandshake,
		tls.QUICEncryptionLevelApplication,
	} {
		if data := src.crypto[lvl]; len(data) > 0 {
			if err := dst.HandleCrypto(lvl, data); err != nil {
				t.Fatalf("HandleCrypto(%v): %v", lvl, err)
			}
			src.crypto[lvl] = nil
		}
	}
}

// TestTLSHandshake_InMemory runs a full TLS 1.3 QUIC handshake between a client
// and server TLSHandshake entirely in memory, shuttling CRYPTO bytes, and checks
// that both complete, the Application traffic secrets match crosswise, packet
// keys derive from them, and ALPN negotiated "h3".
func TestTLSHandshake_InMemory(t *testing.T) {
	cert, pool := genServerCert(t)
	tp := []byte{0x01, 0x02, 0x03}

	client := NewClientHandshake(&tls.Config{ServerName: "example.com", RootCAs: pool}, tp)
	server := &TLSHandshake{
		conn: tls.QUICServer(&tls.QUICConfig{TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h3"},
			MinVersion:   tls.VersionTLS13,
		}}),
		tp: tp,
	}

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cSink, sSink := newMemSink(), newMemSink()
	for round := 0; round < 20 && (!cSink.done || !sSink.done); round++ {
		if err := client.Pump(cSink); err != nil {
			t.Fatalf("client pump: %v", err)
		}
		deliverCrypto(t, cSink, server)
		if err := server.Pump(sSink); err != nil {
			t.Fatalf("server pump: %v", err)
		}
		deliverCrypto(t, sSink, client)
	}

	if !cSink.done || !sSink.done {
		t.Fatalf("handshake did not complete: client=%v server=%v", cSink.done, sSink.done)
	}
	// Client's write secret == server's read secret (client→server direction).
	if !bytes.Equal(cSink.appWrite, sSink.appRead) || len(cSink.appWrite) == 0 {
		t.Fatalf("client write secret != server read secret")
	}
	if !bytes.Equal(cSink.appRead, sSink.appWrite) || len(cSink.appRead) == 0 {
		t.Fatalf("client read secret != server write secret")
	}
	// The shared secret + suite derive identical packet keys on both sides.
	ck, err := KeysFromSecret(cSink.suite, cSink.appWrite)
	if err != nil {
		t.Fatalf("KeysFromSecret: %v", err)
	}
	sk, err := KeysFromSecret(sSink.suite, sSink.appRead)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ck.Key, sk.Key) || !bytes.Equal(ck.IV, sk.IV) || !bytes.Equal(ck.HP, sk.HP) {
		t.Fatal("packet keys derived from the shared secret differ")
	}
	if got := client.ConnectionState().NegotiatedProtocol; got != "h3" {
		t.Fatalf("ALPN = %q, want h3", got)
	}
}

// TestKeysFromSecret_UnsupportedSuite rejects a suite we do not yet protect
// packets with (ChaCha20-Poly1305).
func TestKeysFromSecret_UnsupportedSuite(t *testing.T) {
	if _, err := KeysFromSecret(tls.TLS_CHACHA20_POLY1305_SHA256, make([]byte, 32)); err == nil {
		t.Fatal("expected ErrCryptoSuite for ChaCha20")
	}
}
