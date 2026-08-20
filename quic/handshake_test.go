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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// genServerCert returns a self-signed certificate usable as a server identity,
// with the pool that trusts it. Its leaf carries ExtKeyUsageServerAuth ONLY, so
// presenting it as a client certificate to a verifying server is refused by
// x509 — see genCertForUsage's callers.
func genServerCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	return genCertForUsage(t, x509.ExtKeyUsageServerAuth)
}

// genCertForUsage returns a self-signed certificate carrying exactly eku, with
// the pool that trusts it. The extended key usage is the parameter because it
// is what decides whether a peer will accept the certificate in a given role,
// and a test that gets it wrong measures its own fixture rather than the code.
func genCertForUsage(t *testing.T, eku ...x509.ExtKeyUsage) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "generate the fixture's signing key")
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "example.com"},
		DNSNames:              []string{"example.com"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           eku,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err, "self-sign the fixture certificate")
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err, "re-parse the fixture certificate")
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
			require.NoErrorf(t, dst.HandleCrypto(lvl, data), "HandleCrypto(%v)", lvl)
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

	client := newClientHandshake(&tls.Config{ServerName: "example.com", RootCAs: pool}, tp)
	// Construct the server via the public constructor with a bare config: this
	// also proves NewServerHandshake fills in TLS 1.3 + ALPN "h3" (asserted below).
	server := NewServerHandshake(&tls.Config{Certificates: []tls.Certificate{cert}}, tp)

	ctx := context.Background()
	require.NoError(t, client.Start(ctx), "client TLSHandshake.Start")
	require.NoError(t, server.Start(ctx), "server TLSHandshake.Start")
	cSink, sSink := newMemSink(), newMemSink()

	for round := 0; round < 20 && (!cSink.done || !sSink.done); round++ {
		require.NoErrorf(t, client.Pump(cSink), "client pump, round %d", round)
		deliverCrypto(t, cSink, server)
		require.NoErrorf(t, server.Pump(sSink), "server pump, round %d", round)
		deliverCrypto(t, sSink, client)
	}

	require.Truef(t, cSink.done && sSink.done,
		"handshake did not complete: client=%v server=%v", cSink.done, sSink.done)
	// Client's write secret == server's read secret (client→server direction).
	assert.NotEmpty(t, cSink.appWrite, "no client write secret was installed")
	assert.True(t, bytes.Equal(cSink.appWrite, sSink.appRead),
		"client write secret != server read secret")
	assert.NotEmpty(t, cSink.appRead, "no client read secret was installed")
	assert.True(t, bytes.Equal(cSink.appRead, sSink.appWrite),
		"client read secret != server write secret")
	// The shared secret + suite derive identical packet keys on both sides.
	ck, err := KeysFromSecret(cSink.suite, cSink.appWrite)
	require.NoError(t, err, "KeysFromSecret on the client side")
	sk, err := KeysFromSecret(sSink.suite, sSink.appRead)
	require.NoError(t, err, "KeysFromSecret on the server side")
	assert.True(t,
		bytes.Equal(ck.Key, sk.Key) && bytes.Equal(ck.IV, sk.IV) && bytes.Equal(ck.HP, sk.HP),
		"packet keys derived from the shared secret differ")
	assert.Equalf(t, "h3", client.ConnectionState().NegotiatedProtocol,
		"ALPN = %q, want h3", client.ConnectionState().NegotiatedProtocol)
}

// TestKeysFromSecret_UnsupportedSuite rejects a suite with no defined QUIC header
// protection scheme (TLS_AES_128_CCM_8_SHA256, 0x1305 — RFC 9001 §5.4.1). The
// AES-GCM suites and ChaCha20-Poly1305 are supported and covered elsewhere.
func TestKeysFromSecret_UnsupportedSuite(t *testing.T) {
	const tlsAES128CCM8SHA256 = uint16(0x1305)

	_, err := KeysFromSecret(tlsAES128CCM8SHA256, make([]byte, 32))

	assert.Error(t, err, "expected ErrCryptoSuite for an unsupported suite (RFC 9001 §5.4.1 defines "+
		"no header-protection scheme for it, so deriving keys would produce unusable ones)")
}
