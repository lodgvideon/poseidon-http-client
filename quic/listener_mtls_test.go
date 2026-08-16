package quic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"
)

// genMutualCert returns a self-signed certificate valid for BOTH server and
// client authentication, with the pool that trusts it. genServerCert's leaf
// carries only ExtKeyUsageServerAuth, so presenting it as a client certificate
// to a verifying server is refused by x509 with "incompatible key usage" — a
// legitimate rejection that says nothing about the transport. Client-auth tests
// need a certificate the peer can actually accept, or they measure the fixture.
func genMutualCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
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
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
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

// listenRequiringClientCert starts a listener that demands a verified client
// certificate signed by pool.
func listenRequiringClientCert(t *testing.T, cert tls.Certificate, pool *x509.CertPool) *Listener {
	t.Helper()
	l, err := Listen("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}, ServerTransportParams{MaxStreamsBidi: 16, MaxStreamsUni: 4})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// dialPresentingCert dials l with a client that presents cert, and returns the
// client Conn together with its Establish error.
func dialPresentingCert(t *testing.T, ctx context.Context, l *Listener, cert tls.Certificate, pool *x509.CertPool) (*Conn, error) {
	t.Helper()
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = uc.Close() })
	clientTP := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20,
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	client, err := NewConn(uc, &tls.Config{
		ServerName:   "example.com",
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, clientTP)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, client.Establish(ctx)
}

// TestListener_MutualTLSHandshakeCompletes is the positive arm: a listener
// requiring a client certificate accepts a client presenting a valid one, so a
// failure in the negative arm below is the rejection and not client auth being
// unimplemented. It is also the control for that arm's "Accept never fires":
// Accept demonstrably fires here, on the same code path.
func TestListener_MutualTLSHandshakeCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cert, pool := genMutualCert(t)
	l := listenRequiringClientCert(t, cert, pool)

	client, err := dialPresentingCert(t, ctx, l, cert, pool)
	if err != nil {
		t.Fatalf("client Establish under RequireAndVerifyClientCert: %v", err)
	}
	sc, err := l.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept under RequireAndVerifyClientCert: %v", err)
	}
	if !sc.isServer || !sc.handshakeComplete {
		t.Fatalf("accepted conn: isServer=%v handshakeComplete=%v, want true/true",
			sc.isServer, sc.handshakeComplete)
	}
	if !client.handshakeComplete {
		t.Error("client handshake did not complete")
	}
}

// TestConformance_RFC9001_Sec48_ListenerClosesOnRejectedClientCert pins RFC 9001
// §4.8: "A TLS alert is converted into a QUIC connection error. The
// AlertDescription value is added to 0x0100 to produce a QUIC error code from
// the range reserved for CRYPTO_ERROR", and "The resulting value is sent in a
// QUIC CONNECTION_CLOSE frame of type 0x1c" — the transport variant.
//
// The client presents a certificate the server refuses. A TLS 1.3 client is
// finished once it sends its own Finished, so Establish succeeds and the client
// cannot learn on its own that it was rejected; the close is the only thing that
// tells it. Without one the connection is silently dead at both ends: the client
// believes it is connected and the server's Accept never produces it.
func TestConformance_RFC9001_Sec48_ListenerClosesOnRejectedClientCert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Server-auth-only EKU: valid as the server's own identity, refused as a
	// client certificate. One cert keeps the server's side of the handshake
	// identical to the passing arm above, so the rejection is the only variable.
	cert, pool := genServerCert(t)
	l := listenRequiringClientCert(t, cert, pool)

	client, err := dialPresentingCert(t, ctx, l, cert, pool)
	if err != nil {
		t.Fatalf("client Establish: %v (a TLS 1.3 client completes on sending Finished)", err)
	}

	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pollCancel()
	perr := client.Poll(pollCtx)

	var closed *PeerClosedError
	if !errors.As(perr, &closed) {
		t.Fatalf("client Poll = %v, want a *PeerClosedError carrying the server's CRYPTO_ERROR", perr)
	}
	if closed.App {
		t.Errorf("CONNECTION_CLOSE is the application variant (0x1d); §4.8 requires the transport variant (0x1c)")
	}
	// The alert byte is crypto/tls's choice (bad_certificate = 42 on go1.25.13),
	// so the assertion is the range §4.8 reserves, not one toolchain's pick.
	if closed.Code < ErrCodeCryptoBase || closed.Code > ErrCodeCryptoBase+0xff {
		t.Errorf("close code = %#x, want CRYPTO_ERROR (%#x + a one-byte alert)", closed.Code, ErrCodeCryptoBase)
	}
	if closed.Code == ErrCodeCryptoBase {
		t.Errorf("close code = %#x: alert 0 is close_notify, not a handshake failure", closed.Code)
	}
	t.Logf("client observed CRYPTO_ERROR %#x (alert %d)", closed.Code, closed.Code-ErrCodeCryptoBase)

	// The rejection still stands: no connection is handed to the server's caller.
	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()
	if sc, aerr := l.Accept(acceptCtx); aerr == nil {
		t.Errorf("Accept returned a connection (%v) for a refused client certificate", sc != nil)
	}
}
