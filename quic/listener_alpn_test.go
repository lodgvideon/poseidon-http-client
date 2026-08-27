package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenOfferingALPN starts a listener that accepts exactly one protocol.
func listenOfferingALPN(t *testing.T, cert tls.Certificate, proto string) *Listener {
	t.Helper()
	l, err := Listen("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{proto},
	}, ServerTransportParams{MaxStreamsBidi: 16, MaxStreamsUni: 4})
	require.NoErrorf(t, err, "Listen offering %q", proto)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// dialOfferingALPN dials l with a client offering exactly one protocol, and
// returns the client Conn together with its Establish error.
func dialOfferingALPN(t *testing.T, ctx context.Context, l *Listener, pool *x509.CertPool, proto string) (*Conn, error) {
	t.Helper()
	uc, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
	require.NoError(t, err, "dial the listener over UDP")
	t.Cleanup(func() { _ = uc.Close() })
	clientTP := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20,
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	client, err := NewConn(uc, &tls.Config{
		ServerName: "example.com",
		RootCAs:    pool,
		NextProtos: []string{proto},
	}, clientTP)
	require.NoErrorf(t, err, "NewConn offering %q", proto)
	t.Cleanup(func() { _ = client.Close() })
	return client, client.Establish(ctx)
}

// TestListener_ALPNMatchHandshakeCompletes is the positive control for the
// conformance test below, and it is not decoration: that test asserts a terminal
// error arrives, which a listener broken in some unrelated way would also produce.
// This one shows the same listener code path completing when the only variable —
// the offered protocol — matches.
func TestListener_ALPNMatchHandshakeCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cert, pool := genServerCert(t)
	l := listenOfferingALPN(t, cert, "h3")

	_, err := dialOfferingALPN(t, ctx, l, pool, "h3")

	require.NoError(t, err, "Establish with a matching ALPN")
	acceptCtx, acceptCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acceptCancel()
	sc, aerr := l.Accept(acceptCtx)
	require.NoError(t, aerr, "Accept with a matching ALPN")
	assert.NotNil(t, sc, "Accept returned no connection for a completed handshake")
}

// TestConformance_RFC9001_Sec48_ListenerClosesOnRefusedALPN pins that a handshake
// TLS refuses BEFORE Handshake keys exist is still reported to the client.
//
// #711 made the listener report a rejection that happens after the client's
// Handshake flight arrives; it seals the close with ServerFlight.HandshakeSealer.
// This path has no *ServerFlight at all — StartServerHandshake builds its Initial
// sealer internally and discards it on every error return — so the close has to go
// out in the INITIAL space, which is what initialCloseDatagram does.
//
// The causes are operational rather than hostile: no overlapping ALPN, no mutually
// supported version or cipher suite, a GetCertificate error for an unknown SNI.
// RFC 9001 §4.8 does not distinguish them from a rejected client certificate.
//
// Measured before the fix, this was not merely quiet on the server: the client's
// Establish returned "context deadline exceeded" after 4s against a control arm
// returning nil in 10ms. So the operator-visible defect was an uninformative
// timeout on one side and total silence on the other (#715).
func TestConformance_RFC9001_Sec48_ListenerClosesOnRefusedALPN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cert, pool := genServerCert(t)
	l := listenOfferingALPN(t, cert, "h3")

	start := time.Now()
	_, eerr := dialOfferingALPN(t, ctx, l, pool, "hq-interop")
	elapsed := time.Since(start)

	// The property is that the server SPOKE. A CONNECTION_CLOSE the client can
	// decrypt and parse ends Establish at once; silence ends it at the deadline.
	// Both the error and the timing are asserted because either alone is weak:
	// ErrHandshakeClosed with no bound would also be satisfied by a much later
	// close, and a fast failure with no error check would be satisfied by any
	// early abort.
	require.ErrorIsf(t, eerr, ErrHandshakeClosed,
		"Establish = %v, want ErrHandshakeClosed: the listener refused the ALPN and must "+
			"say so in the Initial space", eerr)
	assert.Lessf(t, elapsed, 2*time.Second,
		"Establish took %v. Before #715 the server sent nothing here and the client sat "+
			"until its deadline; a figure near the deadline means it is still silent", elapsed)
	t.Logf("client Establish failed in %v with %v", elapsed, eerr)

	// The refusal still stands on the server side: no connection reaches Accept.
	acceptCtx, acceptCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acceptCancel()
	sc, aerr := l.Accept(acceptCtx)
	assert.Errorf(t, aerr, "Accept returned a connection (%v) for a refused ALPN", sc != nil)
}

// TestConformance_RFC9001_Sec48_InitialCloseCarriesTheAlert names the mechanism the
// test above can only observe indirectly. The client cannot report the code: on a
// CONNECTION_CLOSE during the handshake, OnConnectionClose sets c.closed without
// latching anything, so Establish returns the generic ErrHandshakeClosed and the
// CRYPTO_ERROR the server sent is dropped at that surface. That is a separate
// defect, filed rather than bundled here, and it is why this test checks the
// datagram directly.
//
// RFC 9001 §4.8 maps a TLS alert to 0x0100 plus the AlertDescription, and the
// transport variant of the frame; closeCodeFor does that mapping, and returning
// nothing for an error that is not a connection error to signal is the other half
// of the contract.
func TestConformance_RFC9001_Sec48_InitialCloseCarriesTheAlert(t *testing.T) {
	ci := &ClientInitial{DCID: bytes.Repeat([]byte{0xa1}, 8), SCID: bytes.Repeat([]byte{0xb2}, 8)}
	scid := bytes.Repeat([]byte{0xc3}, serverCIDLen)

	signalled := initialCloseDatagram(ci, scid, tls.AlertError(120))
	notSignalled := initialCloseDatagram(ci, scid, errors.New("read udp: i/o timeout"))

	assert.NotEmptyf(t, signalled,
		"a TLS alert produced no datagram; §4.8 requires the refusal to be sent")
	assert.Emptyf(t, notSignalled,
		"an I/O error produced a %d-byte datagram; only a connection error is signalled, "+
			"and an unvalidated peer must not be told about our local read failures", len(notSignalled))
	// Small enough that RFC 9000 §8.1's 3x amplification limit cannot bind against
	// a client Initial, which is at least 1200 bytes. No budget counter exists on
	// this path, so the bound is asserted here instead.
	assert.Lessf(t, len(signalled), 1200/3,
		"close datagram is %d bytes; against a 1200-byte Initial that approaches the "+
			"3x anti-amplification budget this listener does not track", len(signalled))
}
