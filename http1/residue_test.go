package http1_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// residuePair returns an http1.Conn wrapping one end of a real loopback TCP
// connection, plus the peer's end. A real socket, not net.Pipe: HasResidue's
// decisive layer is a FIONREAD ioctl on the kernel receive queue, which a pipe
// does not have.
func residuePair(t *testing.T) (*http1.Conn, net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	type accepted struct {
		nc  net.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		nc, aerr := ln.Accept()
		ch <- accepted{nc, aerr}
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatalf("Accept: %v", a.err)
	}
	t.Cleanup(func() { _ = cli.Close(); _ = a.nc.Close() })
	return http1.NewConn(cli), a.nc, cli
}

// waitResidue polls until the verdict is want or the bound elapses, and reports
// what it settled on. Polling a verdict rather than sleeping a fixed span is the
// repo's rule for anything that depends on the network delivering: the bound is
// per-operation and generous, and the assertion is on the verdict, never on how
// long it took.
func waitResidue(c *http1.Conn, want bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := c.HasResidue(); got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Millisecond)
	}
}

// selfSignedCert mints a throwaway certificate for the TLS cases below.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "residue-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestHasResidue_QuietSocket is the false-positive guard: an idle connection
// with nothing on it must be reusable. Without this, "return true" passes every
// other test in this file.
func TestHasResidue_QuietSocket(t *testing.T) {
	c, _, _ := residuePair(t)
	for i := 0; i < 50; i++ {
		if c.HasResidue() {
			t.Fatalf("HasResidue() = true on call %d for a quiet socket; every checkout "+
				"would redial", i)
		}
	}
}

// TestHasResidue_UnsolicitedOctets pins the detection the pool's checkout depends
// on: octets the peer sent that nobody has read.
func TestHasResidue_UnsolicitedOctets(t *testing.T) {
	c, peer, _ := residuePair(t)
	if _, err := peer.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if !waitResidue(c, true) {
		t.Error("HasResidue() = false with a complete unsolicited response on the socket — " +
			"this is the verdict that decides whether the next request reads it as its own")
	}
}

// TestHasResidue_RepeatedCallsAreStable pins that asking does not change the
// answer. HasResidue moves the read deadline and peeks, so a call that consumed
// or masked the evidence would make the second checkout disagree with the first.
func TestHasResidue_RepeatedCallsAreStable(t *testing.T) {
	c, peer, _ := residuePair(t)
	if _, err := peer.Write([]byte("x")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if !waitResidue(c, true) {
		t.Fatal("HasResidue() never saw the octet")
	}
	for i := 0; i < 20; i++ {
		if !c.HasResidue() {
			t.Fatalf("HasResidue() = false on repeat call %d; the first call consumed or "+
				"masked the evidence", i)
		}
	}
}

// TestHasResidue_LeavesConnUsable pins that a negative verdict costs the
// connection nothing. HasResidue installs a deadline in the past and then a zero
// deadline; if it left either behind, the next real read would fail with a
// timeout that has nothing to do with it — the exact bug ReadResponse's
// unconditional deadline install was written to prevent.
func TestHasResidue_LeavesConnUsable(t *testing.T) {
	c, peer, cli := residuePair(t)
	if c.HasResidue() {
		t.Fatal("HasResidue() = true on a quiet socket")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(20 * time.Millisecond)
		_, _ = peer.Write([]byte("hello"))
	}()
	buf := make([]byte, 5)
	if _, err := cli.Read(buf); err != nil {
		t.Fatalf("read after HasResidue = %v; a deadline was left installed", err)
	}
	<-done
}

// TestHasResidue_TLSQuietConn is the false-positive guard that matters most in
// production. A TLS 1.3 server sends NewSessionTicket records on an idle
// connection without being asked, so bytes on the socket are NOT evidence of
// application data — treating them as such would evict a healthy connection
// against most origin servers. The verdict must stay false however many
// post-handshake records arrive.
func TestHasResidue_TLSQuietConn(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		// Complete the handshake, which is what makes the server emit its session
		// tickets, then hold the connection open and quiet.
		buf := make([]byte, 1)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
	}()

	tc, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
	})
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	defer tc.Close()
	if err := tc.Handshake(); err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	c := http1.NewConn(tc)
	// Long enough for tickets to land; the assertion is the verdict, not the wait.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.HasResidue() {
			t.Fatal("HasResidue() = true on a quiet TLS connection — post-handshake records " +
				"(NewSessionTicket, KeyUpdate) are not application data, and evicting on them " +
				"would redial against every TLS 1.3 origin")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHasResidue_TLSUnsolicitedResponse is the other half: real application data
// inside TLS must be seen. It is the case the socket-level check alone cannot
// answer, since crypto/tls may hold a fully-received record in its own buffer
// with the kernel queue empty.
func TestHasResidue_TLSUnsolicitedResponse(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		_, _ = nc.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"))
		buf := make([]byte, 1)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
	}()

	tc, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	defer tc.Close()

	c := http1.NewConn(tc)
	if !waitResidue(c, true) {
		t.Error("HasResidue() = false with an unsolicited response inside TLS")
	}
}

// TestHasResidue_NoAllocations is the cost guard as an assertion rather than a
// number to read: the plain-socket path must allocate nothing.
//
// This is load-bearing, not housekeeping. HasResidue runs on every checkout —
// that is the whole point, since gating it behind an idle threshold was the
// vulnerability — so an allocation here is one per request. It also fails
// exactly when the design is broken: fetching the RawConn or binding the control
// func per call allocates, and so does letting the plain-socket case fall
// through to a peek whose timeout error is heap-allocated.
func TestHasResidue_NoAllocations(t *testing.T) {
	c, _, _ := residuePair(t)
	if c.HasResidue() {
		t.Fatal("HasResidue() = true on a quiet socket")
	}
	got := testing.AllocsPerRun(200, func() { _ = c.HasResidue() })
	if got > 0 {
		t.Errorf("HasResidue() allocates %.1f objects per call on a plain socket, want 0 — "+
			"at one call per checkout this is one allocation per request", got)
	}
}

// BenchmarkHasResidue is the cost guard. The whole reason this check can run on
// every checkout — rather than behind the idle threshold that was the
// vulnerability — is that it is sub-microsecond and allocation-free. An
// allocation here means the cached RawConn or the pre-bound control func was
// lost, and the check has quietly become something a load generator cannot
// afford.
func BenchmarkHasResidue(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Listen: %v", err)
	}
	defer ln.Close()
	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		buf := make([]byte, 1)
		_, _ = nc.Read(buf)
	}()
	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	c := http1.NewConn(cli)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c.HasResidue() {
			b.Fatal("unexpected residue on a quiet socket")
		}
	}
}
