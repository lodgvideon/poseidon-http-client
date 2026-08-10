package client_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_ALPN_SilentH2PeerDoesNotWedgeClose pins that a first request
// stuck in the ALPN transport's protocol detection cannot block Close().
//
// alpnSingleConn.openExchange used to hold its mutex across the whole first dial
// AND the h2 SETTINGS handshake, both unbounded network I/O. A peer that
// completes the TLS handshake with ALPN "h2" and then sends no SETTINGS leaves
// NewClientConn blocked under the lock forever — and Close(), which carries no
// context, blocks on that same lock behind it. Detection now runs with the lock
// released, so the wedged request is contained to its own goroutine and Close()
// is unaffected.
//
// The wedged request itself is expected to hang: there is no transport-level
// read deadline in conn/ yet, which is a separate, larger gap. What this pins is
// that the wedge does not spread.
func TestConformance_ALPN_SilentH2PeerDoesNotWedgeClose(t *testing.T) {
	cert := alpnTestCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer ln.Close()

	var handshaken atomic.Int64
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(nc net.Conn) {
				defer nc.Close()
				if tc, ok := nc.(*tls.Conn); ok {
					_ = tc.Handshake()
					handshaken.Add(1)
				}
				time.Sleep(10 * time.Second)
			}(nc)
		}
	}()

	c, err := client.NewClient(client.ClientOptions{
		Addr:      ln.Addr().String(),
		Transport: client.TransportALPN,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.FlexDialer{Config: &tls.Config{
				InsecureSkipVerify: true,
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// A request that will wedge in the SETTINGS handshake. We do not wait on it.
	go func() {
		var resp client.Response
		_ = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &resp)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for handshaken.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() hung behind a request wedged in ALPN protocol detection — " +
			"the detection lock is held across the network handshake")
	}
}

// TestConformance_ALPN_ConcurrentFirstRequestsShareOneConn pins that racing
// first requests converge on a single connection rather than each dialling.
//
// The old race guard tested `detected != ""`, but a dial that negotiates no ALPN
// leaves NegotiatedProtocol == "", so on that path the guard never armed and
// every racer built and stored its own delegate — all but the last leaking.
// (Reachable through the documented ConnOpts.Dialer extension point; the
// built-in FlexDialer fails such a dial with ErrALPNFailed instead.) The guard
// is now the detecting-channel sentinel, independent of the protocol string.
func TestConformance_ALPN_ConcurrentFirstRequestsShareOneConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	var accepted atomic.Int64
	go func() {
		for {
			nc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			accepted.Add(1)
			go func(nc net.Conn) {
				defer nc.Close()
				buf := make([]byte, 4096)
				for {
					_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
					if _, rerr := nc.Read(buf); rerr != nil {
						return
					}
					_, _ = nc.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
				}
			}(nc)
		}
	}()

	// A plaintext dial gives NegotiatedProtocol "" — the case the old guard
	// missed — while still speaking HTTP/1.1, so the alpn transport builds an h1
	// delegate around it.
	c, err := client.NewClient(client.ClientOptions{
		Addr:          ln.Addr().String(),
		Transport:     client.TransportALPN,
		DefaultScheme: "http",
		ConnOpts: conn.ConnOptions{
			Dialer: alpnPlainDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var resp client.Response
			if err := c.Do(context.Background(),
				&client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, &resp); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("request failed: %v", err)
	}

	// The single-connection transport serialises requests onto one connection, so
	// racing first requests must not dial more than once.
	if got := accepted.Load(); got != 1 {
		t.Errorf("accepted %d connections, want 1 — racing first requests each built "+
			"their own delegate instead of sharing one (the proto=\"\" guard gap)", got)
	}
}

// alpnPlainDialer adapts a func to conn.Dialer.
type alpnPlainDialer func(context.Context, string) (net.Conn, error)

func (f alpnPlainDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return f(ctx, addr)
}

// alpnTestCert mints a throwaway certificate for the TLS case above.
func alpnTestCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "alpn-test"},
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
