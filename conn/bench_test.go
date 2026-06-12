package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func benchSetup(b *testing.B) (*Conn, func()) {
	b.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: "example.com"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
	})
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	return c, func() { _ = c.Close(); srv.Close() }
}

func BenchmarkConn_Roundtrip_Empty(b *testing.B) {
	c, teardown := benchSetup(b)
	defer teardown()
	hdrs := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			b.Fatalf("NewStream: %v", err)
		}
		if err := s.SendHeaders(ctx, hdrs, true); err != nil {
			b.Fatalf("SendHeaders: %v", err)
		}
		for {
			ev, err := s.Recv(ctx)
			if err != nil {
				b.Fatalf("Recv: %v", err)
			}
			if ev.EndStream {
				break
			}
		}
	}
}
