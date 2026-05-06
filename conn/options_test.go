package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdvertisedSettings_Defaulted_FillsRFCDefaults(t *testing.T) {
	s := AdvertisedSettings{}.defaulted()
	if s.HeaderTableSize != 4096 {
		t.Fatalf("HeaderTableSize = %d, want 4096", s.HeaderTableSize)
	}
	if s.MaxConcurrentStreams != 100 {
		t.Fatalf("MaxConcurrentStreams = %d, want 100 (B.2 default)", s.MaxConcurrentStreams)
	}
	if s.InitialWindowSize != 65535 {
		t.Fatalf("InitialWindowSize = %d, want 65535", s.InitialWindowSize)
	}
	if s.MaxFrameSize != 16384 {
		t.Fatalf("MaxFrameSize = %d, want 16384", s.MaxFrameSize)
	}
}

func TestAdvertisedSettings_Defaulted_PreservesNonZero(t *testing.T) {
	s := AdvertisedSettings{HeaderTableSize: 8192}.defaulted()
	if s.HeaderTableSize != 8192 {
		t.Fatalf("HeaderTableSize = %d, want 8192", s.HeaderTableSize)
	}
}

func TestAdvertisedSettings_Defaulted_PreservesCallerConcurrent(t *testing.T) {
	s := AdvertisedSettings{MaxConcurrentStreams: 1000}.defaulted()
	if s.MaxConcurrentStreams != 1000 {
		t.Fatalf("caller value lost: got %d, want 1000", s.MaxConcurrentStreams)
	}
}

func TestConnOptions_Defaulted_FillsAllFields(t *testing.T) {
	o := ConnOptions{}.defaulted()
	if o.StreamEventBuffer != 8 {
		t.Fatalf("StreamEventBuffer = %d, want 8", o.StreamEventBuffer)
	}
	if o.Settings.MaxConcurrentStreams != 100 {
		t.Fatalf("nested settings default not applied: %d", o.Settings.MaxConcurrentStreams)
	}
	if o.Dialer == nil {
		t.Fatalf("Dialer not defaulted")
	}
}

func TestTLSDialer_NegotiatesH2_AgainstHttptest(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}

	d := &TLSDialer{Config: &tls.Config{
		RootCAs:    pool,
		ServerName: "example.com",
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addr := srv.Listener.Addr().String()
	c, err := d.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	tc, ok := c.(*tls.Conn)
	if !ok {
		t.Fatalf("conn type = %T", c)
	}
	if got := tc.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Fatalf("ALPN = %q, want h2", got)
	}
	_ = c.Close()
	_ = net.IPv4zero // keep net import live if reformatted
}

func TestDial_AgainstHttptestServer(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	addr := srv.Listener.Addr().String()

	opts := ConnOptions{
		Dialer: &TLSDialer{Config: &tls.Config{
			RootCAs:    pool,
			ServerName: "example.com",
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
}
