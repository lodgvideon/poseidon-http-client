package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdvertisedSettings_Defaulted_FillsRFCDefaults(t *testing.T) {
	s := AdvertisedSettings{}.defaulted()

	assert.EqualValuesf(t, 4096, s.HeaderTableSize, "HeaderTableSize = %d, want 4096", s.HeaderTableSize)
	assert.EqualValuesf(t, 100, s.MaxConcurrentStreams,
		"MaxConcurrentStreams = %d, want 100 (B.2 default)", s.MaxConcurrentStreams)
	assert.EqualValuesf(t, 65535, s.InitialWindowSize, "InitialWindowSize = %d, want 65535", s.InitialWindowSize)
	assert.EqualValuesf(t, 16384, s.MaxFrameSize, "MaxFrameSize = %d, want 16384", s.MaxFrameSize)
}

func TestAdvertisedSettings_Defaulted_PreservesNonZero(t *testing.T) {
	s := AdvertisedSettings{HeaderTableSize: 8192}.defaulted()

	assert.EqualValuesf(t, 8192, s.HeaderTableSize, "HeaderTableSize = %d, want 8192", s.HeaderTableSize)
}

func TestAdvertisedSettings_Defaulted_PreservesCallerConcurrent(t *testing.T) {
	s := AdvertisedSettings{MaxConcurrentStreams: 1000}.defaulted()

	assert.EqualValuesf(t, 1000, s.MaxConcurrentStreams,
		"caller value lost: got %d, want 1000", s.MaxConcurrentStreams)
}

func TestConnOptions_Defaulted_FillsAllFields(t *testing.T) {
	o := ConnOptions{}.defaulted()

	assert.EqualValuesf(t, 8, o.StreamEventBuffer, "StreamEventBuffer = %d, want 8", o.StreamEventBuffer)
	assert.EqualValuesf(t, 100, o.Settings.MaxConcurrentStreams,
		"nested settings default not applied: %d", o.Settings.MaxConcurrentStreams)
	// == nil, not assert.NotNil: Dialer is an interface, and a reflective check
	// would also accept an interface holding a nil pointer.
	assert.False(t, o.Dialer == nil, "Dialer not defaulted")
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

	c, err := d.Dial(ctx, srv.Listener.Addr().String())

	require.NoErrorf(t, err, "Dial")
	tc, ok := c.(*tls.Conn)
	require.Truef(t, ok, "conn type = %T", c)
	got := tc.ConnectionState().NegotiatedProtocol
	assert.Equalf(t, "h2", got, "ALPN = %q, want h2", got)
	_ = c.Close()
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
	opts := ConnOptions{
		Dialer: &TLSDialer{Config: &tls.Config{
			RootCAs:    pool,
			ServerName: "example.com",
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, err := Dial(ctx, srv.Listener.Addr().String(), opts)

	require.NoErrorf(t, err, "Dial")
	defer c.Close()
}
