package client

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startDualProtoTLSServer starts a TLS server offering BOTH "h2" and "http/1.1"
// over ALPN — the ordinary HTTPS origin, and the one where a dialer that offers
// h2 gets h2 and leaves an HTTP/1.1 transport writing into an HTTP/2 connection.
// It echoes the protocol it served the request over in the x-proto header.
func startDualProtoTLSServer(t *testing.T) (addr string, clientCfg *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-proto", r.Proto)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.EnableHTTP2 = true
	// httptest offers only "h2" when EnableHTTP2 is set; pin both so the ALPN
	// offer the client makes is what decides the protocol.
	srv.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.NextProtos = nil
	return srv.Listener.Addr().String(), cfg
}

// TestH1TLSDialer_PoolRequestOverDualProtoServer is the end-to-end regression
// test for the reported bug: TransportH1Pool against a server that also offers
// h2 used to fail every request with "read status line: EOF". With
// conn.H1TLSDialer the exchange completes over HTTP/1.1.
func TestH1TLSDialer_PoolRequestOverDualProtoServer(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoTLSServer(t)

	c, err := NewH1PoolClient(addr, &conn.H1TLSDialer{Config: cfg}, PoolOptions{MaxConnsPerHost: 2})
	require.NoError(t, err, "NewH1PoolClient")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := &Response{}

	err = c.Do(ctx, GET("/"), resp)

	require.NoError(t, err, "Do over a dual-protocol origin with the HTTP/1.1 dialer")
	require.Equal(t, 200, resp.Status, "the exchange must complete, not fail on framing")
	got, ok := resp.HeaderString("x-proto")
	require.True(t, ok, "the origin did not echo x-proto, so the protocol it served is unknown")
	assert.Equal(t, "HTTP/1.1", got,
		"the origin served %q; an HTTP/1.1 transport that gets h2 writes HTTP/1.1 into an "+
			"HTTP/2 connection and every request fails with \"read status line: EOF\"", got)
}

// TestNewClient_H1TransportRejectsH2Dialer verifies the pairing that produced
// the silent failure is refused at construction, for every H1 transport.
func TestNewClient_H1TransportRejectsH2Dialer(t *testing.T) {
	t.Parallel()
	h2Dialer := &conn.TLSDialer{Config: &tls.Config{
		ServerName: "example.com",
		MinVersion: tls.VersionTLS12,
	}}
	cases := []struct {
		name string
		opts ClientOptions
	}{
		{"H1SingleConn", ClientOptions{
			Addr:      "example.com:443",
			Transport: TransportH1SingleConn,
			ConnOpts:  conn.ConnOptions{Dialer: h2Dialer},
		}},
		{"H1Pool", ClientOptions{
			Addr:      "example.com:443",
			Transport: TransportH1Pool,
			Pool:      &PoolOptions{MaxConnsPerHost: 4},
			ConnOpts:  conn.ConnOptions{Dialer: h2Dialer},
		}},
		{"H1Managed", ClientOptions{
			Transport: TransportH1Managed,
			Resolver:  StaticResolver(Address{Host: "example.com", Port: 443}),
			ConnOpts:  conn.ConnOptions{Dialer: h2Dialer},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.opts)

			if err == nil {
				_ = c.Close()
			}
			require.Error(t, err, "NewClient accepted an h2-asserting dialer on an HTTP/1.1 transport")
			assert.ErrorIs(t, err, ErrALPNProtocolMismatch,
				"the refusal must be classifiable as an ALPN mismatch, not an opaque option error")
		})
	}
}

// TestNewClient_H2TransportRejectsH1Dialer verifies the mirror pairing — an
// HTTP/2 transport handed the HTTP/1.1 dialer — is refused too.
func TestNewClient_H2TransportRejectsH1Dialer(t *testing.T) {
	t.Parallel()
	h1Dialer := &conn.H1TLSDialer{Config: &tls.Config{
		ServerName: "example.com",
		MinVersion: tls.VersionTLS12,
	}}
	for _, kind := range []TransportKind{TransportSingleConn, TransportPool} {
		opts := ClientOptions{
			Addr:      "example.com:443",
			Transport: kind,
			ConnOpts:  conn.ConnOptions{Dialer: h1Dialer},
		}
		if kind == TransportPool {
			opts.Pool = &PoolOptions{MaxConnsPerHost: 2}
		}
		c, err := NewClient(opts)

		if err == nil {
			_ = c.Close()
		}
		require.Errorf(t, err, "transport %d accepted an http/1.1-asserting dialer", kind)
		assert.ErrorIsf(t, err, ErrALPNProtocolMismatch,
			"transport %d: the refusal must be classifiable as an ALPN mismatch", kind)
	}
}

// TestNewClient_AcceptsUnassertedDialers verifies the check only fires on a
// dialer that declares a protocol: FlexDialer and PlaintextDialer stay usable
// with the H1 transports, and TransportALPN accepts anything.
func TestNewClient_AcceptsUnassertedDialers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts ClientOptions
	}{
		{"H1+Plaintext", ClientOptions{
			Addr:      "example.com:80",
			Transport: TransportH1SingleConn,
			ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		}},
		{"H1+Flex", ClientOptions{
			Addr:      "example.com:443",
			Transport: TransportH1SingleConn,
			ConnOpts:  conn.ConnOptions{Dialer: &conn.FlexDialer{}},
		}},
		{"ALPN+TLSDialer", ClientOptions{
			Addr:      "example.com:443",
			Transport: TransportALPN,
			ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		}},
		{"H2+TLSDialer", ClientOptions{
			Addr:      "example.com:443",
			Transport: TransportSingleConn,
			ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.opts)

			require.NoError(t, err,
				"a dialer that asserts no protocol must stay usable; over-rejecting here "+
					"breaks plaintext and flexible dialers that negotiate at dial time")
			_ = c.Close()
		})
	}
}

// TestH1SingleConn_RefusesH2NegotiatedConn is the dial-time backstop: a dialer
// that makes no ALPN assertion (so NewClient cannot check it) but comes back
// with an h2 connection must fail the dial naming the protocol, not proceed to
// write HTTP/1.1 requests into an HTTP/2 connection.
func TestH1SingleConn_RefusesH2NegotiatedConn(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoTLSServer(t)

	// FlexDialer offers h2 + http/1.1; the server prefers h2.
	c, err := NewH1Client(addr, &conn.FlexDialer{Config: cfg})
	require.NoError(t, err, "NewH1Client")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Do(ctx, GET("/"), &Response{})

	require.Error(t, err, "Do succeeded over an h2-negotiated connection")
	assert.ErrorIs(t, err, ErrALPNProtocolMismatch,
		"the dial must fail naming the protocol rather than writing HTTP/1.1 into an HTTP/2 connection")
}

// TestH1Pool_RefusesH2NegotiatedConn is the same backstop on the pooled path.
func TestH1Pool_RefusesH2NegotiatedConn(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoTLSServer(t)

	c, err := NewH1PoolClient(addr, &conn.FlexDialer{Config: cfg}, PoolOptions{MaxConnsPerHost: 2})
	require.NoError(t, err, "NewH1PoolClient")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Do(ctx, GET("/"), &Response{})

	require.Error(t, err, "Do succeeded over an h2-negotiated connection")
	assert.ErrorIs(t, err, ErrALPNProtocolMismatch,
		"the pooled dial must fail naming the protocol rather than pooling an HTTP/2 connection")
}
