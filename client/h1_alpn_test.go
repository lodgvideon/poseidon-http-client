package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	if err != nil {
		t.Fatalf("NewH1PoolClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := &Response{}
	if err := c.Do(ctx, GET("/"), resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	if got, ok := resp.HeaderString("x-proto"); !ok || got != "HTTP/1.1" {
		t.Fatalf("server served %q (present=%v), want HTTP/1.1", got, ok)
	}
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
				t.Fatal("NewClient accepted an h2-asserting dialer on an HTTP/1.1 transport")
			}
			if !errors.Is(err, ErrALPNProtocolMismatch) {
				t.Fatalf("err = %v, want ErrALPNProtocolMismatch", err)
			}
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
			t.Fatalf("transport %d accepted an http/1.1-asserting dialer", kind)
		}
		if !errors.Is(err, ErrALPNProtocolMismatch) {
			t.Fatalf("transport %d: err = %v, want ErrALPNProtocolMismatch", kind, err)
		}
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
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
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
	if err != nil {
		t.Fatalf("NewH1Client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Do(ctx, GET("/"), &Response{})
	if err == nil {
		t.Fatal("Do succeeded over an h2-negotiated connection")
	}
	if !errors.Is(err, ErrALPNProtocolMismatch) {
		t.Fatalf("err = %v, want ErrALPNProtocolMismatch", err)
	}
}

// TestH1Pool_RefusesH2NegotiatedConn is the same backstop on the pooled path.
func TestH1Pool_RefusesH2NegotiatedConn(t *testing.T) {
	t.Parallel()
	addr, cfg := startDualProtoTLSServer(t)

	c, err := NewH1PoolClient(addr, &conn.FlexDialer{Config: cfg}, PoolOptions{MaxConnsPerHost: 2})
	if err != nil {
		t.Fatalf("NewH1PoolClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = c.Do(ctx, GET("/"), &Response{})
	if err == nil {
		t.Fatal("Do succeeded over an h2-negotiated connection")
	}
	if !errors.Is(err, ErrALPNProtocolMismatch) {
		t.Fatalf("err = %v, want ErrALPNProtocolMismatch", err)
	}
}
