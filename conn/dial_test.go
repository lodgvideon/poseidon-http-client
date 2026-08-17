package conn

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/require"
)

// startH2CServer starts an H2C (cleartext HTTP/2) server on a random
// port and returns the "host:port" address. The server is shut down
// when the test ends.
func startH2CServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoErrorf(t, err, "listen")
	// HTTP/1.1 and cleartext HTTP/2 on one listener, as x/net's h2c.NewHandler
	// used to do for us. That package is deprecated in favour of this field.
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           handler,
		Protocols:         protos,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func TestPlaintextDialer_H2C(t *testing.T) {
	addr := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr, ConnOptions{Dialer: &PlaintextDialer{}})
	require.NoErrorf(t, err, "Dial")
	defer c.Close()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")

	err = s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte(addr)},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true)

	require.NoErrorf(t, err, "SendHeaders")
	var status string
	for {
		ev, rerr := s.Recv(ctx)
		require.NoErrorf(t, rerr, "Recv")
		if ev.Type == EventHeaders {
			for _, f := range ev.Headers {
				if string(f.Name) == ":status" {
					status = string(f.Value)
				}
			}
		}
		if ev.EndStream {
			break
		}
	}
	require.Equalf(t, "200", status, "status = %q, want %q", status, "200")
}

// TestNegotiatedProtocol_PlainConn verifies NegotiatedProtocol returns ""
// for a non-TLS connection.
func TestNegotiatedProtocol_PlainConn(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close(); _ = c2.Close() }()

	got := NegotiatedProtocol(c1)

	require.Emptyf(t, got, "NegotiatedProtocol(plain) = %q, want %q", got, "")
}

// TestFlexDialer_NegotiatedProtocol verifies NegotiatedProtocol returns the
// ALPN-negotiated protocol string for a *tls.Conn.
func TestFlexDialer_NegotiatedProtocol(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tlsCfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	fd := &FlexDialer{Config: tlsCfg}

	nc, err := fd.Dial(ctx, srv.Listener.Addr().String())

	require.NoErrorf(t, err, "FlexDialer.Dial")
	defer func() { _ = nc.Close() }()
	proto := NegotiatedProtocol(nc)
	require.Containsf(t, []string{"h2", "http/1.1"}, proto,
		"NegotiatedProtocol = %q, want h2 or http/1.1", proto)
}

// TestFlexDialer_PrependProtos verifies that FlexDialer prepends h2 and
// http/1.1 when the caller's config has no NextProtos, and also exercises
// the MinVersion == 0 branch.
func TestFlexDialer_PrependProtos(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Use a config with no NextProtos and MinVersion=0 to trigger both
	// prepend branches and the MinVersion patch.
	tlsCfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	tlsCfg.NextProtos = nil // FlexDialer must prepend h2 and http/1.1
	tlsCfg.MinVersion = 0   // FlexDialer must set TLS 1.2 minimum
	fd := &FlexDialer{Config: tlsCfg}

	nc, err := fd.Dial(ctx, srv.Listener.Addr().String())

	require.NoErrorf(t, err, "FlexDialer.Dial")
	defer func() { _ = nc.Close() }()
	proto := NegotiatedProtocol(nc)
	require.Containsf(t, []string{"h2", "http/1.1"}, proto,
		"NegotiatedProtocol = %q, want h2 or http/1.1", proto)
}
