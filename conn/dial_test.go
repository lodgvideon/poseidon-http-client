package conn

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xhttp2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// startH2CServer starts an H2C (cleartext HTTP/2) server on a random
// port and returns the "host:port" address. The server is shut down
// when the test ends.
func startH2CServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{
		Handler:           h2c.NewHandler(handler, &xhttp2.Server{}),
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
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte(addr)},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	var status string
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
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
	if status != "200" {
		t.Fatalf("status = %q, want %q", status, "200")
	}
}

// TestNegotiatedProtocol_PlainConn verifies NegotiatedProtocol returns ""
// for a non-TLS connection.
func TestNegotiatedProtocol_PlainConn(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close(); _ = c2.Close() }()
	if got := NegotiatedProtocol(c1); got != "" {
		t.Errorf("NegotiatedProtocol(plain) = %q, want %q", got, "")
	}
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
	if err != nil {
		t.Fatalf("FlexDialer.Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()

	proto := NegotiatedProtocol(nc)
	if proto != "h2" && proto != "http/1.1" {
		t.Errorf("NegotiatedProtocol = %q, want h2 or http/1.1", proto)
	}
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
	base := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	tlsCfg := base
	tlsCfg.NextProtos = nil // FlexDialer must prepend h2 and http/1.1
	tlsCfg.MinVersion = 0   // FlexDialer must set TLS 1.2 minimum
	fd := &FlexDialer{Config: tlsCfg}
	nc, err := fd.Dial(ctx, srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("FlexDialer.Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()

	proto := NegotiatedProtocol(nc)
	if proto != "h2" && proto != "http/1.1" {
		t.Errorf("NegotiatedProtocol = %q, want h2 or http/1.1", proto)
	}
}
