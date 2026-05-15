package conn

import (
	"context"
	"net"
	"net/http"
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
	addr := startH2CServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
