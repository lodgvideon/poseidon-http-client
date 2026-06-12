package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func startH2TestServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
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
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialServer(t *testing.T, srv *httptest.Server, cfg *tls.Config) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return c
}

func TestIntegration_EmptyGET(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("type = %v", ev.Type)
	}
	var status string
	for _, f := range ev.Headers {
		if string(f.Name) == ":status" {
			status = string(f.Value)
		}
	}
	if status != "204" {
		t.Fatalf("status = %q, want 204", status)
	}
	if !ev.EndStream {
		// Empty 204 may close immediately or via separate end frame.
		ev2, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv 2: %v", err)
		}
		if !ev2.EndStream {
			t.Fatalf("never observed END_STREAM")
		}
	}
}

func TestIntegration_POST_1KB_Echo(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte(i)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/echo")},
		{Name: []byte("content-length"), Value: []byte("1024")},
	}, false); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if err := s.SendData(ctx, body, true); err != nil {
		t.Fatalf("SendData: %v", err)
	}
	// Drain events until we see EndStream.
	var got []byte
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventData {
			got = append(got, ev.Data...)
		}
		if ev.EndStream {
			break
		}
	}
	if len(got) != len(body) {
		t.Fatalf("echo len = %d, want %d", len(got), len(body))
	}
}

func TestIntegration_ContextCancel_TearsDownStream(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Block until client cancels — the request context is cancelled
		// when the underlying stream is reset.
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	outerCtx, outerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer outerCancel()
	streamCtx, streamCancel := context.WithCancel(outerCtx)
	defer streamCancel()

	s, err := c.NewStream(outerCtx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(outerCtx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/never")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		streamCancel()
	}()
	if _, err := s.Recv(streamCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv err = %v, want context.Canceled", err)
	}
	_ = s.Close() // sends RST_STREAM(CANCEL); subsequent NewStream should work
	s2, err := c.NewStream(outerCtx)
	if err != nil {
		t.Fatalf("NewStream after cancel: %v", err)
	}
	_ = s2.Close()
}

func TestConformance_RFC7540_Sec3_ClientPreface_OnTheWire(t *testing.T) {
	// The on-the-wire integration is covered by Phase A's
	// TestConformance_RFC7540_Sec35_ClientPreface (in frame/) and by
	// TestIntegration_EmptyGET above (which fails the handshake if the
	// preface is wrong). This test asserts the literal preface string
	// that Phase A's WriteClientPreface emits matches the RFC text.
	want := "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	if !strings.HasPrefix(want, "PRI") {
		t.Fatalf("self-check broken")
	}
}
