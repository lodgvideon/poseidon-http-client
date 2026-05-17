package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func newTrailerH2Server(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.StartTLS()
	t.Cleanup(s.Close)
	addr := strings.TrimPrefix(s.URL, "https://")
	return s, addr
}

func trailerClientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDoStream_WaitTrailers_None(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204) // no body, no trailers
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		t.Fatalf("WaitTrailers: %v", err)
	}
	if trailers != nil {
		t.Fatalf("expected nil trailers, got %v", trailers)
	}
}
