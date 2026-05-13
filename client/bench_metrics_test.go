package client_test

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func benchSetup(b *testing.B, hooks *client.Hooks) (*client.Client, func()) {
	b.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:    hooks,
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	return c, func() {
		_ = c.Close()
		srv.Close()
	}
}

// BenchmarkDo_NoHooks measures Do overhead with no hooks set.
// Gate: ≤ 10 allocs/op (D.1 target). Run: make bench.
func BenchmarkDo_NoHooks(b *testing.B) {
	c, cleanup := benchSetup(b, nil)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()
	var resp client.Response
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp.Reset()
		if err := c.Do(ctx, req, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDo_WithHooks measures Do overhead with all request hooks set.
// Gate: ≤ 10 allocs/op (D.1 target). Run: make bench.
func BenchmarkDo_WithHooks(b *testing.B) {
	hooks := &client.Hooks{
		OnRequestStart:    func(client.RequestStartEvent) {},
		OnRequestComplete: func(client.RequestCompleteEvent) {},
	}
	c, cleanup := benchSetup(b, hooks)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()
	var resp client.Response
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp.Reset()
		if err := c.Do(ctx, req, &resp); err != nil {
			b.Fatal(err)
		}
	}
}
