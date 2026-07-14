package client_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// benchStreamSetup starts a real HTTP/2 server that returns a fixed small body and
// returns a poseidon client pointed at it. Used by the H2 streaming alloc
// benchmarks below, which guard the zero-alloc hot path against regressions when
// the response-body reader seam is generalized for HTTP/3 (the *conn.Stream field
// becomes an interface). These benchmarks use only the public API, so they run
// identically on origin/main for a baseline comparison.
func benchStreamSetup(b *testing.B) (*client.Client, func()) {
	b.Helper()
	body := []byte("streaming-body-payload")
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	c, err := client.NewClient(client.ClientOptions{
		Addr:     srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	return c, func() {
		_ = c.Close()
		srv.Close()
	}
}

// BenchmarkDoStream_H2 measures allocations on the HTTP/2 DoStream hot path: open a
// stream, read the head, drain the DATA events to EndStream, and close. The seam
// generalization for HTTP/3 must NOT add allocs/op here — compare against
// origin/main. Run: go test ./client/ -run '^$' -bench BenchmarkDoStream_H2 -benchmem.
func BenchmarkDoStream_H2(b *testing.B) {
	c, cleanup := benchStreamSetup(b)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()
	var sr client.StreamResponse
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := c.DoStream(ctx, req, &sr); err != nil {
			b.Fatal(err)
		}
		for {
			ev, err := sr.Recv(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if ev.EndStream {
				break
			}
		}
		if err := sr.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBodyStream_H2 measures allocations on the HTTP/2 Do(BodyMode=BodyStream)
// hot path: issue the request, drain Response.BodyReader to EOF, and close. The
// seam generalization for HTTP/3 must NOT add allocs/op here — compare against
// origin/main.
func BenchmarkBodyStream_H2(b *testing.B) {
	c, cleanup := benchStreamSetup(b)
	defer cleanup()
	req := &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}
	ctx := context.Background()
	var resp client.Response
	buf := make([]byte, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp.Reset()
		if err := c.Do(ctx, req, &resp); err != nil {
			b.Fatal(err)
		}
		for {
			_, err := resp.BodyReader.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := resp.BodyReader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
