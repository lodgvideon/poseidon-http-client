package client

// BenchmarkDo_H3 measures client-side allocations on the buffered HTTP/3 Do hot
// path. It drives Client.Do over the TransportH3 adapter (h3Exchange) against a
// zero-alloc fake h3Client seam, so b.ReportAllocs() reflects only the
// adapter + drainResponse allocations, not a live QUIC/QPACK stack. This mirrors
// BenchmarkDo_MockTransport (the H2 buffered-Do alloc bench) and guards the H3
// per-request allocation count as a permanent gate.
//
// Run with:
//
//	go test -run='^$' -bench=BenchmarkDo_H3 -benchmem ./client/...

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// benchH3Client is a zero-alloc test double for the buffered H3 Do path. Unlike
// fakeH3Client it does NOT deep-copy the request, so no per-call allocation
// pollutes the measurement — b.ReportAllocs() then isolates the h3Exchange
// adapter's own per-request allocations.
type benchH3Client struct {
	resp *http3.Response
	body []byte
}

func (f *benchH3Client) Do(context.Context, *http3.Request) (*http3.Response, []byte, error) {
	return f.resp, f.body, nil
}

func (f *benchH3Client) DoStream(context.Context, *http3.Request) (*http3.Response, http3.ResponseBody, error) {
	return f.resp, nil, nil
}

func (f *benchH3Client) Alive() bool     { return true }
func (f *benchH3Client) GoingAway() bool { return false }
func (f *benchH3Client) Close() error    { return nil }

// newBenchH3Client wires a TransportH3 Client whose dialer yields fake instead of
// a live QUIC connection.
func newBenchH3Client(b *testing.B, fake h3Client) *Client {
	b.Helper()
	c, err := NewClient(ClientOptions{
		Addr:      "h3.example:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h3.example"},
	})
	if err != nil {
		b.Fatalf("NewClient(TransportH3): %v", err)
	}
	sc, ok := c.tr.(*singleH3Conn)
	if !ok {
		b.Fatalf("transport is %T, want *singleH3Conn", c.tr)
	}
	sc.dialFn = func(context.Context, string, *tls.Config) (h3Client, error) { return fake, nil }
	return c
}

// BenchmarkDo_H3 benchmarks a buffered GET round-trip over the HTTP/3 adapter.
//
// The Response is declared once outside the loop and Reset between calls, exactly
// like BenchmarkDo_MockTransport, so the bench is not charged Response-init allocs
// a real reusing caller does not pay.
func BenchmarkDo_H3(b *testing.B) {
	fake := &benchH3Client{
		resp: &http3.Response{
			Status:  200,
			Headers: []conn.HeaderField{{Name: []byte("content-type"), Value: []byte("text/plain")}},
		},
		body: []byte("hello h3 body payload"),
	}
	c := newBenchH3Client(b, fake)
	b.Cleanup(func() { _ = c.Close() })

	req := &Request{Method: "GET", Path: "/", Authority: "h3.example", BodyMode: BodyBuffer}
	ctx := context.Background()

	var res Response
	res.Reset() // preallocate Headers/Trailers/blocks backing

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res.Reset()
		if err := c.Do(ctx, req, &res); err != nil {
			b.Fatalf("Do: %v", err)
		}
	}
}
