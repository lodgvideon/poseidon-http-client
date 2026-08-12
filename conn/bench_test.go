package conn

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// benchSetup dials the in-process benchPeer (see bench_peer_test.go for why it
// is not httptest) over plaintext h2c.
//
// TLS is deliberately gone. The old harness dialed httptest.StartTLS, which put
// a TLS handshake's allocations into the first iterations and crypto/tls's
// per-record work into every one of them — none of it this package's. What is
// left here is conn talking to a peer that allocates nothing per request, which
// is the only shape in which B/op means "what conn costs".
func benchSetup(b *testing.B) (*Conn, func()) {
	b.Helper()
	p := newBenchPeer(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, p.addr(), ConnOptions{Dialer: &PlaintextDialer{}})
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	return c, func() { _ = c.Close() }
}

func BenchmarkConn_Roundtrip_Empty(b *testing.B) {
	c, teardown := benchSetup(b)
	defer teardown()
	hdrs := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			b.Fatalf("NewStream: %v", err)
		}
		if err := s.SendHeaders(ctx, hdrs, true); err != nil {
			b.Fatalf("SendHeaders: %v", err)
		}
		// benchDrain closes the stream and returns the pooled buffers. Reading
		// to EndStream and walking away — what this loop used to do — leaves
		// both pools missing on every iteration.
		if err := benchDrain(ctx, s); err != nil {
			b.Fatalf("drain: %v", err)
		}
	}
}
