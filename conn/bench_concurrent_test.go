package conn

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// BenchmarkConn_Roundtrip_Concurrent stresses the single-connection write path
// — wmu serialization plus HPACK EncodeBlock under that lock — with many
// streams in flight at once. This is the load-generator-shaped workload that
// the sequential BenchmarkConn_Roundtrip_Empty does not exercise: there, wmu is
// never contended. Run with -mutexprofile to quantify wmu/fcOutMu contention
// and -cpuprofile to see where the per-request time actually goes.
//
//	go test ./conn/ -run=^$ -bench=BenchmarkConn_Roundtrip_Concurrent \
//	  -benchmem -cpuprofile=cpu.out -mutexprofile=mutex.out -benchtime=2s
func BenchmarkConn_Roundtrip_Concurrent(b *testing.B) {
	c, teardown := benchSetup(b)
	defer teardown()
	hdrs := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	// RunParallel shares the single Conn across GOMAXPROCS goroutines, so every
	// SendHeaders contends for wmu — the real per-connection throughput ceiling.
	// Fatalf is illegal off the test goroutine; use Errorf + return.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := c.NewStream(ctx)
			if err != nil {
				b.Errorf("NewStream: %v", err)
				return
			}
			if err := s.SendHeaders(ctx, hdrs, true); err != nil {
				b.Errorf("SendHeaders: %v", err)
				return
			}
			for {
				ev, err := s.Recv(ctx)
				if err != nil {
					b.Errorf("Recv: %v", err)
					return
				}
				if ev.EndStream {
					break
				}
			}
		}
	})
}
