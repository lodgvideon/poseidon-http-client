package http3

import (
	"fmt"
	"testing"
)

// Receive-path allocation benchmarks.
//
// The repository had no instrument for this. client/bench_h3_alloc_test.go
// substitutes a fake h3Client and never runs http3.Client at all, so it measures
// the client/ adapter and nothing below it — which is why the profile in issue
// #342 came from an out-of-tree driver, and why one of the three sites that
// profile named (AppendData) could go stale without anything noticing.
//
// These drive the real Feed / ReadFrame / dispatchFrame trio at QUIC packet
// granularity, with no test harness inside the measurement: fakeStream.Recv
// coalesces chunks with an append of its own, so a bench routed through fakeConn
// would charge the harness's allocation to the code under test.
//
// Read B/op and allocs/op. ns/op here is dominated by memmove and is not the
// point — the issue is about bytes allocated, i.e. GC pressure.
//
// Burst size is the variable that matters. A response whose frames complete
// inside one burst never climbs the append growth ladder; one that dribbles in
// at ~1200 bytes (a QUIC packet) climbs a rung per doubling, and the
// amplification over the body's own size is what these expose.

var sinkBody []byte

// feedResponse plays a whole response wire image through the reader in bursts of
// burst bytes, dispatching every frame that completes — the loop consumeFrames
// runs, minus the QUIC transport.
func feedResponse(b *testing.B, wire []byte, wantBody, burst int) {
	b.Helper()
	c := &Client{}
	for i := 0; i < b.N; i++ {
		var fr FrameReader
		fr.SetMaxFrameLen(maxResponseBytes)
		rb := &respBuilder{resp: &Response{Status: 200}} // onData nil: the buffered Do path
		for off := 0; off < len(wire); off += burst {
			end := off + burst
			if end > len(wire) {
				end = len(wire)
			}
			fr.Feed(wire[off:end])
			for {
				typ, payload, err := fr.ReadFrame()
				if err != nil {
					break
				}
				if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
					b.Fatalf("dispatchFrame: %v", derr)
				}
			}
		}
		if len(rb.body) != wantBody {
			b.Fatalf("accumulated %d body bytes, want %d", len(rb.body), wantBody)
		}
		sinkBody = rb.body
	}
}

// BenchmarkRecvPath_BufferedBody measures the buffered Do receive path, where the
// body travels FrameReader.buf -> respBuilder.body. That is two of the copies
// issue #342 is about; the third, in quic recvStream, is below this layer.
func BenchmarkRecvPath_BufferedBody(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 16 << 10, 64 << 10} {
		for _, burst := range []int{1200, 1 << 30} { // one QUIC packet, then all at once
			b.Run(fmt.Sprintf("body=%d/burst=%s", size, burstName(burst)), func(b *testing.B) {
				wire := AppendData(nil, make([]byte, size))
				b.ReportAllocs()
				b.ResetTimer()
				feedResponse(b, wire, size, burst)
			})
		}
	}
}

// BenchmarkRecvPath_BodyAcrossFrames covers the multi-DATA-frame shape, where the
// body cannot be adopted from a single payload and must accumulate.
func BenchmarkRecvPath_BodyAcrossFrames(b *testing.B) {
	const chunk = 8 << 10
	for _, frames := range []int{2, 8} {
		b.Run(fmt.Sprintf("frames=%d", frames), func(b *testing.B) {
			var wire []byte
			for i := 0; i < frames; i++ {
				wire = AppendData(wire, make([]byte, chunk))
			}
			b.ReportAllocs()
			b.ResetTimer()
			feedResponse(b, wire, frames*chunk, 1200)
		})
	}
}

func burstName(n int) string {
	if n >= 1<<30 {
		return "whole"
	}
	return fmt.Sprintf("%d", n)
}
