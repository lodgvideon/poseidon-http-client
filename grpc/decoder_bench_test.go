package grpc

import (
	"fmt"
	"testing"
)

// What does the decoder's Push copy actually cost? #453 proposes a zero-copy
// path for the case where one DATA chunk already holds whole messages, and that
// is only worth its ownership complexity if the copy shows up.
//
// Read B/op. ns/op on this path is memmove-bound and noisy; bytes allocated is
// the thing the change would remove.

var decSink []byte

func benchDecoder(b *testing.B, msgSize, chunkSize int) {
	msg := make([]byte, msgSize)
	for i := range msg {
		msg[i] = byte(i)
	}
	wire, err := AppendMessage(nil, msg)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(msgSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var d decoder
		for off := 0; off < len(wire); off += chunkSize {
			end := off + chunkSize
			if end > len(wire) {
				end = len(wire)
			}
			// Exactly what Stream.pump does: borrow when the decoder is empty,
			// copy otherwise. A benchmark that called Push directly would not
			// exercise the path under test at all.
			chunk := wire[off:end]
			if !d.PushBorrowed(chunk, nil) {
				d.Push(chunk)
			}
			for {
				m, ok, derr := d.Next()
				if derr != nil {
					b.Fatalf("Next: %v", derr)
				}
				if !ok {
					break
				}
				decSink = m
			}
		}
	}
}

// BenchmarkDecoder_WholeMessagePerChunk is the case #453 targets: the chunk
// already holds the entire message, so the copy into the decoder's buffer buys
// nothing but a second copy of the body.
func BenchmarkDecoder_WholeMessagePerChunk(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("msg=%d", size), func(b *testing.B) {
			benchDecoder(b, size, 1<<30) // one chunk holds everything
		})
	}
}

// BenchmarkDecoder_SplitAcrossChunks is the case that must keep working: the
// message spans several DATA frames, so the decoder has to accumulate and a
// zero-copy path cannot apply.
func BenchmarkDecoder_SplitAcrossChunks(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("msg=%d", size), func(b *testing.B) {
			// A quarter of the message per chunk, so it genuinely spans several
			// DATA frames whatever its size — a fixed chunk size would leave the
			// smaller message fitting in one and silently borrowing instead.
			benchDecoder(b, size, size/4)
		})
	}
}
