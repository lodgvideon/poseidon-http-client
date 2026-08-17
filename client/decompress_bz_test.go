package client

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// gzipCompress compresses raw with the stdlib gzip writer.
func gzipCompress(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(raw)
	require.NoErrorf(t, err, "gzip write: %v", err)
	err = gw.Close()
	require.NoErrorf(t, err, "gzip close: %v", err)
	return buf.Bytes()
}

// zlibCompress compresses raw with the stdlib zlib writer (the "deflate"
// content coding as servers actually emit it).
func zlibCompress(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	_, err := zw.Write(raw)
	require.NoErrorf(t, err, "zlib write: %v", err)
	err = zw.Close()
	require.NoErrorf(t, err, "zlib close: %v", err)
	return buf.Bytes()
}

// brotliCompress compresses raw with the reference Brotli writer.
func brotliCompress(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	bw := brotli.NewWriter(&buf)
	_, err := bw.Write(raw)
	require.NoErrorf(t, err, "brotli write: %v", err)
	err = bw.Close()
	require.NoErrorf(t, err, "brotli close: %v", err)
	return buf.Bytes()
}

// zstdCompress compresses raw with the reference zstd writer.
func zstdCompress(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1))
	require.NoErrorf(t, err, "zstd writer: %v", err)
	_, err = zw.Write(raw)
	require.NoErrorf(t, err, "zstd write: %v", err)
	err = zw.Close()
	require.NoErrorf(t, err, "zstd close: %v", err)
	return buf.Bytes()
}

// ── detectEncoding ───────────────────────────────────────────────

func TestDetectEncoding_BrotliZstd(t *testing.T) {
	ce := func(v string) []conn.HeaderField {
		return []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte(v)}}
	}
	cases := []struct {
		name    string
		headers []conn.HeaderField
		want    ContentEncoding
	}{
		{"br", ce("br"), EncodingBrotli},
		{"zstd", ce("zstd"), EncodingZstd},
		{"br-uppercase", ce("BR"), EncodingBrotli},
		{"zstd-mixedcase", ce("Zstd"), EncodingZstd},
		{"gzip-uppercase", ce("GZIP"), EncodingGzip},
		{"unknown", ce("compress"), EncodingIdentity},
		{"brotli-longform-not-br", ce("brotli"), EncodingIdentity},
		{"absent", nil, EncodingIdentity},
		{"empty-value", ce(""), EncodingIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := detectEncoding(tc.headers)

			assert.Equalf(t, tc.want, got, "detectEncoding = %v, want %v", got, tc.want)
		})
	}
}

// TestContentEncoding_ConstantsStable pins the wire-visible iota values.
// EncodingIdentity/Gzip/Deflate are exported and predate this change; the
// new codings must append, never renumber.
func TestContentEncoding_ConstantsStable(t *testing.T) {
	cases := []struct {
		enc  ContentEncoding
		want int
	}{
		{EncodingIdentity, 0},
		{EncodingGzip, 1},
		{EncodingDeflate, 2},
		{EncodingBrotli, 3},
		{EncodingZstd, 4},
	}

	for _, tc := range cases {
		assert.Equalf(t, tc.want, int(tc.enc), "ContentEncoding = %d, want %d", int(tc.enc), tc.want)
	}
}

// ── round trip: buffered path ────────────────────────────────────

func TestDecompressFully_BrotliZstd_RoundTrip(t *testing.T) {
	small := []byte("brotli and zstd round-trip payload")
	// Multi-MB, mixed compressible + incompressible so the decoder crosses
	// several blocks and window refills.
	large := make([]byte, 0, 4<<20)
	for i := 0; len(large) < 4<<20; i++ {
		large = append(large, []byte("chunk-")...)
		large = append(large, byte(i), byte(i>>8), byte(i>>16))
		large = append(large, []byte(" filler filler filler ")...)
	}

	for _, payload := range []struct {
		name string
		raw  []byte
	}{
		{"small", small},
		{"large-4MiB", large},
		{"empty", []byte{}},
		{"single-byte", []byte{0x00}},
	} {
		for _, enc := range []struct {
			name string
			enc  ContentEncoding
			comp func(testing.TB, []byte) []byte
		}{
			{"brotli", EncodingBrotli, brotliCompress},
			{"zstd", EncodingZstd, zstdCompress},
		} {
			t.Run(enc.name+"/"+payload.name, func(t *testing.T) {
				compressed := enc.comp(t, payload.raw)

				out, err := decompressFully(enc.enc, compressed, DefaultMaxDecompressedSize)

				require.NoErrorf(t, err, "decompressFully: %v", err)
				assert.Truef(t, bytes.Equal(out, payload.raw),
					"mismatch: got %d bytes, want %d", len(out), len(payload.raw))
			})
		}
	}
}

// ── round trip: streaming path ───────────────────────────────────

func TestNewDecompressingReader_BrotliZstd_RoundTrip(t *testing.T) {
	raw := bytes.Repeat([]byte("streaming decode payload "), 100_000) // ~2.5 MiB
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := io.NopCloser(bytes.NewReader(tc.comp(t, raw)))

			dr, err := newDecompressingReader(tc.enc, src)
			require.NoErrorf(t, err, "newDecompressingReader: %v", err)
			out, err := io.ReadAll(dr)
			require.NoErrorf(t, err, "ReadAll: %v", err)
			err = dr.Close()
			require.NoErrorf(t, err, "Close: %v", err)

			assert.Truef(t, bytes.Equal(out, raw), "mismatch: got %d bytes, want %d", len(out), len(raw))
			// Close must be idempotent: drainResponse and the caller can both
			// reach it, and a double Put would alias a pooled decoder.
			err = dr.Close()
			assert.NoErrorf(t, err, "second Close: %v", err)
		})
	}
}

// TestNewDecompressingReader_BrotliZstd_ClosesSource proves Close releases the
// underlying body, not just the decompressor.
func TestNewDecompressingReader_BrotliZstd_ClosesSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &countingCloser{Reader: bytes.NewReader(tc.comp(t, []byte("x")))}
			dr, err := newDecompressingReader(tc.enc, src)
			require.NoErrorf(t, err, "newDecompressingReader: %v", err)
			_, _ = io.ReadAll(dr)

			_ = dr.Close()

			assert.Equalf(t, 1, src.closes, "source closes = %d, want 1", src.closes)
			_ = dr.Close()
			assert.Equalf(t, 1, src.closes, "after second Close: source closes = %d, want 1", src.closes)
		})
	}
}

type countingCloser struct {
	io.Reader
	closes int
}

func (c *countingCloser) Close() error { c.closes++; return nil }

// probeReader samples the live goroutine count each time the decoder pulls
// from the source. Under an async decoder the source is read from the
// decoder's own pipeline goroutines, so they are counted here; under a
// synchronous decoder the read happens on the caller's goroutine and the
// count is unchanged.
type probeReader struct {
	rd    bytes.Reader
	peak  int
	reads int
}

func (p *probeReader) Read(b []byte) (int, error) {
	p.reads++
	if n := runtime.NumGoroutine(); n > p.peak {
		p.peak = n
	}
	return p.rd.Read(b)
}

func (p *probeReader) Close() error { return nil }

// nopCloserReader is a reusable io.ReadCloser over a byte slice. io.NopCloser
// allocates per call; this lets the streaming benchmarks attribute their
// allocations to the decode path.
type nopCloserReader struct{ rd bytes.Reader }

func (n *nopCloserReader) Reset(b []byte)             { n.rd.Reset(b) }
func (n *nopCloserReader) Read(p []byte) (int, error) { return n.rd.Read(p) }
func (n *nopCloserReader) Close() error               { return nil }

// ── decompression-bomb guard ─────────────────────────────────────

// bombLimit is small enough that a bomb trips it fast, large enough that a
// legitimate body of this size would be accepted.
const bombLimit = 1 << 20 // 1 MiB

// TestDecompressFully_BrotliZstd_BombGuard builds a real compression bomb:
// 256 MiB of zeroes compresses to a few KiB. Decoding it without a guard
// would allocate 256 MiB; the guard must reject it with ErrBodyTooLarge
// after buffering no more than ~maxBytes.
func TestDecompressFully_BrotliZstd_BombGuard(t *testing.T) {
	const bombSize = 256 << 20 // 256 MiB decompressed
	zeroes := make([]byte, bombSize)

	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bomb := tc.comp(t, zeroes)
			ratio := float64(bombSize) / float64(len(bomb))
			require.GreaterOrEqualf(t, ratio, 1000.0,
				"not a bomb: %d -> %d bytes (ratio %.0f), test is not exercising the guard",
				len(bomb), bombSize, ratio)
			t.Logf("%s bomb: %d compressed bytes -> %d decompressed (ratio %.0f:1)",
				tc.name, len(bomb), bombSize, ratio)

			out, err := decompressFully(tc.enc, bomb, bombLimit)

			require.Errorf(t, err, "bomb accepted: got %d bytes, want ErrBodyTooLarge", len(out))
			require.ErrorIsf(t, err, ErrBodyTooLarge, "err = %v, want ErrBodyTooLarge", err)
		})
	}
}

// TestDecompressFully_BrotliZstd_AtLimit pins the boundary: exactly maxBytes
// is accepted, one byte more is rejected. The guard reads maxBytes+1 to tell
// those apart.
func TestDecompressFully_BrotliZstd_AtLimit(t *testing.T) {
	const limit = 4096
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		t.Run(tc.name+"/exactly-at-limit", func(t *testing.T) {
			compressed := tc.comp(t, bytes.Repeat([]byte("a"), limit))

			out, err := decompressFully(tc.enc, compressed, limit)

			require.NoErrorf(t, err, "payload of exactly maxBytes rejected: %v", err)
			assert.Lenf(t, out, limit, "len = %d, want %d", len(out), limit)
		})
		t.Run(tc.name+"/one-over-limit", func(t *testing.T) {
			compressed := tc.comp(t, bytes.Repeat([]byte("a"), limit+1))

			_, err := decompressFully(tc.enc, compressed, limit)

			require.ErrorIsf(t, err, ErrBodyTooLarge, "err = %v, want ErrBodyTooLarge", err)
		})
	}
}

// ── zstd goroutine lifecycle (the pooling hazard) ────────────────

// goroutineBaseline returns the goroutine count after letting stragglers exit.
// It must be called from the same goroutine nesting as the later measurement:
// t.Run runs each subtest on its own goroutine, so a baseline taken in the
// parent reads one lower than a measurement taken inside the subtest.
func goroutineBaseline() int {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	return runtime.NumGoroutine()
}

// settleGoroutines waits for the goroutine count to fall to want, polling
// until timeout. Goroutines exit asynchronously, so a bare NumGoroutine()
// comparison taken immediately after the loop would be flaky; converging on
// the baseline keeps the assertion exact without the flake.
func settleGoroutines(want int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDecompress_Zstd_PooledNoGoroutineGrowth is the load-generator guard.
//
// zstd.NewReader with default options runs an async pipeline: Decoder.Reset
// spawns three goroutines per stream. Pooling such a decoder would multiply
// goroutines by the pool size and leak every one whose decoder is dropped
// without Close. Our pool builds decoders with WithDecoderConcurrency(1),
// which makes Reset take the synchronous path and spawn nothing.
//
// This test decompresses many times through both pooled paths and asserts the
// goroutine count does not grow. With the concurrency option removed it fails
// immediately (hundreds of leaked goroutines).
func TestDecompress_Zstd_PooledNoGoroutineGrowth(t *testing.T) {
	raw := bytes.Repeat([]byte("goroutine stability payload "), 20_000) // ~540 KiB
	compressed := zstdCompress(t, raw)

	// Warm the pool so first-use allocation is not counted as growth.
	for i := 0; i < 4; i++ {
		_, err := decompressFully(EncodingZstd, compressed, DefaultMaxDecompressedSize)
		require.NoErrorf(t, err, "warmup: %v", err)
	}

	const iterations = 200

	t.Run("buffered-path", func(t *testing.T) {
		base := goroutineBaseline()
		t.Logf("baseline goroutines: %d", base)

		for i := 0; i < iterations; i++ {
			out, err := decompressFully(EncodingZstd, compressed, DefaultMaxDecompressedSize)
			require.NoErrorf(t, err, "iter %d: %v", i, err)
			require.Lenf(t, out, len(raw), "iter %d: len = %d, want %d", i, len(out), len(raw))
		}
		got := settleGoroutines(base, 2*time.Second)

		assert.LessOrEqualf(t, got, base,
			"goroutines grew across %d pooled decompressions: %d -> %d", iterations, base, got)
	})

	t.Run("streaming-path", func(t *testing.T) {
		base := goroutineBaseline()
		t.Logf("baseline goroutines: %d", base)

		for i := 0; i < iterations; i++ {
			src := io.NopCloser(bytes.NewReader(compressed))
			dr, err := newDecompressingReader(EncodingZstd, src)
			require.NoErrorf(t, err, "iter %d: %v", i, err)
			_, err = io.Copy(io.Discard, dr)
			require.NoErrorf(t, err, "iter %d: copy: %v", i, err)
			err = dr.Close()
			require.NoErrorf(t, err, "iter %d: close: %v", i, err)
		}
		got := settleGoroutines(base, 2*time.Second)

		assert.LessOrEqualf(t, got, base,
			"goroutines grew across %d pooled stream decodes: %d -> %d", iterations, base, got)
	})

	// The subtests above prove nothing is *leaked*, but they pass even without
	// WithDecoderConcurrency(1), because Reset(nil) drains the async pipeline:
	// the count returns to baseline while still churning three goroutines per
	// request. This subtest closes that gap by measuring the live count from
	// inside the decode, where an async decoder's pipeline is running.
	//
	// It is also the only direct proof that the decode is synchronous: the
	// probe's Read runs on our own goroutine under concurrency 1, but on the
	// decoder's own goroutine under the async path.
	t.Run("no-goroutines-mid-decode", func(t *testing.T) {
		base := goroutineBaseline()
		t.Logf("baseline goroutines: %d", base)
		probe := &probeReader{}
		probe.rd.Reset(compressed)

		dr, err := newDecompressingReader(EncodingZstd, probe)
		require.NoErrorf(t, err, "newDecompressingReader: %v", err)
		n, err := io.Copy(io.Discard, dr)
		require.NoErrorf(t, err, "copy: %v", err)
		err = dr.Close()
		require.NoErrorf(t, err, "close: %v", err)

		require.Equalf(t, int64(len(raw)), n, "decoded %d bytes, want %d", n, len(raw))
		require.GreaterOrEqualf(t, probe.reads, 2,
			"source read %d times: payload too small to observe a running pipeline", probe.reads)
		t.Logf("peak goroutines during decode: %d (%d source reads)", probe.peak, probe.reads)
		assert.LessOrEqualf(t, probe.peak, base,
			"decode ran %d extra goroutines (peak %d, baseline %d): "+
				"the pooled decoder is not synchronous — is WithDecoderConcurrency(1) still set?",
			probe.peak-base, probe.peak, base)
	})

	// A decoder abandoned without Close (caller drops the body) must not
	// strand goroutines either — with concurrency 1 there are none to strand,
	// so the decoder is simply garbage.
	t.Run("abandoned-without-close", func(t *testing.T) {
		base := goroutineBaseline()
		t.Logf("baseline goroutines: %d", base)

		for i := 0; i < iterations; i++ {
			src := io.NopCloser(bytes.NewReader(compressed))
			dr, err := newDecompressingReader(EncodingZstd, src)
			require.NoErrorf(t, err, "iter %d: %v", i, err)
			// Read one byte, then abandon.
			var b [1]byte
			_, _ = dr.Read(b[:])
		}
		got := settleGoroutines(base, 2*time.Second)

		assert.LessOrEqualf(t, got, base,
			"goroutines stranded by %d abandoned decoders: %d -> %d", iterations, base, got)
	})
}

// TestDecompress_Zstd_PooledDecoderReusable proves the pooled decoder is
// returned in a reusable state. Decoder.Close permanently retires a decoder
// (every later Reset returns ErrDecoderClosed), so if Close ever leaked into
// the pool-return path, a recycled decoder would fail here.
func TestDecompress_Zstd_PooledDecoderReusable(t *testing.T) {
	payloads := [][]byte{
		[]byte("first"),
		bytes.Repeat([]byte("second payload "), 1000),
		[]byte(""),
		bytes.Repeat([]byte("fourth "), 50_000),
	}

	// Sequential decodes: each Get is likely to draw the decoder the previous
	// iteration returned, so a poisoned decoder surfaces immediately.
	for round := 0; round < 50; round++ {
		for i, raw := range payloads {
			out, err := decompressFully(EncodingZstd, zstdCompress(t, raw), DefaultMaxDecompressedSize)

			require.NoErrorf(t, err, "round %d payload %d: %v", round, i, err)
			require.Truef(t, bytes.Equal(out, raw),
				"round %d payload %d: mismatch (got %d bytes, want %d)", round, i, len(out), len(raw))
		}
	}
}

// TestDecompress_PooledDecoderReuseAfterError proves an errored decoder is
// safe to recycle: a pooled reader that decoded garbage must not poison the
// next request that draws it.
func TestDecompress_PooledDecoderReuseAfterError(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte("valid payload after a poisoned decode")
			good := tc.comp(t, raw)
			garbage := []byte("this is definitely not a compressed frame at all")

			for i := 0; i < 50; i++ {
				// Poison, then immediately reuse.
				_, _ = decompressFully(tc.enc, garbage, DefaultMaxDecompressedSize)
				out, err := decompressFully(tc.enc, good, DefaultMaxDecompressedSize)

				require.NoErrorf(t, err, "iter %d: decode after error: %v", i, err)
				require.Truef(t, bytes.Equal(out, raw), "iter %d: mismatch after error: got %q", i, out)
			}
		})
	}
}

// TestDecompress_PooledReaderNotPoisonedByPartialRead is a regression test.
//
// A reader abandoned mid-stream keeps its buffered input. brotli's Reset only
// wipes that when the previous decode failed unrecoverably, so recycling a
// reader that was merely *stopped early* — exactly what the bomb guard does —
// prepended its leftover bytes to the next response and corrupted it
// (brotli: PADDING_2). Every early-exit path must therefore retire its reader
// instead of pooling it.
//
// The interleaving matters: the guard trips, the reader goes back, and the
// very next request draws it.
func TestDecompress_PooledReaderNotPoisonedByPartialRead(t *testing.T) {
	const smallLimit = 1 << 16
	// The bomb must be large enough that stopping at smallLimit leaves input
	// the decoder has not consumed — that leftover is what poisons the reader.
	// At 8 MiB the decoder consumes the whole (tiny) input before the guard
	// trips and nothing is stranded, so the test would pass vacuously.
	bombInput := make([]byte, 64<<20)

	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
		{"gzip", EncodingGzip, gzipCompress},
		{"deflate", EncodingDeflate, zlibCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bomb := tc.comp(t, bombInput)
			raw := []byte("the next response must decode correctly")
			good := tc.comp(t, raw)

			for i := 0; i < 30; i++ {
				// Trip the guard: stops reading mid-stream, leaving the
				// decoder with buffered input.
				_, err := decompressFully(tc.enc, bomb, smallLimit)
				require.ErrorIsf(t, err, ErrBodyTooLarge, "iter %d: bomb err = %v, want ErrBodyTooLarge", i, err)

				// Immediately reuse: a poisoned reader corrupts this decode.
				out, err := decompressFully(tc.enc, good, DefaultMaxDecompressedSize)

				require.NoErrorf(t, err, "iter %d: decode after aborted decode: %v", i, err)
				require.Truef(t, bytes.Equal(out, raw), "iter %d: poisoned reader: got %q, want %q", i, out, raw)
			}
		})
	}
}

// TestDecompress_PooledReaderNotPoisonedByPartialStream is the streaming-path
// twin: a caller that reads part of a body and closes must not leave a
// half-consumed reader in the pool.
func TestDecompress_PooledReaderNotPoisonedByPartialStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
		{"gzip", EncodingGzip, gzipCompress},
		{"deflate", EncodingDeflate, zlibCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Large enough that an 8-byte read leaves most of the stream
			// undecoded, so the reader still holds buffered input at Close.
			raw := bytes.Repeat([]byte("partial stream payload "), 400_000) // ~9 MiB
			compressed := tc.comp(t, raw)

			for i := 0; i < 30; i++ {
				// Read one small chunk, then close: the decoder still holds
				// buffered input and undelivered output.
				dr, err := newDecompressingReader(tc.enc, io.NopCloser(bytes.NewReader(compressed)))
				require.NoErrorf(t, err, "iter %d: %v", i, err)
				var b [8]byte
				_, err = io.ReadFull(dr, b[:])
				require.NoErrorf(t, err, "iter %d: short read: %v", i, err)
				err = dr.Close()
				require.NoErrorf(t, err, "iter %d: close: %v", i, err)

				// Next request draws from the same pool and must be intact.
				out, err := decompressFully(tc.enc, compressed, DefaultMaxDecompressedSize)

				require.NoErrorf(t, err, "iter %d: decode after partial stream: %v", i, err)
				require.Truef(t, bytes.Equal(out, raw),
					"iter %d: poisoned reader: got %d bytes, want %d", i, len(out), len(raw))
			}
		})
	}
}

// ── pool reuse ───────────────────────────────────────────────────

// TestDecompress_PoolReuse_NoPerRequestReaderAlloc asserts pooled readers are
// actually recycled: with a warm pool, steady-state decode allocations must
// stay far below the cost of constructing a decoder per request. A brotli or
// zstd decoder is tens of KiB and dozens of allocations; the output buffer and
// its growth are the only allocations we expect to survive here.
func TestDecompress_PoolReuse_NoPerRequestReaderAlloc(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
		{"gzip", EncodingGzip, gzipCompress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compressed := tc.comp(t, bytes.Repeat([]byte("pool reuse payload "), 200))
			// Warm.
			for i := 0; i < 10; i++ {
				_, err := decompressFully(tc.enc, compressed, DefaultMaxDecompressedSize)
				require.NoErrorf(t, err, "warmup: %v", err)
			}

			const iterations = 100
			got := testing.AllocsPerRun(iterations, func() {
				if _, err := decompressFully(tc.enc, compressed, DefaultMaxDecompressedSize); err != nil {
					t.Fatalf("decompressFully: %v", err)
				}
			})

			// A fresh decoder per call would be orders of magnitude above this.
			const maxAllocs = 20
			t.Logf("%s: %.1f allocs/op (pooled)", tc.name, got)
			assert.LessOrEqualf(t, got, float64(maxAllocs),
				"%.1f allocs/op exceeds %d — pooled reader is likely not being reused",
				got, maxAllocs)
		})
	}
}

// ── malformed input ──────────────────────────────────────────────

func TestDecompress_BrotliZstd_MalformedInput(t *testing.T) {
	raw := bytes.Repeat([]byte("truncation test payload "), 500)
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		full := tc.comp(t, raw)
		inputs := []struct {
			name string
			data []byte
		}{
			{"garbage", []byte("not a compressed stream, not even close")},
			{"truncated-head", full[:len(full)/2]},
			{"truncated-one-byte-short", full[:len(full)-1]},
			{"header-only", full[:1]},
			{"trailing-garbage", append(append([]byte{}, full...), 0xff, 0xff, 0xff, 0xff)},
			{"bit-flipped", flipByte(full, len(full)/2)},
			{"all-zeroes", make([]byte, 64)},
			{"all-ones", bytes.Repeat([]byte{0xff}, 64)},
		}
		for _, in := range inputs {
			t.Run(tc.name+"/"+in.name+"/buffered", func(t *testing.T) {
				// Must not panic. An error is expected but not asserted for
				// every case: trailing garbage after a complete frame is
				// tolerated by some decoders.
				_, _ = decompressFully(tc.enc, in.data, DefaultMaxDecompressedSize)
			})
			t.Run(tc.name+"/"+in.name+"/streaming", func(t *testing.T) {
				src := io.NopCloser(bytes.NewReader(in.data))
				dr, err := newDecompressingReader(tc.enc, src)
				if err != nil {
					return // rejected at construction: fine
				}
				_, _ = io.Copy(io.Discard, dr)
				_ = dr.Close()
			})
		}
		// Truncation specifically must be reported, never silently accepted as
		// a short body — that would let a peer truncate a response undetected.
		t.Run(tc.name+"/truncated-is-an-error", func(t *testing.T) {
			truncated := full[:len(full)/2]

			_, err := decompressFully(tc.enc, truncated, DefaultMaxDecompressedSize)

			assert.Error(t, err, "truncated stream accepted, want error")
		})
	}
}

func flipByte(b []byte, i int) []byte {
	out := append([]byte{}, b...)
	if i < len(out) {
		out[i] ^= 0xff
	}
	return out
}

// ── accept-encoding ──────────────────────────────────────────────

func TestAcceptEncodingValue_AdvertisesAllDecodable(t *testing.T) {
	got := string(acceptEncodingValue)

	// Every coding we can decode must be advertised: a server that picks one
	// we cannot decode would hand us an undecodable body.
	for _, want := range []string{"gzip", "deflate", "br", "zstd"} {
		assert.Containsf(t, got, want, "accept-encoding %q missing %q", got, want)
	}
	// Conversely, every advertised coding must be decodable.
	for _, tok := range strings.Split(got, ",") {
		tok = strings.TrimSpace(tok)
		enc := detectEncoding([]conn.HeaderField{
			{Name: []byte("content-encoding"), Value: []byte(tok)},
		})
		assert.NotEqualf(t, EncodingIdentity, enc,
			"advertised %q but detectEncoding does not recognise it", tok)
	}
}

func TestShouldSendAcceptEncoding(t *testing.T) {
	cases := []struct {
		name string
		req  *Request
		want bool
	}{
		{"no-headers", &Request{}, true},
		{"unrelated-header", &Request{Headers: []conn.HeaderField{
			{Name: []byte("x-test"), Value: []byte("1")},
		}}, true},
		{"caller-supplied", &Request{Headers: []conn.HeaderField{
			{Name: []byte("accept-encoding"), Value: []byte("identity")},
		}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldSendAcceptEncoding(tc.req)

			assert.Equalf(t, tc.want, got, "shouldSendAcceptEncoding = %v, want %v", got, tc.want)
		})
	}
}

// ── fuzz ─────────────────────────────────────────────────────────

// fuzzEncodings are the compressed codings the decode path accepts. Identity
// is excluded: it returns the input untouched by design and is bounded
// upstream by MaxResponseBodySize, not by the decompression guard.
var fuzzEncodings = []ContentEncoding{EncodingGzip, EncodingDeflate, EncodingBrotli, EncodingZstd}

// FuzzDecompressFully drives arbitrary bytes through the buffered decode path
// for every coding. A response body is peer-controlled, so a hostile or
// corrupt one must never panic, and the bomb guard must bound the output at
// maxBytes no matter what the input claims. Both properties are asserted for
// every input.
func FuzzDecompressFully(f *testing.F) {
	seedDecompressCorpus(f)

	const maxBytes = 1 << 16 // small, so the guard is exercised by modest inputs

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, enc := range fuzzEncodings {
			out, err := decompressFully(enc, data, maxBytes)
			if err != nil {
				continue
			}
			require.LessOrEqualf(t, int64(len(out)), int64(maxBytes),
				"enc %v: guard breached: %d bytes decoded, limit %d", enc, len(out), maxBytes)
		}
	})
}

// FuzzDecompressStream drives arbitrary bytes through the streaming decode
// path for every coding, exercising the Reset/read/Close pool lifecycle that
// the buffered path does not: a decoder that panics or is returned to the pool
// poisoned would surface here across iterations.
func FuzzDecompressStream(f *testing.F) {
	seedDecompressCorpus(f)

	const readCap = 1 << 16

	f.Fuzz(func(_ *testing.T, data []byte) {
		for _, enc := range fuzzEncodings {
			src := io.NopCloser(bytes.NewReader(data))
			dr, err := newDecompressingReader(enc, src)
			if err != nil {
				continue
			}
			// The streaming path is bounded by the caller's reads, so cap the
			// copy: an input declaring a huge expansion must not be allowed to
			// run unbounded here.
			_, _ = io.Copy(io.Discard, io.LimitReader(dr, readCap))
			_ = dr.Close()
		}
	})
}

// seedDecompressCorpus seeds both fuzz targets with valid frames of every
// coding, edge-case payloads, and structurally-interesting garbage.
func seedDecompressCorpus(f *testing.F) {
	f.Helper()
	payloads := [][]byte{
		{},
		[]byte("a"),
		[]byte("hello compressed world"),
		bytes.Repeat([]byte("repetitive "), 1000), // compresses well
		bytes.Repeat([]byte{0}, 1<<16),            // bomb-shaped
	}
	for _, p := range payloads {
		f.Add(brotliCompress(f, p))
		f.Add(zstdCompress(f, p))
		f.Add(gzipCompress(f, p))
		f.Add(zlibCompress(f, p))
	}
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x1f, 0x8b})             // gzip magic, nothing else
	f.Add([]byte{0x28, 0xb5, 0x2f, 0xfd}) // zstd magic, nothing else
	f.Add([]byte{0x78, 0x9c})             // zlib header, nothing else
	f.Add([]byte{0xce, 0xb2, 0xcf, 0x81}) // brotli-ish noise
	f.Add(bytes.Repeat([]byte{0xff}, 64))
}

// ── benchmarks ───────────────────────────────────────────────────

// BenchmarkDecompressFully reports the steady-state cost of the buffered
// decode path per coding with a warm reader pool. Decompression allocates its
// output buffer by definition, so this is not and cannot be a zero-alloc path
// (client/ is deliberately outside the bench gate). What it pins is that the
// *reader* is pooled: allocs/op stays flat instead of carrying a fresh
// multi-KiB decoder on every request.
func BenchmarkDecompressFully(b *testing.B) {
	raw := bytes.Repeat([]byte("benchmark response payload "), 1000) // ~27 KiB
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"gzip", EncodingGzip, gzipCompress},
		{"deflate", EncodingDeflate, zlibCompress},
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		b.Run(tc.name, func(b *testing.B) {
			compressed := tc.comp(b, raw)
			// Warm the pool so construction is not amortised into the loop.
			if _, err := decompressFully(tc.enc, compressed, DefaultMaxDecompressedSize); err != nil {
				b.Fatalf("warmup: %v", err)
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := decompressFully(tc.enc, compressed, DefaultMaxDecompressedSize)
				if err != nil {
					b.Fatalf("decompressFully: %v", err)
				}
				if len(out) != len(raw) {
					b.Fatalf("len = %d, want %d", len(out), len(raw))
				}
			}
		})
	}
}

// BenchmarkNewDecompressingReader reports the steady-state cost of the
// streaming decode path per coding: pool Get, Reset, drain, Close, pool Put.
func BenchmarkNewDecompressingReader(b *testing.B) {
	raw := bytes.Repeat([]byte("benchmark stream payload "), 1000)
	for _, tc := range []struct {
		name string
		enc  ContentEncoding
		comp func(testing.TB, []byte) []byte
	}{
		{"gzip", EncodingGzip, gzipCompress},
		{"deflate", EncodingDeflate, zlibCompress},
		{"brotli", EncodingBrotli, brotliCompress},
		{"zstd", EncodingZstd, zstdCompress},
	} {
		b.Run(tc.name, func(b *testing.B) {
			compressed := tc.comp(b, raw)
			// Reused across iterations so the benchmark measures the decode
			// path, not the source wrapper.
			rd := &nopCloserReader{}
			buf := make([]byte, 32<<10)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rd.Reset(compressed)
				dr, err := newDecompressingReader(tc.enc, rd)
				if err != nil {
					b.Fatalf("newDecompressingReader: %v", err)
				}
				for {
					_, err := dr.Read(buf)
					if err != nil {
						break
					}
				}
				_ = dr.Close()
			}
		})
	}
}
