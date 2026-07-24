package client

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ContentEncoding identifies the response body encoding.
type ContentEncoding int

const (
	// EncodingIdentity means no compression (identity).
	EncodingIdentity ContentEncoding = iota
	// EncodingGzip means gzip content-encoding.
	EncodingGzip
	// EncodingDeflate means deflate (zlib) content-encoding.
	EncodingDeflate
	// EncodingBrotli means br content-encoding (RFC 7932).
	EncodingBrotli
	// EncodingZstd means zstd content-encoding (RFC 8878).
	EncodingZstd
)

// Header bytes (lowercase, as received from HPACK decoder).
var (
	hdrContentEncoding = []byte("content-encoding")
	hdrAcceptEncoding  = []byte("accept-encoding")
)

// encGzip means gzip content-encoding.
var encGzip = []byte("gzip")

// encXGzip is the deprecated x-gzip alias for gzip. RFC 9110 §8.4.1 keeps it
// equivalent to gzip for historical reasons; a server that labels a gzip body
// "x-gzip" must still be decoded. (No x-compress alias: this client implements
// no LZW decoder, so "compress"/"x-compress" stays Identity.)
var encXGzip = []byte("x-gzip")

// encDeflate means deflate content-encoding.
var encDeflate = []byte("deflate")

// encBrotli means br content-encoding.
var encBrotli = []byte("br")

// encZstd means zstd content-encoding.
var encZstd = []byte("zstd")

// acceptEncodingValue advertises every content-coding this client can decode,
// in the order of the ContentEncoding constants. Pre-built and shared: the
// auto-added accept-encoding header must not allocate per request.
//
// Callers who want a different negotiation set supply their own
// accept-encoding header, which shouldSendAcceptEncoding leaves untouched.
var acceptEncodingValue = []byte("gzip, deflate, br, zstd")

// zstdMaxWindow caps the window a zstd frame may declare, bounding per-decoder
// memory. The decoder's own default is 512 MiB, which a few KiB of hostile
// response could force it to allocate — and a load generator holds many
// decoders at once.
//
// 8 MiB is the interoperability ceiling of RFC 8878 §3.1.1.1.2: decoders are
// recommended to support windows up to 8 MiB and encoders not to emit frames
// needing more. It is what browsers enforce for the zstd content coding, so a
// server that exceeds it is already unusable on the web. A response that does
// is rejected rather than decoded.
const zstdMaxWindow = 8 << 20

// errZstdInit reports a zstd decoder that could not be constructed. Only
// reachable if the pool's decoder options become invalid.
var errZstdInit = errors.New("client: zstd decoder init")

// gzipReaderPool recycles *gzip.Reader instances across requests.
var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

// brotliReaderPool recycles *brotli.Reader instances across requests.
// NewReader(nil) is the documented zero state: every user Resets before
// reading.
var brotliReaderPool = sync.Pool{
	New: func() any { return brotli.NewReader(nil) },
}

// zstdReaderPool recycles *zstd.Decoder instances across requests.
//
// WithDecoderConcurrency(1) is load-bearing, not a tuning knob. At its default
// concurrency the decoder runs an async pipeline: every Reset spawns a stream
// decoder plus two worker goroutines, which a pool would multiply by the
// number of live decoders and strand for any decoder dropped without Close. At
// concurrency 1 Reset takes the synchronous path and spawns nothing, so a
// pooled decoder costs no goroutines and an abandoned one is plain garbage.
// TestDecompress_Zstd_PooledNoGoroutineGrowth pins this.
//
// A decoder from this pool must never be Closed: Close permanently retires it
// (every later Reset returns ErrDecoderClosed). putZstdReader uses Reset(nil),
// which releases the source and leaves the decoder reusable.
var zstdReaderPool = sync.Pool{
	New: func() any {
		zr, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxWindow(zstdMaxWindow),
		)
		if err != nil {
			// Unreachable: both options above are compile-time constants
			// within their valid ranges. Handled at the Get sites rather
			// than panicking in a library.
			return nil
		}
		return zr
	},
}

// getZstdReader draws a decoder from the pool and points it at src.
func getZstdReader(src io.Reader) (*zstd.Decoder, error) {
	zs, ok := zstdReaderPool.Get().(*zstd.Decoder)
	if !ok || zs == nil {
		return nil, errZstdInit
	}
	if err := zs.Reset(src); err != nil {
		putZstdReader(zs)
		return nil, fmt.Errorf("client: zstd reset: %w", err)
	}
	return zs, nil
}

// putZstdReader returns a decoder to the pool. Reset(nil) drops the reference
// to the response body and leaves the decoder reusable; Decoder.Close would
// retire it permanently and must not be used here.
//
// Unlike brotli (see putBrotliReader), a zstd decoder is safe to recycle after
// any outcome: Reset replaces the frame history and source wrapper outright
// and clears the current output buffer, so a decoder abandoned mid-stream
// carries nothing into the next one. TestDecompress_PooledReaderNotPoisonedBy*
// cover zstd alongside brotli, so this stays honest if that ever changes.
func putZstdReader(zs *zstd.Decoder) {
	_ = zs.Reset(nil)
	zstdReaderPool.Put(zs)
}

// getBrotliReader draws a reader from the pool and points it at src.
func getBrotliReader(src io.Reader) *brotli.Reader {
	br := brotliReaderPool.Get().(*brotli.Reader)
	_ = br.Reset(src) // documented to always return nil
	return br
}

// putBrotliReader returns a reader to the pool, but only when its stream was
// drained to EOF. Pass drained=false for any other outcome; the reader is then
// dropped and the next request builds a fresh one.
//
// brotli.Reader.Reset re-initializes the decoder state but does not discard
// input the reader has already buffered: its full wipe runs only when the
// previous decode failed *unrecoverably* (error_code < 0). Two common outcomes
// leave buffered input without setting that: a decode stopped early — which is
// exactly what the bomb guard does, and what a caller closing a body early
// does — and one that ended on trailing garbage (errExcessiveInput). Reset
// then feeds those leftover bytes to the next response that draws the reader,
// which fails loudly if they are undecodable ("brotli: PADDING_2") and,
// worse, could splice one response's bytes onto another's if they are not.
//
// A reader drained to EOF is the one state known to hold nothing. Verified
// empirically: 100 consecutive clean-drain recycles decode correctly, while a
// single stop-early on a large stream poisons the next decode.
// TestDecompress_PooledReaderNotPoisonedByPartialRead pins this.
func putBrotliReader(br *brotli.Reader, drained bool) {
	if !drained {
		return
	}
	_ = br.Reset(nil)
	brotliReaderPool.Put(br)
}

// detectEncoding scans response headers for content-encoding.
//
// Content codings are case-insensitive (RFC 9110 §8.4.1). Header values, unlike
// names, are not normalized by the HPACK/QPACK decoder, so the comparison folds
// case here. EqualFold allocates nothing.
func detectEncoding(headers []conn.HeaderField) ContentEncoding {
	for i := range headers {
		if !bytes.Equal(headers[i].Name, hdrContentEncoding) {
			continue
		}
		v := headers[i].Value
		switch {
		case bytes.EqualFold(v, encGzip), bytes.EqualFold(v, encXGzip):
			return EncodingGzip
		case bytes.EqualFold(v, encDeflate):
			return EncodingDeflate
		case bytes.EqualFold(v, encBrotli):
			return EncodingBrotli
		case bytes.EqualFold(v, encZstd):
			return EncodingZstd
		default:
			return EncodingIdentity
		}
	}
	return EncodingIdentity
}

// decompressingReader wraps a source reader and transparently decompresses
// gzip, deflate, br or zstd content. Close() closes both the decompressor and
// the underlying source.
type decompressingReader struct {
	dec    io.Reader
	source io.ReadCloser

	// drained records that dec reported io.EOF, i.e. the body was consumed to
	// completion and the decompressor holds no leftover input. Callers are
	// free to close a body early, so this is not the common case.
	drained bool

	// At most one of the following is non-nil, identifying the decompressor
	// behind dec so Close can tear it down the way its type requires.
	gz *gzip.Reader   // pooled: Reset + Put on Close
	zl io.Closer      // zlib: Closed on Close, not pooled
	br *brotli.Reader // pooled: recycled on Close only if drained
	zs *zstd.Decoder  // pooled: Reset(nil) + Put on Close — never Closed
}

// Read implements io.Reader.
func (d *decompressingReader) Read(p []byte) (int, error) {
	if d.dec == nil {
		return 0, io.EOF
	}
	n, err := d.dec.Read(p)
	if errors.Is(err, io.EOF) {
		d.drained = true
	}
	return n, err
}

// Close closes the decompressor and underlying source.
// Idempotent.
func (d *decompressingReader) Close() error {
	d.dec = nil
	if d.zl != nil {
		_ = d.zl.Close()
		d.zl = nil
	}
	if d.gz != nil {
		// Reset to zero state before returning to pool
		_ = d.gz.Reset(bytes.NewReader(nil))
		gzipReaderPool.Put(d.gz)
		d.gz = nil
	}
	if d.br != nil {
		putBrotliReader(d.br, d.drained)
		d.br = nil
	}
	if d.zs != nil {
		putZstdReader(d.zs)
		d.zs = nil
	}
	if d.source != nil {
		_ = d.source.Close()
		d.source = nil
	}
	return nil
}

// newDecompressingReader wraps src with the appropriate decompressor
// based on enc. Returns src unchanged when enc == EncodingIdentity.
//
// The returned reader is not size-capped: the streaming path decodes lazily,
// so a decompression bomb costs only what the caller chooses to read. The
// buffered path, which materializes the whole body, is capped in
// decompressFully.
func newDecompressingReader(enc ContentEncoding, src io.ReadCloser) (io.ReadCloser, error) {
	if enc == EncodingIdentity || src == nil {
		return src, nil
	}
	switch enc {
	case EncodingGzip:
		gz := gzipReaderPool.Get().(*gzip.Reader)
		if err := gz.Reset(src); err != nil {
			gzipReaderPool.Put(gz)
			return nil, fmt.Errorf("client: gzip reset: %w", err)
		}
		return &decompressingReader{
			dec:    gz,
			source: src,
			gz:     gz,
		}, nil
	case EncodingDeflate:
		zr, err := zlib.NewReader(src)
		if err != nil {
			return nil, fmt.Errorf("client: zlib init: %w", err)
		}
		return &decompressingReader{
			dec:    zr,
			source: src,
			zl:     zr,
		}, nil
	case EncodingBrotli:
		br := getBrotliReader(src)
		return &decompressingReader{
			dec:    br,
			source: src,
			br:     br,
		}, nil
	case EncodingZstd:
		zs, err := getZstdReader(src)
		if err != nil {
			return nil, err
		}
		// *zstd.Decoder is the reader directly. Decoder.IOReadCloser would
		// allocate a wrapper whose Close retires the decoder — fatal for a
		// pooled one.
		return &decompressingReader{
			dec:    zs,
			source: src,
			zs:     zs,
		}, nil
	default:
		return src, nil
	}
}

// decompressFully reads all decompressed bytes from src into dst.
// Used by drainResponse for the non-streaming path (BodyBuffer).
func decompressFully(enc ContentEncoding, compressed []byte, maxBytes int64) ([]byte, error) {
	if enc == EncodingIdentity || len(compressed) == 0 {
		return compressed, nil
	}
	switch enc {
	case EncodingGzip:
		gz := gzipReaderPool.Get().(*gzip.Reader)
		defer gzipReaderPool.Put(gz)
		if err := gz.Reset(bytes.NewReader(compressed)); err != nil {
			return nil, fmt.Errorf("client: gzip decode: %w", err)
		}
		return readLimited(gz, maxBytes, "gzip")
	case EncodingDeflate:
		zr, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("client: zlib decode: %w", err)
		}
		defer func() { _ = zr.Close() }()
		return readLimited(zr, maxBytes, "zlib")
	case EncodingBrotli:
		// bytes.Reader, not bytes.Buffer: the decoders must pull through the
		// limit below rather than being handed the whole input to expand
		// eagerly.
		br := getBrotliReader(bytes.NewReader(compressed))
		out, err := readLimited(br, maxBytes, "brotli")
		// err == nil is exactly "reached EOF within the limit": the guard
		// tripping and any decode error both leave the reader holding input.
		putBrotliReader(br, err == nil)
		return out, err
	case EncodingZstd:
		zs, err := getZstdReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, err
		}
		defer putZstdReader(zs)
		return readLimited(zs, maxBytes, "zstd")
	default:
		return compressed, nil
	}
}

// readLimited drains dec into a fresh buffer, refusing anything that expands
// past maxBytes. This is the decompression-bomb guard for the buffered path:
// every coding routes through it, since a few KiB of hostile input can declare
// gigabytes of output.
//
// LimitReader returns io.EOF after maxBytes+1 bytes. We read one extra byte to
// distinguish a truncated-but-okay payload from an over-limit one: if Copy
// reads exactly maxBytes and then hits EOF we accept; if it reads maxBytes+1
// we reject as too large.
func readLimited(dec io.Reader, maxBytes int64, what string) ([]byte, error) {
	lr := io.LimitReader(dec, maxBytes+1)
	var buf bytes.Buffer
	n, err := io.Copy(&buf, lr)
	if err != nil {
		return nil, fmt.Errorf("client: %s read: %w", what, err)
	}
	if n > maxBytes {
		return nil, fmt.Errorf("%w: decompressed %d bytes, limit %d", ErrBodyTooLarge, n, maxBytes)
	}
	return buf.Bytes(), nil
}

// shouldSendAcceptEncoding returns true when the request does not
// already carry an accept-encoding header. In that case the client
// auto-adds acceptEncodingValue so the server knows which compressed
// responses we can handle.
func shouldSendAcceptEncoding(req *Request) bool {
	for i := range req.Headers {
		// EqualFold: RFC 9110 §5.1 makes field names case-insensitive, so a caller
		// spelling it "Accept-Encoding" was not recognised as their own and got
		// this client's value appended beside it — two Accept-Encoding field lines
		// where the caller asked for one.
		if bytes.EqualFold(req.Headers[i].Name, hdrAcceptEncoding) {
			return false
		}
	}
	return true
}
