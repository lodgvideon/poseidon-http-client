// Package client — request-body compression (the encode side).
//
// This is the mirror of decompress.go. Where that file decodes a
// content-encoding the *server* chose, this one encodes a body the *caller*
// asked for via Request.CompressBody, and sets content-encoding itself.
//
// Why an explicit field rather than the header: content-encoding is a
// representation header (RFC 9110 §8.4) — it describes what the body already
// is. Callers who compress their own bodies set it today and expect those bytes
// to go out untouched. Treating it as a request to compress would double-encode
// them, and declare a content-length describing the wrong thing. So the header
// keeps meaning "already encoded", and CompressBody means "please encode". This
// is also what aiohttp (compress=) and gRPC (UseCompressor) do.
package client

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// compressChunkSize is the per-Read buffer a compressing body reader pulls from
// the caller's source. It mirrors readChunkSize, the size writeBodyReader reads
// in: the compressed output is chunked again downstream by the transport.
const compressChunkSize = 16 * 1024

// errZstdEncoderInit reports a zstd encoder that could not be constructed. Only
// reachable if the pool's encoder options become invalid.
var errZstdEncoderInit = errors.New("client: zstd encoder init")

// encodingToken returns the content-coding token for enc, as the client will
// write it into the content-encoding header. An unrecognised value is an error
// rather than a silent fall-through to identity: a caller who asked for
// compression and quietly got none would never find out.
func encodingToken(enc ContentEncoding) ([]byte, error) {
	switch enc {
	case EncodingIdentity:
		return nil, nil
	case EncodingGzip:
		return encGzip, nil
	case EncodingDeflate:
		return encDeflate, nil
	case EncodingBrotli:
		return encBrotli, nil
	case EncodingZstd:
		return encZstd, nil
	default:
		return nil, fmt.Errorf("%w: %w: CompressBody=%d",
			ErrInvalidRequest, ErrUnsupportedContentEncoding, int(enc))
	}
}

// ————————————————————————————————————————————————————————————————
// pooled encoders
// ————————————————————————————————————————————————————————————————
//
// Reset/recycle semantics, verified against each library's source and pinned by
// TestCompress_PooledWriterNotPoisonedBy* :
//
// All four Writers are safe to recycle after *any* outcome — including a stream
// abandoned mid-write and never Closed — because each one's Reset is
// unconditional and total. This is the opposite of the decode side, where
// brotli.Reader.Reset runs its full wipe only when the previous decode failed
// unrecoverably, so a reader stopped early poisoned the next use (see
// putBrotliReader). The asymmetry is real and worth stating:
//
//   - gzip.Writer.Reset overwrites the whole struct (*z = Writer{...}),
//     clearing err/closed/wroteHeader.
//   - zlib.Writer.Reset clears err and wroteHeader explicitly.
//   - brotli.Writer.Reset always calls encoderInitState, which rewinds the ring
//     buffer and every input position, and clears err. Note Close sets dst=nil,
//     so a recycled Writer is unusable until Reset — which every Get does.
//   - zstd.Encoder.Reset clears state.err — including the ErrEncoderClosed that
//     Close sets — and truncates the pending input. So unlike zstd.Decoder,
//     whose Close permanently retires it, a zstd *Encoder* must be Closed to
//     emit a valid frame and is still reusable afterwards.
//
// Because none of them carries state across Reset, a writer is returned to its
// pool unconditionally. The one hazard that does exist is a double Put, which
// would hand one encoder to two requests at once; compressingReader.release
// guards it by niling its writer (TestCompress_ReleaseIsIdempotent).

// gzipWriterPool recycles *gzip.Writer instances across requests.
var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(nil) },
}

// zlibWriterPool recycles *zlib.Writer instances across requests. zlib is the
// "deflate" content coding as servers actually implement it, matching the
// decode side's zlib.NewReader.
var zlibWriterPool = sync.Pool{
	New: func() any { return zlib.NewWriter(nil) },
}

// brotliWriterPool recycles *brotli.Writer instances across requests.
var brotliWriterPool = sync.Pool{
	New: func() any { return brotli.NewWriter(nil) },
}

// zstdWriterPool recycles *zstd.Encoder instances across requests.
//
// WithEncoderConcurrency(1) is load-bearing, not a tuning knob — the same trap
// the decode side hit. At its default (GOMAXPROCS) the encoder runs an async
// pipeline: Write spawns two goroutines per block and does not join them until
// the next block or Close, which a pool would multiply by the number of live
// encoders. At concurrency 1 the encoder takes the synchronous path and spawns
// nothing, so a pooled encoder costs no goroutines and an abandoned one is plain
// garbage. TestCompress_Zstd_PooledEncoderIsSynchronous pins this, and carries a
// negative control so the assertion cannot pass vacuously.
var zstdWriterPool = sync.Pool{
	New: func() any {
		zw, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			// Unreachable: the option above is a compile-time constant within
			// its valid range. Handled at the Get site rather than panicking in
			// a library.
			return nil
		}
		return zw
	},
}

// getCompressWriter draws a pooled encoder for enc and points it at dst. The
// returned writer MUST be Closed to emit a valid stream, and MUST be handed to
// putCompressWriter exactly once.
func getCompressWriter(enc ContentEncoding, dst io.Writer) (io.WriteCloser, error) {
	switch enc {
	case EncodingGzip:
		w := gzipWriterPool.Get().(*gzip.Writer)
		w.Reset(dst)
		return w, nil
	case EncodingDeflate:
		w := zlibWriterPool.Get().(*zlib.Writer)
		w.Reset(dst)
		return w, nil
	case EncodingBrotli:
		w := brotliWriterPool.Get().(*brotli.Writer)
		w.Reset(dst)
		return w, nil
	case EncodingZstd:
		w, ok := zstdWriterPool.Get().(*zstd.Encoder)
		if !ok || w == nil {
			return nil, errZstdEncoderInit
		}
		w.Reset(dst)
		return w, nil
	default:
		_, err := encodingToken(enc)
		if err == nil {
			// EncodingIdentity: not an encoder.
			err = fmt.Errorf("%w: %w: identity has no encoder",
				ErrInvalidRequest, ErrUnsupportedContentEncoding)
		}
		return nil, err
	}
}

// putCompressWriter returns w to the pool it came from. Reset(nil) drops the
// reference to the previous destination; see the block comment above for why
// every codec is safe to recycle regardless of how its stream ended.
//
// A writer of an unexpected type is dropped rather than mis-pooled.
func putCompressWriter(w io.WriteCloser) {
	switch v := w.(type) {
	case *gzip.Writer:
		v.Reset(nil)
		gzipWriterPool.Put(v)
	case *zlib.Writer:
		v.Reset(nil)
		zlibWriterPool.Put(v)
	case *brotli.Writer:
		v.Reset(nil)
		brotliWriterPool.Put(v)
	case *zstd.Encoder:
		v.Reset(nil)
		zstdWriterPool.Put(v)
	}
}

// ————————————————————————————————————————————————————————————————
// buffered bodies
// ————————————————————————————————————————————————————————————————

// compressBody encodes src with enc and returns the compressed bytes. Used for
// Request.Body, where the whole body is already in memory and the compressed
// length is therefore knowable before the headers are sent.
//
// The output buffer is deliberately not pooled: it escapes as the effective
// Request.Body and outlives this call (across every retry attempt, in fact), so
// recycling it would mean tracking that lifetime through the retry loop for the
// sake of one allocation next to the encoder's own output. The encoder — tens of
// KiB of state — is what pooling is for.
func compressBody(enc ContentEncoding, src []byte) ([]byte, error) {
	var buf bytes.Buffer
	// Most bodies worth compressing shrink; a modest guess avoids the first few
	// doublings without over-reserving for small ones.
	buf.Grow(len(src)/3 + 64)

	w, err := getCompressWriter(enc, &buf)
	if err != nil {
		return nil, err
	}
	defer putCompressWriter(w)

	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("client: compress request body: %w", err)
	}
	// Close flushes the codec's trailer; without it the stream is truncated and
	// the peer cannot decode it.
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("client: compress request body: %w", err)
	}
	return buf.Bytes(), nil
}

// ————————————————————————————————————————————————————————————————
// streaming bodies
// ————————————————————————————————————————————————————————————————

// compressingReader compresses a Request.BodyReader on the fly: each Read pulls
// from the source, feeds the encoder, and returns whatever the encoder has
// emitted so far. It is pull-based on purpose — an io.Pipe would need a
// goroutine per request, which is exactly the cost the pooled encoders exist to
// avoid.
type compressingReader struct {
	src io.Reader
	buf bytes.Buffer
	w   io.WriteCloser

	chunk []byte

	// done records that the source reached EOF and the encoder was Closed, so
	// buf holds the tail of the stream and nothing more is coming.
	done bool
	// err latches the first failure; Read is not restartable after one.
	err error
}

// compressingReaderPool recycles compressingReader structs, keeping their chunk
// and output buffers across requests.
var compressingReaderPool = sync.Pool{
	New: func() any {
		return &compressingReader{chunk: make([]byte, compressChunkSize)}
	},
}

// getCompressingReader draws a reader from the pool and wires it to compress src
// with enc. The caller MUST call release exactly once (extra calls are no-ops).
func getCompressingReader(enc ContentEncoding, src io.Reader) (*compressingReader, error) {
	c := compressingReaderPool.Get().(*compressingReader)
	c.buf.Reset()
	c.src = src
	c.done = false
	c.err = nil

	w, err := getCompressWriter(enc, &c.buf)
	if err != nil {
		c.src = nil
		compressingReaderPool.Put(c)
		return nil, err
	}
	c.w = w
	return c, nil
}

// Read implements io.Reader, returning compressed bytes.
//
// It loops rather than returning 0 bytes: an encoder buffers input and may emit
// nothing for many source reads, and a Read that returns (0, nil) is legal but
// hostile to callers.
func (c *compressingReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for c.buf.Len() == 0 && !c.done {
		n, rerr := c.src.Read(c.chunk)
		if n > 0 {
			if _, werr := c.w.Write(c.chunk[:n]); werr != nil {
				c.err = fmt.Errorf("client: compress request body: %w", werr)
				return 0, c.err
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				// Surface the source's error verbatim so the caller's
				// errors.Is/As still matches; writeBodyReader adds the context.
				c.err = rerr
				return 0, c.err
			}
			// Close writes the codec trailer. Without it the peer sees a
			// truncated stream.
			if cerr := c.w.Close(); cerr != nil {
				c.err = fmt.Errorf("client: compress request body: %w", cerr)
				return 0, c.err
			}
			c.done = true
		}
	}
	if c.buf.Len() == 0 {
		return 0, io.EOF
	}
	return c.buf.Read(p)
}

// release returns the reader and its encoder to their pools. Idempotent: a
// second call is a no-op, so a double release cannot hand one encoder to two
// requests at once.
func (c *compressingReader) Release() {
	if c.w == nil {
		return
	}
	putCompressWriter(c.w)
	c.w = nil
	c.src = nil
	c.err = nil
	c.done = false
	c.buf.Reset()
	compressingReaderPool.Put(c)
}

// ————————————————————————————————————————————————————————————————
// the seam
// ————————————————————————————————————————————————————————————————

// hasContentEncodingHeader reports whether the caller set content-encoding
// themselves, i.e. declared the body already encoded.
func hasContentEncodingHeader(req *Request) bool {
	for i := range req.Headers {
		if bytes.EqualFold(req.Headers[i].Name, hdrContentEncoding) {
			return true
		}
	}
	return false
}

// validateCompressBody enforces the two up-front rules on Request.CompressBody:
// the coding must be one we can produce, and it must not contradict a
// caller-supplied content-encoding header.
//
// Called from validateRequest so Do rejects before opening a connection, and
// again from prepareCompressedRequest so the invariant holds at the point of use
// whichever layer got there first.
func validateCompressBody(req *Request) error {
	if req.CompressBody == EncodingIdentity {
		return nil
	}
	if _, err := encodingToken(req.CompressBody); err != nil {
		return err
	}
	if hasContentEncodingHeader(req) {
		// Both readings corrupt the body: compressing an already-encoded body
		// double-encodes it, ignoring CompressBody silently sends it raw under
		// a header claiming otherwise. Refuse rather than guess.
		return fmt.Errorf("%w: %w: set one or the other, not both",
			ErrInvalidRequest, ErrConflictingContentEncoding)
	}
	return nil
}

// compressedHeaders builds the effective header slice: the caller's headers with
// any content-length dropped, plus the content-encoding the client is setting
// and, when clen >= 0, the compressed content-length.
//
// The caller's content-length describes the *uncompressed* body, so it can never
// be right once we encode. Dropping it is what lets HTTP/1.1 fall back to
// chunked for a stream of unknowable length (http1.Exchange.WriteRequest adds
// Transfer-Encoding: chunked exactly when no content-length is present and a
// body follows).
func compressedHeaders(src []conn.HeaderField, token []byte, clen int64) []conn.HeaderField {
	out := make([]conn.HeaderField, 0, len(src)+2)
	for i := range src {
		if bytes.EqualFold(src[i].Name, hdrContentLength) {
			continue
		}
		out = append(out, src[i])
	}
	out = append(out, conn.HeaderField{Name: hdrContentEncoding, Value: token})
	if clen >= 0 {
		out = append(out, conn.HeaderField{
			Name:  hdrContentLength,
			Value: strconv.AppendInt(nil, clen, 10),
		})
	}
	return out
}

// prepareCompressedRequest is the single seam for Request.CompressBody.
//
// It returns the request the transport should actually send. For the zero value
// (EncodingIdentity) that is the caller's own *Request, unchanged and
// un-copied — so every existing caller's bytes are produced by exactly the code
// that produced them before this feature existed, which is what
// TestPrepareCompressedRequest_IdentityIsTheSameRequest and the wire goldens in
// compress_wire_test.go pin.
//
// Otherwise it returns a copy whose body is encoded and whose headers carry the
// content-encoding the client chose:
//
//   - Buffered body (Request.Body): the compressed length is knowable, so
//     content-length is set to it. The caller's ContentLength describes the
//     uncompressed body and never reaches the wire.
//   - Streaming body (Request.BodyReader): the compressed length is not knowable
//     before the body is read, so no content-length is emitted at all — neither
//     the header nor via the ContentLength field. HTTP/1.1 then frames the body
//     with chunked transfer-coding; HTTP/2 and HTTP/3 simply end the DATA frames
//     with END_STREAM, which needs no length.
//   - No body at all: nothing to encode and no representation to describe, so
//     the request is returned untouched rather than growing a codec preamble the
//     caller never asked for. (A non-nil BodyReader that yields nothing is a
//     different case — a body *does* follow, it is just empty — and gets a valid
//     empty frame, exactly as the endStream logic in sendRequest already
//     assumes.)
//
// The result is inert: its CompressBody is EncodingIdentity and its
// content-encoding is now an ordinary caller-set header, so passing it back
// through this function is a no-op. That idempotence is what lets the Retryer
// hoist the call above its loop and compress once for every attempt.
//
// release, when non-nil, MUST be called once the body has been written; it
// returns the pooled encoder. It is nil on every path that has no pooled state
// to give back.
func prepareCompressedRequest(req *Request) (eff *Request, release func(), err error) {
	if req.CompressBody == EncodingIdentity {
		return req, nil, nil
	}
	if err := validateCompressBody(req); err != nil {
		return nil, nil, err
	}
	token, err := encodingToken(req.CompressBody)
	if err != nil {
		return nil, nil, err
	}
	if req.BodyReader == nil && len(req.Body) == 0 {
		return req, nil, nil // no representation to encode
	}

	eff = new(Request)
	*eff = *req
	eff.CompressBody = EncodingIdentity
	// Kills the second route to a content-length: buildHeaders emits one from
	// this field whenever BodyReader is set.
	eff.ContentLength = 0

	if req.BodyReader != nil {
		cr, cerr := getCompressingReader(req.CompressBody, req.BodyReader)
		if cerr != nil {
			return nil, nil, cerr
		}
		eff.BodyReader = cr
		eff.Headers = compressedHeaders(req.Headers, token, -1)
		return eff, cr.Release, nil
	}

	body, berr := compressBody(req.CompressBody, req.Body)
	if berr != nil {
		return nil, nil, berr
	}
	eff.Body = body
	eff.Headers = compressedHeaders(req.Headers, token, int64(len(body)))
	return eff, nil, nil
}
