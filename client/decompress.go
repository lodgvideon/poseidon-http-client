package client

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"sync"

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
)

// Header bytes (lowercase, as received from HPACK decoder).
var (
	hdrContentEncoding = []byte("content-encoding")
	hdrAcceptEncoding  = []byte("accept-encoding")
)

// encGzip means gzip content-encoding.
var encGzip = []byte("gzip")

// encDeflate means deflate content-encoding.
var encDeflate = []byte("deflate")

// gzipReaderPool recycles *gzip.Reader instances across requests.
var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

// detectEncoding scans response headers for content-encoding.
func detectEncoding(headers []conn.HeaderField) ContentEncoding {
	for i := range headers {
		if !bytes.Equal(headers[i].Name, hdrContentEncoding) {
			continue
		}
		v := headers[i].Value
		switch {
		case bytes.Equal(v, encGzip):
			return EncodingGzip
		case bytes.Equal(v, encDeflate):
			return EncodingDeflate
		default:
			return EncodingIdentity
		}
	}
	return EncodingIdentity
}

// decompressingReader wraps a source reader and transparently
// decompresses gzip or deflate content. Close() closes both the
// decompressor and the underlying source.
type decompressingReader struct {
	dec    io.ReadCloser
	source io.ReadCloser
	gz     *gzip.Reader // non-nil when gzip; returned to pool on Close
}

// Read implements io.Reader.
func (d *decompressingReader) Read(p []byte) (int, error) {
	if d.dec == nil {
		return 0, io.EOF
	}
	return d.dec.Read(p)
}

// Close closes the decompressor and underlying source.
// Idempotent.
func (d *decompressingReader) Close() error {
	if d.dec != nil {
		_ = d.dec.Close()
		d.dec = nil
	}
	if d.gz != nil {
		// Reset to zero state before returning to pool
		_ = d.gz.Reset(bytes.NewReader(nil))
		gzipReaderPool.Put(d.gz)
		d.gz = nil
	}
	if d.source != nil {
		_ = d.source.Close()
		d.source = nil
	}
	return nil
}

// newDecompressingReader wraps src with the appropriate decompressor
// based on enc. Returns src unchanged when enc == EncodingIdentity.
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
		}, nil
	default:
		return src, nil
	}
}

// decompressFully reads all decompressed bytes from src into dst.
// Used by drainResponse for the non-streaming path (WantBody).
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
		// LimitReader returns io.EOF after maxBytes+1 bytes. We read one
		// extra byte to distinguish a truncated-but-okay payload from an
		// over-limit one: if Copy reads exactly maxBytes and then hits
		// EOF we accept; if it reads maxBytes+1 we reject as too large.
		lr := io.LimitReader(gz, maxBytes+1)
		var buf bytes.Buffer
		n, err := io.Copy(&buf, lr)
		if err != nil {
			return nil, fmt.Errorf("client: gzip read: %w", err)
		}
		if n > maxBytes {
			return nil, fmt.Errorf("%w: decompressed %d bytes, limit %d", ErrBodyTooLarge, n, maxBytes)
		}
		return buf.Bytes(), nil
	case EncodingDeflate:
		zr, err := zlib.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("client: zlib decode: %w", err)
		}
		defer func() { _ = zr.Close() }()
		lr := io.LimitReader(zr, maxBytes+1)
		var buf bytes.Buffer
		n, err := io.Copy(&buf, lr)
		if err != nil {
			return nil, fmt.Errorf("client: zlib read: %w", err)
		}
		if n > maxBytes {
			return nil, fmt.Errorf("%w: decompressed %d bytes, limit %d", ErrBodyTooLarge, n, maxBytes)
		}
		return buf.Bytes(), nil
	default:
		return compressed, nil
	}
}

// shouldSendAcceptEncoding returns true when the request does not
// already carry an accept-encoding header. In that case the client
// auto-adds "gzip" so the server knows we can handle compressed
// responses.
func shouldSendAcceptEncoding(req *Request) bool {
	for i := range req.Headers {
		if bytes.Equal(req.Headers[i].Name, hdrAcceptEncoding) {
			return false
		}
	}
	return true
}
