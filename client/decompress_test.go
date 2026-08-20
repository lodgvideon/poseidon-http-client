package client

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// newDecompressTestClient creates a client against a test server.
func newDecompressTestClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.EnableHTTP2 = true
	srv.TLS = &tls.Config{NextProtos: []string{"h2"}}
	srv.StartTLS()
	c, err := NewClient(ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient: %v", err)
	return c, srv
}

func TestDecompress_Gzip_Do(t *testing.T) {
	raw := bytes.Repeat([]byte("Hello, HTTP/2! "), 100) // 1500 bytes
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(raw)
	gw.Close()

	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/plain")
		w.Write(gzBuf.Bytes())
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	assert.Truef(t, bytes.Equal(resp.Body, raw),
		"body mismatch: got %d bytes, want %d bytes", len(resp.Body), len(raw))
	assert.Equalf(t, int64(gzBuf.Len()), resp.BytesReceived,
		"BytesReceived = %d, want %d (wire bytes)", resp.BytesReceived, gzBuf.Len())
}

func TestDecompress_Deflate_Do(t *testing.T) {
	raw := bytes.Repeat([]byte("deflate-test "), 80)
	var zBuf bytes.Buffer
	zw := zlib.NewWriter(&zBuf)
	zw.Write(raw)
	zw.Close()

	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "deflate")
		w.Write(zBuf.Bytes())
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	assert.Truef(t, bytes.Equal(resp.Body, raw),
		"body mismatch: got %d bytes, want %d", len(resp.Body), len(raw))
}

func TestDecompress_Disabled(t *testing.T) {
	raw := []byte("not compressed")
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(raw)
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:               "GET",
		Path:                 "/",
		BodyMode:             BodyBuffer,
		DisableDecompression: true,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	assert.Truef(t, bytes.Equal(resp.Body, raw),
		"body mismatch: got %q, want %q", resp.Body, raw)
}

func TestDecompress_Identity_NoEncoding(t *testing.T) {
	raw := []byte("plain response body")
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(raw)
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	assert.Truef(t, bytes.Equal(resp.Body, raw),
		"body mismatch: got %q, want %q", resp.Body, raw)
}

func TestDecompress_BodyStream_Gzip(t *testing.T) {
	raw := bytes.Repeat([]byte("stream-gzip "), 200) // 2400 bytes
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write(raw)
	gw.Close()

	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(gzBuf.Bytes())
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyStream,
	}, &resp)
	require.NoErrorf(t, err, "Do: %v", err)
	require.True(t, resp.BodyReader != nil, "BodyReader is nil")
	out, _ := io.ReadAll(resp.BodyReader)
	resp.Reset()

	assert.Truef(t, bytes.Equal(out, raw),
		"decompressed mismatch: got %d bytes, want %d", len(out), len(raw))
}

func TestDecompress_AcceptEncodingSent(t *testing.T) {
	var gotAcceptEncoding string
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAcceptEncoding = r.Header.Get("Accept-Encoding")
		w.Write([]byte("ok"))
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	_ = c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyBuffer,
	}, &resp)

	// The client decodes gzip, deflate, br and zstd, so it advertises all
	// four: advertising a subset would leave servers compressing with a
	// coding we would have decoded for free.
	want := "gzip, deflate, br, zstd"
	assert.Equalf(t, want, gotAcceptEncoding,
		"Accept-Encoding = %q, want %q", gotAcceptEncoding, want)
}

// TestDecompress_AcceptEncodingSuppressed pins that DisableDecompression stops
// the client advertising codings it will not decode.
func TestDecompress_AcceptEncodingSuppressed(t *testing.T) {
	var gotAcceptEncoding string
	var seen bool
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, seen = r.Header["Accept-Encoding"]
		gotAcceptEncoding = r.Header.Get("Accept-Encoding")
		w.Write([]byte("ok"))
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	err := c.Do(context.Background(), &Request{
		Method:               "GET",
		Path:                 "/",
		BodyMode:             BodyBuffer,
		DisableDecompression: true,
	}, &resp)

	require.NoErrorf(t, err, "Do: %v", err)
	assert.Falsef(t, seen,
		"Accept-Encoding = %q, want header absent under DisableDecompression", gotAcceptEncoding)
}

func TestDecompress_CustomAcceptEncodingPreserved(t *testing.T) {
	var gotAcceptEncoding string
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAcceptEncoding = r.Header.Get("Accept-Encoding")
		w.Write([]byte("ok"))
	}))
	defer c.Close()
	defer srv.Close()
	var resp Response

	_ = c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: BodyBuffer,
		Headers: []conn.HeaderField{
			{Name: []byte("accept-encoding"), Value: []byte("br, gzip;q=0.8")},
		},
	}, &resp)

	assert.Equalf(t, "br, gzip;q=0.8", gotAcceptEncoding,
		"Accept-Encoding = %q, want custom value", gotAcceptEncoding)
}

// Unit tests for decompress functions without network.

func TestDetectEncoding(t *testing.T) {
	cases := []struct {
		name    string
		headers []conn.HeaderField
		want    ContentEncoding
	}{
		{"none", nil, EncodingIdentity},
		{"gzip", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("gzip")}}, EncodingGzip},
		{"deflate", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("deflate")}}, EncodingDeflate},
		{"identity", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("identity")}}, EncodingIdentity},
		{"brotli", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("br")}}, EncodingBrotli},
		{"zstd", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("zstd")}}, EncodingZstd},
		{"other-encoding", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("compress")}}, EncodingIdentity},
		// RFC 9110 §8.4.1: x-gzip is a deprecated alias for gzip and must decode.
		{"x-gzip alias", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("x-gzip")}}, EncodingGzip},
		{"X-Gzip case-insensitive", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("X-Gzip")}}, EncodingGzip},
		// Over-rejection guard: no LZW decoder, so x-compress/compress stay Identity.
		{"x-compress stays identity", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("x-compress")}}, EncodingIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := detectEncoding(tc.headers)

			assert.Equalf(t, tc.want, got, "detectEncoding = %v, want %v", got, tc.want)
		})
	}
}

func TestDecompressFully_Gzip(t *testing.T) {
	raw := []byte("test data for gzip compression")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(raw)
	gw.Close()

	out, err := decompressFully(EncodingGzip, buf.Bytes(), DefaultMaxDecompressedSize)

	require.NoErrorf(t, err, "decompressFully: %v", err)
	assert.Truef(t, bytes.Equal(out, raw), "mismatch: got %q, want %q", out, raw)
}

func TestDecompressFully_Deflate(t *testing.T) {
	raw := []byte("test data for deflate compression")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()

	out, err := decompressFully(EncodingDeflate, buf.Bytes(), DefaultMaxDecompressedSize)

	require.NoErrorf(t, err, "decompressFully: %v", err)
	assert.Truef(t, bytes.Equal(out, raw), "mismatch: got %q, want %q", out, raw)
}

func TestDecompressFully_Identity(t *testing.T) {
	raw := []byte("uncompressed")

	out, err := decompressFully(EncodingIdentity, raw, DefaultMaxDecompressedSize)

	require.NoErrorf(t, err, "decompressFully: %v", err)
	assert.True(t, bytes.Equal(out, raw), "mismatch")
}

func TestNewDecompressingReader_Gzip(t *testing.T) {
	raw := []byte("decompressing reader test")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(raw)
	gw.Close()
	src := io.NopCloser(bytes.NewReader(buf.Bytes()))

	dr, err := newDecompressingReader(EncodingGzip, src)
	require.NoErrorf(t, err, "newDecompressingReader: %v", err)
	out, _ := io.ReadAll(dr)
	dr.Close()

	assert.Truef(t, bytes.Equal(out, raw), "mismatch: got %q, want %q", out, raw)
}

func TestNewDecompressingReader_NilSource(t *testing.T) {
	dr, err := newDecompressingReader(EncodingGzip, nil)

	require.NoErrorf(t, err, "unexpected error: %v", err)
	// dr is io.ReadCloser: assert.Nil would pass for an interface holding a
	// nil pointer, which is exactly the value this test must reject.
	assert.Truef(t, dr == nil, "expected nil reader for nil source")
}

func TestNewDecompressingReader_Identity(t *testing.T) {
	src := io.NopCloser(bytes.NewReader([]byte("plain")))

	dr, err := newDecompressingReader(EncodingIdentity, src)

	require.NoErrorf(t, err, "unexpected error: %v", err)
	// Interface identity, not value equality: assert.Equal would pass for a
	// different reader holding the same bytes.
	assert.Truef(t, dr == src, "expected same reader for identity encoding")
}
