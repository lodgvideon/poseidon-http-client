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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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
	if err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if !bytes.Equal(resp.Body, raw) {
		t.Errorf("body mismatch: got %d bytes, want %d bytes", len(resp.Body), len(raw))
	}
	if resp.BytesReceived != int64(gzBuf.Len()) {
		t.Errorf("BytesReceived = %d, want %d (wire bytes)", resp.BytesReceived, gzBuf.Len())
	}
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
	if err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if !bytes.Equal(resp.Body, raw) {
		t.Errorf("body mismatch: got %d bytes, want %d", len(resp.Body), len(raw))
	}
}

func TestDecompress_Disabled(t *testing.T) {
	raw := []byte("not compressed")
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(raw)
	}))
	defer c.Close()
	defer srv.Close()

	var resp Response
	if err := c.Do(context.Background(), &Request{
		Method:               "GET",
		Path:                 "/",
		WantBody:             true,
		DisableDecompression: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if !bytes.Equal(resp.Body, raw) {
		t.Errorf("body mismatch: got %q, want %q", resp.Body, raw)
	}
}

func TestDecompress_Identity_NoEncoding(t *testing.T) {
	raw := []byte("plain response body")
	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(raw)
	}))
	defer c.Close()
	defer srv.Close()

	var resp Response
	if err := c.Do(context.Background(), &Request{
		Method:   "GET",
		Path:     "/",
		WantBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if !bytes.Equal(resp.Body, raw) {
		t.Errorf("body mismatch: got %q, want %q", resp.Body, raw)
	}
}

func TestDecompress_StreamBody_Gzip(t *testing.T) {
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
	if err := c.Do(context.Background(), &Request{
		Method:     "GET",
		Path:       "/",
		WantBody:   true,
		StreamBody: true,
	}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.BodyReader == nil {
		t.Fatal("BodyReader is nil")
	}
	out, _ := io.ReadAll(resp.BodyReader)
	resp.Reset()

	if !bytes.Equal(out, raw) {
		t.Errorf("decompressed mismatch: got %d bytes, want %d", len(out), len(raw))
	}
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
		WantBody: true,
	}, &resp)

	if gotAcceptEncoding != "gzip" {
		t.Errorf("Accept-Encoding = %q, want %q", gotAcceptEncoding, "gzip")
	}
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
		WantBody: true,
		Headers: []conn.HeaderField{
			{Name: []byte("accept-encoding"), Value: []byte("br, gzip;q=0.8")},
		},
	}, &resp)

	if gotAcceptEncoding != "br, gzip;q=0.8" {
		t.Errorf("Accept-Encoding = %q, want custom value", gotAcceptEncoding)
	}
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
		{"other-encoding", []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("br")}}, EncodingIdentity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := detectEncoding(tc.headers); got != tc.want {
				t.Errorf("detectEncoding = %v, want %v", got, tc.want)
			}
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
	if err != nil {
		t.Fatalf("decompressFully: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("mismatch: got %q, want %q", out, raw)
	}
}

func TestDecompressFully_Deflate(t *testing.T) {
	raw := []byte("test data for deflate compression")
	var buf bytes.Buffer
	zw := zlib.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()

	out, err := decompressFully(EncodingDeflate, buf.Bytes(), DefaultMaxDecompressedSize)
	if err != nil {
		t.Fatalf("decompressFully: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("mismatch: got %q, want %q", out, raw)
	}
}

func TestDecompressFully_Identity(t *testing.T) {
	raw := []byte("uncompressed")
	out, err := decompressFully(EncodingIdentity, raw, DefaultMaxDecompressedSize)
	if err != nil {
		t.Fatalf("decompressFully: %v", err)
	}
	if !bytes.Equal(out, raw) {
		t.Errorf("mismatch")
	}
}

func TestNewDecompressingReader_Gzip(t *testing.T) {
	raw := []byte("decompressing reader test")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(raw)
	gw.Close()

	src := io.NopCloser(bytes.NewReader(buf.Bytes()))
	dr, err := newDecompressingReader(EncodingGzip, src)
	if err != nil {
		t.Fatalf("newDecompressingReader: %v", err)
	}
	out, _ := io.ReadAll(dr)
	dr.Close()
	if !bytes.Equal(out, raw) {
		t.Errorf("mismatch: got %q, want %q", out, raw)
	}
}

func TestNewDecompressingReader_NilSource(t *testing.T) {
	dr, err := newDecompressingReader(EncodingGzip, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dr != nil {
		t.Errorf("expected nil reader for nil source")
	}
}

func TestNewDecompressingReader_Identity(t *testing.T) {
	src := io.NopCloser(bytes.NewReader([]byte("plain")))
	dr, err := newDecompressingReader(EncodingIdentity, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dr != src {
		t.Errorf("expected same reader for identity encoding")
	}
}
