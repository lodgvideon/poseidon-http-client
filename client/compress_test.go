package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// ── helpers ──────────────────────────────────────────────────────

// allCodecs is every coding CompressBody accepts, paired with the wire token
// the client must emit and the reference decoder for the round trip.
var allCodecs = []struct {
	name  string
	enc   ContentEncoding
	token string
}{
	{"gzip", EncodingGzip, "gzip"},
	{"deflate", EncodingDeflate, "deflate"},
	{"brotli", EncodingBrotli, "br"},
	{"zstd", EncodingZstd, "zstd"},
}

// headerValue returns the value of the named header in hs, and whether it was
// present. Names are compared case-insensitively.
func headerValue(hs []conn.HeaderField, name string) (string, bool) {
	for i := range hs {
		if bytes.EqualFold(hs[i].Name, []byte(name)) {
			return string(hs[i].Value), true
		}
	}
	return "", false
}

// countHeader returns how many times name appears in hs.
func countHeader(hs []conn.HeaderField, name string) int {
	n := 0
	for i := range hs {
		if bytes.EqualFold(hs[i].Name, []byte(name)) {
			n++
		}
	}
	return n
}

// decodeWith decompresses data using the reference decoder for enc, reusing the
// decode side's own path so the test asserts real interoperability rather than
// symmetry with our own encoder.
func decodeWith(t testing.TB, enc ContentEncoding, data []byte) []byte {
	t.Helper()
	out, err := decompressFully(enc, data, DefaultMaxDecompressedSize)
	if err != nil {
		t.Fatalf("decompress %v: %v", enc, err)
	}
	return out
}

// ── identity: the backward-compatibility proof ───────────────────

// TestPrepareCompressedRequest_IdentityIsTheSameRequest proves the zero value
// changes nothing, by construction rather than by comparison: the returned
// *Request is the caller's own pointer, so every downstream byte is produced by
// the same code operating on the same object it saw before this feature landed.
func TestPrepareCompressedRequest_IdentityIsTheSameRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *Request
	}{
		{"buffered", &Request{Method: "POST", Path: "/", Body: []byte("payload")}},
		{"streaming", &Request{Method: "POST", Path: "/", BodyReader: strings.NewReader("payload")}},
		{"no-body", &Request{Method: "GET", Path: "/"}},
		{"explicit-identity", &Request{Method: "POST", Path: "/", Body: []byte("x"), CompressBody: EncodingIdentity}},
		{"manual-content-encoding", &Request{
			Method: "POST", Path: "/", Body: []byte("x"),
			Headers: []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("gzip")}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, release, err := prepareCompressedRequest(tc.req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			if got != tc.req {
				t.Errorf("returned a copy; identity must return the caller's own request unchanged")
			}
			if release != nil {
				t.Errorf("identity path allocated a release func; want nil")
			}
		})
	}
}

// TestCompress_Identity_H1WireBytesUnchanged pins the zero value to the goldens
// captured before CompressBody existed (see compress_wire_test.go): setting the
// field explicitly to EncodingIdentity must be byte-for-byte indistinguishable
// from not setting it at all.
func TestCompress_Identity_H1WireBytesUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  func() *Request
		want string
	}{
		{
			name: "buffered-body",
			req: func() *Request {
				return &Request{
					Method: "POST", Path: "/upload",
					Body:         []byte("hello wire"),
					CompressBody: EncodingIdentity,
					Headers:      []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("1")}},
				}
			},
			want: goldenBufferedBody,
		},
		{
			name: "streaming-body",
			req: func() *Request {
				return &Request{
					Method: "POST", Path: "/stream",
					BodyReader:   strings.NewReader("hello stream"),
					CompressBody: EncodingIdentity,
					Headers:      []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("1")}},
				}
			},
			want: goldenStreamingBody,
		},
		{
			name: "streaming-body-with-content-length",
			req: func() *Request {
				return &Request{
					Method: "POST", Path: "/stream-cl",
					BodyReader:    strings.NewReader("hello stream"),
					ContentLength: 12,
					CompressBody:  EncodingIdentity,
					Headers:       []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("1")}},
				}
			},
			want: goldenStreamingBodyCL,
		},
		{
			name: "no-body",
			req: func() *Request {
				return &Request{Method: "GET", Path: "/", CompressBody: EncodingIdentity}
			},
			want: goldenNoBody,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := string(captureH1Request(t, tc.req()))
			if got != tc.want {
				t.Errorf("CompressBody: EncodingIdentity changed the wire.\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// TestPrepareCompressedRequest_ManualHeaderAloneIsUntouched pins the RFC-correct
// reading of content-encoding: it describes a body that is *already* encoded. A
// caller who compressed the body themselves and set the header must have those
// bytes passed through verbatim — re-compressing them would be silent
// corruption, which is the whole reason CompressBody is a separate field.
func TestPrepareCompressedRequest_ManualHeaderAloneIsUntouched(t *testing.T) {
	precompressed := gzipCompress(t, []byte("the caller compressed this themselves"))
	req := &Request{
		Method: "POST", Path: "/",
		Body: precompressed,
		Headers: []conn.HeaderField{
			{Name: []byte("content-encoding"), Value: []byte("gzip")},
			{Name: []byte("content-length"), Value: []byte(strconv.Itoa(len(precompressed)))},
		},
	}
	got, _, err := prepareCompressedRequest(req)
	if err != nil {
		t.Fatalf("prepareCompressedRequest: %v", err)
	}
	if !bytes.Equal(got.Body, precompressed) {
		t.Errorf("body was re-encoded: got %d bytes, want the caller's %d", len(got.Body), len(precompressed))
	}
	if v, _ := headerValue(got.Headers, "content-length"); v != strconv.Itoa(len(precompressed)) {
		t.Errorf("content-length = %q, want the caller's %d", v, len(precompressed))
	}
	if countHeader(got.Headers, "content-encoding") != 1 {
		t.Errorf("content-encoding count = %d, want 1", countHeader(got.Headers, "content-encoding"))
	}
}

// ── conflicts and unknown codings ────────────────────────────────

// TestCompressBody_ConflictsWithManualHeader covers the ambiguous request: the
// caller both asked us to compress and claimed the body is already encoded.
// Either reading silently corrupts the body, so the request is refused.
func TestCompressBody_ConflictsWithManualHeader(t *testing.T) {
	for _, hdr := range []string{"content-encoding", "Content-Encoding"} {
		t.Run(hdr, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				Body:         []byte("payload"),
				CompressBody: EncodingGzip,
				Headers:      []conn.HeaderField{{Name: []byte(hdr), Value: []byte("gzip")}},
			}
			_, _, err := prepareCompressedRequest(req)
			if !errors.Is(err, ErrConflictingContentEncoding) {
				t.Errorf("prepareCompressedRequest err = %v, want ErrConflictingContentEncoding", err)
			}
			// validateRequest must reject it too, so Do fails before a
			// connection is opened.
			if err := validateRequest(req); !errors.Is(err, ErrConflictingContentEncoding) {
				t.Errorf("validateRequest err = %v, want ErrConflictingContentEncoding", err)
			}
			// A conflicting request can never succeed, so it must also be a
			// hard stop: retrying it is pure waste.
			if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("validateRequest err = %v, want it to wrap ErrInvalidRequest", err)
			}
			if !isHardStop(validateRequest(req)) {
				t.Error("conflicting content-encoding is retryable; want hard stop")
			}
		})
	}
}

// TestCompressBody_UnknownEncodingIsAnError proves an unrecognised value fails
// loudly rather than silently degrading to identity — a caller who asked for
// compression and got none would never find out.
func TestCompressBody_UnknownEncodingIsAnError(t *testing.T) {
	for _, enc := range []ContentEncoding{99, -1, EncodingZstd + 1} {
		t.Run(strconv.Itoa(int(enc)), func(t *testing.T) {
			req := &Request{Method: "POST", Path: "/", Body: []byte("payload"), CompressBody: enc}
			_, _, err := prepareCompressedRequest(req)
			if !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Errorf("prepareCompressedRequest err = %v, want ErrUnsupportedContentEncoding", err)
			}
			if err := validateRequest(req); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Errorf("validateRequest err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

// ── buffered bodies ──────────────────────────────────────────────

// TestPrepareCompressedRequest_Buffered covers the knowable-length case: the
// client compresses, sets content-encoding itself, and declares the *compressed*
// length. The caller's ContentLength describes the uncompressed body and must
// never reach the wire.
func TestPrepareCompressedRequest_Buffered(t *testing.T) {
	raw := bytes.Repeat([]byte("compress me please "), 500) // ~9.5 KiB, compresses well

	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				Body:          raw,
				ContentLength: int64(len(raw)), // the caller's (uncompressed) claim
				CompressBody:  tc.enc,
				Headers:       []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("1")}},
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			if release != nil {
				defer release()
			}

			// The caller's request must not be mutated.
			if req.CompressBody != tc.enc || !bytes.Equal(req.Body, raw) || len(req.Headers) != 1 {
				t.Error("caller's Request was mutated")
			}

			// The client sets content-encoding itself.
			if v, ok := headerValue(eff.Headers, "content-encoding"); !ok || v != tc.token {
				t.Errorf("content-encoding = %q (present=%v), want %q", v, ok, tc.token)
			}
			// content-length must be the compressed size, not the caller's.
			v, ok := headerValue(eff.Headers, "content-length")
			if !ok {
				t.Fatal("content-length absent; a buffered compressed body has a knowable length")
			}
			if v != strconv.Itoa(len(eff.Body)) {
				t.Errorf("content-length = %q, want %d (the compressed size)", v, len(eff.Body))
			}
			if v == strconv.Itoa(len(raw)) {
				t.Errorf("content-length = %q is the caller's uncompressed size", v)
			}
			// Caller headers survive.
			if hv, _ := headerValue(eff.Headers, "x-test"); hv != "1" {
				t.Error("caller's headers were dropped")
			}
			// The effective request must be inert: a second prepare is a no-op,
			// which is what makes the retry hoist safe.
			if eff.CompressBody != EncodingIdentity {
				t.Error("effective request still asks for compression; a second prepare would double-compress")
			}
			if eff.BodyReader != nil {
				t.Error("buffered body became a stream")
			}

			// The bytes on the wire must decompress back to the original.
			if got := decodeWith(t, tc.enc, eff.Body); !bytes.Equal(got, raw) {
				t.Errorf("round trip mismatch: got %d bytes, want %d", len(got), len(raw))
			}
			if len(eff.Body) >= len(raw) {
				t.Errorf("compressed %d -> %d bytes: not actually compressing", len(raw), len(eff.Body))
			}
		})
	}
}

// TestPrepareCompressedRequest_BufferedStripsCallerContentLength proves a
// caller-set content-length header (describing the uncompressed body) is
// replaced, not duplicated. Leaving it would frame the HTTP/1.1 body at the
// wrong length and hang the exchange.
func TestPrepareCompressedRequest_BufferedStripsCallerContentLength(t *testing.T) {
	raw := bytes.Repeat([]byte("data "), 200)
	req := &Request{
		Method: "POST", Path: "/",
		Body:         raw,
		CompressBody: EncodingGzip,
		Headers: []conn.HeaderField{
			{Name: []byte("Content-Length"), Value: []byte(strconv.Itoa(len(raw)))},
		},
	}
	eff, release, err := prepareCompressedRequest(req)
	if err != nil {
		t.Fatalf("prepareCompressedRequest: %v", err)
	}
	if release != nil {
		defer release()
	}
	if n := countHeader(eff.Headers, "content-length"); n != 1 {
		t.Fatalf("content-length count = %d, want exactly 1", n)
	}
	if v, _ := headerValue(eff.Headers, "content-length"); v != strconv.Itoa(len(eff.Body)) {
		t.Errorf("content-length = %q, want %d (compressed)", v, len(eff.Body))
	}
}

// TestPrepareCompressedRequest_NoBodyIsUntouched pins the bodyless case: there
// is no representation, so there is nothing to encode and nothing to describe.
// Compressing "nothing" would attach a ~20-byte codec preamble to a GET that
// the caller never asked to carry a body.
func TestPrepareCompressedRequest_NoBodyIsUntouched(t *testing.T) {
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{Method: "GET", Path: "/", CompressBody: tc.enc}
			eff, _, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			if eff != req {
				t.Error("bodyless request was rewritten")
			}
			if _, ok := headerValue(eff.Headers, "content-encoding"); ok {
				t.Error("content-encoding set on a request with no body")
			}
		})
	}
}

// ── streaming bodies ─────────────────────────────────────────────

// TestPrepareCompressedRequest_Streaming covers the unknowable-length case. The
// compressed size cannot be known before the body is read, so no content-length
// may be sent — a stale one would truncate or hang the exchange.
func TestPrepareCompressedRequest_Streaming(t *testing.T) {
	raw := bytes.Repeat([]byte("streamed and compressed "), 5000) // ~120 KiB

	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				BodyReader:    bytes.NewReader(raw),
				ContentLength: int64(len(raw)), // the caller's (uncompressed) claim
				CompressBody:  tc.enc,
				Headers: []conn.HeaderField{
					{Name: []byte("content-length"), Value: []byte(strconv.Itoa(len(raw)))},
				},
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			if release == nil {
				t.Fatal("streaming path returned no release; the pooled writer would leak")
			}
			defer release()

			if v, ok := headerValue(eff.Headers, "content-encoding"); !ok || v != tc.token {
				t.Errorf("content-encoding = %q (present=%v), want %q", v, ok, tc.token)
			}
			// Both routes to a content-length must be shut off: the header the
			// caller set, and the ContentLength field buildHeaders reads.
			if v, ok := headerValue(eff.Headers, "content-length"); ok {
				t.Errorf("content-length = %q present; the compressed length is unknowable up front", v)
			}
			if eff.ContentLength != 0 {
				t.Errorf("ContentLength = %d, want 0 so buildHeaders emits no length", eff.ContentLength)
			}
			if eff.BodyReader == nil {
				t.Fatal("BodyReader is nil")
			}

			out, err := io.ReadAll(eff.BodyReader)
			if err != nil {
				t.Fatalf("read compressed stream: %v", err)
			}
			if got := decodeWith(t, tc.enc, out); !bytes.Equal(got, raw) {
				t.Errorf("round trip mismatch: got %d bytes, want %d", len(got), len(raw))
			}
			if len(out) >= len(raw) {
				t.Errorf("compressed %d -> %d bytes: not actually compressing", len(raw), len(out))
			}
		})
	}
}

// TestPrepareCompressedRequest_StreamingEmpty pins the asymmetry with the
// bodyless case: a non-nil BodyReader means a body follows even when it yields
// nothing, exactly as it does today. It must produce a valid (empty) frame the
// peer can decode, not zero bytes claiming to be a compressed stream.
func TestPrepareCompressedRequest_StreamingEmpty(t *testing.T) {
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				BodyReader:   bytes.NewReader(nil),
				CompressBody: tc.enc,
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			defer release()
			out, err := io.ReadAll(eff.BodyReader)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if got := decodeWith(t, tc.enc, out); len(got) != 0 {
				t.Errorf("empty stream decoded to %d bytes, want 0", len(got))
			}
		})
	}
}

// errReader fails after yielding prefix, modelling a request body that dies
// mid-upload.
type failingSourceReader struct {
	prefix []byte
	off    int
	err    error
}

func (e *failingSourceReader) Read(p []byte) (int, error) {
	if e.off < len(e.prefix) {
		n := copy(p, e.prefix[e.off:])
		e.off += n
		return n, nil
	}
	return 0, e.err
}

// TestPrepareCompressedRequest_StreamingSourceError proves a body-read failure
// surfaces verbatim through the compressing wrapper, so errors.Is on the
// caller's own error still works.
func TestPrepareCompressedRequest_StreamingSourceError(t *testing.T) {
	sentinel := errors.New("body exploded")
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				BodyReader:   &failingSourceReader{prefix: bytes.Repeat([]byte("x"), 100), err: sentinel},
				CompressBody: tc.enc,
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("prepareCompressedRequest: %v", err)
			}
			defer release()
			_, err = io.ReadAll(eff.BodyReader)
			if !errors.Is(err, sentinel) {
				t.Errorf("err = %v, want the source's own error", err)
			}
		})
	}
}

// ── end to end: HTTP/1.1 framing ─────────────────────────────────

// TestCompress_H1_BufferedUsesCompressedContentLength walks the real HTTP/1.1
// wire: a buffered compressed body is framed by Content-Length, and that length
// is the compressed size.
func TestCompress_H1_BufferedUsesCompressedContentLength(t *testing.T) {
	raw := bytes.Repeat([]byte("h1 buffered payload "), 100)
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			wire := string(captureH1Request(t, &Request{
				Method: "POST", Path: "/",
				Body:         raw,
				CompressBody: tc.enc,
			}))
			head, body, ok := strings.Cut(wire, "\r\n\r\n")
			if !ok {
				t.Fatalf("malformed request: %q", wire)
			}
			if !strings.Contains(head, "content-encoding: "+tc.token+"\r\n") {
				t.Errorf("head missing content-encoding: %s\n%s", tc.token, head)
			}
			if strings.Contains(head, "Transfer-Encoding: chunked") {
				t.Errorf("chunked used for a body of known length:\n%s", head)
			}
			cl := contentLengthOf(head + "\r\n")
			if cl != len(body) {
				t.Errorf("Content-Length = %d but %d body bytes followed", cl, len(body))
			}
			if cl >= len(raw) {
				t.Errorf("Content-Length = %d >= uncompressed %d: looks like the caller's length", cl, len(raw))
			}
			if got := decodeWith(t, tc.enc, []byte(body)); !bytes.Equal(got, raw) {
				t.Errorf("server-side decode mismatch: got %d bytes, want %d", len(got), len(raw))
			}
		})
	}
}

// TestCompress_H1_StreamingUsesChunked walks the real HTTP/1.1 wire for the
// unknowable-length case: no Content-Length, so http1 must fall back to chunked
// transfer-coding. Sending the caller's uncompressed length here would frame the
// body at the wrong size.
func TestCompress_H1_StreamingUsesChunked(t *testing.T) {
	raw := bytes.Repeat([]byte("h1 streaming payload "), 100)
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			wire := string(captureH1Request(t, &Request{
				Method: "POST", Path: "/",
				BodyReader:    bytes.NewReader(raw),
				ContentLength: int64(len(raw)), // must NOT reach the wire
				CompressBody:  tc.enc,
			}))
			head, body, ok := strings.Cut(wire, "\r\n\r\n")
			if !ok {
				t.Fatalf("malformed request: %q", wire)
			}
			if !strings.Contains(head, "content-encoding: "+tc.token+"\r\n") {
				t.Errorf("head missing content-encoding: %s\n%s", tc.token, head)
			}
			if !strings.Contains(head, "Transfer-Encoding: chunked") {
				t.Errorf("compressed stream not chunked:\n%s", head)
			}
			if strings.Contains(strings.ToLower(head), "content-length") {
				t.Errorf("compressed stream carries a content-length:\n%s", head)
			}
			if got := decodeWith(t, tc.enc, dechunk(t, body)); !bytes.Equal(got, raw) {
				t.Errorf("server-side decode mismatch: got %d bytes, want %d", len(got), len(raw))
			}
		})
	}
}

// dechunk strips RFC 7230 chunk framing, returning the payload.
func dechunk(t testing.TB, body string) []byte {
	t.Helper()
	var out bytes.Buffer
	rest := body
	for {
		line, tail, ok := strings.Cut(rest, "\r\n")
		if !ok {
			t.Fatalf("truncated chunk header in %q", rest)
		}
		n, err := strconv.ParseInt(line, 16, 64)
		if err != nil {
			t.Fatalf("bad chunk size %q: %v", line, err)
		}
		if n == 0 {
			return out.Bytes()
		}
		if int64(len(tail)) < n+2 {
			t.Fatalf("truncated chunk body")
		}
		out.WriteString(tail[:n])
		rest = tail[n+2:]
	}
}

// ── end to end: HTTP/2 through a real server ─────────────────────

// TestCompress_H2_EndToEnd runs every coding through a real net/http HTTP/2
// server, which decodes the body with the reference implementations. This is the
// interoperability check the unit tests cannot make: our encoder output has to
// be readable by somebody else's decoder.
func TestCompress_H2_EndToEnd(t *testing.T) {
	raw := bytes.Repeat([]byte("http/2 request compression "), 400) // ~10 KiB

	type received struct {
		body            []byte
		contentEncoding string
		contentLength   string
		proto           string
	}
	got := make(chan received, 1)

	c, srv := newDecompressTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- received{
			body:            b,
			contentEncoding: r.Header.Get("Content-Encoding"),
			contentLength:   r.Header.Get("Content-Length"),
			proto:           r.Proto,
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer c.Close()
	defer srv.Close()

	for _, tc := range allCodecs {
		for _, mode := range []string{"buffered", "streaming"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				req := &Request{Method: "POST", Path: "/", CompressBody: tc.enc}
				if mode == "buffered" {
					req.Body = raw
				} else {
					req.BodyReader = bytes.NewReader(raw)
					req.ContentLength = int64(len(raw))
				}
				var resp Response
				if err := c.Do(context.Background(), req, &resp); err != nil {
					t.Fatalf("Do: %v", err)
				}
				if resp.Status != http.StatusNoContent {
					t.Fatalf("status = %d", resp.Status)
				}
				r := <-got
				if r.proto != "HTTP/2.0" {
					t.Fatalf("proto = %q, want HTTP/2.0", r.proto)
				}
				if r.contentEncoding != tc.token {
					t.Errorf("server saw content-encoding = %q, want %q", r.contentEncoding, tc.token)
				}
				switch mode {
				case "buffered":
					if r.contentLength != strconv.Itoa(len(r.body)) {
						t.Errorf("server saw content-length = %q, want %d (compressed size)",
							r.contentLength, len(r.body))
					}
				case "streaming":
					if r.contentLength != "" {
						t.Errorf("server saw content-length = %q on a compressed stream, want none",
							r.contentLength)
					}
				}
				if out := decodeWith(t, tc.enc, r.body); !bytes.Equal(out, raw) {
					t.Errorf("server-side body mismatch: got %d bytes, want %d", len(out), len(raw))
				}
			})
		}
	}
}

// ── end to end: HTTP/3 ───────────────────────────────────────────

// TestCompress_H3_EndToEnd is the third protocol. CompressBody lives on the
// shared Request and is applied above the transport, so HTTP/3 must get it for
// free — this pins that it actually does, and that the headers survive the
// re-split into the typed http3.Request rather than being dropped on the floor.
//
// The transport is faked (a live QUIC server is out of reach for a unit test),
// so what this proves is the seam, not the QUIC wire: the encoded body and the
// client-set content-encoding/content-length reach the HTTP/3 layer intact.
func TestCompress_H3_EndToEnd(t *testing.T) {
	raw := bytes.Repeat([]byte("http/3 request compression "), 200)

	for _, tc := range allCodecs {
		for _, mode := range []string{"buffered", "streaming"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				fake := &fakeH3Client{resp: &http3.Response{Status: 201}}
				c := newH3TestClient(t, fake, nil)
				defer func() { _ = c.Close() }()

				req := &Request{
					Method: "POST", Path: "/upload",
					Authority:    "h3.example",
					CompressBody: tc.enc,
				}
				if mode == "buffered" {
					req.Body = raw
				} else {
					req.BodyReader = bytes.NewReader(raw)
					req.ContentLength = int64(len(raw))
				}
				var resp Response
				if err := c.Do(context.Background(), req, &resp); err != nil {
					t.Fatalf("Do: %v", err)
				}
				got := fake.gotReq
				if got == nil {
					t.Fatal("HTTP/3 layer saw no request")
				}
				if v, ok := headerValue(got.Headers, "content-encoding"); !ok || v != tc.token {
					t.Errorf("h3 request content-encoding = %q (present=%v), want %q", v, ok, tc.token)
				}
				switch mode {
				case "buffered":
					if v, _ := headerValue(got.Headers, "content-length"); v != strconv.Itoa(len(got.Body)) {
						t.Errorf("h3 content-length = %q, want %d (compressed size)", v, len(got.Body))
					}
				case "streaming":
					if v, ok := headerValue(got.Headers, "content-length"); ok {
						t.Errorf("h3 content-length = %q on a compressed stream, want none", v)
					}
				}
				if out := decodeWith(t, tc.enc, got.Body); !bytes.Equal(out, raw) {
					t.Errorf("h3 body mismatch: got %d bytes, want %d", len(out), len(raw))
				}
			})
		}
	}
}

// ── retry ────────────────────────────────────────────────────────

// countingDoer stands in for Client.Do and records what each retry attempt was
// handed: the request identity, its body's backing array, and its headers.
type countingDoer struct {
	attempts []attemptRecord
	failFor  int // fail this many attempts before succeeding
}

// attemptRecord captures one attempt. bodyData is the address of the body's
// backing array, which is what distinguishes "compressed once and replayed" from
// "re-compressed identically each time" — the codecs are deterministic, so equal
// bytes alone prove nothing.
type attemptRecord struct {
	req      *Request
	body     []byte
	bodyData *byte
	headers  []conn.HeaderField
	encoding ContentEncoding
}

func (d *countingDoer) record(req *Request) error {
	rec := attemptRecord{
		req:      req,
		body:     append([]byte(nil), req.Body...),
		headers:  req.Headers,
		encoding: req.CompressBody,
	}
	if len(req.Body) > 0 {
		rec.bodyData = &req.Body[0]
	}
	d.attempts = append(d.attempts, rec)
	if len(d.attempts) <= d.failFor {
		return &StreamResetError{Code: conn.ErrCodeRefusedStream}
	}
	return nil
}

func (d *countingDoer) Do(_ context.Context, req *Request, _ *Response) error {
	return d.record(req)
}

func (d *countingDoer) DoStream(_ context.Context, req *Request, _ *StreamResponse) error {
	return d.record(req)
}

// TestRetry_CompressedBodyIsCompressedOnce proves the body is encoded above the
// retry loop rather than inside it.
//
// Equal bytes across attempts would prove nothing on their own — every codec
// here is deterministic, so re-compressing per attempt yields the same bytes.
// The actual proof is that all three attempts are handed the same *Request
// carrying the same body backing array, already encoded and already marked
// EncodingIdentity. The doer is the only per-attempt work in the loop, so a
// body that arrives pre-encoded and inert cannot be encoded again.
func TestRetry_CompressedBodyIsCompressedOnce(t *testing.T) {
	raw := bytes.Repeat([]byte("retry me "), 500)

	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			d := &countingDoer{failFor: 2}
			r := &Retryer{
				d: d,
				opts: RetryOptions{
					MaxAttempts: 3,
					Backoff:     func(int) time.Duration { return 0 },
				},
			}
			req := &Request{
				Method: "PUT", Path: "/", // idempotent, so the loop engages
				Body:         raw,
				CompressBody: tc.enc,
			}
			var resp Response
			if err := r.Do(context.Background(), req, &resp); err != nil {
				t.Fatalf("Do: %v", err)
			}
			if len(d.attempts) != 3 {
				t.Fatalf("attempts = %d, want 3", len(d.attempts))
			}
			first := d.attempts[0]
			if first.bodyData == nil {
				t.Fatal("attempt 0 carried no body")
			}
			for i, got := range d.attempts {
				// The single proof of "compressed once": same request, same
				// bytes, not merely equal ones.
				if got.req != first.req {
					t.Errorf("attempt %d got a different *Request: the body was re-prepared per attempt", i)
				}
				if got.bodyData != first.bodyData {
					t.Errorf("attempt %d got a freshly allocated body: it was re-compressed per attempt", i)
				}
				// Inert on arrival, so Client.Do's own prepare is a no-op and
				// cannot double-encode it.
				if got.encoding != EncodingIdentity {
					t.Errorf("attempt %d still asks for %v: Client.Do would compress it again", i, got.encoding)
				}
				if out := decodeWith(t, tc.enc, got.body); !bytes.Equal(out, raw) {
					t.Errorf("attempt %d does not decode back to the original: got %d bytes, want %d",
						i, len(out), len(raw))
				}
				if v, _ := headerValue(got.headers, "content-length"); v != strconv.Itoa(len(got.body)) {
					t.Errorf("attempt %d content-length = %q, want %d", i, v, len(got.body))
				}
			}
			// The caller's request is untouched, so it stays reusable.
			if !bytes.Equal(req.Body, raw) || req.CompressBody != tc.enc {
				t.Error("Retryer mutated the caller's request")
			}
		})
	}
}

// TestRetry_UncompressedBodyIsUnaffected is the control for the test above: with
// the zero value, the Retryer must hand the transport the caller's own request,
// untouched, exactly as it did before this feature existed.
func TestRetry_UncompressedBodyIsUnaffected(t *testing.T) {
	raw := []byte("plain retry body")
	d := &countingDoer{failFor: 2}
	r := &Retryer{
		d:    d,
		opts: RetryOptions{MaxAttempts: 3, Backoff: func(int) time.Duration { return 0 }},
	}
	req := &Request{Method: "PUT", Path: "/", Body: raw}
	var resp Response
	if err := r.Do(context.Background(), req, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if len(d.attempts) != 3 {
		t.Fatalf("attempts = %d, want 3", len(d.attempts))
	}
	for i, got := range d.attempts {
		if got.req != req {
			t.Errorf("attempt %d did not get the caller's own request", i)
		}
		if !bytes.Equal(got.body, raw) {
			t.Errorf("attempt %d body = %q, want %q", i, got.body, raw)
		}
	}
}

// TestRetry_StreamingBodyStillNotRetried pins that compression does not change
// retry eligibility. A BodyReader cannot be replayed today (the first attempt
// consumes it), and canRetry refuses it for that reason. Wrapping the reader in
// a compressor must not sneak it back into the loop and send a truncated body.
func TestRetry_StreamingBodyStillNotRetried(t *testing.T) {
	for _, enc := range []ContentEncoding{EncodingIdentity, EncodingGzip} {
		t.Run(strconv.Itoa(int(enc)), func(t *testing.T) {
			req := &Request{
				Method: "PUT", Path: "/",
				BodyReader:   bytes.NewReader([]byte("stream")),
				CompressBody: enc,
			}
			r := &Retryer{opts: RetryOptions{MaxAttempts: 3}}
			if r.canRetry(req) {
				t.Error("a streaming body is retryable; the replay would be truncated")
			}
		})
	}
}

// TestRetry_ConflictIsNotRetried proves a request that can never succeed fails
// once instead of burning the whole budget on backoff sleeps.
func TestRetry_ConflictIsNotRetried(t *testing.T) {
	d := &countingDoer{}
	r := &Retryer{
		d:    d,
		opts: RetryOptions{MaxAttempts: 3, Backoff: func(int) time.Duration { return 0 }},
	}
	req := &Request{
		Method: "PUT", Path: "/",
		Body:         []byte("payload"),
		CompressBody: EncodingGzip,
		Headers:      []conn.HeaderField{{Name: []byte("content-encoding"), Value: []byte("gzip")}},
	}
	var resp Response
	err := r.Do(context.Background(), req, &resp)
	if !errors.Is(err, ErrConflictingContentEncoding) {
		t.Errorf("err = %v, want ErrConflictingContentEncoding", err)
	}
	if len(d.attempts) != 0 {
		t.Errorf("transport saw %d attempts, want 0", len(d.attempts))
	}
}

// ── pooled writer hygiene ────────────────────────────────────────

// TestCompress_PooledWriterNotPoisonedByError is the encode-side twin of
// TestDecompress_PooledReaderNotPoisonedByPartialRead.
//
// The decode side found that brotli.Reader.Reset does not discard buffered
// input, so a reader stopped early poisoned the next use. The equivalent
// question for a Writer is whether state survives Reset when a stream is
// abandoned mid-write — never Closed, so never flushed. This interleaves a
// failed compression with a good one and requires the good one to be intact.
func TestCompress_PooledWriterNotPoisonedByError(t *testing.T) {
	sentinel := errors.New("source died")
	raw := []byte("the next request must compress correctly")

	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 30; i++ {
				// Abandon a stream mid-write: the writer holds unflushed state
				// and is never Closed.
				req := &Request{
					Method: "POST", Path: "/",
					BodyReader:   &failingSourceReader{prefix: bytes.Repeat([]byte("poison "), 10_000), err: sentinel},
					CompressBody: tc.enc,
				}
				eff, release, err := prepareCompressedRequest(req)
				if err != nil {
					t.Fatalf("iter %d: prepare: %v", i, err)
				}
				if _, err := io.Copy(io.Discard, eff.BodyReader); !errors.Is(err, sentinel) {
					t.Fatalf("iter %d: err = %v, want the source error", i, err)
				}
				release() // returns the half-used writer to the pool

				// The very next request must draw a clean writer.
				good := &Request{Method: "POST", Path: "/", Body: raw, CompressBody: tc.enc}
				geff, grel, err := prepareCompressedRequest(good)
				if err != nil {
					t.Fatalf("iter %d: prepare after abort: %v", i, err)
				}
				if out := decodeWith(t, tc.enc, geff.Body); !bytes.Equal(out, raw) {
					t.Fatalf("iter %d: poisoned writer: got %q, want %q", i, out, raw)
				}
				if grel != nil {
					grel()
				}
			}
		})
	}
}

// TestCompress_PooledWriterNotPoisonedByPartialRead covers the other abandon
// shape: the caller stops reading the compressed stream partway (a cancelled
// upload) and the request is released with the writer mid-stream.
func TestCompress_PooledWriterNotPoisonedByPartialRead(t *testing.T) {
	raw := bytes.Repeat([]byte("partial upload payload "), 50_000) // ~1.1 MiB
	small := []byte("and this one must still be correct")

	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 30; i++ {
				req := &Request{
					Method: "POST", Path: "/",
					BodyReader:   bytes.NewReader(raw),
					CompressBody: tc.enc,
				}
				eff, release, err := prepareCompressedRequest(req)
				if err != nil {
					t.Fatalf("iter %d: prepare: %v", i, err)
				}
				var b [8]byte
				if _, err := io.ReadFull(eff.BodyReader, b[:]); err != nil {
					t.Fatalf("iter %d: short read: %v", i, err)
				}
				release() // mid-stream: unflushed input, never Closed

				good := &Request{Method: "POST", Path: "/", Body: small, CompressBody: tc.enc}
				geff, grel, err := prepareCompressedRequest(good)
				if err != nil {
					t.Fatalf("iter %d: prepare after partial: %v", i, err)
				}
				if out := decodeWith(t, tc.enc, geff.Body); !bytes.Equal(out, small) {
					t.Fatalf("iter %d: poisoned writer: got %q, want %q", i, out, small)
				}
				if grel != nil {
					grel()
				}
			}
		})
	}
}

// TestCompress_PooledWriterSequentialReuse hammers the common path: back-to-back
// compressions of varying sizes, where each Get is likely to draw the writer the
// previous iteration returned. Any state carried across a recycle shows up here.
func TestCompress_PooledWriterSequentialReuse(t *testing.T) {
	payloads := [][]byte{
		[]byte("first"),
		bytes.Repeat([]byte("second payload "), 1000),
		{0},
		bytes.Repeat([]byte("fourth "), 50_000),
	}
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 50; round++ {
				for i, raw := range payloads {
					req := &Request{Method: "POST", Path: "/", Body: raw, CompressBody: tc.enc}
					eff, release, err := prepareCompressedRequest(req)
					if err != nil {
						t.Fatalf("round %d payload %d: %v", round, i, err)
					}
					if out := decodeWith(t, tc.enc, eff.Body); !bytes.Equal(out, raw) {
						t.Fatalf("round %d payload %d: mismatch (got %d bytes, want %d)",
							round, i, len(out), len(raw))
					}
					if release != nil {
						release()
					}
				}
			}
		})
	}
}

// TestCompress_ReleaseIsIdempotent guards the double-Put hazard: a writer put
// back twice would be handed to two requests at once and interleave their
// output.
func TestCompress_ReleaseIsIdempotent(t *testing.T) {
	req := &Request{
		Method: "POST", Path: "/",
		BodyReader:   bytes.NewReader([]byte("payload")),
		CompressBody: EncodingGzip,
	}
	eff, release, err := prepareCompressedRequest(req)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	_, _ = io.ReadAll(eff.BodyReader)
	release()
	release() // must not double-Put
	release()

	// The pool must still hand out working writers.
	good := &Request{Method: "POST", Path: "/", Body: []byte("still fine"), CompressBody: EncodingGzip}
	geff, _, err := prepareCompressedRequest(good)
	if err != nil {
		t.Fatalf("prepare after double release: %v", err)
	}
	if out := decodeWith(t, EncodingGzip, geff.Body); string(out) != "still fine" {
		t.Errorf("got %q, want %q", out, "still fine")
	}
}

// ── zstd encoder goroutine lifecycle ─────────────────────────────

// probeSink samples the live goroutine count from inside the encoder's output
// path. Under an async zstd encoder the compressed blocks are written by the
// encoder's own goroutines, so those are live and counted here; under a
// synchronous encoder the write happens on the caller's goroutine and the count
// is unchanged.
//
// Sampling here rather than before/after is the whole point: an async encoder
// that is drained on Reset returns the count to baseline, so a before/after
// check passes while still churning goroutines per request.
type probeSink struct {
	dst    bytes.Buffer
	peak   int
	writes int
}

func (p *probeSink) Write(b []byte) (int, error) {
	p.writes++
	if n := runtime.NumGoroutine(); n > p.peak {
		p.peak = n
	}
	return p.dst.Write(b)
}

// zstdProbePayload is large enough to span many encoder blocks, so an async
// encoder has a pipeline running while the sink is being written.
func zstdProbePayload() []byte {
	return bytes.Repeat([]byte("goroutine probe payload "), 200_000) // ~4.8 MiB
}

// TestCompress_Zstd_PooledEncoderIsSynchronous is the load-generator guard, and
// the encode-side twin of TestDecompress_Zstd_PooledNoGoroutineGrowth.
//
// zstd.NewWriter defaults to GOMAXPROCS concurrency, which runs an async
// pipeline: two goroutines per block, spawned from Write and not joined until
// the next block or Close. Pooling such an encoder would multiply goroutines by
// the pool size. WithEncoderConcurrency(1) takes the synchronous path instead
// and spawns nothing.
//
// The negative control below is what gives this test teeth: it runs the same
// probe against a default-concurrency encoder and requires it to *fail* the
// assertion. Without that, a probe that simply never observes anything would
// pass vacuously.
func TestCompress_Zstd_PooledEncoderIsSynchronous(t *testing.T) {
	raw := zstdProbePayload()

	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("GOMAXPROCS < 2: zstd never takes the async path, negative control cannot fire")
	}

	// Warm the pool so first-use construction is not read as growth.
	for i := 0; i < 4; i++ {
		if _, err := compressBody(EncodingZstd, []byte("warm")); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	t.Run("negative-control", func(t *testing.T) {
		// Prove the probe can see an async pipeline. If this subtest ever stops
		// failing to stay under baseline, the probe has lost its teeth and the
		// positive subtest below means nothing.
		base := goroutineBaseline()
		probe := &probeSink{}
		zw, err := zstd.NewWriter(probe) // default concurrency: the bad case
		if err != nil {
			t.Fatalf("zstd.NewWriter: %v", err)
		}
		if _, err := zw.Write(raw); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		t.Logf("async encoder: peak %d goroutines during %d sink writes (baseline %d)",
			probe.peak, probe.writes, base)
		if probe.peak <= base {
			t.Fatalf("probe saw no extra goroutines from a default-concurrency encoder "+
				"(peak %d, baseline %d): the probe has no teeth, so the pooled-encoder "+
				"assertion below proves nothing", probe.peak, base)
		}
	})

	t.Run("pooled-encoder-spawns-nothing", func(t *testing.T) {
		base := goroutineBaseline()
		probe := &probeSink{}
		w, err := getCompressWriter(EncodingZstd, probe)
		if err != nil {
			t.Fatalf("getCompressWriter: %v", err)
		}
		if _, err := w.Write(raw); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		putCompressWriter(w)
		if probe.writes < 2 {
			t.Fatalf("sink written %d times: payload too small to observe a pipeline", probe.writes)
		}
		t.Logf("pooled encoder: peak %d goroutines during %d sink writes (baseline %d)",
			probe.peak, probe.writes, base)
		if probe.peak > base {
			t.Errorf("pooled zstd encoder ran %d extra goroutines (peak %d, baseline %d): "+
				"is WithEncoderConcurrency(1) still set?", probe.peak-base, probe.peak, base)
		}
	})

	t.Run("no-growth-across-requests", func(t *testing.T) {
		base := goroutineBaseline()
		const iterations = 200
		body := bytes.Repeat([]byte("steady state "), 20_000)
		for i := 0; i < iterations; i++ {
			out, err := compressBody(EncodingZstd, body)
			if err != nil {
				t.Fatalf("iter %d: %v", i, err)
			}
			if len(out) == 0 {
				t.Fatalf("iter %d: empty output", i)
			}
		}
		if got := settleGoroutines(base, 2*time.Second); got > base {
			t.Errorf("goroutines grew across %d pooled compressions: %d -> %d", iterations, base, got)
		}
	})

	t.Run("abandoned-streams-strand-nothing", func(t *testing.T) {
		base := goroutineBaseline()
		const iterations = 200
		for i := 0; i < iterations; i++ {
			req := &Request{
				Method: "POST", Path: "/",
				BodyReader:   bytes.NewReader(raw[:1<<20]),
				CompressBody: EncodingZstd,
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("iter %d: %v", i, err)
			}
			var b [1]byte
			_, _ = eff.BodyReader.Read(b[:])
			// Abandon without release: nothing may be stranded.
			_ = release
		}
		if got := settleGoroutines(base, 2*time.Second); got > base {
			t.Errorf("goroutines stranded by %d abandoned encoders: %d -> %d", iterations, base, got)
		}
	})
}

// ── allocation behaviour ─────────────────────────────────────────

// TestCompress_Identity_IsAllocationFree pins the zero value's cost at exactly
// nothing: the common path must not pay for a feature it does not use.
func TestCompress_Identity_IsAllocationFree(t *testing.T) {
	req := &Request{Method: "POST", Path: "/", Body: []byte("payload")}
	got := testing.AllocsPerRun(100, func() {
		eff, release, err := prepareCompressedRequest(req)
		if err != nil || eff != req || release != nil {
			t.Fatal("identity path misbehaved")
		}
	})
	if got != 0 {
		t.Errorf("identity path allocates %.1f/op, want 0", got)
	}
}

// TestCompress_PoolReuse_NoPerRequestWriterAlloc asserts the encoders really are
// recycled. A fresh brotli or zstd encoder is tens of KiB and dozens of
// allocations; with a warm pool only the output buffer should survive.
func TestCompress_PoolReuse_NoPerRequestWriterAlloc(t *testing.T) {
	raw := bytes.Repeat([]byte("pool reuse payload "), 200)
	for _, tc := range allCodecs {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				if _, err := compressBody(tc.enc, raw); err != nil {
					t.Fatalf("warmup: %v", err)
				}
			}
			got := testing.AllocsPerRun(100, func() {
				if _, err := compressBody(tc.enc, raw); err != nil {
					t.Fatalf("compressBody: %v", err)
				}
			})
			// A fresh encoder per call would be orders of magnitude above this.
			const maxAllocs = 20
			t.Logf("%s: %.1f allocs/op (pooled)", tc.name, got)
			if got > maxAllocs {
				t.Errorf("%.1f allocs/op exceeds %d — pooled writer is likely not being reused",
					got, maxAllocs)
			}
		})
	}
}

// ── fuzz ─────────────────────────────────────────────────────────

// FuzzCompressBody drives arbitrary request bodies through every coding and
// requires each to survive a round trip through the decode side. A request body
// is caller-controlled, so no input may panic, and anything we claim to have
// encoded must be decodable — a mismatch here is silent corruption of a user's
// upload.
func FuzzCompressBody(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("a"))
	f.Add([]byte("hello compressed request"))
	f.Add(bytes.Repeat([]byte("repetitive "), 1000))
	f.Add(bytes.Repeat([]byte{0}, 1<<16))
	f.Add([]byte{0xff, 0x00, 0xff, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, tc := range allCodecs {
			out, err := compressBody(tc.enc, data)
			if err != nil {
				t.Fatalf("%s: compress %d bytes: %v", tc.name, len(data), err)
			}
			back, err := decompressFully(tc.enc, out, DefaultMaxDecompressedSize)
			if err != nil {
				t.Fatalf("%s: round trip failed: %v", tc.name, err)
			}
			if !bytes.Equal(back, data) {
				t.Fatalf("%s: round trip mismatch: %d bytes in, %d out", tc.name, len(data), len(back))
			}
		}
	})
}

// FuzzCompressStream is the streaming twin: it exercises the Get/Reset/read/
// release pool lifecycle the buffered path does not, so a writer returned to the
// pool poisoned surfaces across iterations.
func FuzzCompressStream(f *testing.F) {
	f.Add([]byte{}, 1)
	f.Add([]byte("stream me"), 3)
	f.Add(bytes.Repeat([]byte("repetitive "), 1000), 7)
	f.Add(bytes.Repeat([]byte{0}, 1<<16), 64)

	f.Fuzz(func(t *testing.T, data []byte, readSize int) {
		// Vary the read size so the compressor's output is drained in odd
		// increments, which is what a flow-controlled transport does.
		if readSize < 1 {
			readSize = 1
		}
		if readSize > 1<<16 {
			readSize = 1 << 16
		}
		for _, tc := range allCodecs {
			req := &Request{
				Method: "POST", Path: "/",
				BodyReader:   bytes.NewReader(data),
				CompressBody: tc.enc,
			}
			eff, release, err := prepareCompressedRequest(req)
			if err != nil {
				t.Fatalf("%s: prepare: %v", tc.name, err)
			}
			var out bytes.Buffer
			buf := make([]byte, readSize)
			for {
				n, rerr := eff.BodyReader.Read(buf)
				out.Write(buf[:n])
				if rerr != nil {
					if !errors.Is(rerr, io.EOF) {
						t.Fatalf("%s: read: %v", tc.name, rerr)
					}
					break
				}
			}
			release()
			back, err := decompressFully(tc.enc, out.Bytes(), DefaultMaxDecompressedSize)
			if err != nil {
				t.Fatalf("%s: round trip failed (readSize=%d): %v", tc.name, readSize, err)
			}
			if !bytes.Equal(back, data) {
				t.Fatalf("%s: round trip mismatch (readSize=%d): %d in, %d out",
					tc.name, readSize, len(data), len(back))
			}
		}
	})
}

// ── benchmarks ───────────────────────────────────────────────────

// BenchmarkCompressBody reports the steady-state cost of encoding a buffered
// request body per coding, with a warm writer pool. Compression allocates its
// output by definition, so this is not a zero-alloc path (client/ is outside the
// bench gate). What it pins is that the *writer* is pooled: allocs/op stays flat
// instead of carrying a fresh multi-KiB encoder on every request.
func BenchmarkCompressBody(b *testing.B) {
	raw := bytes.Repeat([]byte("benchmark request payload "), 1000) // ~26 KiB
	for _, tc := range allCodecs {
		b.Run(tc.name, func(b *testing.B) {
			if _, err := compressBody(tc.enc, raw); err != nil {
				b.Fatalf("warmup: %v", err)
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := compressBody(tc.enc, raw)
				if err != nil {
					b.Fatalf("compressBody: %v", err)
				}
				if len(out) == 0 {
					b.Fatal("empty output")
				}
			}
		})
	}
}

// BenchmarkPrepareCompressedRequest_Streaming reports the steady-state cost of
// the streaming encode path per coding: pool Get, Reset, drain, release.
func BenchmarkPrepareCompressedRequest_Streaming(b *testing.B) {
	raw := bytes.Repeat([]byte("benchmark stream payload "), 1000)
	for _, tc := range allCodecs {
		b.Run(tc.name, func(b *testing.B) {
			src := bytes.NewReader(nil)
			req := &Request{Method: "POST", Path: "/", BodyReader: src, CompressBody: tc.enc}
			buf := make([]byte, 32<<10)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				src.Reset(raw)
				eff, release, err := prepareCompressedRequest(req)
				if err != nil {
					b.Fatalf("prepare: %v", err)
				}
				for {
					if _, err := eff.BodyReader.Read(buf); err != nil {
						break
					}
				}
				release()
			}
		})
	}
}

// BenchmarkPrepareCompressedRequest_Identity pins the zero value's overhead on
// the path every existing caller takes.
func BenchmarkPrepareCompressedRequest_Identity(b *testing.B) {
	req := &Request{Method: "POST", Path: "/", Body: []byte("payload")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := prepareCompressedRequest(req); err != nil {
			b.Fatal(err)
		}
	}
}
