package client

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// captureH1Request runs one request against a raw TCP listener that records
// every byte the client writes, then replies with a minimal 204. It returns the
// exact request bytes as they went out on the wire.
//
// A raw listener (rather than httptest) is the point: net/http would parse and
// re-render the request, hiding exactly the framing decisions under test
// (Content-Length vs Transfer-Encoding: chunked, header presence and order).
func captureH1Request(t *testing.T, req *Request) []byte {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer func() { _ = ln.Close() }()

	captured := make(chan []byte, 1)
	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			captured <- nil
			return
		}
		defer func() { _ = nc.Close() }()
		captured <- readWholeRequest(t, nc)
		// Minimal final response so the client's Do returns cleanly.
		_, _ = nc.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"))
	}()

	c, err := NewClient(ClientOptions{
		Transport: TransportH1SingleConn,
		Addr:      ln.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	require.NoError(t, err, "NewClient")
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp Response
	require.NoError(t, c.Do(ctx, req, &resp), "Do")

	select {
	case b := <-captured:
		require.NotNil(t, b, "accept failed, so no wire bytes were captured")
		// The listener port is ephemeral; normalise it so the goldens are stable.
		return bytes.Replace(b, []byte("Host: "+ln.Addr().String()+"\r\n"), []byte("Host: %ADDR%\r\n"), 1)
	case <-time.After(5 * time.Second):
		require.Fail(t, "timed out waiting for captured request")
		return nil
	}
}

// readWholeRequest reads the request head, then reads exactly as many body
// bytes as the head's framing declares (Content-Length, or chunked up to the
// terminating zero-length chunk). Returns head+body verbatim.
func readWholeRequest(t *testing.T, nc net.Conn) []byte {
	t.Helper()
	_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))

	br := bufio.NewReader(nc)
	var out bytes.Buffer

	// Head: read to the blank line.
	for {
		line, err := br.ReadBytes('\n')
		out.Write(line)
		if err != nil {
			return out.Bytes()
		}
		if string(line) == "\r\n" {
			break
		}
	}

	head := out.String()
	switch {
	case strings.Contains(head, "Transfer-Encoding: chunked"):
		readChunkedInto(&out, br)
	default:
		if n := contentLengthOf(head); n > 0 {
			body := make([]byte, n)
			if _, err := io.ReadFull(br, body); err == nil {
				out.Write(body)
			}
		}
	}
	return out.Bytes()
}

// readChunkedInto copies the raw chunked body — framing bytes included — up to
// and including the terminating zero-length chunk.
func readChunkedInto(out *bytes.Buffer, br *bufio.Reader) {
	for {
		sizeLine, err := br.ReadBytes('\n')
		out.Write(sizeLine)
		if err != nil {
			return
		}
		var n int
		if _, serr := fscanHex(strings.TrimSpace(string(sizeLine)), &n); serr != nil {
			return
		}
		if n == 0 {
			// Trailing CRLF of the terminating chunk.
			crlf, _ := br.ReadBytes('\n')
			out.Write(crlf)
			return
		}
		chunk := make([]byte, n+2) // data + CRLF
		if _, rerr := io.ReadFull(br, chunk); rerr != nil {
			return
		}
		out.Write(chunk)
	}
}

// fscanHex parses a lowercase/uppercase hex chunk size.
func fscanHex(s string, out *int) (int, error) {
	v := 0
	if s == "" {
		return 0, io.ErrUnexpectedEOF
	}
	for _, r := range s {
		var d int
		switch {
		case r >= '0' && r <= '9':
			d = int(r - '0')
		case r >= 'a' && r <= 'f':
			d = int(r-'a') + 10
		case r >= 'A' && r <= 'F':
			d = int(r-'A') + 10
		default:
			return 0, io.ErrUnexpectedEOF
		}
		v = v*16 + d
	}
	*out = v
	return 1, nil
}

// contentLengthOf extracts the Content-Length value from a request head.
func contentLengthOf(head string) int {
	for _, line := range strings.Split(head, "\r\n") {
		if name, val, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(name), "content-length") {
			n := 0
			for _, r := range strings.TrimSpace(val) {
				if r < '0' || r > '9' {
					return 0
				}
				n = n*10 + int(r-'0')
			}
			return n
		}
	}
	return 0
}

// TestCompress_Baseline_H1WireBytes is the backward-compatibility anchor.
//
// The goldens below were captured from this exact test running on the parent
// commit, before Request.CompressBody existed. They must not change: a plain
// request — buffered body or streaming body — has to keep producing the same
// bytes on the wire once request compression is added.
//
// The companion TestCompress_Identity_H1WireBytesUnchanged asserts that an
// explicit CompressBody: EncodingIdentity produces these same bytes, which is
// what makes the zero value provably free.
func TestCompress_Baseline_H1WireBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  func() *Request
		want string
	}{
		{
			name: "buffered-body",
			req: func() *Request {
				return &Request{
					Method: "POST",
					Path:   "/upload",
					Body:   []byte("hello wire"),
					Headers: []conn.HeaderField{
						{Name: []byte("x-test"), Value: []byte("1")},
					},
				}
			},
			want: goldenBufferedBody,
		},
		{
			name: "streaming-body",
			req: func() *Request {
				return &Request{
					Method:     "POST",
					Path:       "/stream",
					BodyReader: strings.NewReader("hello stream"),
					Headers: []conn.HeaderField{
						{Name: []byte("x-test"), Value: []byte("1")},
					},
				}
			},
			want: goldenStreamingBody,
		},
		{
			name: "streaming-body-with-content-length",
			req: func() *Request {
				return &Request{
					Method:        "POST",
					Path:          "/stream-cl",
					BodyReader:    strings.NewReader("hello stream"),
					ContentLength: 12,
					Headers: []conn.HeaderField{
						{Name: []byte("x-test"), Value: []byte("1")},
					},
				}
			},
			want: goldenStreamingBodyCL,
		},
		{
			name: "no-body",
			req: func() *Request {
				return &Request{Method: "GET", Path: "/"}
			},
			want: goldenNoBody,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := string(captureH1Request(t, tc.req()))

			assert.Equalf(t, tc.want, got,
				"wire bytes changed.\n got: %q\nwant: %q — these goldens were captured before "+
					"Request.CompressBody existed and pin backward compatibility", got, tc.want)
		})
	}
}

// Goldens: the literal request bytes. Captured on the parent commit.
const (
	// A buffered body's length is known before the request is written, so it is
	// declared rather than chunk-framed. This golden previously pinned chunked,
	// which RFC 9112 §6.3 discourages when the length is known AND §6.1 forbids
	// outright against a peer the client has not observed speaking HTTP/1.1 —
	// which is every first request on a connection.
	goldenBufferedBody = "POST /upload HTTP/1.1\r\n" +
		"Host: %ADDR%\r\n" +
		"x-test: 1\r\n" +
		"accept-encoding: gzip, deflate, br, zstd\r\n" +
		"content-length: 10\r\n" +
		"\r\n" +
		"hello wire"

	goldenStreamingBody = "POST /stream HTTP/1.1\r\n" +
		"Host: %ADDR%\r\n" +
		"x-test: 1\r\n" +
		"accept-encoding: gzip, deflate, br, zstd\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"c\r\nhello stream\r\n0\r\n\r\n"

	goldenStreamingBodyCL = "POST /stream-cl HTTP/1.1\r\n" +
		"Host: %ADDR%\r\n" +
		"x-test: 1\r\n" +
		"accept-encoding: gzip, deflate, br, zstd\r\n" +
		"content-length: 12\r\n" +
		"\r\n" +
		"hello stream"

	goldenNoBody = "GET / HTTP/1.1\r\n" +
		"Host: %ADDR%\r\n" +
		"accept-encoding: gzip, deflate, br, zstd\r\n" +
		"\r\n"
)
