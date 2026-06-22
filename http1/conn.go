package http1

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Conn is a persistent HTTP/1.1 connection. At most one Exchange at a time
// (no pipelining). The caller serializes exchanges via an external mutex or
// by using Conn only from one goroutine at a time.
type Conn struct {
	nc     net.Conn
	br     *bufio.Reader
	closed atomic.Bool
}

// NewConn wraps nc in a persistent HTTP/1.1 Conn.
// nc must already be connected (TCP + optional TLS handshake complete).
func NewConn(nc net.Conn) *Conn {
	return &Conn{
		nc: nc,
		br: bufio.NewReaderSize(nc, 16*1024),
	}
}

// IsAlive reports whether the connection is open and usable.
func (c *Conn) IsAlive() bool {
	return !c.closed.Load()
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	c.closed.Store(true)
	return c.nc.Close()
}

// NewExchange allocates and returns a new Exchange for the next HTTP/1.1
// request/response pair. The previous exchange must be fully drained before
// calling NewExchange again.
func (c *Conn) NewExchange() *Exchange {
	return &Exchange{c: c}
}

// crlf and finalChunk are shared immutable slices for writev payloads.
var (
	crlf       = []byte("\r\n")
	finalChunk = []byte("0\r\n\r\n")
)

// Exchange is one HTTP/1.1 request/response pair.
//
// Lifecycle:
//  1. WriteRequest — send request line + headers
//  2. WriteBody (zero or more) — send request body chunks; omit if endStream=true in WriteRequest
//  3. ReadResponse — receive response status + headers
//  4. ReadBodyChunk (zero or more) — receive response body until done=true
type Exchange struct {
	c      *Conn
	method string // request method (from :method pseudo-header)

	// request side
	reqChunked bool // sending chunked request body

	// response side
	statusCode     int
	keepAlive      bool
	respChunked    bool
	contentLen     int64 // -1 = read until connection close
	bodyRead       int64
	chunkRemaining int64
	chunkFinal     bool // terminal 0-chunk received
}

// WriteRequest sends the HTTP/1.1 request line and headers.
// fields must contain H2-style pseudo-headers (:method, :path, :authority,
// :scheme) followed by regular headers. :scheme and :protocol are silently
// dropped. Host is derived from :authority.
//
// When endStream is true no body will follow — the request is fully sent by
// WriteRequest. When endStream is false, WriteBody must be called to send the
// body and signal completion. If no Content-Length header is present and
// endStream is false, WriteRequest adds "Transfer-Encoding: chunked" and
// WriteBody writes RFC 7230 chunk framing.
//
// Uses net.Buffers (writev) to avoid copying all header bytes into one buffer.
func (ex *Exchange) WriteRequest(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	var method, path, authority string
	var hasContentLength bool

	for _, f := range fields {
		switch string(f.Name) {
		case ":method":
			method = string(f.Value)
		case ":path":
			path = string(f.Value)
		case ":authority":
			authority = string(f.Value)
		case "content-length":
			hasContentLength = true
		}
	}
	ex.method = method

	// Determine how to frame the request body.
	if !endStream && !hasContentLength {
		ex.reqChunked = true
	}

	// Build request using net.Buffers for scatter-gather write (writev on Linux).
	var bufs net.Buffers
	bufs = append(bufs,
		[]byte(method+" "+path+" HTTP/1.1\r\n"),
		[]byte("Host: "+authority+"\r\n"),
	)

	for _, f := range fields {
		name := string(f.Name)
		if len(name) == 0 || name[0] == ':' {
			continue // skip pseudo-headers
		}
		lower := strings.ToLower(name)
		switch lower {
		case "host", "connection", "transfer-encoding", "te",
			"proxy-connection", "keep-alive", "upgrade":
			// H2 forbidden / hop-by-hop headers; we manage them ourselves.
			continue
		}
		bufs = append(bufs, []byte(lower+": "+string(f.Value)+"\r\n"))
	}

	// Body framing signals.
	if endStream {
		// No body follows. Add Content-Length: 0 for methods that could carry a
		// body so strict servers don't reject the request.
		switch method {
		case "POST", "PUT", "PATCH":
			bufs = append(bufs, []byte("Content-Length: 0\r\n"))
		}
	} else if ex.reqChunked {
		bufs = append(bufs, []byte("Transfer-Encoding: chunked\r\n"))
	}
	// else: Content-Length already in user-supplied headers.

	bufs = append(bufs, crlf) // blank line ending headers

	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetWriteDeadline(dl)
		defer func() { _ = ex.c.nc.SetWriteDeadline(time.Time{}) }()
	}
	_, err := bufs.WriteTo(ex.c.nc)
	return err
}

// WriteBody writes a body chunk to the wire.
// When fin is true this is the last chunk; WriteBody must not be called again.
// Omit WriteBody entirely when endStream was true in WriteRequest.
func (ex *Exchange) WriteBody(ctx context.Context, p []byte, fin bool) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetWriteDeadline(dl)
		defer func() { _ = ex.c.nc.SetWriteDeadline(time.Time{}) }()
	}

	if ex.reqChunked {
		var bufs net.Buffers
		if len(p) > 0 {
			// Chunk: hex_len\r\n data \r\n
			bufs = append(bufs,
				[]byte(strconv.FormatInt(int64(len(p)), 16)+"\r\n"),
				p,
				crlf,
			)
		}
		if fin {
			bufs = append(bufs, finalChunk)
		}
		if len(bufs) == 0 {
			return nil
		}
		_, err := bufs.WriteTo(ex.c.nc)
		return err
	}

	// Non-chunked: write data directly (Content-Length governs framing).
	if len(p) == 0 {
		return nil
	}
	_, err := ex.c.nc.Write(p)
	return err
}

// ReadResponse reads the HTTP/1.1 response status line and headers.
// It skips 1xx informational responses automatically and blocks until a
// final (≥200) status is received.
// Returns the response headers as []hpack.HeaderField. The first element is
// always the ":status" pseudo-header for compatibility with the client layer.
func (ex *Exchange) ReadResponse(ctx context.Context) (statusCode int, headers []hpack.HeaderField, err error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = ex.c.nc.SetReadDeadline(dl)
	}

	var proto string
	for {
		// Status line: "HTTP/1.x NNN Reason\r\n"
		line, rerr := ex.c.br.ReadString('\n')
		if rerr != nil {
			return 0, nil, fmt.Errorf("http1: read status line: %w", rerr)
		}
		line = strings.TrimRight(line, "\r\n")

		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/1") {
			return 0, nil, fmt.Errorf("http1: malformed status line: %q", line)
		}
		proto = parts[0]
		code, perr := strconv.Atoi(parts[1])
		if perr != nil {
			return 0, nil, fmt.Errorf("http1: invalid status code: %q", parts[1])
		}

		if code >= 200 {
			statusCode = code
			break
		}
		// 1xx informational: drain its headers and loop back for the real response.
		if err = ex.consumeHeaders(nil, false); err != nil {
			return 0, nil, err
		}
	}

	ex.statusCode = statusCode
	// RFC 2616 §8.1: HTTP/1.1 defaults to persistent; HTTP/1.0 defaults to close.
	ex.keepAlive = proto == "HTTP/1.1"
	ex.contentLen = -1

	headers = make([]hpack.HeaderField, 0, 12)
	// Prepend :status for compatibility with the H2-style client layer.
	headers = append(headers, hpack.HeaderField{
		Name:  []byte(":status"),
		Value: []byte(strconv.Itoa(statusCode)),
	})

	err = ex.consumeHeaders(&headers, true)
	return statusCode, headers, err
}

// consumeHeaders reads HTTP/1.1 headers until a blank line.
// When out is non-nil, parsed headers are appended to *out.
// When parseBody is true, it also updates ex.contentLen, ex.respChunked,
// and ex.keepAlive from the header values.
func (ex *Exchange) consumeHeaders(out *[]hpack.HeaderField, parseBody bool) error {
	for {
		line, err := ex.c.br.ReadString('\n')
		if err != nil {
			return fmt.Errorf("http1: read header line: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return nil // blank line = end of headers
		}

		colon := strings.IndexByte(line, ':')
		if colon < 0 {
			continue // skip malformed header lines
		}
		name := strings.ToLower(strings.TrimSpace(line[:colon]))
		value := strings.TrimSpace(line[colon+1:])

		if parseBody {
			switch name {
			case "content-length":
				// RFC 2616 §4.4 Rule 3: Transfer-Encoding wins; ignore CL when chunked.
				if n, perr := strconv.ParseInt(value, 10, 64); perr == nil && !ex.respChunked && ex.contentLen < 0 {
					ex.contentLen = n
				}
			case "transfer-encoding":
				if strings.Contains(strings.ToLower(value), "chunked") {
					ex.respChunked = true
					ex.contentLen = -2 // sentinel: chunked overrides content-length
				}
			case "connection":
				lower := strings.ToLower(value)
				if strings.Contains(lower, "close") {
					ex.keepAlive = false
				} else if strings.Contains(lower, "keep-alive") {
					ex.keepAlive = true
				}
			}
		}

		if out != nil {
			*out = append(*out, hpack.HeaderField{
				Name:  []byte(name),
				Value: []byte(value),
			})
		}
	}
}

// ReadBodyChunk reads up to len(buf) bytes of the response body.
// Returns (n, done, err). done=true when the response body is fully received.
// ReadBodyChunk must not be called after done=true is returned.
//
// For HEAD responses ReadBodyChunk returns (0, true, nil) immediately without
// reading any bytes (the server must not send a body for HEAD per RFC 7230 §3.3).
func (ex *Exchange) ReadBodyChunk(buf []byte) (n int, done bool, err error) {
	// HEAD responses carry no body regardless of Content-Length.
	if ex.method == "HEAD" {
		return 0, true, nil
	}

	// 204 No Content and 304 Not Modified also have no body.
	if ex.statusCode == 204 || ex.statusCode == 304 {
		return 0, true, nil
	}

	if ex.respChunked {
		return ex.readChunkedChunk(buf)
	}

	// Content-Length known.
	if ex.contentLen >= 0 {
		if ex.contentLen == 0 || ex.bodyRead >= ex.contentLen {
			return 0, true, nil
		}
		remaining := ex.contentLen - ex.bodyRead
		if int64(len(buf)) > remaining {
			buf = buf[:remaining]
		}
		n, err = ex.c.br.Read(buf)
		ex.bodyRead += int64(n)
		done = ex.bodyRead >= ex.contentLen
		if err == io.EOF {
			if !done {
				// Premature EOF before Content-Length satisfied.
				return n, true, fmt.Errorf("http1: premature EOF: got %d of %d bytes", ex.bodyRead, ex.contentLen)
			}
			// Final body bytes arrived coalesced with io.EOF in a single
			// Read (bufio passes through the underlying (n, io.EOF) when
			// the caller buffer is >= bufio's buffer). The body is now
			// complete, so surface the bytes with a nil error instead of
			// discarding n. The EOF means the peer closed the socket, so the
			// connection is no longer reusable — do not let it be pooled.
			ex.keepAlive = false
			err = nil
		}
		return n, done, err
	}

	// contentLen == -1: read until connection close.
	n, err = ex.c.br.Read(buf)
	if err == io.EOF {
		ex.keepAlive = false
		return n, true, nil
	}
	return n, false, err
}

// readChunkedChunk reads the next chunk worth of data from a chunked body.
func (ex *Exchange) readChunkedChunk(buf []byte) (n int, done bool, err error) {
	if ex.chunkFinal {
		return 0, true, nil
	}

	// Need to start a new chunk?
	for ex.chunkRemaining == 0 {
		// Read chunk-size line: "hex[;extension]\r\n"
		line, lerr := ex.c.br.ReadString('\n')
		if lerr != nil {
			return 0, false, fmt.Errorf("http1: read chunk size: %w", lerr)
		}
		line = strings.TrimRight(line, "\r\n")
		if semi := strings.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi] // strip chunk extensions
		}
		line = strings.TrimSpace(line)
		size, perr := strconv.ParseInt(line, 16, 64)
		if perr != nil {
			return 0, false, fmt.Errorf("http1: invalid chunk size %q: %w", line, perr)
		}
		if size < 0 {
			// chunk-size is 1*HEXDIG (unsigned) per RFC 7230 §4.1;
			// ParseInt accepts a leading '-', so reject it explicitly
			// before it becomes a negative slice bound below. The chunked
			// framing is now corrupt and the stream position indeterminate,
			// so the connection must not be pooled.
			ex.keepAlive = false
			return 0, false, fmt.Errorf("http1: invalid chunk size %q: negative", line)
		}
		if size == 0 {
			// Terminal chunk. Consume optional trailers.
			for {
				tline, terr := ex.c.br.ReadString('\n')
				if terr != nil || strings.TrimRight(tline, "\r\n") == "" {
					break
				}
			}
			ex.chunkFinal = true
			return 0, true, nil
		}
		ex.chunkRemaining = size
	}

	// Read up to min(len(buf), chunkRemaining) bytes from this chunk.
	toRead := ex.chunkRemaining
	if int64(len(buf)) < toRead {
		toRead = int64(len(buf))
	}
	n, err = ex.c.br.Read(buf[:toRead])
	ex.chunkRemaining -= int64(n)
	if err != nil {
		return n, false, err
	}

	// After exhausting a chunk, consume its trailing CRLF.
	if ex.chunkRemaining == 0 {
		if _, lerr := ex.c.br.ReadString('\n'); lerr != nil {
			return n, false, fmt.Errorf("http1: read chunk CRLF: %w", lerr)
		}
	}

	return n, false, nil
}

// KeepAlive reports whether the underlying connection should be returned to
// a pool after this exchange completes. Returns false when the server sent
// "Connection: close" or used HTTP/1.0 without "Connection: keep-alive".
func (ex *Exchange) KeepAlive() bool {
	return ex.keepAlive
}
