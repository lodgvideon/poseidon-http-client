package http1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// scriptConn is a net.Conn that replays a fixed script of server bytes and then
// reports EOF. Unlike hostileConn it is finite, which is what a fuzz iteration
// needs: every target must terminate on its own so the fuzzer can move on.
type scriptConn struct {
	script []byte
}

func (s *scriptConn) Read(p []byte) (int, error) {
	if len(s.script) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.script)
	s.script = s.script[n:]
	return n, nil
}

func (s *scriptConn) Write(p []byte) (int, error)      { return len(p), nil }
func (s *scriptConn) Close() error                     { return nil }
func (s *scriptConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (s *scriptConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (s *scriptConn) SetDeadline(time.Time) error      { return nil }
func (s *scriptConn) SetReadDeadline(time.Time) error  { return nil }
func (s *scriptConn) SetWriteDeadline(time.Time) error { return nil }

// scriptExchange drives a request onto a scripted server response.
func scriptExchange(t *testing.T, method string, script []byte) *http1.Exchange {
	t.Helper()
	c := http1.NewConn(&scriptConn{script: script})
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return ex
}

// FuzzReadResponse drives arbitrary server bytes through the response status
// line and header parser, including the 1xx drain loop. Every byte here is
// peer-controlled, so the parser must never panic and must never retain more
// than the peer actually sent — the failure that motivated the caps, where a
// line without a delimiter grew a fragment list without limit.
//
// The retention invariants are expressed against len(data) rather than against
// the caps themselves: a fuzz-sized input never approaches 8 MiB, so asserting
// "under the cap" would be vacuously true and catch nothing. "The client never
// retains more than the server sent" is the same property, checkable at any
// input size, and it is what a mutation of the cap has to violate.
func FuzzReadResponse(f *testing.F) {
	f.Add([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	f.Add([]byte("HTTP/1.1 204 No Content\r\n\r\n"))
	f.Add([]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"))
	// The 1xx drain loop, then a final response.
	f.Add([]byte("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	// A 1xx flood with no final response: the interim cap is the only exit.
	f.Add([]byte(strings.Repeat("HTTP/1.1 100 Continue\r\n\r\n", 200)))
	// Header lines that are individually legal but endless: the block cap.
	f.Add([]byte("HTTP/1.1 200 OK\r\n" + strings.Repeat("a: b\r\n", 4096)))
	// A line that never terminates.
	f.Add([]byte("HTTP/1.1 200 " + strings.Repeat("A", 40000)))
	// Malformed shapes.
	f.Add([]byte("HTTP/1.1\r\n\r\n"))                   // too few status-line parts
	f.Add([]byte("HTTP/1.1 abc OK\r\n\r\n"))            // non-numeric status
	f.Add([]byte("GARBAGE\r\n\r\n"))                    // not HTTP/1
	f.Add([]byte("HTTP/1.1 200 OK\r\nnocolon\r\n\r\n")) // header line with no colon
	f.Add([]byte(""))
	f.Add([]byte("\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ex := scriptExchange(t, "GET", data)

		status, headers, err := ex.ReadResponse(context.Background())
		if err != nil {
			// No half-built response may escape beside an error.
			if headers != nil {
				t.Fatalf("err=%v returned with %d headers; want nil headers on the error path",
					err, len(headers))
			}
			if status != 0 {
				t.Fatalf("err=%v returned with status=%d; want 0 on the error path", err, status)
			}
			// The buffering caps are the one failure class a caller can classify,
			// and they must always leave the connection un-poolable.
			if errors.Is(err, http1.ErrResponseTooLarge) && ex.KeepAlive() {
				t.Fatalf("ErrResponseTooLarge left the connection poolable: %v", err)
			}
			return
		}

		if status < 200 {
			t.Fatalf("ReadResponse returned interim status %d; the 1xx loop must not exit early", status)
		}
		// Retention: a successful parse never holds more field bytes than the
		// server sent. The synthetic :status field is the only text the client
		// invents, hence the small allowance.
		const synthetic = len(":status") + 8
		var retained int
		for _, h := range headers {
			retained += len(h.Name) + len(h.Value)
		}
		if retained > len(data)+synthetic {
			t.Fatalf("retained %d B of header fields from %d B of input: the parser amplifies",
				retained, len(data))
		}
		// Field count: the shortest line that yields a field is ":\r\n", so the
		// count cannot outrun the input either.
		if len(headers) > len(data)+1 {
			t.Fatalf("%d header fields from %d B of input", len(headers), len(data))
		}
	})
}

// FuzzReadChunkedBody drives arbitrary bytes through the chunked body reader
// (RFC 7230 §4.1) behind a fixed, valid chunked response head, so every
// iteration reaches readChunkedChunk: the chunk-size line, the extension
// stripping, the size parse, the terminal chunk and its trailer section.
//
// A negative chunk size once panicked here on a negative slice bound, so the
// no-panic property is load-bearing and not merely hygienic.
func FuzzReadChunkedBody(f *testing.F) {
	f.Add([]byte("0\r\n\r\n"))                                // terminal chunk, no trailers
	f.Add([]byte("4\r\nbody\r\n0\r\n\r\n"))                   // one chunk then terminal
	f.Add([]byte("4;ext=1\r\nbody\r\n0\r\n\r\n"))             // chunk extension
	f.Add([]byte("0\r\nX-T: v\r\n\r\n"))                      // terminal chunk with a trailer
	f.Add([]byte("-1\r\n"))                                   // negative size (the old panic)
	f.Add([]byte("ffffffffffffffffff\r\n"))                   // size overflows int64
	f.Add([]byte("7fffffffffffffff\r\nx"))                    // size = MaxInt64
	f.Add([]byte("zz\r\n"))                                   // not hex
	f.Add([]byte(strings.Repeat("A", 40000)))                 // size line that never ends
	f.Add([]byte("0\r\n" + strings.Repeat("a: b\r\n", 4096))) // endless trailers
	f.Add([]byte("4\r\nbo"))                                  // truncated mid-chunk
	f.Add([]byte(""))

	const head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"

	f.Fuzz(func(t *testing.T, body []byte) {
		ex := scriptExchange(t, "GET", append([]byte(head), body...))

		if _, _, err := ex.ReadResponse(context.Background()); err != nil {
			t.Fatalf("fixed valid head failed to parse: %v", err)
		}

		buf := make([]byte, 512)
		var total int
		// Every iteration either consumes input or ends the body, so the loop
		// terminates; the bound is a guard on that reasoning, not a substitute.
		for i := 0; i < 1<<16; i++ {
			n, done, err := ex.ReadBodyChunk(buf)
			if n < 0 || n > len(buf) {
				t.Fatalf("ReadBodyChunk returned n=%d, out of range for a %d B buffer", n, len(buf))
			}
			total += n
			if errors.Is(err, http1.ErrResponseTooLarge) && ex.KeepAlive() {
				t.Fatalf("ErrResponseTooLarge left the connection poolable: %v", err)
			}
			if err != nil || done {
				break
			}
		}
		// The body handed to the caller can never exceed what the server sent.
		if total > len(body) {
			t.Fatalf("delivered %d B of body from %d B of input: the reader amplifies", total, len(body))
		}
	})
}

// FuzzReadResponse_ValidRoundTrip builds a well-formed response out of the
// fuzzer's bytes and asserts it parses back byte-for-byte. The other targets
// prove hostile input is refused; this one proves the caps did not make the
// parser refuse input it must accept — an over-strict bound is as much a bug as
// a missing one, and is exactly what a load generator would hit against a real
// server.
func FuzzReadResponse_ValidRoundTrip(f *testing.F) {
	f.Add(200, "content-type", "text/plain")
	f.Add(404, "x-request-id", "abc123")
	f.Add(500, "server", "nginx/1.25.3")
	// A header line near, but under, the per-line cap: the case a real server
	// with a large cookie or JWT would produce.
	f.Add(200, "set-cookie", strings.Repeat("j", 8000))
	f.Add(204, "a", "")

	f.Fuzz(func(t *testing.T, status int, name, value string) {
		// Constrain to the domain the function is defined on: a legal status
		// code and a header that cannot smuggle framing or leading/trailing
		// whitespace (which the parser trims by design).
		if status < 200 || status > 599 {
			t.Skip()
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if name == "" || strings.ContainsAny(name, ":\r\n \t") {
			t.Skip()
		}
		if strings.ContainsAny(value, "\r\n") {
			t.Skip()
		}
		// Stay inside the documented per-line bound; over-long lines are the
		// other targets' business.
		if len(name)+len(value)+2 > 16*1024-2 {
			t.Skip()
		}
		// Hop-by-hop and framing headers change the parse; they are not what
		// this round-trip is about.
		switch name {
		case "content-length", "transfer-encoding", "connection":
			t.Skip()
		}

		script := fmt.Sprintf("HTTP/1.1 %d Reason\r\n%s: %s\r\nContent-Length: 0\r\n\r\n",
			status, name, value)
		ex := scriptExchange(t, "GET", []byte(script))

		gotStatus, headers, err := ex.ReadResponse(context.Background())
		if err != nil {
			t.Fatalf("a valid response was rejected: %v\nscript=%q", err, script)
		}
		if gotStatus != status {
			t.Fatalf("status = %d, want %d", gotStatus, status)
		}
		// headers[0] is the synthetic :status; the fuzzed field must survive
		// verbatim.
		var found bool
		for _, h := range headers {
			if string(h.Name) != name {
				continue
			}
			found = true
			if string(h.Value) != value {
				t.Fatalf("value round-trip: got %q, want %q", h.Value, value)
			}
		}
		if !found {
			t.Fatalf("header %q did not survive the parse (got %d fields)", name, len(headers))
		}
	})
}
