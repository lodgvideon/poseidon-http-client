package http1_test

// Conformance tests for RFC 9112 §7.1 chunk-size.
//
//	chunk          = chunk-size [ chunk-ext ] CRLF chunk-data CRLF
//	chunk-size     = 1*HEXDIG
//	last-chunk     = 1*("0") [ chunk-ext ] CRLF
//
// A chunk-size this client cannot parse is a chunk boundary it cannot find, so
// it no longer knows where the response ends. Both halves of the remedy matter:
// the error tells the caller, and KeepAlive()=false keeps the socket out of the
// pool. An error alone leaves a mid-stream connection reusable, and the next
// request on it reads whatever the server planted as its status line.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// chunkedResp wraps a raw chunk-size line in an otherwise well-formed chunked
// response, followed by bytes a desynced parser would read as a new response.
func chunkedResp(size string) string {
	return "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
		size + "\r\nhello\r\n0\r\n\r\n" +
		"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\npwn"
}

// TestConformance_RFC9112_Sec7_1_InvalidChunkSizeRejected pins that every
// non-`1*HEXDIG` chunk-size is refused AND condemns the connection.
//
// The negative case already cleared keepAlive; the others returned the error
// with the socket still marked reusable — same corrupt framing, same
// indeterminate stream position, opposite verdict. KeepAlive() is http1's
// documented pooling signal and http1 is a public package, so a consumer
// following the contract pooled a mid-stream socket.
func TestConformance_RFC9112_Sec7_1_InvalidChunkSizeRejected(t *testing.T) {
	for _, tc := range []struct{ name, size string }{
		{"non_hex", "ZZZ"},
		{"empty", ""},
		{"leading_plus", "+5"},  // ParseInt accepts a sign; 1*HEXDIG does not
		{"leading_minus", "-5"}, // the case that already had a guard
		{"space_inside", "5 5"},
		{"hex_prefix", "0x5"},             // "x" is not a HEXDIG
		{"overflow", "FFFFFFFFFFFFFFFFF"}, // past int64, must not wrap
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", chunkedResp(tc.size))
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			buf := make([]byte, 64)
			_, _, err := ex.ReadBodyChunk(buf)
			if err == nil {
				t.Fatalf("chunk-size %q accepted; RFC 9112 §7.1: chunk-size = 1*HEXDIG", tc.size)
			}
			if !errors.Is(err, http1.ErrInvalidChunkSize) {
				t.Errorf("error = %v, want it to wrap ErrInvalidChunkSize", err)
			}
			if ex.KeepAlive() {
				t.Errorf("KeepAlive() = true after an unparseable chunk-size %q — the "+
					"chunk boundary is unknown, so the stream position is "+
					"indeterminate and this socket must not be pooled", tc.size)
			}
		})
	}
}

// TestConformance_RFC9112_Sec7_1_ValidChunkSizeAccepted is the over-rejection
// guard: every legal 1*HEXDIG spelling must still frame a body. Without it, a
// parser that refused everything would pass the test above.
func TestConformance_RFC9112_Sec7_1_ValidChunkSizeAccepted(t *testing.T) {
	for _, tc := range []struct {
		name, size string
		want       string
	}{
		{"single_digit", "5", "hello"},
		{"leading_zeros", "0005", "hello"},
		{"max_hexdig_lower", "5", "hello"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
					tc.size+"\r\nhello\r\n0\r\n\r\n")
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			if body := drainBody(t, ex); body != tc.want {
				t.Errorf("body = %q, want %q for chunk-size %q", body, tc.want, tc.size)
			}
		})
	}
}

// TestConformance_RFC9112_Sec7_1_HexDigitsAcceptedBothCases pins that a-f and
// A-F are both HEXDIG, using a chunk long enough that a case-folding slip shows
// up as a wrong length rather than a parse failure.
func TestConformance_RFC9112_Sec7_1_HexDigitsAcceptedBothCases(t *testing.T) {
	body := make([]byte, 0xab)
	for i := range body {
		body[i] = 'x'
	}
	for _, size := range []string{"ab", "AB", "aB"} {
		t.Run(size, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
					size+"\r\n"+string(body)+"\r\n0\r\n\r\n")
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			if got := drainBody(t, ex); len(got) != 0xab {
				t.Errorf("read %d bytes for chunk-size %q, want %d", len(got), size, 0xab)
			}
		})
	}
}
