package http1_test

// An unterminated quoted-string in a Transfer-Encoding transfer-parameter.
//
// RFC 9110 §10.1.4: transfer-parameter = token BWS "=" BWS ( token /
// quoted-string ). A quoted-string that never closes is not a valid value, so
// the field is malformed. The scanner tracks quote state to keep a quoted comma
// from splitting the list (that is what closed the #254 vector); the hazard here
// is the opposite — an unclosed quote swallows the rest of the list into one
// runaway element, so `chunked;x=", gzip` (last real coding gzip) resolves to
// final coding "chunked", chunk-frames the body, and pools the socket.
//
// The safe verdict for a malformed framing declaration: read the body until the
// server closes (§6.3 rule 4), and do NOT pool — the boundary is indeterminate.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"
)

// TestConformance_RFC9110_Sec10_1_4_UnterminatedQuotedTENotChunked pins that a
// runaway quote does not resolve to chunked on a poolable socket.
func TestConformance_RFC9110_Sec10_1_4_UnterminatedQuotedTENotChunked(t *testing.T) {
	for _, te := range []string{
		`chunked;x=", gzip`,     // quote before the comma swallows the whole list
		`gzip;a="oops, chunked`, // runaway quote ends the value mid-parameter
		`chunked;x="`,           // a lone open quote after the coding
		`gzip, chunked;x="\`,    // quoted-pair at end leaves the quote open
	} {
		t.Run(te, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: "+te+"\r\n\r\n5\r\nHELLO\r\n0\r\n\r\nLEFTOVER")
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse: %v", err)
			}
			buf := make([]byte, 512)
			var body []byte
			for {
				n, done, e := ex.ReadBodyChunk(buf)
				body = append(body, buf[:n]...)
				if e != nil || done {
					break
				}
			}
			if string(body) == "HELLO" {
				t.Errorf("chunk-framed a malformed Transfer-Encoding %q — an unterminated "+
					"quoted-string is not a valid value, so the final coding cannot be "+
					"trusted; the leftover bytes were left on the wire", te)
			}
			if ex.KeepAlive() {
				t.Errorf("KeepAlive() = true for a malformed Transfer-Encoding %q — the body "+
					"boundary is indeterminate, so the socket must not be pooled", te)
			}
		})
	}
}

// TestConformance_RFC9110_Sec10_1_4_TerminatedQuotedTEStillFrames is the
// over-rejection guard: a correctly TERMINATED quoted-string parameter is legal,
// so the coding after it still decides the framing normally.
func TestConformance_RFC9110_Sec10_1_4_TerminatedQuotedTEStillFrames(t *testing.T) {
	// gzip carrying a quoted parameter, then chunked as the final coding: a legal
	// list whose final coding is chunked.
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip;a=\"x, y\", chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if body := drainBody(t, ex); body != "HELLO" {
		t.Errorf("body = %q, want %q — a terminated quoted parameter is legal and chunked "+
			"is still the final coding", body, "HELLO")
	}
}
