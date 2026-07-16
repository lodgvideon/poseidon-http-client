package http1_test

// Conformance tests for the RFC 9112 §7 Transfer-Encoding grammar.
//
//	Transfer-Encoding  = #transfer-coding
//	transfer-coding    = token *( OWS ";" OWS transfer-parameter )
//	transfer-parameter = token BWS "=" BWS ( token / quoted-string )
//
// The framing question is only ever "is chunked the final coding" (§6.3 rules 3
// and 4), but answering it wrongly in either direction is a desync: reading a
// non-chunked value as chunked leaves the server's bytes on the socket for the
// next response to parse as a status line (§11.1 response splitting), and
// reading a chunked value as non-chunked truncates a legitimate body.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// teFraming reports whether the client chunk-framed the body for a given
// Transfer-Encoding value, plus whether it kept the connection poolable.
//
// The fixture body is a well-formed chunked body followed by LEFTOVER. Chunked
// framing therefore yields exactly "HELLO" and strands LEFTOVER on the wire;
// read-until-close yields the raw bytes. The two are unmistakable.
func teFraming(t *testing.T, te string) (chunked, keepAlive bool, body string) {
	t.Helper()
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: "+te+"\r\n\r\n5\r\nHELLO\r\n0\r\n\r\nLEFTOVER")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse for %q: %v", te, err)
	}
	body = readAllTolerant(ex)
	return body == "HELLO", ex.KeepAlive(), body
}

func readAllTolerant(ex *http1.Exchange) string {
	buf := make([]byte, 512)
	var out []byte
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		out = append(out, buf[:n]...)
		if err != nil || done {
			return string(out)
		}
	}
}

// TestConformance_RFC9112_Sec7_QuotedCommaIsNotAListDelimiter pins that a comma
// inside a quoted transfer-parameter is data, not a delimiter.
//
// `gzip;a=", chunked;x=1"` is ONE coding — gzip, carrying parameter a whose
// value is the quoted-string ", chunked;x=1". chunked is NOT the final coding,
// so §6.3 rule 4 hands the body length to connection close.
//
// Splitting the raw value on "," instead manufactured a final "chunked": the
// body was chunk-framed, KeepAlive stayed true, and the server's remaining bytes
// were left on a pooled socket for the next response to read as its status line.
// That is response splitting (§11.1) — the primitive this parse exists to deny.
func TestConformance_RFC9112_Sec7_QuotedCommaIsNotAListDelimiter(t *testing.T) {
	for _, te := range []string{
		`gzip;a=", chunked;x=1"`,
		`gzip;a=", chunked "`,
		`gzip;a="\", chunked;x=1"`, // quoted-pair hides the closing quote
		`gzip;a="x,y", deflate;b=", chunked"`,
	} {
		t.Run(te, func(t *testing.T) {
			chunked, keepAlive, body := teFraming(t, te)
			if chunked {
				t.Errorf("chunk-framed %q, but the quoted comma is parameter data, "+
					"not a list delimiter — chunked is not the final coding, so §6.3 "+
					"rule 4 requires reading until close. Chunk framing leaves the "+
					"rest of the response on a %v-poolable socket: response splitting",
					te, keepAlive)
			}
			if body == "" {
				t.Errorf("no body read for %q", te)
			}
		})
	}
}

// TestConformance_RFC9112_Sec7_ChunkedFinalAfterParameters is the other
// direction, and the control: a genuinely chunked-final value must still be
// chunk-framed however the list is spelled. Without these, a parse that simply
// answered "never chunked" would pass the test above.
//
// Covers OWS around the comma (§7's list ABNF) and ";"-parameters on the chunked
// coding itself — the two normalization steps whose deletion previously survived
// the entire suite of both packages, because every fixture used the bare token.
func TestConformance_RFC9112_Sec7_ChunkedFinalAfterParameters(t *testing.T) {
	for _, te := range []string{
		"chunked",
		"gzip, chunked",     // OWS after the comma
		"gzip,chunked",      // no OWS
		"gzip ,  chunked  ", // OWS both sides
		"chunked;q=1",       // parameter on the final coding
		"gzip, chunked,",    // trailing empty element (RFC 9110 §5.6.1: ignore)
		"gzip, CHUNKED",     // §7: coding names are case-insensitive
	} {
		t.Run(te, func(t *testing.T) {
			chunked, _, body := teFraming(t, te)
			if !chunked {
				t.Errorf("did not chunk-frame %q, but chunked IS the final coding "+
					"(§6.3 rule 4); body=%q — a legitimate chunked response was "+
					"truncated or misread", te, body)
			}
		})
	}
}

// TestConformance_RFC9112_Sec7_NonChunkedFinalReadsUntilClose pins the plain
// non-chunked cases alongside the quoted ones, so the rule-4 path is covered
// independently of the quoted-string machinery.
func TestConformance_RFC9112_Sec7_NonChunkedFinalReadsUntilClose(t *testing.T) {
	for _, te := range []string{
		"chunked, gzip", // chunked present but not final
		"not-chunked",   // a different §5.6.2 token that merely contains "chunked"
		"gzip",
		"chunkedx",
		"xchunked",
	} {
		t.Run(te, func(t *testing.T) {
			if chunked, _, _ := teFraming(t, te); chunked {
				t.Errorf("chunk-framed %q, but chunked is not its final coding; "+
					"§6.3 rule 4 requires reading until the server closes", te)
			}
		})
	}
}
