package http1_test

// Conformance tests for the seam between Transfer-Encoding and Content-Length,
// RFC 9112 §6.3 rules 3, 4 and 5.
//
// These exist because neither half of the fix could see this case alone. The
// Content-Length half gates its verdict on "is a Transfer-Encoding present";
// before the final-coding parse landed, the only available signal was
// respChunked, which a substring match set for any field containing "chunked".
// Once "TE: gzip" started leaving respChunked false, that proxy diverged from
// the question rule 5 actually asks, and a Content-Length could re-frame a body
// rule 4 says to read until close. Each half passed its own suite throughout.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9112_Sec6_3_Rule4_TENotFinalIgnoresValidCL pins that a
// present-but-not-chunked Transfer-Encoding overrides even a syntactically
// valid Content-Length.
//
// RFC 9112 §6.3 rule 3: "If a message is received with both a Transfer-Encoding
// and a Content-Length header field, the Transfer-Encoding overrides the
// Content-Length."
// Rule 4: "If a Transfer-Encoding header field is present in a response and the
// chunked transfer coding is not the final encoding, the message body length is
// determined by reading the connection until it is closed by the server."
//
// So the whole body must be delivered. Framing at the Content-Length instead
// truncates it and leaves the tail on the socket for the next response on a
// pooled connection to parse as its status line.
func TestConformance_RFC9112_Sec6_3_Rule4_TENotFinalIgnoresValidCL(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: 5\r\n\r\nhelloEXTRA")
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainUntilClose(t, ex)

	assert.Equalf(t, "helloEXTRA", body,
		"body = %q, want %q — a Content-Length must not re-frame a body "+
			"whose length rule 4 hands to connection close; %d bytes were left on "+
			"the wire", body, "helloEXTRA", len("helloEXTRA")-len(body))
}

// TestConformance_RFC9112_Sec6_3_Rule5_ScopedToMessagesWithoutTE pins the
// scope of rule 5, which is the whole reason the gate is field presence.
//
// Rule 5 opens: "If a message is received WITHOUT Transfer-Encoding and with an
// invalid Content-Length header field ...". Transfer-Encoding is present here,
// so rule 5's unrecoverable-error path must not fire however malformed the
// Content-Length is — rule 4 already decided the framing without consulting it.
//
// (Rule 3 separately says such a message "ought to be handled as an error"; we
// honour that by refusing to pool the connection, which KeepAlive() reports.)
func TestConformance_RFC9112_Sec6_3_Rule5_ScopedToMessagesWithoutTE(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: not-a-number\r\n\r\nhello")

	_, _, err := ex.ReadResponse(context.Background())

	assert.NoErrorf(t, err, "ReadResponse err = %v, want nil — RFC 9112 §6.3 rule 5 is scoped to "+
		"a message received WITHOUT Transfer-Encoding, and the field is present, "+
		"so its discard path is out of scope; rule 4 frames this body", err)
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true, want false — rule 3 says a message with both "+
			"Transfer-Encoding and Content-Length ought to be handled as an error, "+
			"so the socket must not be reused")
}

// TestConformance_RFC9112_Sec6_3_Rule5_InvalidCLWithoutTEStillRejected is the
// control: strip the Transfer-Encoding and rule 5 applies again, unchanged. It
// guards against "fixing" the scope by disabling rule 5 altogether.
func TestConformance_RFC9112_Sec6_3_Rule5_InvalidCLWithoutTEStillRejected(t *testing.T) {
	ex, err := readResponseErr(t,
		"HTTP/1.1 200 OK\r\nContent-Length: not-a-number\r\n\r\nhello")

	wantRejected(t, ex, err, "invalid Content-Length, no Transfer-Encoding")
}

// drainUntilClose reads a read-until-close body: it ends in a read error at
// EOF, which is the framing working rather than failing.
func drainUntilClose(t *testing.T, ex interface {
	ReadBodyChunk([]byte) (int, bool, error)
}) string {
	t.Helper()
	buf := make([]byte, 256)
	var out []byte
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		out = append(out, buf[:n]...)
		if err != nil || done {
			return string(out)
		}
	}
}
