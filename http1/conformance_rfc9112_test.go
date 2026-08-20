package http1_test

// RFC 9112 conformance tests — message framing when Transfer-Encoding and
// Content-Length disagree. RFC 9112 obsoletes RFC 7230; the older
// TestConformance_RFC2616_* tests in conformance_test.go pin chunked-wins in
// both header orders and are not repeated here. These cover the shapes they do
// not: chunked that is not the final coding, an unknown coding, a coding whose
// name merely contains "chunked", and the connection-reuse consequence of
// rule 3.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9112_Sec6_3_Rule4_ChunkedNotFinalReadsUntilClose pins
// RFC 9112 §6.3 rule 4: "If a Transfer-Encoding header field is present in a
// response and the chunked transfer coding is not the final encoding, the
// message body length is determined by reading the connection until it is closed
// by the server."
//
// "chunked, gzip" names chunked first and gzip last, so chunked is NOT final
// and the body is raw bytes up to EOF — not chunk framing.
func TestConformance_RFC9112_Sec6_3_Rule4_ChunkedNotFinalReadsUntilClose(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked, gzip\r\n" +
		"\r\n" +
		"hello" // raw body, terminated by the server closing the connection
	ex := wireExchange(t, "GET", resp)
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainBody(t, ex)

	assert.Equalf(t, "hello", body, "body = %q, want %q (chunked is not the final coding)", body, "hello")
	assert.False(t, ex.KeepAlive(), "KeepAlive() = true after a read-until-close body, want false")
}

// TestConformance_RFC9112_Sec6_3_Rule4_UnknownCodingReadsUntilClose pins the
// same rule 4 for a coding whose name merely *contains* "chunked". The field is
// an ordered list of transfer-coding tokens (RFC 9112 §6.1, element grammar in
// RFC 9110 §10.1.4), and "not-chunked" is a different token from "chunked" — a
// substring test reads it as chunked framing and desyncs on the first body byte.
func TestConformance_RFC9112_Sec6_3_Rule4_UnknownCodingReadsUntilClose(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: not-chunked\r\n" +
		"\r\n" +
		"hello"
	ex := wireExchange(t, "GET", resp)
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainBody(t, ex)

	assert.Equalf(t, "hello", body, "body = %q, want %q (\"not-chunked\" is not the chunked coding)", body, "hello")
}

// TestConformance_RFC9112_Sec6_3_Rule3_ContentLengthFirst_TEOverrides pins
// RFC 9112 §6.3 rule 3: "If a message is received with both a Transfer-Encoding
// and a Content-Length header field, the Transfer-Encoding overrides the
// Content-Length."
//
// Content-Length arrives FIRST here, so it is already parsed by the time
// Transfer-Encoding lands: this is the order in which the override has to
// undo a decision, not merely decline to make one. Content-Length says 5;
// rule 3 makes it inoperative and rule 4 (gzip is the final coding, not
// chunked) makes the body run to EOF, so all 10 bytes belong to this response.
// Framing at 5 would leave "EXTRA" on the wire to be read as the head of the
// next response — the response-splitting half of §11.
func TestConformance_RFC9112_Sec6_3_Rule3_ContentLengthFirst_TEOverrides(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5\r\n" +
		"Transfer-Encoding: gzip\r\n" +
		"\r\n" +
		"helloEXTRA"
	ex := wireExchange(t, "GET", resp)

	_, _, err := ex.ReadResponse(context.Background())

	require.NoError(t, err, "ReadResponse")
	// Asserted before the body is drained on purpose: after the drain, the
	// read-until-close EOF marks the conn dead anyway, so a post-drain check
	// would pass with rule 4 deleted. Rule 4 must condemn the connection at
	// header-parse time, on the two headers alone.
	assert.False(t, ex.KeepAlive(), "KeepAlive() = true for a TE+CL response head, want false (rule 4 MUST close)")
	body := drainBody(t, ex)
	assert.Equalf(t, "helloEXTRA", body,
		"body = %q, want %q (Transfer-Encoding overrides Content-Length)", body, "helloEXTRA")
}

// TestConformance_RFC9112_Sec6_3_Rule3_TransferEncodingFirst_CLIgnored is the
// opposite header order of the test above: Transfer-Encoding is parsed before
// Content-Length arrives. Rule 4 is order-independent, so the late
// Content-Length must not reinstate length framing.
func TestConformance_RFC9112_Sec6_3_Rule3_TransferEncodingFirst_CLIgnored(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: gzip\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		"helloEXTRA"
	ex := wireExchange(t, "GET", resp)

	_, _, err := ex.ReadResponse(context.Background())

	require.NoError(t, err, "ReadResponse")
	// Asserted before the body is drained on purpose: after the drain, the
	// read-until-close EOF marks the conn dead anyway, so a post-drain check
	// would pass with rule 4 deleted. Rule 4 must condemn the connection at
	// header-parse time, on the two headers alone.
	assert.False(t, ex.KeepAlive(), "KeepAlive() = true for a TE+CL response head, want false (rule 4 MUST close)")
	body := drainBody(t, ex)
	assert.Equalf(t, "helloEXTRA", body,
		"body = %q, want %q (a late Content-Length must not re-frame the body)", body, "helloEXTRA")
}

// TestConformance_RFC9112_Sec6_3_Rule3_ChunkedPlusCLNotReusable pins the
// MUST-close half of rule 4 on the classic smuggling shape: "Transfer-Encoding:
// chunked" plus "Content-Length: 5". The framing question is settled (chunked
// wins — that is the existing RFC2616 §4.4 rule 3 pair), but the message "might
// indicate an attempt to perform request smuggling (§11.2)" and the connection
// MUST NOT survive it.
//
// The framing assertion is included so the test still fails if a fix breaks
// chunked-wins while getting keepAlive right.
func TestConformance_RFC9112_Sec6_3_Rule3_ChunkedPlusCLNotReusable(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		"5\r\nhello\r\n" +
		"0\r\n\r\n"
	ex := wireExchange(t, "GET", resp)
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainBody(t, ex)

	assert.Equalf(t, "hello", body, "body = %q, want %q", body, "hello")
	assert.False(t, ex.KeepAlive(), "KeepAlive() = true for TE:chunked + CL, want false (rule 4 MUST close)")
}
