package http1_test

// Conformance tests for Content-Length framing, keyed to the current specs:
// RFC 9112 (HTTP/1.1 message syntax, obsoletes RFC 7230) and RFC 9110 (HTTP
// semantics). The older TestConformance_RFC2616_* tests in conformance_test.go
// pin chunked-wins-over-Content-Length in both header orders and are not
// repeated here.
//
// The shape under test is the CL.CL desync: a response whose Content-Length is
// invalid or self-contradictory has no trustworthy body boundary, so any value
// this client picks leaves the rest on the socket for the next response on that
// connection to parse as its status line.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// clDesyncBody is the fixture body for the reject cases: a response claiming
// some length, followed by bytes that a wrong or ignored Content-Length would
// leave unread. "HELLO" is 5 octets; "EXTRA" is what smuggles.
const clDesyncBody = "HELLOEXTRA"

// readResponseErr runs one exchange against resp and returns ReadResponse's
// error, plus the exchange for a KeepAlive() check.
func readResponseErr(t *testing.T, resp string) (*http1.Exchange, error) {
	t.Helper()
	ex := wireExchange(t, "GET", resp)
	_, _, err := ex.ReadResponse(context.Background())
	return ex, err
}

// wantRejected asserts the two halves of RFC 9112 §6.3 rule 5's remedy for a
// user agent: "discard the received response" (an error, typed so a caller can
// classify it) and "close the connection to the server" (KeepAlive() false, the
// flag client/h1_pool.go's handleRelease evicts on).
//
// Both halves are asserted because either alone is a silent desync: an error
// without keepAlive=false pools a poisoned socket, and keepAlive=false without
// an error hands the caller a body assembled from a framing the client just
// admitted it could not trust.
func wantRejected(t *testing.T, ex *http1.Exchange, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: ReadResponse returned nil error, want ErrInvalidContentLength", what)
	}
	if !errors.Is(err, http1.ErrInvalidContentLength) {
		t.Errorf("%s: error = %v, want it to wrap ErrInvalidContentLength", what, err)
	}
	if ex.KeepAlive() {
		t.Errorf("%s: KeepAlive() = true, want false — RFC 9112 §6.3 rule 5 requires "+
			"the user agent to close the connection, and a pooled conn would start "+
			"the next response mid-body", what)
	}
}

// TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejected is the
// canonical CL.CL desync. Two Content-Length field lines that disagree are, per
// RFC 9110 §5.3's combining rule, the single value "5, 10" — not 1*DIGIT, so
// RFC 9112 §6.3 rule 5 makes the framing invalid and unrecoverable.
//
// Before the fix the first value silently won: status=200, body="HELLO",
// err=nil, KeepAlive()=true, with "EXTRA" left on the socket.
func TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejected(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5\r\n" +
		"Content-Length: 10\r\n" +
		"\r\n" +
		clDesyncBody
	ex, err := readResponseErr(t, resp)
	wantRejected(t, ex, err, "Content-Length: 5 + Content-Length: 10")
}

// TestConformance_RFC9112_Sec6_3_Rule5_CommaListDifferingValuesRejected pins the
// same message with the duplication already folded onto one line by an upstream
// processor. Rule 5's exception is scoped to a list whose values are "all the
// same", so a differing list stays unrecoverable.
func TestConformance_RFC9112_Sec6_3_Rule5_CommaListDifferingValuesRejected(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5, 10\r\n" +
		"\r\n" +
		clDesyncBody
	ex, err := readResponseErr(t, resp)
	wantRejected(t, ex, err, "Content-Length: 5, 10")
}

// TestConformance_RFC9112_Sec6_3_Rule5_NonNumericContentLengthRejected pins a
// single field line with an invalid value, rule 5's plain case.
//
// Before the fix strconv.ParseInt's error was dropped with no else branch, so
// contentLen stayed -1 and the response silently degraded to rule 8's
// read-until-close — delivering "HELLOEXTRA" as the body of a response that
// declared its length.
func TestConformance_RFC9112_Sec6_3_Rule5_NonNumericContentLengthRejected(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: abc\r\n" +
		"\r\n" +
		clDesyncBody
	ex, err := readResponseErr(t, resp)
	wantRejected(t, ex, err, "Content-Length: abc")
}

// TestConformance_RFC9110_Sec8_6_SignedContentLengthRejected pins the ABNF
// itself: RFC 9110 §8.6 defines Content-Length = 1*DIGIT, which admits no sign.
//
// strconv.ParseInt accepts both signs, which is the whole reason this needs its
// own digits-only parser. Before the fix "+5" was accepted as 5, and "-5" was
// stored as -5 and then failed ReadBodyChunk's contentLen >= 0 test, degrading
// to read-until-close.
func TestConformance_RFC9110_Sec8_6_SignedContentLengthRejected(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"+5", "-5"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			resp := "HTTP/1.1 200 OK\r\n" +
				"Content-Length: " + value + "\r\n" +
				"\r\n" +
				clDesyncBody
			ex, err := readResponseErr(t, resp)
			wantRejected(t, ex, err, "Content-Length: "+value)
		})
	}
}

// TestConformance_RFC9110_Sec8_6_OverflowContentLengthRejected pins RFC 9110
// §8.6: "a recipient MUST anticipate potentially large decimal numerals and
// prevent parsing errors due to integer conversion overflows". A numeral past
// int64 must be refused outright — not wrapped to a negative, not silently
// dropped into read-until-close.
//
// The values bracket the boundary: MaxInt64 itself must parse (it is valid
// 1*DIGIT), MaxInt64+1 and a 40-digit numeral must not.
func TestConformance_RFC9110_Sec8_6_OverflowContentLengthRejected(t *testing.T) {
	t.Parallel()
	overflow := []string{
		"9223372036854775808",                      // MaxInt64 + 1
		"18446744073709551616",                     // MaxUint64 + 1
		"1111111111111111111111111111111111111111", // 40 digits
	}
	for _, value := range overflow {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			resp := "HTTP/1.1 200 OK\r\n" +
				"Content-Length: " + value + "\r\n" +
				"\r\n" +
				clDesyncBody
			ex, err := readResponseErr(t, resp)
			wantRejected(t, ex, err, "Content-Length: "+value)
		})
	}
}

// TestConformance_RFC9110_Sec8_6_MaxInt64ContentLengthAccepted is the other side
// of the overflow boundary: 9223372036854775807 is valid 1*DIGIT and must be
// accepted, so the overflow guard cannot be an off-by-one that rejects the
// largest legal value. The body is not read — declaring a length is not sending
// it — only the header verdict is under test.
func TestConformance_RFC9110_Sec8_6_MaxInt64ContentLengthAccepted(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 9223372036854775807\r\n" +
		"\r\n"
	ex, err := readResponseErr(t, resp)
	if err != nil {
		t.Fatalf("Content-Length: MaxInt64 is valid 1*DIGIT, got error: %v", err)
	}
	if !ex.KeepAlive() {
		t.Error("KeepAlive() = false, want true — a valid Content-Length is not a framing error")
	}
}

// TestConformance_RFC9112_Sec6_3_Rule5_IdenticalDuplicatesAccepted pins the
// half of rule 5 that is an obligation NOT to over-reject. Duplicate field lines
// carrying the same value combine (RFC 9110 §5.3) to "5, 5", which rule 5
// exempts: "unless the field value can be successfully parsed as a
// comma-separated list, all values in the list are valid, and all values in the
// list are the same (in which case, the message is processed with that single
// value used as the Content-Length field value)". RFC 9110 §8.6 says the same,
// noting it "likely indicates that a duplicate was generated or combined by an
// upstream message processor" — a relayed response, not an attack.
//
// The assertion is that 5 is actually used as the length: body is "HELLO", not
// "HELLOEXTRA", and the connection stays poolable because the body ended at a
// known boundary rather than at EOF.
func TestConformance_RFC9112_Sec6_3_Rule5_IdenticalDuplicatesAccepted(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		// Exactly the declared 5 octets. Trailing bytes would be unsolicited data
		// that costs the connection on their own (see
		// TestConformance_RFC9112_Sec6_3_ExtraOctetsAfterResponse), masking what
		// this test is about: that identical values do not condemn it.
		"HELLO"
	ex := wireExchange(t, "GET", resp)
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("identical duplicate Content-Length must be accepted, got: %v", err)
	}
	if body := drainBody(t, ex); body != "HELLO" {
		t.Errorf("body = %q, want %q — the single value 5 must delimit the body", body, "HELLO")
	}
	if !ex.KeepAlive() {
		t.Error("KeepAlive() = false, want true — the body ended at a known boundary")
	}
}

// TestConformance_RFC9112_Sec6_3_Rule5_CommaListIdenticalValuesAccepted is the
// same exception with the list already on one line, which is the literal shape
// rule 5 and RFC 9110 §8.6's "Content-Length: 42, 42" example describe.
func TestConformance_RFC9112_Sec6_3_Rule5_CommaListIdenticalValuesAccepted(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 5, 5\r\n" +
		"\r\n" +
		"HELLO"
	ex := wireExchange(t, "GET", resp)
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("identical comma-list Content-Length must be accepted, got: %v", err)
	}
	if body := drainBody(t, ex); body != "HELLO" {
		t.Errorf("body = %q, want %q — the single value 5 must delimit the body", body, "HELLO")
	}
	if !ex.KeepAlive() {
		t.Error("KeepAlive() = false, want true — the body ended at a known boundary")
	}
}

// TestConformance_RFC9112_Sec6_3_Rule3_InvalidContentLengthIgnoredWhenChunked
// pins the scope of rule 5 against rule 3. Rule 5's fatal case is a message
// "received without Transfer-Encoding"; rule 3 says that when both are present
// "the Transfer-Encoding overrides the Content-Length". So a chunk-framed
// response must not be failed over a Content-Length that decides nothing —
// rejecting it would break responses that are merely relayed by a processor that
// mangled a header the framing does not consult.
//
// Both header orders are run because the client must reach the same verdict
// whichever arrives first: the value is only invalid in the absence of a
// Transfer-Encoding, and that absence is not known until the blank line.
func TestConformance_RFC9112_Sec6_3_Rule3_InvalidContentLengthIgnoredWhenChunked(t *testing.T) {
	t.Parallel()
	const chunked = "5\r\nhello\r\n0\r\n\r\n"
	cases := map[string]string{
		"CLFirst": "HTTP/1.1 200 OK\r\n" +
			"Content-Length: abc\r\n" +
			"Transfer-Encoding: chunked\r\n" +
			"\r\n" + chunked,
		"TEFirst": "HTTP/1.1 200 OK\r\n" +
			"Transfer-Encoding: chunked\r\n" +
			"Content-Length: abc\r\n" +
			"\r\n" + chunked,
	}
	for name, resp := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ex := wireExchange(t, "GET", resp)
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("chunked framing must ignore Content-Length, got: %v", err)
			}
			if body := drainBody(t, ex); body != "hello" {
				t.Errorf("body = %q, want %q", body, "hello")
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejectedOrderIndependent
// pins that the reject verdict itself does not depend on header order either:
// the conflicting pair is fatal whether or not it straddles other fields.
func TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejectedOrderIndependent(t *testing.T) {
	t.Parallel()
	resp := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 10\r\n" +
		"Server: test\r\n" +
		"Content-Length: 5\r\n" +
		"\r\n" +
		clDesyncBody
	ex, err := readResponseErr(t, resp)
	wantRejected(t, ex, err, "Content-Length: 10 then 5, separated")
}
