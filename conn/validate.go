package conn

import "github.com/lodgvideon/poseidon-http-client/hpack"

// Validation of decoded response fields.
//
// This mirrors http3/validate.go deliberately, rule for rule. HTTP/2 and HTTP/3
// inherit the same requirements from the same place, and the receive path here
// had none of them: emitHeaderBlock decoded an HPACK block straight into the
// event handed to the caller, so a server could put anything an HPACK literal can
// encode into a response header and this client passed it on.
//
// RFC 7540 §10.3, verbatim:
//
//	"Requests or responses containing invalid header field names MUST be treated
//	 as malformed (Section 8.1.2.6). ... Any request or response that contains a
//	 character not permitted in a header field value MUST be treated as malformed
//	 (Section 8.1.2.6)."
//
// That MUST binds any receiver, not only an intermediary — the intermediary is
// the section's stated *motivation* (an H2->H1 translator turning a CR/LF value
// into a response split), not the addressee. §8.1.2.6 leaves no room either:
//
//	"Clients MUST NOT accept a malformed response."
//	"Malformed requests or responses that are detected MUST be treated as a
//	 stream error (Section 5.4.2) of type PROTOCOL_ERROR."
//
// and closes with why it is worth the strictness:
//
//	"Note that these requirements are intended to protect against several types
//	 of common attacks against HTTP; they are deliberately strict because being
//	 permissive can expose implementations to these vulnerabilities."

// validFieldName reports whether name is a valid HTTP/2 field name.
//
// RFC 7540 §8.1.2 requires field names to be lowercase, and the set is RFC 7230
// §3.2.6's `token` minus the upper-case letters: a name carrying anything else —
// an upper-case letter, a space, a colon, a control byte — is malformed
// (§8.1.2.6 lists "the inclusion of uppercase header field names" explicitly).
//
// Empty is invalid: a zero-length name is not a token, and HPACK can encode one.
func validFieldName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' ||
			c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_' ||
			c == '`' || c == '|' || c == '~':
		default:
			return false
		}
	}
	return true
}

// validFieldValue reports whether value carries no CR, LF or NUL.
//
// RFC 7540 §10.3 names exactly these three and says why: "While most of the
// values that can be encoded will not alter header field parsing, carriage return
// (CR, ASCII 0xd), line feed (LF, ASCII 0xa), and the zero character (NUL, ASCII
// 0x0) might be exploited by an attacker if they are translated verbatim."
//
// HPACK is length-prefixed, so these bytes cannot split anything on the HTTP/2
// wire itself — which is exactly why this is easy to skip and worth not skipping.
// The damage is downstream of us: a caller that copies the value into an HTTP/1.1
// message, a log line, or a header of its own gets the split we refused to make.
func validFieldValue(value []byte) bool {
	for _, c := range value {
		if c == 0x00 || c == 0x0a || c == 0x0d {
			return false
		}
	}
	return true
}

// forbiddenResponseField reports whether a field is prohibited in an HTTP/2
// RESPONSE (RFC 7540 §8.1.2.2).
//
// The client already refuses to SEND these
// (client/conformance_forbidden_headers_test.go pins that). §8.1.2.2 is not
// one-directional: "An endpoint MUST NOT generate an HTTP/2 message containing
// connection-specific header fields; any message containing connection-specific
// header fields MUST be treated as malformed (Section 8.1.2.6)." So a response
// carrying one is malformed on arrival too. Transfer-Encoding is the interesting
// one — it means nothing to HTTP/2 framing, so accepting it can only mislead
// whatever reads the field set next.
//
// "te" is forbidden here at ANY value, which is where this deliberately diverges
// from http3/validate.go's shared helper. §8.1.2.2's exception is scoped to one
// direction: "The only exception to this is the TE header field, which MAY be
// present in an HTTP/2 request; when it is, it MUST NOT contain any value other
// than "trailers"." A request may carry te: trailers; a response may not
// carry te at all. Mirroring the sibling verbatim would have quietly imported a
// request-side exception into the response path — the sibling is the right shape
// to copy, not the right text.
func forbiddenResponseField(name []byte) bool {
	switch string(name) {
	case "connection", "transfer-encoding", "keep-alive", "upgrade", "proxy-connection", "te":
		return true
	}
	return false
}

// responseContentLength extracts the response's Content-Length from decoded
// fields: its value, whether it was present, and whether it is valid. A value
// that is not a non-negative run of ASCII digits, or repeated values that
// disagree, make it present-but-invalid — RFC 9110 §8.6 defines it as 1*DIGIT,
// and a self-contradictory declaration cannot be believed. Mirrors
// http3/response.go's function of the same shape.
func responseContentLength(fields []hpack.HeaderField) (n int64, present, valid bool) {
	for i := range fields {
		if string(fields[i].Name) != "content-length" {
			continue
		}
		v, ok := parseDecimalDigits(fields[i].Value)
		if !ok || (present && v != n) {
			return 0, true, false
		}
		n, present = v, true
	}
	return n, present, present
}

// parseDecimalDigits parses a non-empty run of ASCII digits into a non-negative
// int64, rejecting a leading sign, empty input, and any overflow of int64. The
// accumulator is checked before each multiply so a long numeral is refused
// rather than wrapped (RFC 9110 §8.6: "a recipient MUST anticipate potentially
// large decimal numerals and prevent parsing errors due to integer conversion
// overflows").
func parseDecimalDigits(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if n > (maxInt64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// maxInt64 is the int64 ceiling, spelled out to keep parseDecimalDigits free of
// a math import.
const maxInt64 = int64(^uint64(0) >> 1)

// statusCanHaveContent reports whether a response with this status and request
// method is defined to carry content. RFC 7540 §8.1.2.6 exempts exactly these
// from the Content-Length-equals-DATA check: "A response that is defined to have
// no payload ... can have a non-zero content-length header field, even though no
// content is included in DATA frames." A HEAD response never has content
// (RFC 9110 §9.3.2); 204 and 304 never do.
func statusCanHaveContent(status int, reqIsHead bool) bool {
	if reqIsHead {
		return false
	}
	return status != 204 && status != 304
}
