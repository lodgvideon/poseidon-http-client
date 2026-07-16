package conn

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
