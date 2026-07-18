package http3

import "errors"

// ErrH3Message is returned when a message violates the HTTP/3 message rules — a
// missing or malformed pseudo-header, a pseudo-header after a regular one, an
// invalid field name or value, or a connection-specific field. It maps to
// H3_MESSAGE_ERROR (RFC 9114 §4.1.2), a stream error.
var ErrH3Message = errors.New("http3: malformed message")

// validFieldName reports whether name is a valid lowercase HTTP field name: a
// non-empty run of tchar (RFC 9110 §5.1) containing no uppercase letter
// (RFC 9114 §4.2). Pseudo-header names (leading ':') are validated separately.
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

// validFieldValue reports whether value contains no octet forbidden in a field
// value (RFC 9110 §5.5): NUL, CR, or LF — the header-injection vectors.
func validFieldValue(value []byte) bool {
	for _, c := range value {
		if c == 0x00 || c == 0x0a || c == 0x0d {
			return false
		}
	}
	return true
}

// connectionSpecificField reports whether name is one of the connection-specific
// header fields RFC 9114 §4.2 forbids in HTTP/3 in either direction: "any message
// containing connection-specific fields MUST be treated as malformed." "te" is
// excluded here because it is the one field whose treatment differs by direction
// (see forbiddenRequestField / forbiddenResponseField).
func connectionSpecificField(name []byte) bool {
	switch string(name) {
	case "connection", "transfer-encoding", "keep-alive", "upgrade", "proxy-connection":
		return true
	}
	return false
}

// forbiddenRequestField reports whether a field is prohibited in an HTTP/3
// REQUEST (RFC 9114 §4.2). "te" is allowed here iff its value is exactly
// "trailers": "The only exception to this is the TE header field, which MAY be
// present in an HTTP/3 request header; when it is, it MUST NOT contain any value
// other than "trailers"."
func forbiddenRequestField(name, value []byte) bool {
	if string(name) == "te" {
		return string(value) != "trailers"
	}
	return connectionSpecificField(name)
}

// forbiddenResponseField reports whether a field is prohibited in an HTTP/3
// RESPONSE or trailer section (RFC 9114 §4.2). The §4.2 te exception is scoped to
// a request ("an HTTP/3 request header"), so a response carrying "te" at ANY
// value is a connection-specific field and therefore malformed. Splitting the two
// directions is exactly what conn/validate.go did for HTTP/2; sharing one helper
// let the request-only te:trailers exemption leak onto the response and trailer
// receive paths, silently accepting a malformed response and forwarding a
// hop-by-hop te header downstream.
func forbiddenResponseField(name []byte) bool {
	return string(name) == "te" || connectionSpecificField(name)
}
