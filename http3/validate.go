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

// forbiddenField reports whether a field is prohibited in HTTP/3 (RFC 9114
// §4.2): the connection-specific header fields, and "te" with any value other
// than "trailers".
func forbiddenField(name, value []byte) bool {
	switch string(name) {
	case "connection", "transfer-encoding", "keep-alive", "upgrade", "proxy-connection":
		return true
	case "te":
		return string(value) != "trailers"
	}
	return false
}
