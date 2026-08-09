package grpc

import (
	"errors"
	"fmt"
)

// ErrInvalidMetadata is reported when a metadata field would not be a legal
// HTTP/2 field on the wire.
var ErrInvalidMetadata = errors.New("grpc: invalid metadata field")

// tokenChar marks the bytes RFC 9110 §5.6.2 admits in a field name, minus the
// uppercase letters: RFC 9113 §8.2.1 requires HTTP/2 field names to be
// lowercase, and gRPC metadata keys are case-insensitive, so an uppercase name
// is always a caller mistake rather than an intent we should honour.
var tokenChar = func() [256]bool {
	var t [256]bool
	for c := 'a'; c <= 'z'; c++ {
		t[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		t[c] = true
	}
	for _, c := range []byte("!#$%&'*+-.^_`|~") {
		t[c] = true
	}
	return t
}()

// validMetadataName rejects a field name that is not a lowercase token. A name
// carrying CR, LF, NUL, a space or a colon is a request-splitting vector: HPACK
// length-prefixes the field so it cannot split the HTTP/2 wire itself, but an
// HTTP/2-to-HTTP/1.1 downgrading intermediary re-serialises it into a format
// where those bytes ARE the delimiters. This mirrors `isTokenName` in
// client/request.go, rule for rule.
func validMetadataName(name []byte) error {
	if len(name) == 0 {
		return fmt.Errorf("%w: empty name", ErrInvalidMetadata)
	}
	for _, c := range name {
		if !tokenChar[c] {
			return fmt.Errorf("%w: name %q is not a lowercase token (RFC 9110 §5.6.2, "+
				"RFC 9113 §8.2.1); a name carrying CR, LF, NUL, a space, a colon or an "+
				"uppercase letter is a request-splitting vector", ErrInvalidMetadata, name)
		}
	}
	return nil
}

// validMetadataValue rejects a field value carrying CR, LF or NUL, or one that
// begins or ends with SP/HTAB. RFC 7540 §10.3 names the first three as the
// bytes that "might be exploited by an attacker if they are translated
// verbatim"; RFC 9113 §8.2.1 makes the leading/trailing whitespace malformed.
// This mirrors `hasFieldInjectionByte` + `edgeWhitespace` in client/request.go.
func validMetadataValue(name, value []byte) error {
	for _, c := range value {
		if c == '\r' || c == '\n' || c == 0 {
			return fmt.Errorf("%w: value of %q carries CR, LF or NUL (request-splitting "+
				"vector, RFC 7540 §10.3)", ErrInvalidMetadata, name)
		}
	}
	if len(value) > 0 {
		ws := func(b byte) bool { return b == 0x20 || b == 0x09 }
		if ws(value[0]) || ws(value[len(value)-1]) {
			return fmt.Errorf("%w: value of %q starts or ends with SP/HTAB (RFC 9113 §8.2.1)",
				ErrInvalidMetadata, name)
		}
	}
	return nil
}

// defaultSensitiveField reports whether a field name carries credentials by
// convention and so must never enter the HPACK dynamic table — a compression
// context shared by every request on the connection, which makes an indexed
// credential both long-lived and a compression-oracle target (RFC 9113 §7.1.3,
// RFC 7541 §7.1). The caller can mark any other field with
// hpack.HeaderField.Sensitive; this is the floor, not the ceiling, because a
// caller who never sets the flag would otherwise get exactly the default-path
// exposure the rule exists to prevent. Mirrors http3/request.go rule for rule —
// per-RPC credentials are the normal reason gRPC metadata exists, so the H2
// path needs the guard at least as much as the H3 one.
func defaultSensitiveField(name []byte) bool {
	switch string(name) {
	case "authorization", "proxy-authorization", "cookie":
		return true
	}
	return false
}

// isTChar reports whether c is an RFC 9110 §5.6.2 token character.
func isTChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// validContentSubtype checks Options.ContentSubtype. Empty is valid and means
// no subtype.
//
// The subtype reaches a header value, and neither conn nor hpack validates
// outbound fields, so this is the only gate between a caller's string and the
// wire — a value carrying CR or LF there is a request-splitting vector at any
// HTTP/1.1 downgrading hop, exactly as for metadata.
func validContentSubtype(s string) error {
	if s == "" {
		return nil
	}
	for i := 0; i < len(s); i++ {
		if !isTChar(s[i]) {
			return fmt.Errorf("%w: ContentSubtype %q is not a token (RFC 9110 §5.6.2): "+
				"byte %d is %q", ErrInvalidMetadata, s, i, s[i])
		}
	}
	return nil
}

// contentTypeFor renders the request content-type once per connection.
func contentTypeFor(subtype string) []byte {
	if subtype == "" {
		return valApplicationGRPC
	}
	out := make([]byte, 0, len(valApplicationGRPC)+1+len(subtype))
	out = append(out, valApplicationGRPC...)
	out = append(out, '+')
	return append(out, subtype...)
}

// validMetadata checks one caller-supplied field end to end: a legal name, a
// name the transport does not own, and a legal value. It is the single gate —
// neither conn nor hpack validates on the send path (conn/validate.go covers
// decoded *response* fields; hpack encodes verbatim), so a field that gets past
// here reaches the wire unexamined.
func validMetadata(name, value []byte) error {
	if err := validMetadataName(name); err != nil {
		return err
	}
	if err := checkMetadataKey(string(name)); err != nil {
		return err
	}
	return validMetadataValue(name, value)
}
