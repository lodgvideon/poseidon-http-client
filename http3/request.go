package http3

import (
	"errors"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// ErrFieldSectionTooLarge is returned by EncodeHeaders when the request's field
// section exceeds the peer's SETTINGS_MAX_FIELD_SECTION_SIZE (RFC 9114 §4.2.2).
// The request is refused locally rather than sent for the server to reject.
var ErrFieldSectionTooLarge = errors.New("http3: field section exceeds peer SETTINGS_MAX_FIELD_SECTION_SIZE")

// fieldLineOverhead is the per-field byte cost added to the name and value
// lengths when sizing a field section (RFC 9114 §4.2.2).
const fieldLineOverhead = 32

// Request is the subset of an HTTP request a client maps onto an HTTP/3 HEADERS
// frame: the request pseudo-headers plus regular header fields. The CONNECT and
// asterisk-form (OPTIONS *) request forms, which omit or specialize pseudo-
// headers (§4.4), are not yet supported.
type Request struct {
	Method    string
	Scheme    string
	Authority string // omitted from the field section when empty
	Path      string
	Headers   []hpack.HeaderField
	Body      []byte // optional request body, sent in a DATA frame after HEADERS
}

// requiresAuthority reports whether a scheme has a mandatory authority component;
// "http" and "https" do (RFC 9114 §4.3.1).
func requiresAuthority(scheme string) bool {
	return scheme == "http" || scheme == "https"
}

// hostHeaderValue returns the value of the Host header (lowercase "host" in
// HTTP/3) among the regular fields, and whether one is present.
func hostHeaderValue(headers []hpack.HeaderField) (string, bool) {
	for i := range headers {
		if string(headers[i].Name) == "host" {
			return string(headers[i].Value), true
		}
	}
	return "", false
}

// EncodeHeaders QPACK-encodes the request's field section — the request pseudo-
// headers first, then the regular headers (RFC 9114 §4.3.1) — and appends it to
// dst wrapped in a HEADERS frame (§7.2.2). It returns ErrH3Message if a required
// pseudo-header is missing or a regular header is invalid or connection-specific
// (§4.2), so the client never generates a malformed request on the wire.
//
// maxFieldSection is the peer's SETTINGS_MAX_FIELD_SECTION_SIZE; a field section
// whose uncompressed size exceeds it is refused with ErrFieldSectionTooLarge
// (§4.2.2). Pass the maximum uint64 when the peer imposes no limit.
func (r *Request) EncodeHeaders(enc *qpack.Encoder, dst []byte, maxFieldSection uint64) ([]byte, error) {
	if r.Method == "" || r.Scheme == "" || r.Path == "" {
		return nil, ErrH3Message // required request pseudo-headers (§4.3.1)
	}
	// Pseudo-header values are field values: they MUST NOT contain NUL, CR, or LF
	// (RFC 9114 §4.2), exactly as the regular field values are checked below.
	if !validFieldValue([]byte(r.Method)) || !validFieldValue([]byte(r.Scheme)) ||
		!validFieldValue([]byte(r.Authority)) || !validFieldValue([]byte(r.Path)) {
		return nil, ErrH3Message
	}
	host, haveHost := hostHeaderValue(r.Headers)
	// An http or https request MUST carry an :authority pseudo-header or a Host
	// header field (RFC 9114 §4.3.1).
	if r.Authority == "" && requiresAuthority(r.Scheme) && !haveHost {
		return nil, ErrH3Message
	}
	// The authority MUST NOT include the deprecated userinfo subcomponent for an
	// http or https URI (RFC 9114 §4.3.1); its presence is marked by an '@'.
	if requiresAuthority(r.Scheme) && strings.ContainsRune(r.Authority, '@') {
		return nil, ErrH3Message
	}
	// If both the Host header and :authority are present they MUST carry the same
	// value, and neither present field may be empty (RFC 9114 §4.3.1).
	if haveHost {
		if host == "" || (r.Authority != "" && host != r.Authority) {
			return nil, ErrH3Message
		}
	}
	fields := make([]hpack.HeaderField, 0, 4+len(r.Headers))
	fields = append(fields,
		hpack.HeaderField{Name: []byte(":method"), Value: []byte(r.Method)},
		hpack.HeaderField{Name: []byte(":scheme"), Value: []byte(r.Scheme)},
	)
	if r.Authority != "" {
		fields = append(fields, hpack.HeaderField{Name: []byte(":authority"), Value: []byte(r.Authority)})
	}
	fields = append(fields, hpack.HeaderField{Name: []byte(":path"), Value: []byte(r.Path)})

	for i := range r.Headers {
		h := r.Headers[i]
		if !validFieldName(h.Name) || !validFieldValue(h.Value) || forbiddenField(h.Name, h.Value) {
			return nil, ErrH3Message
		}
		fields = append(fields, h)
	}
	// The field-section size is the uncompressed cost of every field — name plus
	// value plus a 32-byte overhead each (§4.2.2). The peer advises a sender SHOULD
	// NOT exceed its limit; this client enforces it strictly and refuses to send.
	// Summed on uint64 so a crafted huge header cannot overflow.
	var size uint64
	for i := range fields {
		size += uint64(len(fields[i].Name)) + uint64(len(fields[i].Value)) + fieldLineOverhead
	}
	if size > maxFieldSection {
		return nil, ErrFieldSectionTooLarge
	}
	section := enc.EncodeFieldSection(nil, fields)
	return AppendHeaders(dst, section), nil
}
