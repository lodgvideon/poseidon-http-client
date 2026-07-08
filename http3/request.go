package http3

import (
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

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
}

// EncodeHeaders QPACK-encodes the request's field section — the request pseudo-
// headers first, then the regular headers (RFC 9114 §4.3.1) — and appends it to
// dst wrapped in a HEADERS frame (§7.2.2). It returns ErrH3Message if a required
// pseudo-header is missing or a regular header is invalid or connection-specific
// (§4.2), so the client never generates a malformed request on the wire.
func (r *Request) EncodeHeaders(enc *qpack.Encoder, dst []byte) ([]byte, error) {
	if r.Method == "" || r.Scheme == "" || r.Path == "" {
		return nil, ErrH3Message // required request pseudo-headers (§4.3.1)
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
	section := enc.EncodeFieldSection(nil, fields)
	return AppendHeaders(dst, section), nil
}
