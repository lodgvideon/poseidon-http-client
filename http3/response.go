package http3

import (
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// Response is the parsed head of an HTTP/3 response: the :status pseudo-header
// and the regular response header fields. Interim holds any informational (1xx)
// responses that preceded the final one, and Trailers the trailing field section
// that followed the body, both in receive order (RFC 9114 §4.1).
type Response struct {
	Status   int
	Headers  []hpack.HeaderField
	Interim  []*Response
	Trailers []hpack.HeaderField
}

// DecodeResponseHeaders QPACK-decodes a HEADERS field section into a Response,
// enforcing the response message rules (RFC 9114 §4.1.2, §4.3.2): :status is
// required and must precede every regular header, it is the only permitted
// pseudo-header, and every regular field name/value is validated (§4.2, §5.5).
//
// It returns ErrH3Message (→ H3_MESSAGE_ERROR, a stream error) for a rule
// violation, or qpack.ErrDecompressionFailed (→ QPACK_DECOMPRESSION_FAILED, a
// connection error, RFC 9204 §2.2) if the field section itself is malformed.
// The caller maps these to different error codes and scopes.
func DecodeResponseHeaders(dec *qpack.Decoder, fieldSection []byte) (*Response, error) {
	resp := &Response{}
	var haveStatus, sawRegular bool
	err := dec.DecodeFieldSection(fieldSection, func(name, value []byte) error {
		if len(name) == 0 {
			return ErrH3Message
		}
		if name[0] == ':' {
			if sawRegular {
				return ErrH3Message // pseudo-header after a regular header
			}
			if string(name) != ":status" || haveStatus {
				return ErrH3Message // only :status, and only once
			}
			status, ok := parseStatus(value)
			if !ok {
				return ErrH3Message
			}
			resp.Status = status
			haveStatus = true
			return nil
		}
		if !validFieldName(name) || !validFieldValue(value) || forbiddenField(name, value) {
			return ErrH3Message
		}
		sawRegular = true
		resp.Headers = append(resp.Headers, hpack.HeaderField{
			Name:  append([]byte(nil), name...),
			Value: append([]byte(nil), value...),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !haveStatus {
		return nil, ErrH3Message
	}
	return resp, nil
}

// DecodeTrailers QPACK-decodes a trailing HEADERS field section into its regular
// header fields (RFC 9114 §4.1). A trailer section carries no pseudo-headers, so
// any field name beginning with ':' (or an empty name) is a malformed message;
// regular names/values are validated exactly as in a header section (§4.2, §5.5).
// It returns ErrH3Message for a rule violation, or qpack.ErrDecompressionFailed
// if the field section itself is malformed.
func DecodeTrailers(dec *qpack.Decoder, fieldSection []byte) ([]hpack.HeaderField, error) {
	var fields []hpack.HeaderField
	err := dec.DecodeFieldSection(fieldSection, func(name, value []byte) error {
		if len(name) == 0 || name[0] == ':' {
			return ErrH3Message // trailers carry no pseudo-headers (§4.3)
		}
		if !validFieldName(name) || !validFieldValue(value) || forbiddenField(name, value) {
			return ErrH3Message
		}
		fields = append(fields, hpack.HeaderField{
			Name:  append([]byte(nil), name...),
			Value: append([]byte(nil), value...),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fields, nil
}

// parseStatus parses a :status value: exactly three ASCII digits in the valid
// range 100–599 (RFC 9110 §15). Anything else is a malformed pseudo-header.
func parseStatus(v []byte) (int, bool) {
	if len(v) != 3 {
		return 0, false
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if n < 100 || n > 599 {
		return 0, false
	}
	return n, true
}
