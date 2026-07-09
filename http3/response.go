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

// canHaveContent reports whether a final response with this status, for a request
// using this method, is defined as able to carry content — so its Content-Length
// must match the received DATA (RFC 9114 §4.1.2). A response that never has
// content (204, 304, or any response to a HEAD request; 1xx are handled as
// interim) may carry an anticipatory Content-Length that the DATA does not match.
func canHaveContent(status int, method string) bool {
	if method == "HEAD" {
		return false
	}
	return status != 204 && status != 304
}

// responseContentLength scans a response's header fields for Content-Length. It
// reports the value, whether the field was present, and whether it is valid — a
// single non-negative integer, or repeated fields that all agree (RFC 9110 §8.6);
// a non-numeric value or conflicting repeats make it invalid, which the caller
// treats as a malformed message.
func responseContentLength(headers []hpack.HeaderField) (n int64, present, valid bool) {
	for _, h := range headers {
		if string(h.Name) != "content-length" {
			continue
		}
		v, ok := parseDecimal(h.Value)
		if !ok || (present && v != n) {
			return 0, true, false
		}
		n, present = v, true
	}
	return n, present, present
}

// parseDecimal parses a non-empty run of ASCII digits into a non-negative int64,
// rejecting an empty or non-digit value and any absurdly large length (past 2^62)
// that could not be a real body.
func parseDecimal(v []byte) (int64, bool) {
	if len(v) == 0 {
		return 0, false
	}
	var n int64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
		if n > ((1<<62)-9)/10 {
			return 0, false // guard before the multiply so n cannot overflow int64
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
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
