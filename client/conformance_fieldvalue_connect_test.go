package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Batch 5 — client-side request generation conformance:
//   - RFC 9113 §8.2.1: a field value MUST NOT start or end with SP/HTAB (send
//     side; internal whitespace stays legal).
//   - RFC 9113 §8.5: a CONNECT request omits :scheme and :path and carries the
//     target authority in :authority; RFC 8441 extended CONNECT (:protocol set)
//     keeps :scheme and :path.

func baseReq() *Request {
	return &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
}

// TestConformance_RFC9113_Sec8_2_1_FieldValueEdgeWhitespace_Rejected pins that a
// regular request header value beginning or ending with SP/HTAB is refused.
func TestConformance_RFC9113_Sec8_2_1_FieldValueEdgeWhitespace_Rejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  string
	}{
		{"leading space", " x"},
		{"trailing space", "x "},
		{"leading htab", "\tx"},
		{"trailing htab", "x\t"},
		{"only space", " "},
		{"only htab", "\t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := baseReq()
			r.Headers = []conn.HeaderField{{Name: []byte("x-test"), Value: []byte(tc.val)}}

			err := validateRequest(r)

			assert.Errorf(t, err, "validateRequest accepted a field value %q with edge whitespace", tc.val)
		})
	}
}

// TestConformance_RFC9113_Sec8_2_1_InternalWhitespace_Accepted is the
// over-rejection guard: SP/HTAB inside a value is ordinary and must be accepted.
func TestConformance_RFC9113_Sec8_2_1_InternalWhitespace_Accepted(t *testing.T) {
	r := baseReq()
	r.Headers = []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("a b\tc")}}

	err := validateRequest(r)

	assert.NoError(t, err, "validateRequest rejected legal internal whitespace")
}

// TestConformance_RFC9113_Sec8_2_1_TrailerEdgeWhitespace_Rejected pins the same
// rule on static request trailers.
func TestConformance_RFC9113_Sec8_2_1_TrailerEdgeWhitespace_Rejected(t *testing.T) {
	r := baseReq()
	r.Trailers = []conn.HeaderField{{Name: []byte("x-trailer"), Value: []byte("v ")}}

	err := validateRequest(r)

	assert.Error(t, err, "validateRequest accepted a trailer value ending with SP")
}

// TestConformance_RFC9113_Sec8_5_ConnectOmitsSchemeAndPath pins that a regular
// (non-extended) CONNECT request emits :method and :authority but omits :scheme
// and :path.
func TestConformance_RFC9113_Sec8_5_ConnectOmitsSchemeAndPath(t *testing.T) {
	req := &Request{Method: "CONNECT", Authority: "example.com:443"}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	hdrs := buildHeaders(req, "default.example.com:443", "https", sp)

	var method, authority bool
	for _, h := range hdrs {
		switch string(h.Name) {
		case ":scheme":
			assert.Fail(t, ":scheme must be omitted for a regular CONNECT (RFC 9113 §8.5)")
		case ":path":
			assert.Fail(t, ":path must be omitted for a regular CONNECT (RFC 9113 §8.5)")
		case ":method":
			method = true
			assert.Equalf(t, "CONNECT", string(h.Value), ":method = %q, want CONNECT", h.Value)
		case ":authority":
			authority = true
		}
	}
	assert.True(t, method && authority, ":method and :authority are required for a CONNECT request")
}

// TestConformance_RFC9113_Sec8_5_ExtendedConnectKeepsSchemeAndPath is the
// over-rejection guard: RFC 8441 extended CONNECT keeps :scheme and :path.
func TestConformance_RFC9113_Sec8_5_ExtendedConnectKeepsSchemeAndPath(t *testing.T) {
	req := &Request{Method: "CONNECT", Scheme: "https", Authority: "example.com", Path: "/chat", Protocol: "websocket"}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	hdrs := buildHeaders(req, "d", "https", sp)

	var scheme, path bool
	for _, h := range hdrs {
		if string(h.Name) == ":scheme" {
			scheme = true
		}
		if string(h.Name) == ":path" {
			path = true
		}
	}
	assert.True(t, scheme && path, "extended CONNECT must keep :scheme and :path (RFC 8441 §4)")
}

// TestConformance_RFC9113_Sec8_5_ConnectRequestValidation pins the validateRequest
// rules for a regular CONNECT: a :path must be omitted (empty), :authority is
// required, and extended CONNECT still follows the normal path rules.
func TestConformance_RFC9113_Sec8_5_ConnectRequestValidation(t *testing.T) {
	withPath := validateRequest(&Request{Method: "CONNECT", Authority: "h:443", Path: "/x"})
	pathOmitted := validateRequest(&Request{Method: "CONNECT", Authority: "h:443"})
	noAuthority := validateRequest(&Request{Method: "CONNECT"})
	extended := validateRequest(&Request{Method: "CONNECT", Scheme: "https", Authority: "h", Path: "/ws", Protocol: "websocket"})

	assert.Error(t, withPath, "a regular CONNECT with a Path was accepted; :path must be omitted (RFC 9113 §8.5)")
	assert.NoError(t, pathOmitted, "a regular CONNECT with omitted path + authority was rejected")
	assert.Error(t, noAuthority, "a CONNECT with no authority was accepted; :authority is required (RFC 9113 §8.5)")
	assert.NoError(t, extended, "an extended CONNECT with a path was rejected")
}
