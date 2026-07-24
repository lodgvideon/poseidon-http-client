package client

import (
	"testing"

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
			if err := validateRequest(r); err == nil {
				t.Errorf("validateRequest accepted a field value %q with edge whitespace", tc.val)
			}
		})
	}
}

// TestConformance_RFC9113_Sec8_2_1_InternalWhitespace_Accepted is the
// over-rejection guard: SP/HTAB inside a value is ordinary and must be accepted.
func TestConformance_RFC9113_Sec8_2_1_InternalWhitespace_Accepted(t *testing.T) {
	r := baseReq()
	r.Headers = []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("a b\tc")}}
	if err := validateRequest(r); err != nil {
		t.Errorf("validateRequest rejected legal internal whitespace: %v", err)
	}
}

// TestConformance_RFC9113_Sec8_2_1_TrailerEdgeWhitespace_Rejected pins the same
// rule on static request trailers.
func TestConformance_RFC9113_Sec8_2_1_TrailerEdgeWhitespace_Rejected(t *testing.T) {
	r := baseReq()
	r.Trailers = []conn.HeaderField{{Name: []byte("x-trailer"), Value: []byte("v ")}}
	if err := validateRequest(r); err == nil {
		t.Error("validateRequest accepted a trailer value ending with SP")
	}
}

// TestConformance_RFC9113_Sec8_5_ConnectOmitsSchemeAndPath pins that a regular
// (non-extended) CONNECT request emits :method and :authority but omits :scheme
// and :path.
func TestConformance_RFC9113_Sec8_5_ConnectOmitsSchemeAndPath(t *testing.T) {
	req := &Request{Method: "CONNECT", Authority: "example.com:443"}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	hdrs := buildHeaders(req, "default.example.com:443", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	var method, authority bool
	for _, h := range hdrs {
		switch string(h.Name) {
		case ":scheme":
			t.Error(":scheme must be omitted for a regular CONNECT (RFC 9113 §8.5)")
		case ":path":
			t.Error(":path must be omitted for a regular CONNECT (RFC 9113 §8.5)")
		case ":method":
			method = true
			if string(h.Value) != "CONNECT" {
				t.Errorf(":method = %q, want CONNECT", h.Value)
			}
		case ":authority":
			authority = true
		}
	}
	if !method || !authority {
		t.Error(":method and :authority are required for a CONNECT request")
	}
}

// TestConformance_RFC9113_Sec8_5_ExtendedConnectKeepsSchemeAndPath is the
// over-rejection guard: RFC 8441 extended CONNECT keeps :scheme and :path.
func TestConformance_RFC9113_Sec8_5_ExtendedConnectKeepsSchemeAndPath(t *testing.T) {
	req := &Request{Method: "CONNECT", Scheme: "https", Authority: "example.com", Path: "/chat", Protocol: "websocket"}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	hdrs := buildHeaders(req, "d", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	var scheme, path bool
	for _, h := range hdrs {
		if string(h.Name) == ":scheme" {
			scheme = true
		}
		if string(h.Name) == ":path" {
			path = true
		}
	}
	if !scheme || !path {
		t.Error("extended CONNECT must keep :scheme and :path (RFC 8441 §4)")
	}
}

// TestConformance_RFC9113_Sec8_5_ConnectRequestValidation pins the validateRequest
// rules for a regular CONNECT: a :path must be omitted (empty), :authority is
// required, and extended CONNECT still follows the normal path rules.
func TestConformance_RFC9113_Sec8_5_ConnectRequestValidation(t *testing.T) {
	if err := validateRequest(&Request{Method: "CONNECT", Authority: "h:443", Path: "/x"}); err == nil {
		t.Error("a regular CONNECT with a Path was accepted; :path must be omitted (RFC 9113 §8.5)")
	}
	if err := validateRequest(&Request{Method: "CONNECT", Authority: "h:443"}); err != nil {
		t.Errorf("a regular CONNECT with omitted path + authority was rejected: %v", err)
	}
	if err := validateRequest(&Request{Method: "CONNECT"}); err == nil {
		t.Error("a CONNECT with no authority was accepted; :authority is required (RFC 9113 §8.5)")
	}
	if err := validateRequest(&Request{Method: "CONNECT", Scheme: "https", Authority: "h", Path: "/ws", Protocol: "websocket"}); err != nil {
		t.Errorf("an extended CONNECT with a path was rejected: %v", err)
	}
}
