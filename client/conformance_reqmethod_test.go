package client

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC9110_Sec9_3_8_TRACEBodyRejected pins that the shared
// request validator refuses a TRACE request carrying content. RFC 9110 §9.3.8:
// "A client MUST NOT send content in a TRACE request." A TRACE body is framed
// and sent like any other method's without this guard.
func TestConformance_RFC9110_Sec9_3_8_TRACEBodyRejected(t *testing.T) {
	reject := []struct {
		name string
		mut  func(*Request)
	}{
		{"TRACE with Body", func(r *Request) { r.Method = "TRACE"; r.Body = []byte("x") }},
		{"TRACE with BodyReader", func(r *Request) { r.Method = "TRACE"; r.BodyReader = bytes.NewReader([]byte("x")) }},
	}
	for _, tc := range reject {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{Method: "GET", Path: "/"}
			tc.mut(req)
			if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("validateRequest = %v, want ErrInvalidRequest — a TRACE body must "+
					"not be sent (RFC 9110 §9.3.8)", err)
			}
		})
	}
	// Over-rejection guard: a bodyless TRACE is legal.
	if err := validateRequest(&Request{Method: "TRACE", Path: "/"}); err != nil {
		t.Fatalf("validateRequest(bodyless TRACE) = %v, want nil", err)
	}
}

// TestConformance_RFC9110_Sec9_3_7_OptionsContentRequiresContentType pins that an
// OPTIONS request with content is refused unless it carries a Content-Type. RFC
// 9110 §9.3.7: "A client that generates an OPTIONS request containing content
// MUST send a valid Content-Type header field describing the representation
// media type." Presence is the enforceable part; media-type validity stays the
// caller's, and the general §8.3 SHOULD is not enforced.
func TestConformance_RFC9110_Sec9_3_7_OptionsContentRequiresContentType(t *testing.T) {
	if err := validateRequest(&Request{Method: "OPTIONS", Path: "/", Body: []byte("x")}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("OPTIONS+content, no Content-Type = %v, want ErrInvalidRequest (RFC 9110 §9.3.7)", err)
	}

	accept := []struct {
		name string
		req  *Request
	}{
		{"OPTIONS+content+Content-Type", &Request{Method: "OPTIONS", Path: "/", Body: []byte("x"),
			Headers: []conn.HeaderField{{Name: []byte("Content-Type"), Value: []byte("application/json")}}}},
		{"OPTIONS, no content", &Request{Method: "OPTIONS", Path: "/"}},
		// §9.3.7 binds OPTIONS specifically; the general §8.3 SHOULD is the
		// caller's, so a bodied POST without Content-Type must NOT be rejected.
		{"POST+content, no Content-Type", &Request{Method: "POST", Path: "/", Body: []byte("x")}},
	}
	for _, tc := range accept {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRequest(tc.req); err != nil {
				t.Fatalf("validateRequest = %v, want nil", err)
			}
		})
	}
}

// TestConformance_RFC9110_Sec4_2_4_AuthorityUserinfoRejected pins that a caller
// :authority carrying a userinfo "@" is refused rather than emitted verbatim.
// RFC 9110 §4.2.4 deprecates the userinfo subcomponent; RFC 9112 §3.2 requires
// the Host field value to exclude it. The http3 sibling already rejects this;
// the shared H1/H2 path did not, so "user@host" reached Host / :authority.
func TestConformance_RFC9110_Sec4_2_4_AuthorityUserinfoRejected(t *testing.T) {
	for _, auth := range []string{"user@example.com", "user:pass@example.com", "u@example.com:8443"} {
		t.Run(auth, func(t *testing.T) {
			err := validateRequest(&Request{Method: "GET", Path: "/", Authority: auth})
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("validateRequest(Authority=%q) = %v, want ErrInvalidRequest "+
					"(RFC 9110 §4.2.4, RFC 9112 §3.2)", auth, err)
			}
		})
	}
	// Over-rejection guard: a bare host[:port] has no '@' and must pass.
	for _, auth := range []string{"example.com", "example.com:8443", "[::1]:443"} {
		if err := validateRequest(&Request{Method: "GET", Path: "/", Authority: auth}); err != nil {
			t.Fatalf("validateRequest(Authority=%q) = %v, want nil", auth, err)
		}
	}
}

// TestConformance_RFC9110_Sec9_1_MethodAndTargetRejectControlBytes pins that the
// shared validator refuses a method or path carrying a control byte other than
// the CR/LF/NUL already covered. RFC 9110 §9.1 makes the method a token; RFC 9112
// §3 delimits the request-line by SP/CRLF, so any control byte is malformed on an
// http1 downgrade (RFC 7540 §8.1.2.6). http1.WriteRequest already refuses these;
// the shared H2/H3 gate checked only for whitespace, so e.g. 0x1F passed.
func TestConformance_RFC9110_Sec9_1_MethodAndTargetRejectControlBytes(t *testing.T) {
	reject := []struct {
		name string
		mut  func(*Request)
	}{
		{"method with US control", func(r *Request) { r.Method = "GET\x1f" }},
		{"method with VT", func(r *Request) { r.Method = "P\x0bOST" }},
		{"method with DEL", func(r *Request) { r.Method = "GET\x7f" }},
		{"path with SOH", func(r *Request) { r.Path = "/a\x01b" }},
		{"path with US control", func(r *Request) { r.Path = "/a\x1fb" }},
		{"path with DEL", func(r *Request) { r.Path = "/a\x7fb" }},
	}
	for _, tc := range reject {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{Method: "GET", Path: "/"}
			tc.mut(req)
			if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("validateRequest = %v, want ErrInvalidRequest — a control byte in the "+
					"method/target is malformed on an http1 downgrade (RFC 7540 §8.1.2.6)", err)
			}
		})
	}
	// Over-rejection guard: ordinary methods and targets (incl. query, %-encoding,
	// and asterisk-form) must pass.
	accept := []*Request{
		{Method: "GET", Path: "/"},
		{Method: "POST", Path: "/a/b?c=d&e=f"},
		{Method: "PROPFIND", Path: "/dav/%20path"},
		{Method: "OPTIONS", Path: "*"},
	}
	for _, req := range accept {
		if err := validateRequest(req); err != nil {
			t.Fatalf("validateRequest(%s %s) = %v, want nil", req.Method, req.Path, err)
		}
	}
}
