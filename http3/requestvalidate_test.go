package http3

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// TestConformance_RFC9114_Sec42_PseudoHeaderValueValidation checks that a
// forbidden octet (NUL, CR, or LF) in a request pseudo-header value makes the
// request malformed, so the client never emits a header-injection vector on the
// wire (RFC 9114 §4.2).
func TestConformance_RFC9114_Sec42_PseudoHeaderValueValidation(t *testing.T) {
	var enc qpack.Encoder
	cases := []struct {
		name string
		req  *Request
	}{
		{"crlf-in-path", &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/x\r\ninjected: 1"}},
		{"nul-in-authority", &Request{Method: "GET", Scheme: "https", Authority: "h\x00", Path: "/"}},
		{"lf-in-method", &Request{Method: "GE\nT", Scheme: "https", Authority: "h", Path: "/"}},
		{"cr-in-scheme", &Request{Method: "GET", Scheme: "http\rs", Authority: "h", Path: "/"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0)); err != ErrH3Message {
				t.Fatalf("a pseudo-header with a forbidden octet: err = %v, want ErrH3Message", err)
			}
		})
	}
}

// TestConformance_RFC9114_Sec431_AuthorityRequired checks that an http or https
// request MUST carry an :authority pseudo-header or a Host header field, and that
// either one satisfies the requirement (RFC 9114 §4.3.1).
func TestConformance_RFC9114_Sec431_AuthorityRequired(t *testing.T) {
	var enc qpack.Encoder

	bad := &Request{Method: "GET", Scheme: "https", Path: "/"} // neither :authority nor Host
	if _, err := bad.EncodeHeaders(&enc, nil, ^uint64(0)); err != ErrH3Message {
		t.Fatalf("https without :authority or Host: err = %v, want ErrH3Message", err)
	}

	withHost := &Request{Method: "GET", Scheme: "https", Path: "/", Headers: []hpack.HeaderField{hf("host", "example.com")}}
	if _, err := withHost.EncodeHeaders(&enc, nil, ^uint64(0)); err != nil {
		t.Fatalf("https with a Host header: err = %v, want nil", err)
	}

	withAuthority := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	if _, err := withAuthority.EncodeHeaders(&enc, nil, ^uint64(0)); err != nil {
		t.Fatalf("https with :authority: err = %v, want nil", err)
	}
}
