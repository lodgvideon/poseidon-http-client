package http3

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
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
			_, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0))

			require.Equalf(t, ErrH3Message, err,
				"a pseudo-header with a forbidden octet: err = %v, want ErrH3Message — an "+
					"encoded CR/LF/NUL is a header-injection vector the peer reserializes", err)
		})
	}
}

// TestConformance_RFC9114_Sec431_AuthorityRequired checks that an http or https
// request MUST carry an :authority pseudo-header or a Host header field, and that
// either one satisfies the requirement (RFC 9114 §4.3.1).
func TestConformance_RFC9114_Sec431_AuthorityRequired(t *testing.T) {
	var enc qpack.Encoder
	cases := []struct {
		name string
		req  *Request
		want error
	}{
		{"without :authority or Host", &Request{Method: "GET", Scheme: "https", Path: "/"}, ErrH3Message},
		{"with a Host header", &Request{Method: "GET", Scheme: "https", Path: "/", Headers: []header.Field{hf("host", "example.com")}}, nil},
		{"with :authority", &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0))

			require.Equalf(t, c.want, err,
				"https %s: err = %v, want %v — §4.3.1 makes the authority mandatory and "+
					"satisfiable by EITHER carrier, so a client that refuses one of them "+
					"refuses a conformant request", c.name, err, c.want)
		})
	}
}

// TestConformance_RFC9114_Sec431_AuthorityUserinfoAndHostMatch checks the §4.3.1
// MUSTs on the authority: it MUST NOT include the deprecated userinfo subcomponent
// (an '@'), and if both a Host header and :authority are present they MUST carry the
// same non-empty value.
func TestConformance_RFC9114_Sec431_AuthorityUserinfoAndHostMatch(t *testing.T) {
	cases := []struct {
		name string
		req  *Request
		want error
	}{
		{":authority with userinfo",
			&Request{Method: "GET", Scheme: "https", Authority: "user@example.com", Path: "/"},
			ErrH3Message},
		{"Host != :authority",
			&Request{Method: "GET", Scheme: "https", Authority: "a.com", Path: "/", Headers: []header.Field{hf("host", "b.com")}},
			ErrH3Message},
		{"empty Host header",
			&Request{Method: "GET", Scheme: "https", Authority: "a.com", Path: "/", Headers: []header.Field{hf("host", "")}},
			ErrH3Message},
		{"Host == :authority",
			&Request{Method: "GET", Scheme: "https", Authority: "a.com", Path: "/", Headers: []header.Field{hf("host", "a.com")}},
			nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var enc qpack.Encoder

			_, err := c.req.EncodeHeaders(&enc, nil, ^uint64(0))

			require.Equalf(t, c.want, err,
				"%s: err = %v, want %v — a client that lets these through hands the peer two "+
					"disagreeing authorities to route on", c.name, err, c.want)
		})
	}
}
