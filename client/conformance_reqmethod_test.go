package client

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

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

			err := validateRequest(req)

			require.ErrorIs(t, err, ErrInvalidRequest,
				"a TRACE body must not be sent (RFC 9110 §9.3.8)")
		})
	}

	// Over-rejection guard: a bodyless TRACE is legal.
	err := validateRequest(&Request{Method: "TRACE", Path: "/"})

	require.NoError(t, err, "a bodyless TRACE is legal and must not be refused")
}

// TestConformance_RFC9110_Sec9_3_7_OptionsContentRequiresContentType pins that an
// OPTIONS request with content is refused unless it carries a Content-Type. RFC
// 9110 §9.3.7: "A client that generates an OPTIONS request containing content
// MUST send a valid Content-Type header field describing the representation
// media type." Presence is the enforceable part; media-type validity stays the
// caller's, and the general §8.3 SHOULD is not enforced.
func TestConformance_RFC9110_Sec9_3_7_OptionsContentRequiresContentType(t *testing.T) {
	err := validateRequest(&Request{Method: "OPTIONS", Path: "/", Body: []byte("x")})

	require.ErrorIs(t, err, ErrInvalidRequest,
		"OPTIONS with content and no Content-Type must be refused (RFC 9110 §9.3.7)")

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
			err := validateRequest(tc.req)

			require.NoError(t, err, "§9.3.7 binds only OPTIONS-with-content-and-no-Content-Type")
		})
	}
}

// TestConformance_RFC9113_Sec8_3_1_HostHeaderRefused pins that a caller-supplied
// host header is refused rather than emitted next to the client's own authority.
//
// RFC 9110 §7.7 and RFC 9113 §8.3.1 require Host and :authority to agree; a
// caller header carrying a different (or empty) host rode the H2 wire alongside
// :authority, and the pair collapses at any HTTP/2->HTTP/1.1 downgrading
// intermediary. http1's WriteRequest drops it and derives Host from :authority,
// and http3 rejects it — the shared gate now agrees with both.
func TestConformance_RFC9113_Sec8_3_1_HostHeaderRefused(t *testing.T) {
	for _, v := range []string{"other.example.com", "", "example.com"} {
		t.Run("host="+v, func(t *testing.T) {
			req := &Request{
				Method: "GET", Path: "/", Authority: "example.com",
				Headers: []conn.HeaderField{{Name: []byte("Host"), Value: []byte(v)}},
			}

			err := validateRequest(req)

			require.ErrorIsf(t, err, ErrInvalidRequest,
				"validateRequest(host header %q) = %v, want ErrInvalidRequest", v, err)
		})
	}

	// Over-rejection guard: the authority alone is how a caller sets the host.
	err := validateRequest(&Request{Method: "GET", Path: "/", Authority: "example.com"})

	require.NoError(t, err, "an authority with no host header is the supported way to set the host")
}

// TestConformance_RFC9110_Sec10_1_1_Expect100OnBodylessRefused pins that a
// 100-continue expectation is refused on a request with no content. RFC 9110
// §10.1.1 forbids generating one there: nothing can be withheld, so the exchange
// only pays a round trip, or stalls against a server that waits.
func TestConformance_RFC9110_Sec10_1_1_Expect100OnBodylessRefused(t *testing.T) {
	for _, v := range []string{"100-continue", "100-Continue"} {
		t.Run(v, func(t *testing.T) {
			req := &Request{
				Method: "POST", Path: "/",
				Headers: []conn.HeaderField{{Name: []byte("Expect"), Value: []byte(v)}},
			}

			err := validateRequest(req)

			require.ErrorIsf(t, err, ErrInvalidRequest,
				"validateRequest(Expect: %s, no body) = %v, want ErrInvalidRequest", v, err)
		})
	}

	// Over-rejection guards: with content it is exactly what the expectation is
	// for, and an unrelated Expect value is the caller's business.
	accept := []*Request{
		{Method: "POST", Path: "/", Body: []byte("x"),
			Headers: []conn.HeaderField{{Name: []byte("Expect"), Value: []byte("100-continue")}}},
		{Method: "POST", Path: "/",
			Headers: []conn.HeaderField{{Name: []byte("Expect"), Value: []byte("other-extension")}}},
	}
	for _, req := range accept {
		err := validateRequest(req)

		require.NoError(t, err, "§10.1.1 binds only 100-continue on a request with no content")
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

			require.ErrorIsf(t, err, ErrInvalidRequest,
				"validateRequest(Authority=%q) = %v, want ErrInvalidRequest "+
					"(RFC 9110 §4.2.4, RFC 9112 §3.2)", auth, err)
		})
	}

	// Over-rejection guard: a bare host[:port] has no '@' and must pass.
	for _, auth := range []string{"example.com", "example.com:8443", "[::1]:443"} {
		err := validateRequest(&Request{Method: "GET", Path: "/", Authority: auth})

		require.NoErrorf(t, err, "validateRequest(Authority=%q) = %v, want nil", auth, err)
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

			err := validateRequest(req)

			require.ErrorIs(t, err, ErrInvalidRequest,
				"a control byte in the method/target is malformed on an http1 downgrade (RFC 7540 §8.1.2.6)")
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
		err := validateRequest(req)

		require.NoErrorf(t, err, "validateRequest(%s %s) = %v, want nil", req.Method, req.Path, err)
	}
}
