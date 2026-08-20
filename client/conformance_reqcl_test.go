package client

// The client layer must not emit contradictory framing either.
//
// http1's WriteRequest was fixed in #253: it appended its own "Content-Length: 0"
// for a bodyless POST beside a caller-supplied one, and the fix gated that on
// !hasContentLength. buildHeaders — one layer up, and shared with the HTTP/2
// path — has the same shape and never got the guard, so the defect survived the
// fix that named it.
//
// Two disagreeing Content-Length field lines are the CL.CL desync (RFC 9112
// §11.2): a front end honouring one value and a back end the other disagree
// about where the request ends. This package refuses to PARSE such a response
// (http1.ErrInvalidContentLength); it must not EMIT one.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func contentLengths(f []conn.HeaderField) []string {
	var out []string
	for i := range f {
		if bytes.EqualFold(f[i].Name, hdrContentLength) {
			out = append(out, string(f[i].Value))
		}
	}
	return out
}

// TestBuildHeaders_SingleContentLength pins that a caller-supplied
// Content-Length is replaced by the managed one rather than joined by it.
//
// Replacing is what compressedHeaders already does for the compressed path, and
// what Request.CompressBody's doc already promises: "content-length is managed
// for you and any caller-supplied one is replaced". Request.ContentLength governs
// how many body bytes actually get written, so it is the value that can be true.
//
// Both spellings are tested because RFC 9110 §5.1 makes field names
// case-insensitive — the exact-match version of this guard is what let the
// canonical spelling through in http1 (#253).
func TestBuildHeaders_SingleContentLength(t *testing.T) {
	for _, spelling := range []string{"content-length", "Content-Length", "CONTENT-LENGTH"} {
		t.Run(spelling, func(t *testing.T) {
			req := &Request{
				Method:        "POST",
				Path:          "/",
				ContentLength: 10,
				BodyReader:    strings.NewReader("0123456789"),
				Headers:       []conn.HeaderField{{Name: []byte(spelling), Value: []byte("5")}},
			}
			var sp []conn.HeaderField

			got := contentLengths(buildHeaders(req, "x.test", "https", &sp))

			require.Lenf(t, got, 1, "content-length fields = %v, want exactly one — two disagreeing "+
				"values is the CL.CL desync (RFC 9112 §11.2), emitted by this library", got)
			assert.Equalf(t, "10", got[0], "content-length = %q, want %q (Request.ContentLength governs how "+
				"many body bytes are written, so it is the one that can be true)", got[0], "10")
		})
	}
}

// TestBuildHeaders_CallerContentLengthKeptWithoutBody is a WHITE-BOX unit test
// of buildHeaders in isolation, and says so because its old rationale was
// stale (#848).
//
// It used to be justified as an over-rejection guard protecting "requests that
// were already correct". That request shape is no longer one this client
// accepts: validateRequest refuses a caller-supplied Content-Length whenever
// managesContentLength(r) is false, and for a request with neither Body nor
// BodyReader it is false. buildHeaders has exactly one production call site,
// inside do, downstream of both validateRequest gates — so no Do or DoStream
// caller can reach it with this request, and a reader trusting the old comment
// would conclude the opposite.
//
// The property is still worth pinning at this level: buildHeaders must not
// strip a field it did not manage, whatever the layer above decides to accept.
// The end-to-end half of the pair is
// TestConformance_RFC9110_Sec8_6_BodylessCallerContentLengthRefusedByDo below.
//
// The witness value is a distinctive "42", not "0". buildHeaders' managed path
// appends "Content-Length: 0" for a bodyless request that reaches it, so a "0"
// here proves nothing — a strip-then-re-append bug and a correct pass-through
// produce the identical ["0"]. "42" is a value the managed path never emits, so
// it survives only if the caller's own field was carried through verbatim.
func TestBuildHeaders_CallerContentLengthKeptWithoutBody(t *testing.T) {
	req := &Request{
		Method:  "POST",
		Path:    "/",
		Headers: []conn.HeaderField{{Name: []byte("Content-Length"), Value: []byte("42")}},
	}
	var sp []conn.HeaderField

	got := contentLengths(buildHeaders(req, "x.test", "https", &sp))

	assert.Equalf(t, []string{"42"}, got,
		"content-length fields = %v, want exactly [\"42\"] — with no BodyReader "+
			"nothing managed is appended, so the caller's field must pass through unchanged", got)
}

// TestConformance_RFC9110_Sec8_6_BodylessCallerContentLengthRefusedByDo is the
// end-to-end half nothing in this file asserted (#848).
//
// RFC 9110 section 8.6 makes Content-Length a claim about the representation
// being sent. A request with neither Body nor BodyReader sends none, so a
// caller-supplied Content-Length is a claim this client cannot verify and
// validateRequest refuses it — one layer ABOVE the buildHeaders unit test just
// above, which is why that test's fixture is not a shape Do accepts.
//
// The refusal has to be classifiable: a caller that cannot tell ErrInvalidRequest
// from a transport failure retries a request that will never succeed.
func TestConformance_RFC9110_Sec8_6_BodylessCallerContentLengthRefusedByDo(t *testing.T) {
	cases := []struct {
		name string
		req  *Request
		want bool
	}{
		{"bodyless request with a caller Content-Length", &Request{
			Method:  "POST",
			Path:    "/",
			Headers: []conn.HeaderField{hf("Content-Length", "42")},
		}, false},
		{"BodyReader with ContentLength 0 and a caller Content-Length", &Request{
			Method:        "POST",
			Path:          "/",
			BodyReader:    strings.NewReader(""),
			ContentLength: 0,
			Headers:       []conn.HeaderField{hf("Content-Length", "42")},
		}, false},
		{"the same request without the caller field is accepted", &Request{
			Method: "POST",
			Path:   "/",
		}, true},
		{"a managed Content-Length is accepted", &Request{
			Method:        "POST",
			Path:          "/",
			BodyReader:    strings.NewReader("abc"),
			ContentLength: 3,
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateRequest(c.req)

			if c.want {
				assert.NoErrorf(t, err,
					"validateRequest refused a request this client can verify: %v; over-"+
						"rejection here breaks callers that were already correct", err)
				return
			}
			require.Errorf(t, err,
				"validateRequest accepted a caller Content-Length it cannot verify against "+
					"any body; the field would go on the wire as a claim about a "+
					"representation that is not being sent (RFC 9110 section 8.6)")
			assert.ErrorIsf(t, err, ErrInvalidRequest,
				"refusal = %v, want ErrInvalidRequest — a caller that cannot classify this "+
					"retries a request that can never succeed", err)
		})
	}
}

// TestBuildHeaders_OtherHeadersUnaffected pins that the filter removes only
// Content-Length. A loop that dropped more than it meant to would be a silent
// header-loss bug, and nothing else in the suite would notice.
func TestBuildHeaders_OtherHeadersUnaffected(t *testing.T) {
	req := &Request{
		Method:        "POST",
		Path:          "/",
		ContentLength: 3,
		BodyReader:    strings.NewReader("abc"),
		Headers: []conn.HeaderField{
			{Name: []byte("x-one"), Value: []byte("1")},
			{Name: []byte("Content-Length"), Value: []byte("999")},
			{Name: []byte("x-two"), Value: []byte("2")},
		},
	}
	var sp []conn.HeaderField

	out := buildHeaders(req, "x.test", "https", &sp)

	for _, want := range []string{"x-one", "x-two"} {
		found := false
		for i := range out {
			if string(out[i].Name) == want {
				found = true
			}
		}
		assert.Truef(t, found, "%q was dropped; the filter must remove only content-length", want)
	}
	assert.Equalf(t, []string{"3"}, contentLengths(out),
		"content-length = %v, want [\"3\"]", contentLengths(out))
}
