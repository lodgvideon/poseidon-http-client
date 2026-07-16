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
			if len(got) != 1 {
				t.Fatalf("content-length fields = %v, want exactly one — two disagreeing "+
					"values is the CL.CL desync (RFC 9112 §11.2), emitted by this library", got)
			}
			if got[0] != "10" {
				t.Errorf("content-length = %q, want %q (Request.ContentLength governs how "+
					"many body bytes are written, so it is the one that can be true)", got[0], "10")
			}
		})
	}
}

// TestBuildHeaders_CallerContentLengthKeptWithoutBody is the over-rejection
// guard: with no BodyReader there is no managed Content-Length to append, so a
// caller-supplied one must survive untouched. Stripping it would silently change
// requests that were already correct.
func TestBuildHeaders_CallerContentLengthKeptWithoutBody(t *testing.T) {
	req := &Request{
		Method:  "POST",
		Path:    "/",
		Headers: []conn.HeaderField{{Name: []byte("Content-Length"), Value: []byte("0")}},
	}
	var sp []conn.HeaderField
	got := contentLengths(buildHeaders(req, "x.test", "https", &sp))
	if len(got) != 1 || got[0] != "0" {
		t.Errorf("content-length fields = %v, want exactly [\"0\"] — with no BodyReader "+
			"nothing managed is appended, so the caller's field must pass through", got)
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
		if !found {
			t.Errorf("%q was dropped; the filter must remove only content-length", want)
		}
	}
	if got := contentLengths(out); len(got) != 1 || got[0] != "3" {
		t.Errorf("content-length = %v, want [\"3\"]", got)
	}
}
