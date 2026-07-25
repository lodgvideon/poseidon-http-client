package client

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC9113_Sec8_2_1_LowercasesCallerHeaderNames pins RFC 9113
// §8.2.1: "Field names MUST be converted to lowercase when constructing an HTTP/2
// message." A caller-supplied Request.Headers name with uppercase is folded to
// lowercase on the wire rather than emitted verbatim as a malformed field.
func TestConformance_RFC9113_Sec8_2_1_LowercasesCallerHeaderNames(t *testing.T) {
	req := &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/",
		Headers: []conn.HeaderField{
			{Name: []byte("Content-Type"), Value: []byte("text/plain")},
			{Name: []byte("X-Custom-Header"), Value: []byte("v")},
			{Name: []byte("already-lower"), Value: []byte("v")},
		},
	}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	hdrs := buildHeaders(req, "default", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	for _, h := range hdrs {
		for _, b := range h.Name {
			if b >= 'A' && b <= 'Z' {
				t.Errorf("field name %q emitted with an uppercase letter (RFC 9113 §8.2.1)", h.Name)
				break
			}
		}
	}
	want := map[string]bool{"content-type": false, "x-custom-header": false, "already-lower": false}
	for _, h := range hdrs {
		if _, ok := want[string(h.Name)]; ok {
			want[string(h.Name)] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("expected lowercased header %q not present on the wire", name)
		}
	}
}

// TestLowerHeaderName covers the fold helper directly, including the no-alloc
// fast path for an already-lowercase name.
func TestLowerHeaderName(t *testing.T) {
	in := []byte("x-already-lower")
	if got := lowerHeaderName(in); &got[0] != &in[0] {
		t.Error("lowerHeaderName copied an already-lowercase name instead of returning it unchanged")
	}
	if got := string(lowerHeaderName([]byte("Content-Type"))); got != "content-type" {
		t.Errorf("lowerHeaderName(Content-Type) = %q, want content-type", got)
	}
	if got := string(lowerHeaderName([]byte("X-ÀB"))); got != "x-Àb" {
		t.Errorf("lowerHeaderName folds only ASCII A-Z, got %q", got)
	}
}
