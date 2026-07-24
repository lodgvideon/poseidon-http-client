package client

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func hf(name, value string) conn.HeaderField {
	return conn.HeaderField{Name: []byte(name), Value: []byte(value)}
}

// TestConformance_RFC9110_Sec10_1_1_ExpectIdentifiedByToken pins that the
// 100-continue guard identifies the expectation the way a recipient does.
//
// §10.1.1: "A client MUST NOT generate a 100-continue expectation in a request
// that does not include content", over the grammar `expectation = token [ "="
// ( token / quoted-string ) parameters ]`. The guard compared whole list
// members, so anything carrying a parameter walked past it — while a server
// reading the token up to its delimiter still sees a 100-continue expectation
// and still waits for content this request will never send.
func TestConformance_RFC9110_Sec10_1_1_ExpectIdentifiedByToken(t *testing.T) {
	refused := []string{
		"100-continue",
		"100-continue;x=1",
		"100-Continue;x=1",
		"100-continue=y",
		" 100-continue ; a=b",
		"other, 100-continue;x=1",
		"100-continue;x=1, other",
	}
	for _, v := range refused {
		t.Run("refuse "+v, func(t *testing.T) {
			r := &Request{Method: "GET", Path: "/", Headers: []conn.HeaderField{hf("expect", v)}}
			err := validateRequest(r)
			if err == nil {
				t.Fatalf("Expect: %q on a bodyless request accepted; a recipient reads the "+
					"leading token and waits for content that never comes", v)
			}
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}

	// The control: the rule is about the 100-continue expectation specifically,
	// and about bodyless requests specifically. A guard that refused any Expect,
	// or refused regardless of content, would pass the cases above for the wrong
	// reason.
	accepted := []struct {
		name string
		r    *Request
	}{
		{"a different expectation", &Request{Method: "GET", Path: "/",
			Headers: []conn.HeaderField{hf("expect", "other-thing;x=1")}}},
		{"a token that merely starts the same", &Request{Method: "GET", Path: "/",
			Headers: []conn.HeaderField{hf("expect", "100-continue-ish")}}},
		{"100-continue with content", &Request{Method: "POST", Path: "/", Body: []byte("hi"),
			Headers: []conn.HeaderField{hf("expect", "100-continue")}}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			if err := validateRequest(tc.r); err != nil {
				t.Errorf("validateRequest = %v, want nil", err)
			}
		})
	}
}

// TestConformance_RFC9110_Sec8_6_UnverifiableContentLengthRefused pins that a
// caller-supplied Content-Length only reaches the wire where this client can
// hold it to account.
//
// §8.6: "Because Content-Length is used for message delimitation in HTTP/1.1,
// its field value can impact how the message is parsed by downstream
// recipients", and a mismatch "might cause a security failure due to request
// smuggling or response splitting".
//
// HTTP/1.1 already refused these shapes in http1.WriteRequest; HTTP/2 and
// HTTP/3 emitted the caller's number unchecked, and an h2→h1.1 gateway rewrites
// it into exactly the framing field §8.6 warns about. The gate is shared, so
// closing it here closes it for all three.
func TestConformance_RFC9110_Sec8_6_UnverifiableContentLengthRefused(t *testing.T) {
	refused := []struct {
		name string
		r    *Request
	}{
		{"streaming body with no declared length", &Request{Method: "POST", Path: "/",
			BodyReader: strings.NewReader("hello"),
			Headers:    []conn.HeaderField{hf("content-length", "5")}}},
		{"streaming body, canonical spelling", &Request{Method: "POST", Path: "/",
			BodyReader: strings.NewReader("hello"),
			Headers:    []conn.HeaderField{hf("Content-Length", "5")}}},
		{"no body at all", &Request{Method: "POST", Path: "/",
			Headers: []conn.HeaderField{hf("content-length", "5")}}},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			err := validateRequest(tc.r)
			if err == nil {
				t.Fatal("accepted; the value goes to the wire with nothing reconciling it " +
					"against the octets actually sent")
			}
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}

	// The control: every shape whose length this client owns must still work,
	// including the caller redundantly spelling it out. A guard that refused any
	// caller Content-Length would break ordinary requests.
	accepted := []struct {
		name string
		r    *Request
	}{
		{"buffered body", &Request{Method: "POST", Path: "/", Body: []byte("hello"),
			Headers: []conn.HeaderField{hf("content-length", "5")}}},
		{"streaming body with declared length", &Request{Method: "POST", Path: "/",
			BodyReader: strings.NewReader("hello"), ContentLength: 5,
			Headers:    []conn.HeaderField{hf("content-length", "5")}}},
		{"compressed body", &Request{Method: "POST", Path: "/", Body: []byte("hello"),
			CompressBody: EncodingGzip,
			Headers:      []conn.HeaderField{hf("content-length", "5")}}},
		{"no Content-Length header at all", &Request{Method: "POST", Path: "/",
			BodyReader: strings.NewReader("hello")}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			if err := validateRequest(tc.r); err != nil {
				t.Errorf("validateRequest = %v, want nil", err)
			}
		})
	}
}

// TestConformance_RFC9110_Sec5_1_AcceptEncodingDedupFoldsCase pins that the
// caller's own Accept-Encoding suppresses this client's, however they spelled
// it. RFC 9110 §5.1 makes field names case-insensitive, so an exact-match
// comparison sent a second Accept-Encoding line beside the caller's.
func TestConformance_RFC9110_Sec5_1_AcceptEncodingDedupFoldsCase(t *testing.T) {
	for _, spelling := range []string{"accept-encoding", "Accept-Encoding", "ACCEPT-ENCODING"} {
		t.Run(spelling, func(t *testing.T) {
			req := &Request{Method: "GET", Path: "/",
				Headers: []conn.HeaderField{hf(spelling, "br")}}
			var sp []conn.HeaderField
			out := buildHeaders(req, "example.com", "https", &sp)

			n := 0
			for i := range out {
				if bytes.EqualFold(out[i].Name, hdrAcceptEncoding) {
					n++
				}
			}
			if n != 1 {
				t.Errorf("%d accept-encoding field lines, want 1 — the caller asked for one "+
					"and this client appended its own beside it", n)
			}
		})
	}
}

// errAfterN is an io.Reader that yields n octets and then fails, standing in for
// a body source that dies mid-upload.
type errAfterN struct {
	n int
}

func (r *errAfterN) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, errors.New("body source failed")
	}
	k := len(p)
	if k > r.n {
		k = r.n
	}
	for i := 0; i < k; i++ {
		p[i] = 'x'
	}
	r.n -= k
	return k, nil
}

var _ io.Reader = (*errAfterN)(nil)
