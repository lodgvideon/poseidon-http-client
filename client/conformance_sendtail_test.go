package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

			require.Errorf(t, err, "Expect: %q on a bodyless request accepted; a recipient reads the "+
				"leading token and waits for content that never comes", v)
			assert.Truef(t, errors.Is(err, ErrInvalidRequest),
				"error = %v, want ErrInvalidRequest", err)
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
			err := validateRequest(tc.r)

			assert.NoErrorf(t, err, "validateRequest = %v, want nil", err)
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

			require.Error(t, err, "accepted; the value goes to the wire with nothing reconciling it "+
				"against the octets actually sent")
			assert.Truef(t, errors.Is(err, ErrInvalidRequest),
				"error = %v, want ErrInvalidRequest", err)
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
			Headers: []conn.HeaderField{hf("content-length", "5")}}},
		{"compressed body", &Request{Method: "POST", Path: "/", Body: []byte("hello"),
			CompressBody: EncodingGzip,
			Headers:      []conn.HeaderField{hf("content-length", "5")}}},
		{"no Content-Length header at all", &Request{Method: "POST", Path: "/",
			BodyReader: strings.NewReader("hello")}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			err := validateRequest(tc.r)

			assert.NoErrorf(t, err, "validateRequest = %v, want nil", err)
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
			assert.Equalf(t, 1, n, "%d accept-encoding field lines, want 1 — the caller asked for one "+
				"and this client appended its own beside it", n)
		})
	}
}

// errBodySource is the sentinel errAfterN fails with, so a test can assert the
// caller got THE READER'S OWN error rather than merely "an error". The two
// existing read-error tests in coverage_test.go assert only err != nil, which a
// framing failure, a reset or a deadline satisfies just as well.
var errBodySource = errors.New("body source failed")

// errAfterN is an io.Reader that yields n octets and then fails, standing in for
// a body source that dies mid-upload.
type errAfterN struct {
	n int
}

func (r *errAfterN) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, errBodySource
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

// TestConformance_RFC9113_Sec8_1_BodySourceFailureAtTheSendTail is the test
// errAfterN was staged for and never got (#863).
//
// This file is named for the request SEND TAIL — where the body finishes and
// trailers go out — and held no send-tail test at all; errAfterN sat unreferenced,
// kept alive from `unused` only by its own io.Reader assertion.
//
// The uncovered case is a body source that dies where the trailer section is
// about to be written. The two nearby tests do not reach it:
// TestClient_Do_WriteBodyReader_ReadError_AfterBytes asserts only err != nil, and
// TestConformance_RFC9113_Sec8_1_BenignResetDuringTrailers covers a PEER reset
// there, not a local source failure. Two properties, both unpinned:
//
//  1. the caller gets the reader's own error, not an opaque framing one — a load
//     generator distinguishes "my body source broke" from "the peer went away"
//     on exactly this, and they have different operational answers;
//  2. the connection is not left half-open. A send abandoned mid-body must reset
//     its own stream, so the NEXT request on the same connection still works.
func TestConformance_RFC9113_Sec8_1_BodySourceFailureAtTheSendTail(t *testing.T) {
	cases := []struct {
		name     string
		yield    int
		trailers []conn.HeaderField
	}{
		{"dies before any octet, with trailers pending", 0,
			[]conn.HeaderField{hf("x-checksum", "deadbeef")}},
		{"dies mid-body, with trailers pending", 4096,
			[]conn.HeaderField{hf("x-checksum", "deadbeef")}},
		{"dies mid-body, no trailer section", 4096, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "ok")
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			defer srv.Close()
			cl, err := NewClient(ClientOptions{
				Addr: srv.Listener.Addr().String(),
				ConnOpts: conn.ConnOptions{
					Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
				},
			})
			require.NoError(t, err, "NewClient")
			defer func() { _ = cl.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var resp Response

			err = cl.Do(ctx, &Request{
				Method:        "POST",
				Path:          "/upload",
				BodyReader:    &errAfterN{n: c.yield},
				ContentLength: int64(c.yield) + 1<<20, // more than the source will ever give
				Trailers:      c.trailers,
			}, &resp)

			require.Error(t, err, "a request whose body source failed reported success")
			assert.ErrorIsf(t, err, errBodySource,
				"Do returned %v; the caller must be handed the body source's OWN error, or "+
					"a broken file handle is indistinguishable from a peer that went away "+
					"and the operator retries the wrong thing", err)
			// The stream must have been cleaned up rather than left half-open:
			// the next request on the same connection still completes.
			var next Response
			nerr := cl.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &next)
			require.NoErrorf(t, nerr,
				"the request after an abandoned upload failed with %v — the failed send left "+
					"its stream open, so the connection is poisoned for every later caller", nerr)
			assert.Equalf(t, 200, next.Status, "follow-up status = %d, want 200", next.Status)
		})
	}
}
