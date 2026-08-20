package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestConformance_RFC7540_Sec10_3_OutgoingRequestValueInjection_Rejected pins
// that the shared request validator refuses to emit a request field value
// carrying CR, LF or NUL. RFC 7540 §10.3: "carriage return (CR, ASCII 0xd), line
// feed (LF, ASCII 0xa), and the zero character (NUL, ASCII 0x0) might be exploited
// by an attacker if they are translated verbatim." §8.1.2.6 makes such a message
// malformed.
//
// HPACK length-prefixes a value, so these bytes cannot split the HTTP/2 wire
// itself — which is why this was easy to miss and worth not missing: the damage
// is at an HTTP/2->HTTP/1.1 downgrading intermediary, where CR and LF are the
// delimiters. http1.WriteRequest already refused these on its own wire; the H2
// and H3 send paths, which share validateRequest, did not, so a value like
// "a\r\nX-Injected: 1" reached the encoder verbatim. validateRequest is the one
// gate every Do / DoStream passes through before a byte leaves.
//
// Every caller-controlled value is covered: the four synthesized pseudo-headers,
// :protocol, regular header values and trailer values.
func TestConformance_RFC7540_Sec10_3_OutgoingRequestValueInjection_Rejected(t *testing.T) {
	base := func() *Request { return &Request{Method: "GET", Path: "/"} }

	cases := []struct {
		name string
		mut  func(*Request)
	}{
		{"header value CR", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-evil"), Value: []byte("a\rb")}}
		}},
		{"header value LF", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-evil"), Value: []byte("a\nb")}}
		}},
		{"header value NUL", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-evil"), Value: []byte("a\x00b")}}
		}},
		{"header value CRLF injects a field", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-evil"), Value: []byte("a\r\nX-Injected: 1")}}
		}},
		{"authority CRLF", func(r *Request) { r.Authority = "example.com\r\nX-Injected: 1" }},
		{"scheme CRLF", func(r *Request) { r.Scheme = "https\r\nX: 1" }},
		{"method NUL", func(r *Request) { r.Method = "GE\x00T" }},
		{"path NUL", func(r *Request) { r.Path = "/a\x00b" }},
		{"protocol CRLF on CONNECT", func(r *Request) {
			r.Method = "CONNECT"
			r.Protocol = "websocket\r\nX: 1"
		}},
		{"trailer value CRLF", func(r *Request) {
			r.Trailers = []conn.HeaderField{{Name: []byte("x-checksum"), Value: []byte("a\r\nX: 1")}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := base()
			tc.mut(req)

			err := validateRequest(req)

			require.ErrorIs(t, err, ErrInvalidRequest,
				"a CR/LF/NUL request value is a splitting vector through a downgrading "+
					"intermediary (RFC 7540 §10.3) and must never reach the encoder")
		})
	}
}

// TestConformance_RFC7540_Sec10_3_LegalOutgoingRequestValuesAccepted is the
// over-rejection guard. §10.3 forbids exactly CR, LF and NUL; a client that
// rejected more would break ordinary requests. SP and HTAB inside a value,
// obs-text (high-bit bytes, RFC 9110 §5.5), and an empty value are all legal
// field content and must pass.
func TestConformance_RFC7540_Sec10_3_LegalOutgoingRequestValuesAccepted(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Request)
	}{
		{"space in value", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("user-agent"), Value: []byte("poseidon 1.0")}}
		}},
		{"htab in value", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-tab"), Value: []byte("a\tb")}}
		}},
		{"high-bit obs-text in value", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-obs"), Value: []byte{0x80, 0xff}}}
		}},
		{"empty value", func(r *Request) {
			r.Headers = []conn.HeaderField{{Name: []byte("x-empty"), Value: []byte("")}}
		}},
		{"ordinary authority and scheme", func(r *Request) {
			r.Authority = "example.com:8443"
			r.Scheme = "https"
		}},
		{"legal trailer value", func(r *Request) {
			r.Trailers = []conn.HeaderField{{Name: []byte("x-checksum"), Value: []byte("deadbeef")}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{Method: "GET", Path: "/"}
			tc.mut(req)

			err := validateRequest(req)

			require.NoError(t, err,
				"§10.3 forbids only CR, LF and NUL, so this legal value must be accepted")
		})
	}
}
