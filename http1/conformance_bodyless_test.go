package http1_test

// A status that RFC 9112 §6.3 rule 1 makes bodyless, whose head declares a body.
//
// Rule 1 is honoured correctly — a 204/304 ends at the blank line whatever the
// fields say — and that is what makes this dangerous. The declared bytes are
// never read, so they stay on the socket, and the connection is pooled. The next
// request parses attacker-chosen bytes as its status line; because the attacker
// chose them they can be a complete well-formed response, so request N+1 gets a
// response the server never sent for it, with err=nil.
//
// §6.3 forbids exactly that: "A client MUST NOT process, cache, or forward such
// extra data as a separate response, since such behavior would be vulnerable to
// cache poisoning."
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"
)

// bodylessKeepAlive drives one response head and reports whether the socket
// stayed poolable after rule 1 ended the body at the blank line.
func bodylessKeepAlive(t *testing.T, head string) bool {
	t.Helper()
	// The bytes after the head are a complete response: what a pooled socket
	// would hand to the next request.
	ex := wireExchange(t, "GET", head+"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\npwn")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if n, done, err := ex.ReadBodyChunk(make([]byte, 64)); n != 0 || !done || err != nil {
		t.Fatalf("rule 1 broken: n=%d done=%v err=%v — a 204/304 ends at the blank line", n, done, err)
	}
	return ex.KeepAlive()
}

// TestConformance_RFC9112_Sec6_3_Rule1_BodylessStatusDeclaringBodyNotPooled pins
// that a head declaring a body the status forbids costs the connection.
//
// The two fields on a 204 are both MUST NOTs — RFC 9110 §8.6: "A server MUST NOT
// send a Content-Length header field in any response with a status code of 1xx
// (Informational) or 204 (No Content)", and RFC 9112 §6.1 says the same of
// Transfer-Encoding. Their presence means the peer is broken or hostile, and
// body-shaped bytes may be on the socket.
func TestConformance_RFC9112_Sec6_3_Rule1_BodylessStatusDeclaringBodyNotPooled(t *testing.T) {
	for _, tc := range []struct{ name, head string }{
		{"204_content_length", "HTTP/1.1 204 No Content\r\nContent-Length: 38\r\n\r\n"},
		{"204_content_length_zero", "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"},
		{"204_transfer_encoding", "HTTP/1.1 204 No Content\r\nTransfer-Encoding: chunked\r\n\r\n"},
		{"304_transfer_encoding", "HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: chunked\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if bodylessKeepAlive(t, tc.head) {
				t.Errorf("KeepAlive() = true — the head declared a body that §6.3 rule 1 " +
					"says must not be read, so the declared bytes stay on the socket. " +
					"Pooling it makes the next request read them as its status line, which " +
					"§6.3 forbids: \"A client MUST NOT process, cache, or forward such extra " +
					"data as a separate response, since such behavior would be vulnerable to " +
					"cache poisoning.\"")
			}
		})
	}
}

// TestConformance_RFC9110_Sec8_6_ContentLengthOn304StaysPoolable is the
// over-rejection guard, and the reason this fix is a switch rather than one
// condition.
//
// RFC 9110 §8.6: "A server MAY send a Content-Length header field in a 304 (Not
// Modified) response to a conditional GET request". It describes the selected
// representation, not a message body, so no bytes follow it. Origin servers send
// it routinely — evicting on it would cost a connection per conditional GET,
// which is a worse failure than the one being prevented.
func TestConformance_RFC9110_Sec8_6_ContentLengthOn304StaysPoolable(t *testing.T) {
	if !bodylessKeepAlive(t, "HTTP/1.1 304 Not Modified\r\nContent-Length: 38\r\n\r\n") {
		t.Error("KeepAlive() = false for a 304 with Content-Length, want true — §8.6 " +
			"explicitly permits it on a conditional GET, where it describes the " +
			"representation rather than a body")
	}
}

// TestConformance_RFC9110_Sec9_3_2_ContentLengthOnHeadStaysPoolable is the other
// over-rejection guard.
//
// §6.3 rule 1 makes a HEAD response bodyless too, so the naive reading of this
// fix would evict on it. But RFC 9110 §9.3.2 — "The server SHOULD send the same
// header fields in response to a HEAD request as it would have sent if the
// request method had been GET" — makes Content-Length normal there, describing
// the body a GET would have returned. Evicting would discard a pooled connection
// after every HEAD.
func TestConformance_RFC9110_Sec9_3_2_ContentLengthOnHeadStaysPoolable(t *testing.T) {
	ex := wireExchange(t, "HEAD", "HTTP/1.1 200 OK\r\nContent-Length: 38\r\n\r\n")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if _, done, err := ex.ReadBodyChunk(make([]byte, 64)); !done || err != nil {
		t.Fatalf("HEAD response should end immediately: done=%v err=%v", done, err)
	}
	if !ex.KeepAlive() {
		t.Error("KeepAlive() = false after a HEAD with Content-Length, want true — " +
			"§9.3.2 makes that field normal on a HEAD response; evicting would cost a " +
			"connection after every HEAD")
	}
}
