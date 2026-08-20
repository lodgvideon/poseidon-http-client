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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bodylessKeepAlive drives one response head — and NOTHING after it — and
// reports whether the socket stayed poolable after rule 1 ended the body at the
// blank line.
//
// The bare head is the whole point of the helper, and it used to append a second
// complete response so that the fixture also demonstrated the attack. That made
// every row here double-satisfied: leftover octets condemn a connection on their
// own, through ReadBodyChunk's unsolicited-residue defer, so the table could not
// distinguish "rule 1 evicted this" from "there was rubbish on the socket". One
// row was decided ENTIRELY by the residue defer and has moved to the test named
// for that mechanism (#799). With nothing on the wire after the head,
// checkBodylessStatusFraming is the only thing that can produce a false verdict.
func bodylessKeepAlive(t *testing.T, head string) bool {
	t.Helper()
	ex := wireExchange(t, "GET", head)
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	n, done, err := ex.ReadBodyChunk(make([]byte, 64))

	require.Truef(t, n == 0 && done && err == nil,
		"rule 1 broken: n=%d done=%v err=%v — a 204/304 ends at the blank line", n, done, err)
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
	// No "Content-Length: 0" row. That head declares NO octets, so this check
	// deliberately does not fire on it (see the over-rejection guard below), and
	// the row that used to sit here passed only because the fixture appended a
	// second response for the residue defer to find (#799).
	for _, tc := range []struct{ name, head string }{
		{"204_content_length", "HTTP/1.1 204 No Content\r\nContent-Length: 38\r\n\r\n"},
		{"204_transfer_encoding", "HTTP/1.1 204 No Content\r\nTransfer-Encoding: chunked\r\n\r\n"},
		{"304_transfer_encoding", "HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: chunked\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolable := bodylessKeepAlive(t, tc.head)

			assert.False(t, poolable,
				"KeepAlive() = true — the head declared a body that §6.3 rule 1 "+
					"says must not be read, so the declared bytes stay on the socket. "+
					"Pooling it makes the next request read them as its status line, which "+
					"§6.3 forbids: \"A client MUST NOT process, cache, or forward such extra "+
					"data as a separate response, since such behavior would be vulnerable to "+
					"cache poisoning.\"")
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
// The wire carries the 304 head and nothing else, which is what an origin
// actually sends: the Content-Length describes the representation, so no body
// follows it. (Driving it through bodylessKeepAlive would append a second
// response, and leftover octets are unsolicited data that costs the connection
// on their own — see TestConformance_RFC9112_Sec6_3_ExtraOctetsAfterResponse.)
func TestConformance_RFC9110_Sec8_6_ContentLengthOn304StaysPoolable(t *testing.T) {
	ex := wireExchange(t, "GET", "HTTP/1.1 304 Not Modified\r\nContent-Length: 38\r\n\r\n")
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	_, done, err := ex.ReadBodyChunk(make([]byte, 64))

	require.Truef(t, done && err == nil, "304 should end immediately: done=%v err=%v", done, err)
	assert.True(t, ex.KeepAlive(),
		"KeepAlive() = false for a 304 with Content-Length, want true — §8.6 "+
			"explicitly permits it on a conditional GET, where it describes the "+
			"representation rather than a body")
}

// TestConformance_RFC9110_Sec8_6_ZeroContentLengthOn204StaysPoolable is the
// over-rejection guard for the branch above, and the one the suite was missing
// entirely (#799).
//
// The eviction is keyed on the VALUE, not on presence, and that is a decision
// this repo made and then reverted TO. §8.6's "A server MUST NOT send a
// Content-Length header field in any response with a status code of 1xx
// (Informational) or 204 (No Content)" is what makes the field illegal here, but
// the danger the branch exists for is body-shaped octets left unread on the
// socket — and a Content-Length of 0 describes none. Evicting on presence alone
// cost a connection per request against the many endpoints that answer 204 with
// an explicit zero (generate_204 and friends): a self-inflicted outage in
// exchange for no safety.
//
// Nothing held that down. Reinstating the reverted behaviour — condemning on
// `ex.respTE || ex.respCL` — left the whole package green, so the regression
// could come back for free. The wire carries the head and nothing else, exactly
// as the 304 guard below does, so leftover octets cannot be what keeps it
// poolable.
func TestConformance_RFC9110_Sec8_6_ZeroContentLengthOn204StaysPoolable(t *testing.T) {
	ex := wireExchange(t, "GET", "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	_, done, err := ex.ReadBodyChunk(make([]byte, 64))

	require.Truef(t, done && err == nil, "204 should end immediately: done=%v err=%v", done, err)
	assert.True(t, ex.KeepAlive(),
		"KeepAlive() = false for a 204 carrying Content-Length: 0, want true. The "+
			"field is illegal there, but it declares no octets, so nothing is left on "+
			"the socket and there is nothing to be unsafe about. Evicting on the "+
			"field's mere presence discards a healthy connection on every request to "+
			"the endpoints that answer 204 this way")
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
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	_, done, err := ex.ReadBodyChunk(make([]byte, 64))

	require.Truef(t, done && err == nil, "HEAD response should end immediately: done=%v err=%v", done, err)
	assert.True(t, ex.KeepAlive(),
		"KeepAlive() = false after a HEAD with Content-Length, want true — "+
			"§9.3.2 makes that field normal on a HEAD response; evicting would cost a "+
			"connection after every HEAD")
}
