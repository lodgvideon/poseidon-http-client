package http1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smuggled is a complete, well-formed response an attacker appends after the
// real one. If the connection is pooled with these octets still buffered, the
// NEXT request parses them as its own status line and gets a response the server
// never sent for it, with err == nil.
const smuggled = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\npwn"

// drainToDone reads the body to completion, requiring every chunk read to
// succeed. It is the shared Act step of the pooling-verdict tests below.
func drainToDone(t *testing.T, ex interface {
	ReadBodyChunk([]byte) (int, bool, error)
}) {
	t.Helper()
	for {
		_, done, err := ex.ReadBodyChunk(make([]byte, 64))
		require.NoError(t, err, "ReadBodyChunk")
		if done {
			return
		}
	}
}

// TestConformance_RFC9112_Sec6_3_ExtraOctetsAfterResponse pins that a connection
// carrying octets left over after a complete response is not poolable, whatever
// framing delimited that response.
//
// RFC 9112 §6.3: "A client MUST NOT process, cache, or forward such extra data
// as a separate response, since such behavior would be vulnerable to cache
// poisoning." This client does not pipeline — one exchange per connection at a
// time — so at message completion there is by construction no outstanding
// request those octets could belong to.
//
// Before this the check existed only for the narrow case of a 204/304 whose head
// declared a body. Every other framing mode returned done=true without looking
// at the reader, so a well-formed response followed by attacker-chosen bytes went
// back to the idle set with the poison inside it.
func TestConformance_RFC9112_Sec6_3_ExtraOctetsAfterResponse(t *testing.T) {
	cases := []struct {
		name   string
		method string
		wire   string
	}{
		{"content-length body", "GET",
			"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHELLO" + smuggled},
		{"chunked body", "GET",
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n" + smuggled},
		{"chunked body with trailer section", "GET",
			"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\nx-sum: 1\r\n\r\n" + smuggled},
		{"empty body", "GET",
			"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" + smuggled},
		{"304 with nothing declared", "GET",
			"HTTP/1.1 304 Not Modified\r\n\r\n" + smuggled},
		// A 204 carrying an explicit zero Content-Length is deliberately NOT evicted
		// by the §6.3 rule 1 framing check: the field is illegal there but declares
		// no octets, so evicting on its presence would cost a connection on every
		// request to the endpoints that answer 204 that way. This defer is the
		// mechanism that decides it when octets ARE present — and the row used to
		// sit in the rule-1 table, where it read as evidence for a check that never
		// fired on it (#799).
		{"204 with an explicit zero Content-Length", "GET",
			"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n" + smuggled},
		// A HEAD response must carry no body; bytes on the wire after its head are
		// unsolicited even though Content-Length is legitimate there (§9.3.2).
		{"HEAD that actually sent a body", "HEAD",
			"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, tc.method, tc.wire)
			_, _, err := ex.ReadResponse(context.Background())
			require.NoError(t, err, "ReadResponse")

			drainToDone(t, ex)

			assert.False(t, ex.KeepAlive(),
				"KeepAlive() = true with octets still buffered after the response — "+
					"the connection would be pooled with a peer-chosen response inside it, and "+
					"the next request would parse those bytes as its own status line (RFC 9112 §6.3)")
		})
	}
}

// TestConformance_RFC9112_Sec6_3_CleanResponseStaysPoolable is the
// over-rejection guard: a response that ends exactly where its framing says,
// with nothing after it, must still be reusable. Evicting on every response
// would cost a connection per request.
func TestConformance_RFC9112_Sec6_3_CleanResponseStaysPoolable(t *testing.T) {
	cases := []struct {
		name   string
		method string
		wire   string
	}{
		{"content-length body", "GET", "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHELLO"},
		{"chunked body", "GET", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n"},
		{"empty body", "GET", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"},
		{"HEAD with Content-Length and no body", "HEAD", "HTTP/1.1 200 OK\r\nContent-Length: 38\r\n\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, tc.method, tc.wire)
			_, _, err := ex.ReadResponse(context.Background())
			require.NoError(t, err, "ReadResponse")

			drainToDone(t, ex)

			assert.True(t, ex.KeepAlive(), "KeepAlive() = false for a response with nothing left over, want true")
		})
	}
}

// TestConformance_RFC9112_Sec6_1_Http10TransferEncodingNotPoolable pins that an
// HTTP/1.0 response carrying Transfer-Encoding is not reused, even when it also
// says "Connection: keep-alive".
//
// RFC 9112 §6.1 makes the framing of an HTTP/1.0 message with Transfer-Encoding
// faulty — a 1.0 hop is not required to understand chunked — and requires the
// connection to be closed after processing it. The version seed defaults 1.0 to
// close, but a Connection: keep-alive field line flipped it back and nothing
// re-consulted the version, so this response was decoded and pooled.
func TestConformance_RFC9112_Sec6_1_Http10TransferEncodingNotPoolable(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n")
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainBody(t, ex)

	assert.Equalf(t, "HELLO", body,
		"body = %q, want %q — chunked is self-delimiting, so the caller still gets its bytes", body, "HELLO")
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true for an HTTP/1.0 response carrying Transfer-Encoding, want false — "+
			"RFC 9112 §6.1 makes that framing faulty and requires closing the connection after it")
}

// TestConformance_RFC9112_Sec6_1_Http11TransferEncodingStaysPoolable is the
// over-rejection guard: chunked on HTTP/1.1 is ordinary and must stay reusable.
func TestConformance_RFC9112_Sec6_1_Http11TransferEncodingStaysPoolable(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHELLO\r\n0\r\n\r\n")
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")

	body := drainBody(t, ex)

	assert.Equalf(t, "HELLO", body, "body = %q, want %q", body, "HELLO")
	assert.True(t, ex.KeepAlive(), "KeepAlive() = false for HTTP/1.1 chunked, want true")
}

// TestConformance_RFC9110_Sec7_6_1_ConnectionIsATokenList pins that the
// Connection field is matched as an RFC 9110 §7.6.1 #token list rather than by
// substring. A substring test let "x-keep-alive-probe" — a field this client does
// not otherwise honour — flip an HTTP/1.0 response, which defaults to close, back
// to poolable.
func TestConformance_RFC9110_Sec7_6_1_ConnectionIsATokenList(t *testing.T) {
	cases := []struct {
		name          string
		connection    string
		wantKeepAlive bool
	}{
		{"bogus option containing keep-alive", "x-keep-alive-probe", false},
		{"bogus option suffix", "keep-alive-probe", false},
		{"real keep-alive", "keep-alive", true},
		{"real keep-alive in a list", "foo, keep-alive", true},
		{"keep-alive with OWS", " keep-alive ", true},
		{"close wins over keep-alive", "close, keep-alive", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// HTTP/1.0 so the default is close: only a real keep-alive option may
			// flip it, which is exactly what the token-list rule decides.
			ex := wireExchange(t, "GET",
				"HTTP/1.0 200 OK\r\nConnection: "+tc.connection+"\r\nContent-Length: 5\r\n\r\nHELLO")
			_, _, err := ex.ReadResponse(context.Background())
			require.NoError(t, err, "ReadResponse")

			body := drainBody(t, ex)

			require.Equalf(t, "HELLO", body, "body = %q, want %q", body, "HELLO")
			got := ex.KeepAlive()
			assert.Equalf(t, tc.wantKeepAlive, got,
				"Connection: %q → KeepAlive() = %v, want %v", tc.connection, got, tc.wantKeepAlive)
		})
	}
}

// TestReadResponse_TruncatedHeaderBlockIsNotPoolable pins that a response whose
// header block ends without its blank line leaves the connection unusable.
//
// This is the exit ReadResponse's deferred condemn exists for, and nothing
// pinned it. The status line parses first, so persistence is already seeded from
// the version — keepAlive is true by the time the block is read. consumeHeaders
// then hits EOF mid-block and returns that error unchanged; readLine condemns
// only for a line longer than the buffer or a bare CR, and a truncated block is
// neither. So no site on this path clears keepAlive and the deferred condemn is
// the only thing that does.
//
// Without it a caller honouring KeepAlive()'s documented contract pools a socket
// whose stream position is indeterminate: the block was cut at a point nobody
// knows, so whatever the peer sends next is what a later request would parse as
// its own status line.
func TestReadResponse_TruncatedHeaderBlockIsNotPoolable(t *testing.T) {
	ex := wireExchange(t, "GET", "HTTP/1.1 200 OK\r\nX-Cut-Here: no blank line follows\r\n")

	_, _, err := ex.ReadResponse(context.Background())

	require.Error(t, err, "a header block ending without its blank line must report an error")
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true after a truncated header block, want false — the "+
			"block was cut at an unknown point, so the connection must not be reused")
}
