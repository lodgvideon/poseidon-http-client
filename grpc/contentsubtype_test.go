package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Options.ContentSubtype closes an asymmetry: validContentType has always
// accepted a subtype FROM the server, while the request content-type was a
// constant. A JSON or custom-codec client could not say what its bytes were.

func subtypeHeaders(t *testing.T, subtype string) []conn.HeaderField {
	t.Helper()
	cc := newClientConn(nil, Options{Authority: "example.com", ContentSubtype: subtype}.defaulted(), false)
	sc := headerScratchPool.Get().(*headerScratch)
	defer putHeaderScratch(sc)
	return append([]conn.HeaderField(nil), cc.buildHeaders(context.Background(), "/t.S/M", nil, nil, sc)...)
}

// TestContentSubtype_RendersOntoTheWire is the point of the option.
func TestContentSubtype_RendersOntoTheWire(t *testing.T) {
	cases := []struct{ subtype, want string }{
		{"", "application/grpc"},
		{"proto", "application/grpc+proto"},
		{"json", "application/grpc+json"},
		{"vnd.acme.v2", "application/grpc+vnd.acme.v2"},
	}

	for _, tc := range cases {
		got, ok := hdrValue(subtypeHeaders(t, tc.subtype), "content-type")

		require.Truef(t, ok, "subtype %q: no content-type in the header block", tc.subtype)
		assert.Equalf(t, tc.want, got, "subtype %q rendered %q, want %q", tc.subtype, got, tc.want)
	}
}

// TestContentSubtype_RoundTripsThroughTheReceiveCheck pins the asymmetry this
// closes: whatever the client can now send, the receive side must accept. A
// subtype that made validContentType reject our own request would be worse than
// not sending one.
func TestContentSubtype_RoundTripsThroughTheReceiveCheck(t *testing.T) {
	subtypes := []string{"", "proto", "json", "vnd.acme.v2"}

	for _, subtype := range subtypes {
		ct := string(contentTypeFor(subtype))
		fields := []conn.HeaderField{{Name: []byte("content-type"), Value: []byte(ct)}}

		assert.Truef(t, validContentType(fields),
			"the client would send %q, which its own receive check rejects", ct)
	}
}

// TestContentSubtype_RejectsInjection is the security gate. The subtype reaches
// a header value, and neither conn nor hpack validates outbound fields, so this
// is the only thing between a caller's string and the wire.
func TestContentSubtype_RejectsInjection(t *testing.T) {
	bad := []string{
		"json\r\nx-evil: 1", // request splitting
		"json\nx-evil: 1",
		"json\rx",
		"json\x00",
		"json x",   // space is not a tchar
		"json;q=1", // separators are not tchars
		"json/2",
		"json\"x\"",
		"json@x",
	}

	for _, s := range bad {
		err := validContentSubtype(s)

		assert.ErrorIsf(t, err, ErrInvalidMetadata,
			"validContentSubtype(%q) = %v, want ErrInvalidMetadata", s, err)
	}

	// And the rejection happens at construction, not silently at send time.
	_, err := NewClientConn(&conn.Conn{}, Options{Authority: "example.com", ContentSubtype: "json\r\nevil: 1"})

	assert.ErrorIsf(t, err, ErrInvalidMetadata,
		"NewClientConn with an injecting subtype = %v, want ErrInvalidMetadata", err)
}

// TestContentSubtype_AcceptsEveryTokenChar pins the accept side of the grammar,
// so a future tightening that broke a legal custom subtype shows up here.
func TestContentSubtype_AcceptsEveryTokenChar(t *testing.T) {
	const tchars = "abcXYZ019!#$%&'*+-.^_`|~"

	err := validContentSubtype(tchars)

	assert.NoErrorf(t, err, "validContentSubtype(%q) = %v, want nil — every one of these is a tchar",
		tchars, err)
}

// TestContentSubtype_DefaultAllocatesNothingNew pins that the common case still
// hands out the shared constant rather than building a string per connection.
func TestContentSubtype_DefaultAllocatesNothingNew(t *testing.T) {
	got := contentTypeFor("")

	require.NotEmpty(t, got, "contentTypeFor(\"\") returned an empty slice")
	assert.Same(t, &valApplicationGRPC[0], &got[0],
		"the empty subtype built a fresh buffer instead of reusing the constant")
	assert.NotContainsf(t, string(got), "+", "the empty subtype rendered %q", got)
}
