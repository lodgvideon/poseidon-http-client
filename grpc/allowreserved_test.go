package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Options.AllowReservedMetadata exempts a name from the "grpc- is reserved"
// check and from NOTHING else. The value of the feature is entirely in that
// narrowness: an allowlist that also opened content-type or grpc-timeout would
// let a caller break the RPC framing it is trying to trace.

// allowConn dials the mock peer with an allowlist. Local rather than a change
// to dialMockPeer, whose third parameter is a write counter and whose call
// sites are many.
func allowConn(t *testing.T, allow ...string) *ClientConn {
	t.Helper()
	p := newMockGRPCPeer(t)
	cc, err := Dial(context.Background(), p.addr(), Options{
		Conn:                  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		Scheme:                "http",
		Authority:             "bench.local",
		AllowReservedMetadata: allow,
	})
	require.NoError(t, err, "Dial")
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// TestAllowReserved_DefaultStillRefusesTheNamespace pins the protective default:
// with no allowlist, grpc- is refused exactly as before.
func TestAllowReserved_DefaultStillRefusesTheNamespace(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	md := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("x")}}

	_, streamErr := cc.NewStream(context.Background(), "/t.S/M", md)
	_, appendErr := AppendMetadata(nil, "grpc-trace-bin", []byte("x"))

	assert.ErrorIsf(t, streamErr, ErrReservedMetadata,
		"grpc-trace-bin with no allowlist = %v, want ErrReservedMetadata", streamErr)
	assert.ErrorIsf(t, appendErr, ErrReservedMetadata,
		"package AppendMetadata = %v, want ErrReservedMetadata — it cannot see a "+
			"connection's allowlist and must stay strict", appendErr)
}

// TestAllowReserved_ExemptsOnlyTheListedName is the point of the feature, and
// its bound: the listed name goes through, a different grpc- name does not.
func TestAllowReserved_ExemptsOnlyTheListedName(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin")
	ctx := context.Background()
	listed := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("dHJhY2U=")}}
	notListed := []conn.HeaderField{{Name: []byte("grpc-tags-bin"), Value: []byte("dA==")}}

	s, listedErr := cc.NewStream(ctx, "/t.S/M", listed)
	_, notListedErr := cc.NewStream(ctx, "/t.S/M", notListed)

	require.NoError(t, listedErr, "the allowlisted name was refused")
	defer func() { _ = s.Close() }()
	assert.ErrorIsf(t, notListedErr, ErrReservedMetadata,
		"a grpc- name that is NOT listed = %v, want ErrReservedMetadata", notListedErr)
}

// TestAllowReserved_CannotForgeTransportHeaders is the security bound. Every one
// of these is refused whatever the allowlist says, because the pseudo-header and
// reservedKeys gates run before the namespace check.
func TestAllowReserved_CannotForgeTransportHeaders(t *testing.T) {
	// Ask for the moon: list every header the transport owns.
	cc := allowConn(t,
		"content-type", "te", "grpc-timeout", "grpc-encoding", "grpc-accept-encoding",
		"user-agent", "connection", "host", ":method", ":path",
	)
	ctx := context.Background()
	transportOwned := []string{
		"content-type", "te", "grpc-timeout", "grpc-encoding", "grpc-accept-encoding",
		"user-agent", "connection", "keep-alive", "proxy-connection",
		"transfer-encoding", "upgrade", "host",
	}
	// Pseudo-headers are refused one gate EARLIER, by name syntax: a colon is not
	// a legal character in a field name, so validMetadataName rejects them before
	// checkMetadataKey ever runs. Worth pinning as its own case rather than
	// folding into the above — the two sentinels are not interchangeable, and a
	// future reshuffle that made a pseudo-header reach the allowlist would show
	// up here as a changed error rather than as silence.
	pseudoHeaders := []string{":method", ":path", ":authority", ":scheme"}

	// Transport-owned names: refused by the reservedKeys gate.
	for _, name := range transportOwned {
		md := []conn.HeaderField{{Name: []byte(name), Value: []byte("v")}}

		_, err := cc.NewStream(ctx, "/t.S/M", md)

		assert.ErrorIsf(t, err, ErrReservedMetadata,
			"%q was accepted because it appeared in the allowlist: %v — the "+
				"exemption must apply to the grpc- prefix check ONLY", name, err)
	}
	for _, name := range pseudoHeaders {
		md := []conn.HeaderField{{Name: []byte(name), Value: []byte("v")}}

		_, err := cc.NewStream(ctx, "/t.S/M", md)

		assert.ErrorIsf(t, err, ErrInvalidMetadata,
			"%q = %v, want ErrInvalidMetadata from the name-syntax gate", name, err)
		assert.Errorf(t, err, "%q was accepted", name)
	}
}

// TestAllowReserved_SyntaxIsStillChecked pins the other half of "that check
// only": an exempted name still has to be a legal HTTP/2 field, and its value
// still cannot carry a request-splitting sequence.
func TestAllowReserved_SyntaxIsStillChecked(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin", "grpc-Bad")
	ctx := context.Background()
	// Uppercase in the name: not a lowercase token.
	uppercase := []conn.HeaderField{{Name: []byte("grpc-Bad"), Value: []byte("v")}}
	// CRLF in the value of an exempted name.
	inject := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("a\r\nx-evil: 1")}}

	_, uppercaseErr := cc.NewStream(ctx, "/t.S/M", uppercase)
	_, injectErr := cc.NewStream(ctx, "/t.S/M", inject)

	assert.ErrorIsf(t, uppercaseErr, ErrInvalidMetadata,
		"an uppercase allowlisted name = %v, want ErrInvalidMetadata", uppercaseErr)
	assert.ErrorIsf(t, injectErr, ErrInvalidMetadata,
		"CRLF in an exempted value = %v, want ErrInvalidMetadata", injectErr)
}

// TestAllowReserved_MatchesCaseInsensitively pins the documented matching rule,
// since HTTP/2 names are lowercase on the wire but a caller configures Options
// in whatever case they like.
func TestAllowReserved_MatchesCaseInsensitively(t *testing.T) {
	cc := allowConn(t, "GRPC-Trace-Bin")
	md := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("dA==")}}

	s, err := cc.NewStream(context.Background(), "/t.S/M", md)

	require.NoError(t, err,
		"an allowlist entry in mixed case did not match the lowercase name")
	_ = s.Close()
}

// TestAllowReserved_ConnAppendMetadataEncodesBin is why the method exists: a
// caller who enables the allowlist needs the -bin base64 encoding that the
// package-level function performs but refuses to reach.
func TestAllowReserved_ConnAppendMetadataEncodesBin(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin")

	md, err := cc.AppendMetadata(nil, "grpc-trace-bin", []byte{0x00, 0xFF, 0x10})
	// Still bound by the same rules as everything else.
	_, reservedErr := cc.AppendMetadata(nil, "content-type", []byte("x"))

	require.NoError(t, err, "(*ClientConn).AppendMetadata")
	require.Lenf(t, md, 1, "built %d fields: %+v", len(md), md)
	require.Equalf(t, "grpc-trace-bin", string(md[0].Name), "built %d fields: %+v", len(md), md)
	got, ok, valueErr := MetadataValue(md, "grpc-trace-bin")
	require.NoErrorf(t, valueErr,
		"MetadataValue = (ok=%v, err=%v) — the value is not valid base64", ok, valueErr)
	require.Truef(t, ok,
		"MetadataValue = (ok=%v, err=%v) — the value is not valid base64", ok, valueErr)
	assert.Equalf(t, []byte{0x00, 0xFF, 0x10}, got, "round-tripped % x, want 00 ff 10", got)
	assert.ErrorIsf(t, reservedErr, ErrReservedMetadata,
		"(*ClientConn).AppendMetadata(content-type) = %v, want ErrReservedMetadata", reservedErr)
}
