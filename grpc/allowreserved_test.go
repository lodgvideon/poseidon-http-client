package grpc

import (
	"context"
	"errors"
	"testing"

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
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// TestAllowReserved_DefaultStillRefusesTheNamespace pins the protective default:
// with no allowlist, grpc- is refused exactly as before.
func TestAllowReserved_DefaultStillRefusesTheNamespace(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	md := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("x")}}
	if _, err := cc.NewStream(context.Background(), "/t.S/M", md); !errors.Is(err, ErrReservedMetadata) {
		t.Errorf("grpc-trace-bin with no allowlist = %v, want ErrReservedMetadata", err)
	}
	if _, err := AppendMetadata(nil, "grpc-trace-bin", []byte("x")); !errors.Is(err, ErrReservedMetadata) {
		t.Errorf("package AppendMetadata = %v, want ErrReservedMetadata — it cannot see a "+
			"connection's allowlist and must stay strict", err)
	}
}

// TestAllowReserved_ExemptsOnlyTheListedName is the point of the feature, and
// its bound: the listed name goes through, a different grpc- name does not.
func TestAllowReserved_ExemptsOnlyTheListedName(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin")
	ctx := context.Background()

	ok := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("dHJhY2U=")}}
	s, err := cc.NewStream(ctx, "/t.S/M", ok)
	if err != nil {
		t.Fatalf("the allowlisted name was refused: %v", err)
	}
	_ = s.Close()

	notListed := []conn.HeaderField{{Name: []byte("grpc-tags-bin"), Value: []byte("dA==")}}
	if _, err := cc.NewStream(ctx, "/t.S/M", notListed); !errors.Is(err, ErrReservedMetadata) {
		t.Errorf("a grpc- name that is NOT listed = %v, want ErrReservedMetadata", err)
	}
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

	// Transport-owned names: refused by the reservedKeys gate.
	for _, name := range []string{
		"content-type", "te", "grpc-timeout", "grpc-encoding", "grpc-accept-encoding",
		"user-agent", "connection", "keep-alive", "proxy-connection",
		"transfer-encoding", "upgrade", "host",
	} {
		md := []conn.HeaderField{{Name: []byte(name), Value: []byte("v")}}
		if _, err := cc.NewStream(ctx, "/t.S/M", md); !errors.Is(err, ErrReservedMetadata) {
			t.Errorf("%q was accepted because it appeared in the allowlist: %v — the "+
				"exemption must apply to the grpc- prefix check ONLY", name, err)
		}
	}

	// Pseudo-headers are refused one gate EARLIER, by name syntax: a colon is not
	// a legal character in a field name, so validMetadataName rejects them before
	// checkMetadataKey ever runs. Worth pinning as its own case rather than
	// folding into the above — the two sentinels are not interchangeable, and a
	// future reshuffle that made a pseudo-header reach the allowlist would show
	// up here as a changed error rather than as silence.
	for _, name := range []string{":method", ":path", ":authority", ":scheme"} {
		md := []conn.HeaderField{{Name: []byte(name), Value: []byte("v")}}
		_, err := cc.NewStream(ctx, "/t.S/M", md)
		if !errors.Is(err, ErrInvalidMetadata) {
			t.Errorf("%q = %v, want ErrInvalidMetadata from the name-syntax gate", name, err)
		}
		if errors.Is(err, nil) {
			t.Errorf("%q was accepted", name)
		}
	}
}

// TestAllowReserved_SyntaxIsStillChecked pins the other half of "that check
// only": an exempted name still has to be a legal HTTP/2 field, and its value
// still cannot carry a request-splitting sequence.
func TestAllowReserved_SyntaxIsStillChecked(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin", "grpc-Bad")
	ctx := context.Background()

	// Uppercase in the name: not a lowercase token.
	bad := []conn.HeaderField{{Name: []byte("grpc-Bad"), Value: []byte("v")}}
	if _, err := cc.NewStream(ctx, "/t.S/M", bad); !errors.Is(err, ErrInvalidMetadata) {
		t.Errorf("an uppercase allowlisted name = %v, want ErrInvalidMetadata", err)
	}
	// CRLF in the value of an exempted name.
	inject := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("a\r\nx-evil: 1")}}
	if _, err := cc.NewStream(ctx, "/t.S/M", inject); !errors.Is(err, ErrInvalidMetadata) {
		t.Errorf("CRLF in an exempted value = %v, want ErrInvalidMetadata", err)
	}
}

// TestAllowReserved_MatchesCaseInsensitively pins the documented matching rule,
// since HTTP/2 names are lowercase on the wire but a caller configures Options
// in whatever case they like.
func TestAllowReserved_MatchesCaseInsensitively(t *testing.T) {
	cc := allowConn(t, "GRPC-Trace-Bin")
	md := []conn.HeaderField{{Name: []byte("grpc-trace-bin"), Value: []byte("dA==")}}
	s, err := cc.NewStream(context.Background(), "/t.S/M", md)
	if err != nil {
		t.Fatalf("an allowlist entry in mixed case did not match the lowercase name: %v", err)
	}
	_ = s.Close()
}

// TestAllowReserved_ConnAppendMetadataEncodesBin is why the method exists: a
// caller who enables the allowlist needs the -bin base64 encoding that the
// package-level function performs but refuses to reach.
func TestAllowReserved_ConnAppendMetadataEncodesBin(t *testing.T) {
	cc := allowConn(t, "grpc-trace-bin")

	md, err := cc.AppendMetadata(nil, "grpc-trace-bin", []byte{0x00, 0xFF, 0x10})
	if err != nil {
		t.Fatalf("(*ClientConn).AppendMetadata: %v", err)
	}
	if len(md) != 1 || string(md[0].Name) != "grpc-trace-bin" {
		t.Fatalf("built %d fields: %+v", len(md), md)
	}
	got, ok, err := MetadataValue(md, "grpc-trace-bin")
	if err != nil || !ok {
		t.Fatalf("MetadataValue = (ok=%v, err=%v) — the value is not valid base64", ok, err)
	}
	if len(got) != 3 || got[0] != 0x00 || got[1] != 0xFF || got[2] != 0x10 {
		t.Errorf("round-tripped % x, want 00 ff 10", got)
	}

	// Still bound by the same rules as everything else.
	if _, err := cc.AppendMetadata(nil, "content-type", []byte("x")); !errors.Is(err, ErrReservedMetadata) {
		t.Errorf("(*ClientConn).AppendMetadata(content-type) = %v, want ErrReservedMetadata", err)
	}
}
