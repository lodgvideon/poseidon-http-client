package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// hdrValue returns the first value for name in a built header block.
func hdrValue(hdrs []conn.HeaderField, name string) (string, bool) {
	for i := range hdrs {
		if string(hdrs[i].Name) == name {
			return string(hdrs[i].Value), true
		}
	}
	return "", false
}

// buildWith runs buildHeaders with both metadata sources, the way NewStream
// does once the options are resolved.
func buildWith(t *testing.T, md, optMD []conn.HeaderField) []conn.HeaderField {
	t.Helper()
	cc := newClientConn(nil, Options{Authority: "example.com"}.defaulted(), false)
	sc := headerScratchPool.Get().(*headerScratch)
	defer putHeaderScratch(sc)
	return append([]conn.HeaderField(nil), cc.buildHeaders(context.Background(), "/t.S/M", md, optMD, sc)...)
}

// TestWithMetadata_ReachesTheHeaderBlock is the point of the option: a call
// expressed entirely as (ctx, ..., opts...) still sends its metadata.
func TestWithMetadata_ReachesTheHeaderBlock(t *testing.T) {
	hdrs := buildWith(t, nil, []conn.HeaderField{
		{Name: []byte("x-tenant"), Value: []byte("acme")},
	})
	if got, ok := hdrValue(hdrs, "x-tenant"); !ok || got != "acme" {
		t.Errorf("x-tenant = %q (present=%v), want \"acme\"", got, ok)
	}
}

// TestWithMetadata_CoexistsWithPositional pins that the option adds to the
// positional argument rather than replacing it — both signatures keep working,
// and a caller may use either or both.
func TestWithMetadata_CoexistsWithPositional(t *testing.T) {
	hdrs := buildWith(t,
		[]conn.HeaderField{{Name: []byte("x-from-arg"), Value: []byte("1")}},
		[]conn.HeaderField{{Name: []byte("x-from-opt"), Value: []byte("2")}},
	)
	for _, want := range []struct{ name, val string }{{"x-from-arg", "1"}, {"x-from-opt", "2"}} {
		if got, ok := hdrValue(hdrs, want.name); !ok || got != want.val {
			t.Errorf("%s = %q (present=%v), want %q", want.name, got, ok, want.val)
		}
	}
}

// TestWithMetadata_Accumulates pins that several options compose rather than
// the last one winning, which is what lets a generator hand through options it
// did not author.
func TestWithMetadata_Accumulates(t *testing.T) {
	co := callOptions{}
	for _, o := range []CallOption{
		WithMetadata([]conn.HeaderField{{Name: []byte("a"), Value: []byte("1")}}),
		WithMetadata([]conn.HeaderField{{Name: []byte("b"), Value: []byte("2")}}),
	} {
		o.apply(&co)
	}
	if len(co.md) != 2 {
		t.Fatalf("two WithMetadata options produced %d fields, want 2", len(co.md))
	}
	hdrs := buildWith(t, nil, co.md)
	for _, name := range []string{"a", "b"} {
		if _, ok := hdrValue(hdrs, name); !ok {
			t.Errorf("%q missing: a later WithMetadata replaced an earlier one", name)
		}
	}
}

// TestWithMetadata_CredentialsMarkedSensitive is the security half. The
// never-indexed default exists because an unmarked credential enters the
// connection's HPACK dynamic table and outlives the RPC; option-supplied
// metadata has to get the same treatment as positional, or the option becomes a
// way to bypass it.
func TestWithMetadata_CredentialsMarkedSensitive(t *testing.T) {
	hdrs := buildWith(t, nil, []conn.HeaderField{
		{Name: []byte("authorization"), Value: []byte("Bearer secret")},
		{Name: []byte("cookie"), Value: []byte("session=abc")},
		{Name: []byte("x-request-id"), Value: []byte("req-1")},
	})
	want := map[string]bool{"authorization": true, "cookie": true, "x-request-id": false}
	seen := map[string]bool{}
	for _, h := range hdrs {
		n := string(h.Name)
		if _, tracked := want[n]; !tracked {
			continue
		}
		seen[n] = true
		if h.Sensitive() != want[n] {
			t.Errorf("%q via WithMetadata: Sensitive = %v, want %v", n, h.Sensitive(), want[n])
		}
	}
	for n := range want {
		if !seen[n] {
			t.Errorf("%q missing from the header block", n)
		}
	}
}

// TestWithMetadata_IsValidated pins that the option does not route around the
// last gate before the wire. Neither conn nor hpack checks field syntax on the
// send path, so a malformed name reaching the encoder through this door would
// be exactly the injection the positional check exists to stop.
func TestWithMetadata_IsValidated(t *testing.T) {
	p := newMockGRPCPeer(t)
	cc := dialMockPeer(t, p, nil)
	ctx := context.Background()

	bad := []struct {
		name string
		md   []conn.HeaderField
	}{
		{"uppercase name", []conn.HeaderField{{Name: []byte("X-Bad"), Value: []byte("v")}}},
		{"reserved prefix", []conn.HeaderField{{Name: []byte("grpc-status"), Value: []byte("0")}}},
		{"pseudo-header", []conn.HeaderField{{Name: []byte(":method"), Value: []byte("GET")}}},
		{"newline in value", []conn.HeaderField{{Name: []byte("x-ok"), Value: []byte("a\r\nb")}}},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			_, err := cc.NewStream(ctx, "/t.S/M", nil, WithMetadata(c.md))
			if err == nil {
				t.Fatal("NewStream accepted invalid metadata supplied through WithMetadata")
			}
		})
	}

	// And the same value passed positionally is rejected identically, so the two
	// doors cannot drift apart.
	for _, c := range bad {
		if _, err := cc.NewStream(ctx, "/t.S/M", c.md); err == nil {
			t.Errorf("%s: rejected via WithMetadata but accepted positionally", c.name)
		}
	}
}

// TestWithMetadata_EndToEnd drives a real RPC with option-supplied metadata, so
// the option is exercised through NewStream rather than only through
// buildHeaders.
func TestWithMetadata_EndToEnd(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	got, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil,
		WithMetadata([]conn.HeaderField{{Name: []byte("x-tenant"), Value: []byte("acme")}}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("echo = %q, want \"hello\"", got)
	}
}

// TestWithMetadata_NilIsHarmless pins the shape generated code will emit when a
// caller passes no metadata at all.
func TestWithMetadata_NilIsHarmless(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(context.Background(), "/t.S/M", nil, WithMetadata(nil))
	if err != nil {
		t.Fatalf("NewStream with WithMetadata(nil): %v", err)
	}
	if err := s.Close(); err != nil && !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Close: %v", err)
	}
}
