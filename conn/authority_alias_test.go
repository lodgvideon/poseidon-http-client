package conn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// The :authority a request carried is kept so the push accept path can compare a
// PUSH_PROMISE's :authority against it — a server is authoritative for what it
// already answered over the cert-validated connection (RFC 9113 §8.4 / §10.1).
//
// It is stored as a COPY, and that copy is load-bearing rather than incidental.
// The fields slice belongs to the application: it is what the caller passes to
// SendHeaders, and nothing in that interface forbids reusing or overwriting the
// buffer once the call returns. Aliasing it would let an ordinary caller reusing
// its own header buffer silently rewrite the reference value a cross-origin push
// check compares against — a security property quietly turning into whatever the
// caller last wrote.
//
// Dropping the copy is a tempting one-line allocation win (#578), which is why
// this test exists: it is the failure that change would introduce, and it is
// invisible to every other test here because they all build a fresh slice per
// request.

// TestStream_RequestAuthority_DoesNotAliasCallerFields overwrites the caller's
// header bytes in place after SendHeaders and requires the stored authority not
// to follow.
//
// Two requests on one connection, because Stream is pooled: the second runs on a
// recycled struct, where a value left over from the previous lifetime would
// otherwise pass unnoticed. The second authority is deliberately SHORTER than
// the first, so a stale tail surviving in the reused buffer fails the comparison
// rather than hiding inside it.
func TestStream_RequestAuthority_DoesNotAliasCallerFields(t *testing.T) {
	p := newBenchPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, p.addr(), ConnOptions{Dialer: &PlaintextDialer{}})
	require.NoError(t, err, "Dial the in-process peer")
	defer func() { _ = c.Close() }()

	for _, tc := range []struct {
		name      string
		authority string
	}{
		{"first request", "first.example"},
		{"second request, recycled stream, shorter authority", "b.io"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A caller-owned buffer, exactly as an application would build it.
			authority := []byte(tc.authority)
			hdrs := []header.Field{
				{Name: []byte(":method"), Value: []byte("GET")},
				{Name: []byte(":scheme"), Value: []byte("http")},
				{Name: []byte(":authority"), Value: authority},
				{Name: []byte(":path"), Value: []byte("/")},
			}

			ref, nerr := c.NewStream(ctx)
			require.NoError(t, nerr, "NewStream")
			require.NoError(t, ref.SendHeaders(ctx, hdrs, true), "SendHeaders")
			// The caller reuses its own buffer, which it is entitled to do the
			// moment SendHeaders returns.
			for i := range authority {
				authority[i] = 'x'
			}

			s := ref.Stream()
			assert.Truef(t, s.hasRequestAuthority([]byte(tc.authority)),
				"after the caller overwrote its own header buffer, the stream no "+
					"longer reports the authority the request was sent with (%q) — the "+
					"stored value aliases the caller's bytes, so a cross-origin push check "+
					"now compares against whatever the caller last wrote", tc.authority)
			assert.Falsef(t, s.hasRequestAuthority(authority),
				"the stream reports the caller's MUTATED bytes (%q) as the "+
					"request authority", authority)

			// Drain and close so the struct goes back to the pool and the next
			// subtest runs on a recycled one.
			require.NoError(t, benchDrain(ctx, ref), "drain the response")
		})
	}
}
