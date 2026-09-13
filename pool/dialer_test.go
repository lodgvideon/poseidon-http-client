package pool

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubConn returns a net.Conn that is never read from or written to. These
// tests assert only WHICH conn value came back from DialResolved, so the pipe
// exists to produce a distinguishable, closeable net.Conn and nothing else.
func stubConn(t *testing.T) net.Conn {
	t.Helper()
	c, peer := net.Pipe()
	t.Cleanup(func() { _ = c.Close(); _ = peer.Close() })
	return c
}

// fakeDialer implements only the plain Dial method, recording every addr it
// receives so a test can assert what was actually dialed.
type fakeDialer struct {
	conn     net.Conn
	err      error
	gotAddrs []string
}

func (d *fakeDialer) Dial(_ context.Context, addr string) (net.Conn, error) {
	d.gotAddrs = append(d.gotAddrs, addr)
	return d.conn, d.err
}

// fakeAddressDialer implements both Dial and DialAddress, recording every
// DialAddress call so a test can assert what it received.
type fakeAddressDialer struct {
	fakeDialer
	gotAddrs    []Address
	addressConn net.Conn
	addressErr  error
}

func (d *fakeAddressDialer) DialAddress(_ context.Context, addr Address) (net.Conn, error) {
	d.gotAddrs = append(d.gotAddrs, addr)
	return d.addressConn, d.addressErr
}

var _ AddressDialer = (*fakeAddressDialer)(nil)

func TestDialResolved_PrefersAddressDialerWhenImplemented(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443, Attributes: map[string]string{"zone": "us-west-2a"}}
	d := &fakeAddressDialer{addressConn: want}

	got, err := DialResolved(context.Background(), d, resolved, resolved.String())

	require.NoError(t, err, "DialResolved")
	require.Lenf(t, d.gotAddrs, 1, "DialAddress call count = %d, want 1", len(d.gotAddrs))
	assert.Equalf(t, resolved, d.gotAddrs[0],
		"DialAddress got Address = %+v, want %+v — Attributes must pass through unchanged", d.gotAddrs[0], resolved)
	assert.Samef(t, want, got, "DialResolved = %v, want the AddressDialer path's conn unchanged", got)
}

func TestDialResolved_FallsBackWhenDialerLacksCapability(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{conn: want} // implements Dial only

	got, err := DialResolved(context.Background(), d, resolved, resolved.String())

	require.NoError(t, err, "DialResolved")
	assert.Equalf(t, []string{"10.0.0.5:8443"}, d.gotAddrs,
		"Dial got addrs = %v, want exactly one call with the given fallback address", d.gotAddrs)
	assert.Samef(t, want, got, "DialResolved = %v, want the plain Dial path's conn unchanged", got)
}

// TestDialResolved_UsesGivenFallbackAddrVerbatim pins that the fallback path
// dials fallbackAddr exactly as given, rather than recomputing resolved.String()
// itself — the reason fallbackAddr is a parameter instead of being derived
// internally on every call: a real caller's flattened address string is
// already computed once, at sub-pool construction (see WrapForAddressDialer),
// so DialResolved rebuilding it on every dial would be pure waste. A
// fallbackAddr that deliberately does not match resolved.String() ("10.0.0.5:8443")
// makes the two possible behaviors distinguishable.
func TestDialResolved_UsesGivenFallbackAddrVerbatim(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{conn: want}

	got, err := DialResolved(context.Background(), d, resolved, "given-verbatim:9999")

	require.NoError(t, err, "DialResolved")
	assert.Equalf(t, []string{"given-verbatim:9999"}, d.gotAddrs,
		"Dial got addrs = %v, want exactly the given fallbackAddr — DialResolved must not recompute resolved.String() itself", d.gotAddrs)
	assert.Samef(t, want, got, "DialResolved = %v, want the plain Dial path's conn unchanged", got)
}

func TestDialResolved_PropagatesAddressDialerError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeAddressDialer{addressErr: wantErr}

	got, err := DialResolved(context.Background(), d, resolved, resolved.String())

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the AddressDialer path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}

func TestDialResolved_PropagatesPlainDialError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{err: wantErr}

	got, err := DialResolved(context.Background(), d, resolved, resolved.String())

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the plain Dial path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}

func TestWrapForAddressDialer_NilDialerReturnsNil(t *testing.T) {
	t.Parallel()

	got := WrapForAddressDialer(nil, Address{Host: "10.0.0.5", Port: 8443})

	assert.Nilf(t, got, "WrapForAddressDialer(nil, ...) = %v, want nil — a nil dialer must not be wrapped, "+
		"so a caller's own nil-dialer handling downstream stays in effect", got)
}

func TestWrapForAddressDialer_PrefersAddressDialer(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443, Attributes: map[string]string{"zone": "us-west-2a"}}
	d := &fakeAddressDialer{addressConn: want}
	wrapped := WrapForAddressDialer(d, resolved)
	require.NotNil(t, wrapped, "WrapForAddressDialer")

	got, err := wrapped.Dial(context.Background(), "irrelevant-flattened-string:0")

	require.NoError(t, err, "wrapped.Dial")
	require.Lenf(t, d.gotAddrs, 1, "DialAddress call count = %d, want 1", len(d.gotAddrs))
	assert.Equalf(t, resolved, d.gotAddrs[0], "DialAddress got Address = %+v, want %+v — the wrap must offer "+
		"the resolved Address regardless of the addr string it was dialed with", d.gotAddrs[0], resolved)
	assert.Samef(t, want, got, "wrapped.Dial = %v, want the AddressDialer path's conn unchanged", got)
}

// The addr wrapped.Dial is called with deliberately does not match
// resolved.String() ("10.0.0.5:8443"), so a wrap that recomputed the
// address from resolved instead of forwarding what it was given would be
// observably different — see TestDialResolved_UsesGivenFallbackAddrVerbatim
// for why that distinction matters.
func TestWrapForAddressDialer_FallsBackAndForwardsGivenAddr(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{conn: want} // implements Dial only
	wrapped := WrapForAddressDialer(d, resolved)
	require.NotNil(t, wrapped, "WrapForAddressDialer")

	got, err := wrapped.Dial(context.Background(), "given-verbatim:9999")

	require.NoError(t, err, "wrapped.Dial")
	assert.Equalf(t, []string{"given-verbatim:9999"}, d.gotAddrs, "Dial got addrs = %v, want exactly the addr "+
		"wrapped.Dial was called with, forwarded unchanged — no recomputed address string", d.gotAddrs)
	assert.Samef(t, want, got, "wrapped.Dial = %v, want the plain Dial path's conn unchanged", got)
}
