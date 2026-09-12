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

	got, err := DialResolved(context.Background(), d, resolved)

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

	got, err := DialResolved(context.Background(), d, resolved)

	require.NoError(t, err, "DialResolved")
	assert.Equalf(t, []string{"10.0.0.5:8443"}, d.gotAddrs,
		"Dial got addrs = %v, want exactly one call with the flattened Address", d.gotAddrs)
	assert.Samef(t, want, got, "DialResolved = %v, want the plain Dial path's conn unchanged", got)
}

// TestDialResolved_FlattensIPv6AddressCorrectly pins the fallback path's use
// of Address.String() (net.JoinHostPort) rather than a naive "Host:Port"
// concatenation — an IPv6 literal needs bracketing or the flattened string is
// ambiguous with the port separator.
func TestDialResolved_FlattensIPv6AddressCorrectly(t *testing.T) {
	t.Parallel()
	want := stubConn(t)
	resolved := Address{Host: "2001:db8::1", Port: 8443}
	d := &fakeDialer{conn: want}

	got, err := DialResolved(context.Background(), d, resolved)

	require.NoError(t, err, "DialResolved")
	assert.Equalf(t, []string{"[2001:db8::1]:8443"}, d.gotAddrs,
		"Dial got addrs = %v, want the IPv6 host bracketed by net.JoinHostPort", d.gotAddrs)
	assert.Samef(t, want, got, "DialResolved = %v, want the plain Dial path's conn unchanged", got)
}

func TestDialResolved_PropagatesAddressDialerError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeAddressDialer{addressErr: wantErr}

	got, err := DialResolved(context.Background(), d, resolved)

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the AddressDialer path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}

func TestDialResolved_PropagatesPlainDialError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("dial refused")
	resolved := Address{Host: "10.0.0.5", Port: 8443}
	d := &fakeDialer{err: wantErr}

	got, err := DialResolved(context.Background(), d, resolved)

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the plain Dial path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}
