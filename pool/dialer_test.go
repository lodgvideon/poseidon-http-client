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

// fakeDialer implements only the plain Dial method.
type fakeDialer struct {
	conn net.Conn
	err  error
}

func (d *fakeDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
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
	d := &fakeDialer{err: wantErr}

	got, err := DialResolved(context.Background(), d, Address{Host: "10.0.0.5", Port: 8443})

	assert.Samef(t, wantErr, err, "DialResolved error = %v, want the plain Dial path's error unchanged", err)
	assert.Nilf(t, got, "DialResolved conn = %v, want nil on error", got)
}
