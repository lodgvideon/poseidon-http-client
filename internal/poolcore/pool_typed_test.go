package poolcore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// fakeTypedConn stands in for the wrapped connection type a consumer will keep
// in ManagedConn.Typed — a *grpc.ClientConn is the one issue #940 is for, and
// this package must never learn about it. Only Close matters here: the real
// one is a no-op unless the wrapper owns the connection, so a pool that tore a
// connection down through the wrapper would leave the socket open and say
// nothing. This one records the call, which turns that silent leak into a
// failed assertion.
type fakeTypedConn struct {
	raw    *conn.Conn
	closes atomic.Int32
}

// Close records that something asked the WRAPPER to close. Nothing in poolcore
// may call it; the error exists so that even a caller discarding the return
// value still trips the counter.
func (f *fakeTypedConn) Close() error {
	f.closes.Add(1)
	return errors.New("poolcore closed the wrapper instead of the raw conn")
}

// liveDialer returns a fakeDialer whose in-process server stays up for the rest
// of the test, so a conn dialled through it dies only when something closes it.
// The bare fakeDialer's server returns as soon as the handshake is done and
// closes the pipe behind it, which would let "the conn is no longer alive" pass
// without the pool having closed anything at all.
func liveDialer(t *testing.T) *fakeDialer {
	t.Helper()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	return &fakeDialer{srvAfter: func(*frame.Framer) { <-stop }}
}

// TestPool_WrapBuildsTypedOncePerConn pins both halves of the typed seam: a
// pool built with a wrap hands the wrapped value back on the ManagedConn, and
// builds it at DIAL time rather than per acquire. The second half is why the
// field exists instead of a wrap call inside Acquire — a wrapper's fields are
// computed once from options that never change, so rebuilding one per acquire
// is pure allocation on the request path.
func TestPool_WrapBuildsTypedOncePerConn(t *testing.T) {
	t.Parallel()
	var wrapCalls atomic.Int32
	p := New("typed.test:443", conn.ConnOptions{Dialer: liveDialer(t)},
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		nil, nil, func(c *conn.Conn) (any, error) {
			wrapCalls.Add(1)
			return &fakeTypedConn{raw: c}, nil
		})
	t.Cleanup(func() { _ = p.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, firstErr := p.Acquire(ctx)
	second, secondErr := p.Acquire(ctx)

	require.NoError(t, firstErr, "first acquire against the in-process fake server")
	require.NoError(t, secondErr,
		"second acquire — one conn with a four-stream cap is meant to serve both")
	require.Same(t, first, second,
		"the two acquires landed on different connections, so this test never re-acquired the "+
			"conn wrap ran for and cannot see a per-acquire re-wrap at all")
	require.NotNil(t, first.Typed,
		"Typed is nil: the pool dialled but never ran wrap, so a consumer has no way to get "+
			"its own connection type back out of the pool")
	assert.Equalf(t, int32(1), wrapCalls.Load(),
		"wrap ran %d times for one connection, want 1 — wrapping per acquire allocates a fresh "+
			"wrapper on every request for a value that cannot have changed",
		wrapCalls.Load())
	assert.Same(t, first.C, first.Typed.(*fakeTypedConn).raw,
		"wrap was handed a different conn from the one the pool kept, so the wrapper and the "+
			"pool would be talking about two different sockets")
}
