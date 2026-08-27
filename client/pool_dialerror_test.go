package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestManagedPool_FailsOverOnFirstDialFailure pins the reason the H2 and H3
// pools wrap their dial failures.
//
// managedPool's acquire loop moves to the next address only when the error is
// dial-only — isDialOnlyErr matches ErrDialBackoff, ErrPoolClosed, or a
// *DialError. The H1 pool has always wrapped; H2 and H3 sent the bare dialer
// error, so the loop treated a dead first address as a fatal answer and never
// tried the second. Multi-address failover is a documented headline feature,
// and it was off for two of the three transports.
func TestManagedPool_FailsOverOnFirstDialFailure(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	dead := deadAddress(t)
	live := addrs[0]
	mp, err := newManagedPool(
		StaticResolver(dead, live),
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		nil, nil,
	)
	require.NoError(t, err, "newManagedPool")
	defer mp.Close()

	// RoundRobin starts on the dead address, so the loop must reach the live
	// one inside this single acquire.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, release, _, err := mp.Acquire(ctx)

	require.NoErrorf(t, err,
		"acquire = %v; a live second address was available, so the loop stopped on the dead one", err)
	require.NotNil(t, c, "acquire returned no connection and no error")
	release()
}

// TestPool_DialFailureIsTypedForClassification is the narrow unit: whatever the
// dialer returns, what leaves the pool must be classifiable. Both consumers key
// on the type, and DialError.Unwrap keeps errors.Is working on the cause, so
// nothing that inspected the underlying error loses anything.
func TestPool_DialFailureIsTypedForClassification(t *testing.T) {
	sentinel := errors.New("connect refused (synthetic)")
	p := newPool("203.0.113.1:1", conn.ConnOptions{Dialer: &refusingDialer{err: sentinel}}, PoolOptions{
		MaxConnsPerHost:   1,
		MaxStreamsPerConn: 1,
		HealthCheckPeriod: time.Hour,
	}, nil, nil)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := p.Acquire(ctx)

	require.Error(t, err, "acquire succeeded against a dialer that refuses everything")
	var de *DialError
	require.Truef(t, errors.As(err, &de),
		"acquire error = %v (%T), want a *DialError: managed failover and the retry classifier both key on it",
		err, err)
	require.ErrorIsf(t, err, sentinel, "wrapping lost the cause: %v", err)
	require.True(t, isDialOnlyErr(err),
		"isDialOnlyErr rejected a pool dial failure, so managed failover would abort on it")
	// The retry classifier's view — this is the behaviour change the wrap
	// carries with it, so it is asserted rather than left implicit.
	require.True(t, builtinShouldRetry(err),
		"builtinShouldRetry rejected a pool dial failure; nothing was sent, so it is retryable")
}

// deadAddress returns an Address nothing listens on.
func deadAddress(t *testing.T) Address {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	ta := l.Addr().(*net.TCPAddr)
	_ = l.Close()
	return Address{Host: ta.IP.String(), Port: ta.Port}
}

// TestH3Pool_DialFailureIsTypedForClassification is the H3 sibling of the test
// above. It exists because the first mutation run showed nothing covered the
// H3 wrap: removing it left the whole suite green, so the fix was deletable.
func TestH3Pool_DialFailureIsTypedForClassification(t *testing.T) {
	sentinel := errors.New("quic dial refused (synthetic)")
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   1,
		HealthCheckPeriod: time.Hour,
	}, func(context.Context, string, *tls.Config) (h3Client, error) {
		return nil, sentinel
	}, nil, nil)
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := p.Acquire(ctx)

	require.Error(t, err, "acquire succeeded against a dialer that refuses everything")
	var de *DialError
	require.Truef(t, errors.As(err, &de), "acquire error = %v (%T), want a *DialError", err, err)
	require.ErrorIsf(t, err, sentinel, "wrapping lost the cause: %v", err)
	require.True(t, isDialOnlyErr(err),
		"isDialOnlyErr rejected an H3 pool dial failure, so managed failover would abort on it")
}
