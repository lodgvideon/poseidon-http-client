package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// managedCore.acquire: every address present and every one failing (#874).
//
// The failover suite covers the two ends of the partition and not the middle:
//
//	resolver empty                      -> TestManagedPool_NoAddresses_ReturnsErrNoAddresses
//	one address dead, one live          -> TestManagedPool_FailsOverOnFirstDialFailure (+ h1/h3)
//	EVERY address present, all failing  -> nothing, on any of the three protocols
//
// In that third class the loop marks each address tried, the pruned set empties,
// and acquire falls through to `if lastErr != nil { return lastErr }`. Replacing
// that with ErrNoAddresses is SURVIVOR 2/2 against the whole managed filter — and
// it is the difference between an operator learning "every backend refused the
// connection" and being told the resolver returned nothing, which for three
// perfectly good addresses is actively misleading. A retry classifier keys on the
// same distinction.
//
// These tests PIN what the code does today. They deliberately do not decide
// whether the surfaced error should also carry ErrNoAddresses as a secondary
// classification — that design question is left on #874 for the maintainer.
// ————————————————————————————————————————————————————————————————

// errAllBackendsRefused is the sentinel every dialer below fails with, so the
// assertion can name the mechanism rather than settle for "an error".
var errAllBackendsRefused = errors.New("simulated: connection refused by every backend")

// allFailDialer refuses every TCP dial with errAllBackendsRefused.
type allFailDialer struct{}

func (allFailDialer) Dial(context.Context, string) (net.Conn, error) {
	return nil, errAllBackendsRefused
}

func TestManagedPool_EveryAddressFailing_SurfacesTheDialErrorNotErrNoAddresses(t *testing.T) {
	t.Parallel()
	addrs := []Address{{Host: "10.0.0.1", Port: 8080}, {Host: "10.0.0.1", Port: 8081}, {Host: "10.0.0.1", Port: 8082}}
	opts := newConnOpts()
	opts.Dialer = allFailDialer{}
	mp, err := newManagedPool(
		StaticResolver(addrs...), RoundRobin(), DrainGraceful, opts,
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		nil, nil,
	)
	require.NoError(t, err, "newManagedPool")
	defer mp.close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, _, err = mp.acquire(ctx)

	require.Error(t, err, "acquire succeeded against three addresses that all refuse")
	assert.ErrorIsf(t, err, errAllBackendsRefused,
		"acquire err = %v; the caller must be told why every backend refused, not merely "+
			"that the attempt failed", err)
	assert.Falsef(t, errors.Is(err, ErrNoAddresses),
		"acquire reported ErrNoAddresses for a resolver that returned %d perfectly good "+
			"addresses (%v); an operator reading that goes and looks at DNS, and a retry "+
			"classifier treats a transport failure as a configuration one", len(addrs), err)
}

func TestH1ManagedPool_EveryAddressFailing_SurfacesTheDialErrorNotErrNoAddresses(t *testing.T) {
	t.Parallel()
	addrs := h1Addrs(3)
	mp, err := newH1ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		allFailDialer{}, h1ManagedPoolOpts(), nil, nil)
	require.NoError(t, err, "newH1ManagedPool")
	defer func() { _ = mp.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, _, err = mp.acquire(ctx)

	require.Error(t, err, "acquire succeeded against three addresses that all refuse")
	assert.ErrorIsf(t, err, errAllBackendsRefused,
		"acquire err = %v, want the dialer's own error", err)
	assert.Falsef(t, errors.Is(err, ErrNoAddresses),
		"acquire reported ErrNoAddresses for three resolved addresses: %v", err)
}

func TestH3ManagedPool_EveryAddressFailing_SurfacesTheDialErrorNotErrNoAddresses(t *testing.T) {
	t.Parallel()
	addrs := h3Addrs(3)
	dialFn := func(context.Context, string, *tls.Config) (h3Client, error) {
		return nil, errAllBackendsRefused
	}
	mp, err := newH3ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"},
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		dialFn, nil, nil)
	require.NoError(t, err, "newH3ManagedPool")
	defer func() { _ = mp.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, _, err = mp.acquire(ctx)

	require.Error(t, err, "acquire succeeded against three addresses that all refuse")
	assert.ErrorIsf(t, err, errAllBackendsRefused,
		"acquire err = %v, want the dialer's own error", err)
	assert.Falsef(t, errors.Is(err, ErrNoAddresses),
		"acquire reported ErrNoAddresses for three resolved addresses: %v", err)
}

// TestManagedPool_NoAddressesStillMeansErrNoAddresses is the control: the
// three tests above are satisfied by an acquire that has stopped returning
// ErrNoAddresses at all. The genuinely empty set must still say so.
func TestManagedPool_NoAddressesStillMeansErrNoAddresses(t *testing.T) {
	t.Parallel()
	opts := newConnOpts()
	opts.Dialer = allFailDialer{}
	mp, err := newManagedPool(
		StaticResolver(), RoundRobin(), DrainGraceful, opts,
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		nil, nil,
	)
	require.NoError(t, err, "newManagedPool")
	defer mp.close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err = mp.acquire(ctx)

	assert.ErrorIsf(t, err, ErrNoAddresses,
		"an empty resolver set reported %v; with no address there is no dial error to "+
			"surface and ErrNoAddresses is the only honest answer", err)
	assert.Falsef(t, errors.Is(err, errAllBackendsRefused),
		"an empty set reported a dial error, so acquire invented one: %v", err)
}

// #875 asked for a decision on three draining guards that survive mutation. The
// decision is recorded in coreSubPool's doc comment (kept as defence-in-depth,
// invariant written down) and NO test was added for them: all three mutations
// survive 2/2 against the whole managed filter INCLUDING the tests above, which
// is what "equivalent mutant" means here. An invariant test was written first
// and deleted — the only mutation it caught (applySet no longer reviving a
// returning address) is already caught 2/2 by TestManagedPool_ReviveBeatsDrainWatcher,
// TestManagedPool_AddressReAddedAfterRemoval and their h1/h3 twins.
