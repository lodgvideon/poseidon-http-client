// ManagedPool: per-address sub-pool fan-out driven
// by a Resolver and Selector.

package poolcore

import (
	"errors"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/pool"
)

// defaultManagedPoolTickerPeriod is the fallback poll period when the
// Resolver does not support Watch (returns ErrWatchUnsupported).
const defaultManagedPoolTickerPeriod = 30 * time.Second

// ManagedPool fans Acquire across per-address HTTP/2 sub-pools driven by a
// Resolver and Selector. Goroutine-safe.
//
// An ALIAS of the shared core (managed_core.go). Verified before the move: every
// method body except acquire and getOrCreateSubPool is identical to the core's
// once identifiers are normalised and comments dropped — including watchDrain,
// whose only divergence was a back-off comment H1 and H3 never carried. The two
// that differ are exactly the injection points.
type ManagedPool = ManagedCore[*Pool, *ManagedConn, *conn.Conn, func()]

// NewManagedPool constructs a ManagedPool and starts its Watch/ticker
// goroutine. It performs an initial Resolve to surface hard errors
// early; if Resolve returns 0 addrs the pool starts empty (Acquire
// returns ErrNoAddresses).
func NewManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, obs pool.Observer, rec pool.Recorder) (*ManagedPool, error) {
	mp, err := BuildManagedPool(r, s, dm, co, po, obs, rec)
	if err != nil {
		return nil, err
	}
	go mp.Run()
	return mp, nil
}

// BuildManagedPool constructs and initialises a ManagedPool without
// starting its background goroutine. Tests that need to configure
// fields (e.g. tickerPeriod) before the goroutine reads them call
// this and start the goroutine themselves via go mp.Run().
func BuildManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions, obs pool.Observer, rec pool.Recorder) (*ManagedPool, error) {
	if rec == nil {
		rec = pool.NopRecorder{}
	}
	if obs == nil {
		obs = pool.NopObserver{}
	}
	return NewCore(CoreConfig[*Pool, *ManagedConn, *conn.Conn, func()]{
		Resolver: r, Selector: s, DrainMode: dm, PoolOpts: po, Obs: obs, Rec: rec,
		NewSub: func(key string) *Pool { return New(key, co, po, obs, rec, nil) },
		ConnOf: func(mc *ManagedConn) *conn.Conn { return mc.C },
		MkRelease: func(p *Pool, mc *ManagedConn) func() {
			return func() { p.Release(mc) }
		},
	})
}

// IsDialOnlyErr reports whether err means "this backend could not be reached"
// rather than "this request failed". Only the former is worth trying the next
// address for; retrying a request error against a different backend would
// replay work the first one may already have done.
func IsDialOnlyErr(err error) bool {
	if errors.Is(err, ErrDialBackoff) || errors.Is(err, ErrPoolClosed) {
		return true
	}
	var de *DialError
	return errors.As(err, &de)
}
