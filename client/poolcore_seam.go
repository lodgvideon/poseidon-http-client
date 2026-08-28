package client

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"

	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
	"github.com/lodgvideon/poseidon-http-client/pool"
)

// The pool machinery lives in internal/poolcore so grpc can reuse it without
// importing this package. These aliases keep the names the transports and the
// three pools have always used, so the move touched their call sites only where
// a signature genuinely changed. See docs/adr/0001.
type (
	// Pool is the per-host HTTP/2 connection pool.
	Pool = poolcore.Pool
	// managedConn is the pool's per-conn record.
	managedConn = poolcore.ManagedConn
	// dialEnv is the slice of a pool that a dial needs.
	dialEnv = poolcore.DialEnv
	// managedPool fans acquire across per-address HTTP/2 sub-pools.
	managedPool = poolcore.ManagedPool
	// managedCore is the address fan-out shared by the three stacks.
	managedCore[P poolcore.SubPoolBackend[MC], MC any, C any, R any] = poolcore.ManagedCore[P, MC, C, R]
)

const defaultDialTimeout = poolcore.DefaultDialTimeout

var (
	dialCtx              = poolcore.DialCtx
	dialTimeoutOrDefault = poolcore.DialTimeoutOrDefault
	addrString           = poolcore.AddrString
	effectiveStreamCap   = poolcore.EffectiveStreamCap
	inDialBackoff        = poolcore.InDialBackoff
	mapAcquireErr        = poolcore.MapAcquireErr
	isDialOnlyErr        = poolcore.IsDialOnlyErr
)

// The wrappers below keep this package's existing call sites unchanged: the
// pool layer now speaks pool.Observer and pool.Recorder, and these translate at
// the boundary. Both conversions are free — see observerFor and recorderFor.

func newPool(addr string, connOpts conn.ConnOptions, opts PoolOptions,
	hooksRef *atomic.Pointer[Hooks], metrics *Metrics,
) *Pool {
	return poolcore.New(addr, connOpts, opts, observerFor(hooksRef), recorderFor(metrics))
}

func newManagedPool(r Resolver, s Selector, dm DrainMode, co conn.ConnOptions, po PoolOptions,
	hooksRef *atomic.Pointer[Hooks], metrics *Metrics,
) (*managedPool, error) {
	return poolcore.NewManagedPool(r, s, dm, co, po, observerFor(hooksRef), recorderFor(metrics))
}

func notifyConnClose(addr string, reason CloseReason, metrics *Metrics, hooksRef *atomic.Pointer[Hooks]) {
	poolcore.NotifyConnClose(addr, reason, observerFor(hooksRef), recorderFor(metrics))
}

// dialAttempt and dialObserved are generic, so they cannot be aliased through a
// var. The wrappers keep their call sites unchanged.

func dialAttempt[C any](env dialEnv, dial func(context.Context) (C, error)) (C, error) {
	return poolcore.DialAttempt(env, dial)
}

func dialObserved[C any](ctx context.Context, addr string, timeout time.Duration,
	metrics *Metrics, hooksRef *atomic.Pointer[Hooks], dial func(context.Context) (C, error),
) (C, error) {
	return poolcore.DialObserved(ctx, addr, timeout, recorderFor(metrics), observerFor(hooksRef), dial)
}

// hooksObserver and metricsRecorder adapt this package's observability to the
// pool layer's two interfaces.
//
// They are DEFINED TYPES over the existing ones rather than structs wrapping
// them, so observerFor and recorderFor are pointer conversions and allocate
// nothing. A struct wrapper would cost two allocations on every connection
// close and every dial, which is exactly the kind of per-event cost the
// //go:build !race alloc gates in this package exist to catch.
type hooksObserver atomic.Pointer[Hooks]

// load reads the current hooks. The observer is installed once and lives as
// long as the pool, while the hooks behind it may be swapped at any time, so
// the read happens per call rather than at construction.
func (o *hooksObserver) load() *Hooks { return (*atomic.Pointer[Hooks])(o).Load() }

func (o *hooksObserver) OnDial(e pool.DialEvent) {
	if h := o.load(); h != nil && h.OnDial != nil {
		h.OnDial(e)
	}
}

func (o *hooksObserver) OnConnClose(e pool.ConnCloseEvent) {
	if h := o.load(); h != nil && h.OnConnClose != nil {
		h.OnConnClose(e)
	}
}

func (o *hooksObserver) OnResolverUpdate(e pool.ResolverUpdateEvent) {
	if h := o.load(); h != nil && h.OnResolverUpdate != nil {
		h.OnResolverUpdate(e)
	}
}

type metricsRecorder Metrics

func (r *metricsRecorder) DialAttempted()  { r.Counters.DialsAttempted.Add(1) }
func (r *metricsRecorder) DialFailed()     { r.Counters.DialsFailed.Add(1) }
func (r *metricsRecorder) ConnClosed()     { r.Counters.ConnsClosed.Add(1) }
func (r *metricsRecorder) GoAwayReceived() { r.Counters.GoAwaysReceived.Add(1) }

func (r *metricsRecorder) ObserveDial(d time.Duration) { r.Latency.Dial.Observe(d) }

func (r *metricsRecorder) ObserveAcquire(d time.Duration) { r.Latency.Acquire.Observe(d) }

// observerFor and recorderFor translate a nil into the pool layer's nop, which
// is what the old nil checks at each call site did. Both returns are free: a
// pointer conversion for the live case, and a zero-size value for the nop.
func observerFor(hooksRef *atomic.Pointer[Hooks]) pool.Observer {
	if hooksRef == nil {
		return pool.NopObserver{}
	}
	return (*hooksObserver)(hooksRef)
}

func recorderFor(m *Metrics) pool.Recorder {
	if m == nil {
		return pool.NopRecorder{}
	}
	return (*metricsRecorder)(m)
}
