package client

import (
	"context"
	"sync/atomic"
	"time"
)

// Shared helpers for the three per-host pool actors — the parts that carry no
// protocol content at all.
//
// The bar for living here is that the SIGNATURE has to be smaller than the
// body it replaces. That rules a lot out, and the exclusions are the more
// useful half of this file's contents:
//
//   - countLive and sumActive look like candidates. They are three-line loops;
//     a generic version behind a predicate closure moves the complexity rather
//     than concentrating it, and reads worse at the call site.
//   - evictIdle needs an eligibility predicate, a lastUsed accessor and a
//     teardown — a six-argument function standing in for thirty lines of
//     obvious code. h1's copy has since grown a guard the others do not have,
//     which is the shape that would have been flattened away.
//   - The per-conn records and the pool structs stay as they are. The H2 and
//     H3 tests construct managedConn and h3Pool as struct literals, so moving
//     a FIELD into a shared type churns the regression gate. Methods are free;
//     fields are not. That is why everything here is a free function over the
//     slice rather than an embedded core.
//   - The tick sequences, warmup, evictDead and acquire are genuinely
//     different programs and each file says why. They are different step
//     LISTS, not different step bodies, so a shared skeleton would be three
//     functions wearing one signature.
//
// That last exclusion is about the THREE-way comparison, and H1 is what makes
// it true. #480 measured the two-way H2/H3 case and found ten methods identical
// after substituting type names alone — run, handleAcquire, handleWarmup,
// flushStrandedWaiters, serveWaiters, handleStats, warmup, notifyClose, Close
// and dialEnv. Re-measured on this tree, all ten still match H2 to H3, while
// run, handleAcquire, serveWaiters and warmup differ H2 to H1 and handleWarmup
// does not exist there at all. Both statements are true of different
// comparisons; neither refutes the other.
//
// Applying the bar above to those ten, method by method, only notifyClose
// clears it — and it did not even need a new helper: notifyConnClose was
// already here, serving the single-conn and transport close paths, while the
// three pools each carried their own byte-identical copy. They delegate to it
// now. The rest, and why they stay put:
//
//   - run is a select over SEVEN channels dispatching to SEVEN handlers, and
//     the channel element types differ per protocol. Sharing 25 lines would
//     mean injecting about fifteen things. This is the clearest failure of the
//     bar in the package.
//   - handleAcquire needs pickLeastLoaded, countLive, replyAcquire, dialOne,
//     two opts fields and the conn record's active/lastUsed — eight injections
//     for 23 lines, and the call site reads worse for it.
//   - serveWaiters, flushStrandedWaiters and handleStats are the same shape,
//     smaller: four to five injections for 9-15 lines.
//   - warmup is nine lines behind a three-argument signature and is shared by
//     only two of the three pools, H1 having gone straight to warmupDials. Too
//     marginal to be worth the indirection.
//   - Close is five lines over three fields. The signature would be the body.
//
// The ten identical bodies are identical BECAUSE the divergence was pushed
// into the helpers they call, and at least one of those differs in behaviour
// rather than name: spareStreamCapacity skips conns failing IsAlive, while
// h3SpareStreamCapacity also skips those draining after GOAWAY. Any future
// attempt at this must check countLive/h3CountLive and sumActive/h3SumActive
// the same way — a dedup that hands one protocol the other's liveness rule is
// a behaviour change wearing a refactor's clothes, and an invisible one,
// because those ten call sites read identically either way.

// dialEnv is the slice of a pool that a dial needs: when to give up, who to
// tell, and what to count. It exists so dialAttempt takes two arguments
// instead of six.
type dialEnv struct {
	closedCh <-chan struct{}
	timeout  time.Duration
	addr     string
	metrics  *Metrics
	hooksRef *atomic.Pointer[Hooks]
}

// dialAttempt runs everything around a pool dial that is the same in all three
// protocols: bound the attempt by DialTimeout, cancel it if the pool closes
// underneath, time it, count it, and report it to the caller's OnDial hook.
// The protocol-specific dial is the only injected part.
//
// The watchdog goroutine is what lets Close return promptly with a dial still
// in flight, and it retires on the deferred close(stopWatch) rather than
// living as long as the pool.
//
// dial runs INSIDE the timed section on purpose. HTTP/1.1 validates the
// negotiated ALPN there: a peer that answered h2 has cost a real dial, and the
// hook should see that attempt fail rather than see it succeed and the caller
// then get a DialError from nowhere.
//
// The timeout is applied unconditionally. Every pool constructor floors
// DialTimeout at 30s and none of them reassign opts afterwards, so the
// `> 0` guard the three copies carried could not be false.
func dialAttempt[C any](env dialEnv, dial func(context.Context) (C, error)) (C, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, dlCancel := context.WithTimeout(ctx, env.timeout)
	defer dlCancel()

	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-env.closedCh:
			cancel()
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)

	start := time.Now()
	env.metrics.Counters.DialsAttempted.Add(1)
	c, err := dial(ctx)
	dur := time.Since(start)

	env.metrics.Latency.Dial.Observe(dur)
	if err != nil {
		env.metrics.Counters.DialsFailed.Add(1)
	}
	if hr := env.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnDial != nil {
			h.OnDial(DialEvent{Addr: env.addr, Err: err, Duration: dur})
		}
	}
	return c, err
}

// defaultDialTimeout is the ceiling every transport puts on one dial attempt.
// The three pools floor PoolOptions.DialTimeout at this value; the
// single-connection transports use it when ClientOptions.DialTimeout is unset.
//
// It exists as one constant because the alternative was two answers to the same
// question: pooled dials were bounded and single-conn dials were not, so a
// black-hole host — one that completes the TCP handshake and then says nothing,
// or drops SYNs outright — hung a single-conn Do for as long as the caller's
// context allowed.
const defaultDialTimeout = 30 * time.Second

// dialTimeoutOrDefault applies defaultDialTimeout to a zero or negative value,
// matching what the pool constructors do with PoolOptions.DialTimeout.
func dialTimeoutOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultDialTimeout
	}
	return d
}

// dialCtx bounds a dial attempt by timeout, deriving from the caller's ctx so a
// cancelled request still aborts the dial promptly. The caller must call the
// returned cancel.
//
// A non-positive timeout means the default, NOT "already expired". The default
// is applied here rather than only at construction because these transports are
// also built directly — every test does — and a zero field turning every dial
// into an instant deadline is a failure mode worth making unreachable rather
// than documenting.
// dialObserved runs one dial with the observability every dial in this package
// carries — bounded by timeout, timed, counted, reported to OnDial — on the
// CALLER's context.
//
// It is dialAttempt's sibling for the single-connection transports, and the
// difference is the context, which is why they are not one function.
// dialAttempt deliberately roots its context at Background so a pool dial
// outlives the request that triggered it and is cancelled by the POOL closing;
// a single-conn transport dials for one caller, so cancelling that caller must
// cancel the dial. Merging them would have to pick one of those, and both are
// right for their own side.
//
// dial runs INSIDE the timed section on purpose, the same reason dialAttempt
// gives: HTTP/1.1 validates the negotiated ALPN there, and a peer that answered
// h2 has cost a real dial, so the hook should see that attempt fail rather than
// see it succeed and the caller then get a DialError from nowhere.
func dialObserved[C any](ctx context.Context, addr string, timeout time.Duration,
	metrics *Metrics, hooksRef *atomic.Pointer[Hooks], dial func(context.Context) (C, error),
) (C, error) {
	start := time.Now()
	metrics.Counters.DialsAttempted.Add(1)
	dctx, dcancel := dialCtx(ctx, timeout)
	c, err := dial(dctx)
	dcancel()
	dur := time.Since(start)
	metrics.Latency.Dial.Observe(dur)
	if err != nil {
		metrics.Counters.DialsFailed.Add(1)
	}
	if hooksRef != nil {
		if h := hooksRef.Load(); h != nil && h.OnDial != nil {
			h.OnDial(DialEvent{Addr: addr, Err: err, Duration: dur})
		}
	}
	return c, err
}

// notifyConnClose counts a closed connection and reports it to OnConnClose.
//
// Every transport that closes a connection calls this, which is the point: the
// hook's contract is "every close this client performs", and it was previously
// honoured by the pools and by the HTTP/3 single conn while the HTTP/2 and
// HTTP/1.1 single conns closed silently.
func notifyConnClose(addr string, reason CloseReason, metrics *Metrics, hooksRef *atomic.Pointer[Hooks]) {
	if metrics != nil {
		metrics.Counters.ConnsClosed.Add(1)
	}
	if hooksRef != nil {
		if h := hooksRef.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: addr, Reason: reason})
		}
	}
}

func dialCtx(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, dialTimeoutOrDefault(timeout))
}
