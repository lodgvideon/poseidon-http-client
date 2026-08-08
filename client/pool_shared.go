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
