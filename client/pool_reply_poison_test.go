package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPool_ReplyChannelNotPoisonedUnderAbandonment is a regression test for
// the global replyPool channel-recycling race.
//
// Bug: replyAcquire sends on a buffered (cap-1) reply channel, so its select
// could take the send branch even when the caller's ctx was already cancelled.
// If the abandoning acquire recycled the channel to replyPool before the
// actor's late send landed, that send poisoned the channel for its next
// owner — a *different* Pool — surfacing as a spurious ErrPoolClosed (or a
// cross-pool conn). Fix: only recycle the reply channel when the actor can no
// longer send on it (first-select abandonment or happy-path receive).
//
// The test runs healthy "open" pools that must never observe ErrPoolClosed,
// concurrently with "victim" pools that queue waiters with cancelled contexts
// and then Close — handleClose replies ErrPoolClosed into those abandoned
// channels, which (pre-fix) would be recycled and poison an open pool.
//
// The three counters are load-bearing, not diagnostics. "No open pool saw
// ErrPoolClosed" is also true of a run in which no healthy acquire ever
// succeeded and no victim pool was ever churned — that run passes identically
// while testing nothing. healthyOK is the control arm (the pools really are
// serving), victimRounds and abandonedWaiters are the injection counts.
func TestPool_ReplyChannelNotPoisonedUnderAbandonment(t *testing.T) {
	addrs, _, cleanup := startH2Servers(t, 1)
	defer cleanup()
	addr := addrs[0].String()
	co := newConnOpts()

	const openPools = 8
	open := make([]*Pool, openPools)
	for i := range open {
		open[i] = newPool(addr, co, PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 50}, nil, nil)
	}
	defer func() {
		for _, p := range open {
			_ = p.Close()
		}
	}()

	stop := make(chan struct{})
	var poisoned atomic.Bool
	var healthyOK, victimRounds, abandonedWaiters atomic.Int64
	var wg sync.WaitGroup

	// Healthy acquirers on open pools — must never see ErrPoolClosed.
	for i := 0; i < openPools; i++ {
		p := open[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				mc, err := p.acquire(ctx)
				if err == nil {
					healthyOK.Add(1)
					p.release(mc)
				} else if errors.Is(err, ErrPoolClosed) {
					poisoned.Store(true)
					cancel()
					return
				}
				cancel()
			}
		}()
	}

	// Victim-pool churn: each victim occupies its single slot, queues waiters
	// that abandon immediately (cancelled ctx), then Closes — generating
	// ErrPoolClosed replies into abandoned channels.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			vp := newPool(addr, co, PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 1}, nil, nil)
			occCtx, occCancel := context.WithTimeout(context.Background(), time.Second)
			mc, err := vp.acquire(occCtx)

			var wwg sync.WaitGroup
			for k := 0; k < 8; k++ {
				wwg.Add(1)
				go func() {
					defer wwg.Done()
					wctx, wcancel := context.WithCancel(context.Background())
					go func() {
						time.Sleep(time.Millisecond)
						wcancel()
					}()
					_, _ = vp.acquire(wctx)
					abandonedWaiters.Add(1)
					wcancel()
				}()
			}
			time.Sleep(2 * time.Millisecond)
			if err == nil {
				vp.release(mc)
			}
			occCancel()
			_ = vp.Close()
			wwg.Wait()
			victimRounds.Add(1)
		}
	}()

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("control arm: %d healthy acquires served; injections fired: %d victim rounds, %d abandoned waiters",
		healthyOK.Load(), victimRounds.Load(), abandonedWaiters.Load())
	require.Positive(t, healthyOK.Load(),
		"no healthy acquire ever succeeded, so 'no pool saw ErrPoolClosed' says nothing — "+
			"the open pools were never serving in the first place")
	require.Positive(t, victimRounds.Load(),
		"no victim pool completed a churn round, so no ErrPoolClosed reply was ever "+
			"generated and the poisoning path was never exercised")
	require.Positive(t, abandonedWaiters.Load(),
		"no waiter was ever abandoned, so no reply channel was recycled while the actor "+
			"could still send on it — the race under test never had a chance to occur")
	require.False(t, poisoned.Load(),
		"healthy acquire on an open pool returned ErrPoolClosed: "+
			"reply channel was poisoned by a recycled channel from another pool")
}
