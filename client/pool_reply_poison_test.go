package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
					p.release(mc, nil)
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
					wcancel()
				}()
			}
			time.Sleep(2 * time.Millisecond)
			if err == nil {
				vp.release(mc, nil)
			}
			occCancel()
			_ = vp.Close()
			wwg.Wait()
		}
	}()

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()

	if poisoned.Load() {
		t.Fatal("healthy acquire on an open pool returned ErrPoolClosed: " +
			"reply channel was poisoned by a recycled channel from another pool")
	}
}
