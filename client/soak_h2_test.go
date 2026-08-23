//go:build soak

package client

import (
	"context"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Soak test: the TCP path's sibling to the HTTP/3 pair in soak_test.go.
//
// The H3 soak exists because receive-path resource exhaustion was a bug class here
// (PRs #162-166). The TCP path has its own long-lived-connection state that nothing
// soaked — the stream registry, the pooled conn.Stream free list, the managed pool's
// sweep — and its bug history is that same shape: the pooled-stream reset class and
// the conn recycle race (#329). A pooled Stream that keeps one field of the previous
// response, or a recycle that races teardown, is invisible to a request-count
// assertion and shows up here as a footprint that grows with elapsed load (#649).
//
// The nearest thing before this was client/e2e_stress_test.go, which is tagged
// e2e_remote, points at a public host, and says in its own header that it is
// inherently flaky because the remote may RST_STREAM, GOAWAY or rate-limit at any
// time. That is a smoke test against the internet, not a leak gate.
//
// Run against the integration compose stack, which is already paid for:
//
//	make it-up
//	POSEIDON_SOAK_DURATION=120s POSEIDON_SOAK_WORKERS=64 \
//	  go test ./client/ -tags soak -run TestSoak_H2 -v -timeout 15m
//
// Or simply `make h2-soak`, which brings the stack up and tears it down. Off the PR
// path, like the H3 soak.
//
// Two transports, because they are different code: the single-connection transport
// never touches the pool's acquire/release path, which is where the H3 twin's slot
// leak lived.

// soakH2Addr is the h2c endpoint to soak. Undertow's cleartext port by default: h2c
// keeps TLS session state out of a test that is measuring the client's own
// footprint, so a growing heap cannot be blamed on the peer's handshake caching.
func soakH2Addr() string {
	if v := os.Getenv("POSEIDON_SOAK_H2_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:18082"
}

// soakH2Path is the endpoint driven under load. /healthz on purpose: a two-byte
// response keeps the wire cheap so the run is bounded by client work rather than by
// the peer. It used to also cite a 65535-byte budget on concurrent request bodies
// against nginx; that was never a peer budget, it was this client dropping the
// connection-level WINDOW_UPDATE nginx sends during the SETTINGS exchange (#701).
const soakH2Path = "/healthz"

// runH2Soak drives continuous concurrent GETs through c for POSEIDON_SOAK_DURATION
// and asserts that goroutine count and live heap return to within a bounded multiple
// of a mid-run steady-state baseline.
//
// The baseline is taken after a warmup rather than at t=0 deliberately: at t=0 the
// pool has not dialled, so a baseline there measures an idle client and every
// connection the run legitimately opens looks like growth.
func runH2Soak(t *testing.T, c *Client, label string) {
	t.Helper()
	dur := soakDuration()
	workers := soakWorkers()
	addr := soakH2Addr()
	t.Logf("soak: %s addr=%s duration=%s workers=%d", label, addr, dur, workers)

	doGet := func(ctx context.Context) error {
		var resp Response
		resp.Reset()
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return c.Do(rctx, &Request{
			Method: "GET", Path: soakH2Path, BodyMode: BodyBuffer,
		}, &resp)
	}

	// Prime the connection, retrying until the peer is listening. A soak that starts
	// against a server still booting spends its warmup on dial failures and takes its
	// baseline from a client that has not reached steady state.
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := doGet(context.Background()); err == nil {
			break
		}
		require.Falsef(t, time.Now().After(deadline),
			"soak: %s never became reachable — is the integration stack up? (make it-up)", addr)
		time.Sleep(500 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var reqs, errs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := doGet(ctx); err != nil {
					if ctx.Err() != nil {
						return // shutdown, not a failure
					}
					errs.Add(1)
					continue
				}
				reqs.Add(1)
			}
		}()
	}

	warmup := dur / 6
	if warmup < 5*time.Second {
		warmup = 5 * time.Second
	}
	if warmup > dur/2 {
		warmup = dur / 2
	}
	time.Sleep(warmup)
	baseGoroutines := runtime.NumGoroutine()
	baseHeap := heapInuse()
	baseReqs := reqs.Load()
	t.Logf("soak: baseline goroutines=%d heapInuse=%.1fMiB reqs=%d (after %s warmup)",
		baseGoroutines, float64(baseHeap)/(1<<20), baseReqs, warmup)

	sampleDone := make(chan struct{})
	go func() {
		tk := time.NewTicker(10 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				close(sampleDone)
				return
			case <-tk.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				t.Logf("soak: sample goroutines=%d heapInuse=%.1fMiB reqs=%d errs=%d",
					runtime.NumGoroutine(), float64(m.HeapInuse)/(1<<20), reqs.Load(), errs.Load())
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()
	<-sampleDone

	// Let reader and pool goroutines quiesce before measuring, so a goroutine that is
	// on its way out is not counted as one that never leaves.
	time.Sleep(2 * time.Second)
	finalGoroutines := runtime.NumGoroutine()
	finalHeap := heapInuse()

	total := reqs.Load()
	rps := float64(total) / dur.Seconds()
	t.Logf("soak: DONE %s reqs=%d errs=%d (%.0f req/s) | goroutines %d->%d | heapInuse %.1f->%.1f MiB",
		label, total, errs.Load(), rps, baseGoroutines, finalGoroutines,
		float64(baseHeap)/(1<<20), float64(finalHeap)/(1<<20))

	require.NotZero(t, total, "soak: zero successful requests")
	// The load actually has to continue past the baseline, or the ceilings below are
	// comparing a steady state against itself and would hold for a client that
	// stopped working entirely after warmup.
	require.Greaterf(t, total, baseReqs,
		"soak: no requests completed after the baseline (%d total, %d at baseline) — "+
			"the ceilings below would be comparing steady state against itself", total, baseReqs)
	assert.LessOrEqualf(t, errs.Load(), total/20, // >5% error rate is a red flag
		"soak: high error rate: %d errs / %d ok", errs.Load(), total)
	// Goroutine ceiling: steady state must not grow with elapsed load. A leak of one
	// goroutine per request would be orders of magnitude past this slack.
	assert.LessOrEqualf(t, finalGoroutines, baseGoroutines+workers+16,
		"soak: goroutine growth suggests a leak: baseline %d, final %d (workers=%d)",
		baseGoroutines, finalGoroutines, workers)
	// Heap ceiling: after a GC settle, live heap must return near the steady-state
	// baseline. 2x + 8 MiB tolerates fragmentation without hiding unbounded growth.
	//
	// This arm's sensitivity is duration-dependent and the 8 MiB term is why, which
	// matters when checking that the arm still works. Measured by retaining every
	// response body forever: at POSEIDON_SOAK_DURATION=15s the leak reached 6.1 ->
	// 13.9 MiB and PASSED, because the growth had not yet cleared the fixed slack;
	// at the 60s default the same leak reached 10.2 -> 50.5 MiB and failed. So a
	// short run cannot exercise this ceiling, and a leak that survives one is not
	// evidence the check is dead. Verify it at the default duration or longer.
	assert.LessOrEqualf(t, finalHeap, baseHeap*2+(8<<20),
		"soak: heap growth suggests a leak: baseline %.1fMiB, final %.1fMiB",
		float64(baseHeap)/(1<<20), float64(finalHeap)/(1<<20))
}

// TestSoak_H2ConnStability soaks the single-connection HTTP/2 transport, where one
// conn.Conn carries every request and its stream registry and pooled Stream free
// list are reused for the whole run.
func TestSoak_H2ConnStability(t *testing.T) {
	c, err := NewSingleConnClient(soakH2Addr(), &conn.PlaintextDialer{})
	require.NoErrorf(t, err, "NewSingleConnClient(%s)", soakH2Addr())
	t.Cleanup(func() { _ = c.Close() })
	runH2Soak(t, c, "H2ConnStability")
}

// TestSoak_H2PoolConnStability is the pooled twin, so the pool's acquire/release
// path is soaked too — the code the single-connection transport never touches, and
// where the H3 pair's slot leak lived. MaxConnsPerHost is above 1 so connection
// churn and the sweep participate rather than being reduced to one conn held open.
func TestSoak_H2PoolConnStability(t *testing.T) {
	c, err := NewPoolClient(soakH2Addr(), &conn.PlaintextDialer{}, PoolOptions{
		MaxConnsPerHost: 4,
	})
	require.NoErrorf(t, err, "NewPoolClient(%s)", soakH2Addr())
	t.Cleanup(func() { _ = c.Close() })
	runH2Soak(t, c, "H2PoolConnStability")
}
