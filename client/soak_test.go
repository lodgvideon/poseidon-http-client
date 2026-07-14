//go:build soak

package client

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Soak test: sustained HTTP/3 load against a live interop server, asserting the
// managed client does not leak goroutines or heap across a long run. Receive-path
// resource exhaustion was a past bug class (PRs #162-166), so this guards the
// invariant that a long-lived pooled connection under continuous load returns to a
// bounded goroutine + heap footprint.
//
// Run inside the interop Docker network (reuses H3_INTEROP_ADDRS from the compose):
//
//	docker compose -f test/integration/http3/docker-compose.yml run --rm \
//	  -e POSEIDON_SOAK_DURATION=120s -e POSEIDON_SOAK_WORKERS=64 \
//	  runner go test ./client/ -tags soak -run TestSoak_H3 -v -timeout 10m

// soakServer picks the first H3_INTEROP_ADDRS entry, preferring caddy (quic-go,
// the most permissive peer for a long reuse run). Falls back to localhost:4433.
func soakServer() (name, addr string) {
	list := os.Getenv("H3_INTEROP_ADDRS")
	if list == "" {
		return "default", "localhost:4433"
	}
	for _, e := range strings.Split(list, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		n, a, ok := strings.Cut(e, "=")
		if !ok {
			a = n
		}
		if name == "" {
			name, addr = n, a
		}
		if n == "caddy" { // prefer caddy if present
			return n, a
		}
	}
	return name, addr
}

func soakDuration() time.Duration {
	if v := os.Getenv("POSEIDON_SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 60 * time.Second
}

func soakWorkers() int {
	if v := os.Getenv("POSEIDON_SOAK_WORKERS"); v != "" {
		n := 0
		ok := len(v) > 0
		for _, r := range v {
			if r < '0' || r > '9' {
				ok = false
				break
			}
			n = n*10 + int(r-'0')
		}
		if ok && n > 0 {
			return n
		}
	}
	return 32
}

// heapInuse returns live heap bytes after a GC settle, so a transient allocation
// spike is not mistaken for a leak.
func heapInuse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestSoak_H3ConnStability drives continuous concurrent GETs over one managed H3
// client for POSEIDON_SOAK_DURATION and asserts that, after the run and a GC
// settle, goroutine count and live heap return to within a bounded multiple of the
// post-warmup baseline. A per-request goroutine or buffer that is never released
// would grow without bound and trip these ceilings.
func TestSoak_H3ConnStability(t *testing.T) {
	name, addr := soakServer()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	dur := soakDuration()
	workers := soakWorkers()
	t.Logf("soak: server=%s addr=%s duration=%s workers=%d", name, addr, dur, workers)

	c, err := NewH3Client(addr,
		&tls.Config{ServerName: host, InsecureSkipVerify: true}) //nolint:gosec // interop dials self-signed
	if err != nil {
		t.Fatalf("NewH3Client(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })

	doGet := func(ctx context.Context) error {
		var resp Response
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return c.Do(rctx, &Request{
			Method: "GET", Scheme: "https", Authority: host, Path: "/", BodyMode: BodyBuffer,
		}, &resp)
	}

	// Prime the connection (retry until the server is listening).
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := doGet(context.Background()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("soak: server %s never became reachable", addr)
		}
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

	// Warm up, then snapshot the baseline mid-run (steady state, pool conns up).
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
	t.Logf("soak: baseline goroutines=%d heapInuse=%.1fMiB (after %s warmup)",
		baseGoroutines, float64(baseHeap)/(1<<20), warmup)

	// Periodic sampling for a visible trend in the log.
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

	// Let reader/pool goroutines quiesce, then measure.
	time.Sleep(2 * time.Second)
	finalGoroutines := runtime.NumGoroutine()
	finalHeap := heapInuse()

	total := reqs.Load()
	rps := float64(total) / dur.Seconds()
	t.Logf("soak: DONE reqs=%d errs=%d (%.0f req/s) | goroutines %d->%d | heapInuse %.1f->%.1f MiB",
		total, errs.Load(), rps, baseGoroutines, finalGoroutines,
		float64(baseHeap)/(1<<20), float64(finalHeap)/(1<<20))

	if total == 0 {
		t.Fatal("soak: zero successful requests")
	}
	if errs.Load() > total/20 { // >5% error rate is a red flag
		t.Errorf("soak: high error rate: %d errs / %d ok", errs.Load(), total)
	}
	// Goroutine ceiling: steady state should not grow with elapsed load. Allow
	// slack for scheduler jitter and pool churn; a leak (one goroutine/req) would
	// be orders of magnitude past this.
	if finalGoroutines > baseGoroutines+workers+16 {
		t.Errorf("soak: goroutine growth suggests a leak: baseline %d, final %d (workers=%d)",
			baseGoroutines, finalGoroutines, workers)
	}
	// Heap ceiling: after GC, live heap should return near the steady-state
	// baseline. 2x + 8 MiB tolerates fragmentation without hiding unbounded growth.
	if finalHeap > baseHeap*2+(8<<20) {
		t.Errorf("soak: heap growth suggests a leak: baseline %.1fMiB, final %.1fMiB",
			float64(baseHeap)/(1<<20), float64(finalHeap)/(1<<20))
	}
}
