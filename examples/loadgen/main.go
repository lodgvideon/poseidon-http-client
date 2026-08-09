// Command loadgen is a minimal load generator built on the poseidon client. It
// spins up a pooled client against a single target, fans out N worker goroutines
// that issue GET requests for a fixed duration under an optional global QPS cap,
// then prints a latency + outcome summary derived from the client's built-in
// MetricsSnapshot.
//
// -transport selects h2 (the default) or h3. The h3 arm exists for the
// syscall-floor measurement in #361: HTTP/3 puts one UDP socket behind each
// connection, so the many-connections-times-small-requests regime — where
// nothing coalesces and GSO/GRO have nothing to amortise — is only reachable
// there.
//
// Example:
//
//	go run ./examples/loadgen -url https://localhost:8443/ \
//	    -conns 4 -workers 64 -duration 30s -rps 5000
//
//	go run ./examples/loadgen -transport h3 -url https://localhost:443/ \
//	    -conns 10000 -workers 256 -duration 30s
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	var (
		target    = flag.String("url", "https://localhost:8443/", "target URL (https://host:port/path)")
		conns     = flag.Int("conns", 4, "max connections in the pool")
		workers   = flag.Int("workers", 64, "concurrent worker goroutines")
		duration  = flag.Duration("duration", 30*time.Second, "test duration")
		rps       = flag.Float64("rps", 0, "global request rate cap (0 = unlimited)")
		transport = flag.String("transport", "h2", "transport: h2 or h3")
	)
	flag.Parse()
	if *transport != "h2" && *transport != "h3" {
		log.Fatalf("loadgen: -transport must be h2 or h3, got %q", *transport)
	}

	u, err := url.Parse(*target)
	if err != nil {
		log.Fatalf("loadgen: bad -url %q: %v", *target, err)
	}
	addr := u.Host // "host:port"
	path := u.RequestURI()

	// Count dial outcomes via a hook so the summary can report how many
	// physical connections the pool actually established.
	var dials, dialErrs atomic.Int64
	hooks := &client.Hooks{
		OnDial: func(ev client.DialEvent) {
			dials.Add(1)
			if ev.Err != nil {
				dialErrs.Add(1)
				log.Printf("loadgen: dial %s failed: %v", ev.Addr, ev.Err)
			}
		},
	}

	// InsecureSkipVerify keeps the example self-contained against a
	// self-signed test server; drop it for real targets.
	dialer := &conn.TLSDialer{
		Config: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // example/test target
	}

	pool := client.PoolOptions{
		MaxConnsPerHost:   *conns,
		MaxStreamsPerConn: 250,
		HealthCheckPeriod: 5 * time.Second,
	}

	var c *client.Client
	if *transport == "h3" {
		// TransportH3Pool owns its own QUIC dialing, so it takes the TLS config
		// directly rather than a conn.Dialer.
		//
		// ServerName is set explicitly. Without it the ClientHello carries no
		// SNI, and a server that selects its certificate by name closes the
		// handshake — which looks like a QUIC fault rather than a missing field.
		host := u.Hostname()
		c, err = client.NewClient(client.ClientOptions{
			Addr:      addr,
			Transport: client.TransportH3Pool,
			Pool:      &pool,
			TLSConfig: &tls.Config{
				ServerName:         host,
				InsecureSkipVerify: true, //nolint:gosec // example/test target
			},
			Hooks:              hooks,
			RateLimitPerSecond: *rps,
		})
	} else {
		c, err = client.NewPoolClient(addr, dialer, pool,
			client.WithRateLimit(*rps, 0),
			client.WithHooks(hooks),
		)
	}
	if err != nil {
		log.Fatalf("loadgen: build %s client: %v", *transport, err)
	}
	defer func() { _ = c.Close() }()

	// Pre-dial so the first requests don't pay the handshake.
	c.Warmup(*conns)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	log.Printf("loadgen: hitting %s via %s — %d workers, %d conns, %s, rps=%.0f",
		path, addr, *workers, *conns, *duration, *rps)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go func() {
			defer wg.Done()
			worker(ctx, c, path)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	printSummary(c.MetricsSnapshot(), elapsed, dials.Load(), dialErrs.Load())
}

// worker issues GET requests in a tight loop until ctx is cancelled,
// reusing a single Response across iterations to keep per-request
// allocations off the hot path.
func worker(ctx context.Context, c *client.Client, path string) {
	resp := &client.Response{}
	for ctx.Err() == nil {
		resp.Reset()
		// GET sets WantBody so the body is drained (flow-control refunds run).
		if err := c.Do(ctx, client.GET(path), resp); err != nil {
			// Context cancellation at end-of-test is expected; other errors
			// are already tallied in RequestsErrored by the client.
			if ctx.Err() != nil {
				return
			}
			continue
		}
	}
}

// printSummary renders the MetricsSnapshot as a human-readable report.
func printSummary(m client.MetricsSnapshot, elapsed time.Duration, dials, dialErrs int64) {
	cs := m.Counters
	lat := m.Latency.Request

	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	throughput := float64(cs.RequestsStarted) / secs

	log.Printf("==== loadgen summary (%.1fs) ====", secs)
	log.Printf("connections:     %d dialed (%d failed)", dials, dialErrs)
	log.Printf("requests started: %d (%.0f req/s)", cs.RequestsStarted, throughput)
	log.Printf("  2xx:            %d", cs.Responses2xx)
	log.Printf("  non-2xx:        %d", cs.ResponsesNon2xx)
	log.Printf("  errored:        %d", cs.RequestsErrored)
	log.Printf("latency mean:     %s", lat.Mean())
	log.Printf("latency p50:      %s", lat.Quantile(0.50))
	log.Printf("latency p99:      %s", lat.Quantile(0.99))
}
