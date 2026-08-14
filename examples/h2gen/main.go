// Command h2gen is a framer-shaped HTTP/2 load generator built directly on
// package conn, and it exists as the acceptance test for a seam set rather than
// as a tool: if it needs anything conn does not export, the seams are wrong.
//
// The architecture is ozontech/framer's, with the parts poseidon's own
// measurements refuted left out (docs/H2_RAW_FRAMES_DESIGN.md §7a). Per
// connection:
//
//   - N worker goroutines each open a stream, hand a request to the sender, and
//     wait for the response.
//   - ONE sender goroutine coalesces whatever requests have arrived — up to
//     -batch of them — into a single conn.SendBatch call. That is one write(2),
//     and over TLS one record, for the whole group.
//
// The sender is the whole point. Without it every request costs its own
// transport write, which at load-generator concurrency is the largest single
// item in the CPU profile. -batch 1 turns the coalescing off and is the control
// to measure against; the summary prints writes/req either way, which is the
// number the design is about.
//
// What is deliberately NOT here, because it belongs to a generator and not to a
// library: the rate schedule, the request-file format, the report format, and
// the timeout policy. What is deliberately not in conn either: caller-built
// frame bytes — see the "Why this takes requests and not frames" note on
// conn.SendBatch.
//
// Example:
//
//	go run ./examples/h2gen -url http://localhost:8080/ -conns 2 -workers 64 \
//	    -batch 32 -duration 10s
//
//	go run ./examples/h2gen -url https://localhost:8443/ -insecure -batch 1
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/header"
)

func main() {
	var (
		target   = flag.String("url", "http://localhost:8080/", "target URL")
		conns    = flag.Int("conns", 1, "connections to open")
		workers  = flag.Int("workers", 64, "concurrent in-flight requests per connection")
		batch    = flag.Int("batch", 32, "max requests coalesced into one transport write (1 disables)")
		linger   = flag.Duration("linger", 200*time.Microsecond, "how long the sender waits to fill a batch")
		duration = flag.Duration("duration", 10*time.Second, "test duration")
		bodySize = flag.Int("body", 64, "request body bytes")
		bufSize  = flag.Int("writebuf", 0, "conn write buffer bytes (0 = 16 KiB default)")
		insecure = flag.Bool("insecure", false, "skip TLS certificate verification")
	)
	flag.Parse()

	u, err := url.Parse(*target)
	if err != nil {
		log.Fatalf("h2gen: bad -url %q: %v", *target, err)
	}
	if *batch < 1 || *workers < 1 || *conns < 1 {
		log.Fatal("h2gen: -batch, -workers and -conns must all be >= 1")
	}

	counter := &writeCounter{}
	opts := conn.ConnOptions{
		Dialer:          countingDialer{inner: dialerFor(u, *insecure), c: counter},
		WriteBufferSize: *bufSize,
		// Our own gate on in-flight streams is min(what we advertise, what the
		// peer does); the default 100 would turn a 256-worker run into an
		// ErrTooManyStreams storm before the peer ever became the constraint.
		Settings: conn.AdvertisedSettings{MaxConcurrentStreams: 16384},
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	stats := &stats{}
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *conns; i++ {
		c, derr := conn.Dial(context.Background(), addrOf(u), opts)
		if derr != nil {
			log.Fatalf("h2gen: dial %s: %v", addrOf(u), derr)
		}
		defer func() { _ = c.Close() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			runConn(ctx, c, u, *workers, *batch, *linger, *bodySize, stats)
		}()
	}
	wg.Wait()
	report(time.Since(start), counter, stats)
}

// --- the sender: many streams, one write.

// pending is one worker's request waiting to be coalesced. done carries the
// send outcome back, so the worker learns whether to expect a response.
type pending struct {
	ref    conn.StreamRef
	fields []header.Field
	body   []byte
	done   chan error
}

// runConn wires one connection's workers to its single sender.
func runConn(ctx context.Context, c *conn.Conn, u *url.URL, workers, batch int,
	linger time.Duration, bodySize int, st *stats,
) {
	queue := make(chan *pending, workers)
	var senderDone sync.WaitGroup
	senderDone.Add(1)
	go func() {
		defer senderDone.Done()
		sender(ctx, c, queue, batch, linger)
	}()

	body := make([]byte, bodySize)
	fields := requestFields(u)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One reply channel per worker, reused for every request it makes:
			// a channel per request would allocate at the full request rate.
			p := &pending{fields: fields, body: body, done: make(chan error, 1)}
			for ctx.Err() == nil {
				if err := oneRequest(ctx, c, queue, p, st); err != nil {
					if ctx.Err() != nil {
						return
					}
					st.fail(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(queue)
	senderDone.Wait()
}

// sender is framer's batchedProcessor: take whatever is queued, up to a bound,
// and put it on the wire in one call.
//
// It waits only to FILL a batch, never for flow control or for another writer —
// SendBatch refuses an entry it cannot pay for rather than blocking, which is
// what keeps one slow stream from stalling the group.
func sender(ctx context.Context, c *conn.Conn, queue <-chan *pending, batch int, linger time.Duration) {
	entries := make([]conn.BatchEntry, 0, batch)
	waiting := make([]*pending, 0, batch)
	timer := time.NewTimer(linger)
	defer timer.Stop()

	flush := func() {
		if len(entries) == 0 {
			return
		}
		err := c.SendBatch(ctx, entries)
		for i := range waiting {
			if e := entries[i].Err; e != nil {
				waiting[i].done <- e
			} else {
				waiting[i].done <- err
			}
		}
		entries = entries[:0]
		waiting = waiting[:0]
	}

	for {
		p, ok := <-queue
		if !ok {
			flush()
			return
		}
		entries = append(entries, conn.BatchEntry{
			Stream: p.ref, Fields: p.fields, Body: p.body, EndStream: true,
		})
		waiting = append(waiting, p)

		// Fill the batch with whatever else is already queued, then give
		// stragglers `linger` to arrive. Both bounds matter: without the size
		// bound a fast producer starves the flush, and without the time bound a
		// slow one leaves the first request sitting in the buffer.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(linger)
	fill:
		for len(entries) < batch {
			select {
			case p, ok = <-queue:
				if !ok {
					flush()
					return
				}
				entries = append(entries, conn.BatchEntry{
					Stream: p.ref, Fields: p.fields, Body: p.body, EndStream: true,
				})
				waiting = append(waiting, p)
			case <-timer.C:
				break fill
			case <-ctx.Done():
				break fill
			}
		}
		flush()
	}
}

// oneRequest is the worker side: open a stream, queue it, wait for the sender,
// then drain the response and throw it away.
func oneRequest(ctx context.Context, c *conn.Conn, queue chan<- *pending, p *pending, st *stats) error {
	ref, err := c.NewStream(ctx)
	if err != nil {
		return err
	}
	p.ref = ref
	started := time.Now()

	select {
	case queue <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err = <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err != nil {
		// ErrNoCredit is the only outcome that leaves the request half-made:
		// the HEADERS went out, the body did not. Finish it the blocking way.
		if !errors.Is(err, conn.ErrNoCredit) {
			return err
		}
		if err = ref.SendData(ctx, p.body, true); err != nil {
			return err
		}
		st.noCredit.Add(1)
	}

	for {
		ev, rerr := ref.Recv(ctx)
		if rerr != nil {
			return rerr
		}
		// conn hands the pooled header block and payload buffer to the caller and
		// does NOT reclaim them. A generator written straight against conn has to
		// return them itself; one that forgets pays a fresh allocation per header
		// block and per DATA frame. Release covers the headers; the payload buffer
		// goes back by POINTER, not slice, or it escapes to the heap.
		ev.Release()
		if ev.DataSlab != nil {
			conn.GetDataBufPool().Put(ev.DataSlab)
		}
		if ev.EndStream {
			break
		}
	}
	st.ok(time.Since(started))
	return ref.Close()
}

func requestFields(u *url.URL) []header.Field {
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	return []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte(scheme)},
		{Name: []byte(":authority"), Value: []byte(u.Host)},
		{Name: []byte(":path"), Value: []byte(u.RequestURI())},
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
		{Name: []byte("te"), Value: []byte("trailers")},
	}
}

// --- measurement.

// writeCounter tallies transport writes. One write on h2c is one syscall; over
// TLS it is additionally one record. Both are what batching spends less of, so
// writes/req is the metric this program exists to print.
type writeCounter struct {
	writes atomic.Int64
	bytes  atomic.Int64
}

type countingConn struct {
	net.Conn
	c *writeCounter
}

func (c countingConn) Write(p []byte) (int, error) {
	c.c.writes.Add(1)
	c.c.bytes.Add(int64(len(p)))
	return c.Conn.Write(p)
}

type countingDialer struct {
	inner conn.Dialer
	c     *writeCounter
}

func (d countingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	nc, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: nc, c: d.c}, nil
}

// AssertsALPN forwards the wrapped dialer's ALPN guarantee, so wrapping a
// dialer to count its writes does not lose the pairing check
// conn.ALPNAsserter exists for.
func (d countingDialer) AssertsALPN() string {
	if a, ok := d.inner.(conn.ALPNAsserter); ok {
		return a.AssertsALPN()
	}
	return ""
}

type stats struct {
	mu        sync.Mutex
	latencies []time.Duration
	done      atomic.Int64
	noCredit  atomic.Int64
	failures  atomic.Int64
	firstErr  error
}

func (s *stats) ok(d time.Duration) {
	s.done.Add(1)
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *stats) fail(err error) {
	s.failures.Add(1)
	s.mu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.mu.Unlock()
}

func report(elapsed time.Duration, wc *writeCounter, s *stats) {
	done := s.done.Load()
	fmt.Printf("requests    %d in %v (%.0f rps)\n", done, elapsed.Round(time.Millisecond),
		float64(done)/elapsed.Seconds())
	if done > 0 {
		fmt.Printf("writes/req  %.3f        <- the number batching moves\n",
			float64(wc.writes.Load())/float64(done))
		fmt.Printf("bytes/req   %.1f\n", float64(wc.bytes.Load())/float64(done))
	}
	s.mu.Lock()
	lat := s.latencies
	firstErr := s.firstErr
	s.mu.Unlock()
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		fmt.Printf("latency     p50 %v  p99 %v  max %v\n",
			pct(lat, 0.50).Round(time.Microsecond),
			pct(lat, 0.99).Round(time.Microsecond),
			lat[len(lat)-1].Round(time.Microsecond))
	}
	if n := s.noCredit.Load(); n > 0 {
		fmt.Printf("no-credit   %d (%.2f%%) finished with a blocking SendData\n",
			n, 100*float64(n)/float64(max(done, 1)))
	}
	if n := s.failures.Load(); n > 0 {
		fmt.Printf("failures    %d, first: %v\n", n, firstErr)
		os.Exit(1)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func dialerFor(u *url.URL, insecure bool) conn.Dialer {
	if u.Scheme == "https" {
		return &conn.TLSDialer{Config: &tls.Config{
			ServerName: u.Hostname(),
			NextProtos: []string{"h2"},
			// -insecure is the caller's explicit choice, for the self-signed
			// servers under test/integration.
			InsecureSkipVerify: insecure,
		}}
	}
	return &conn.PlaintextDialer{}
}

func addrOf(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "https" {
		return net.JoinHostPort(u.Hostname(), "443")
	}
	return net.JoinHostPort(u.Hostname(), "80")
}
