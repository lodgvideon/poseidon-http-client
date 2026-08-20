package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// h1Exchange is allocated once per request by every H1 transport's
// openExchange, so anything inline in it is paid per request. It used to carry
// a 16 KiB scratch array for ReadBodyChunk, which by bytes — not by allocation
// count, where it looks like an unremarkable single object — was 69.5% of
// everything the client allocated under load.
//
// TestH1_AllocatedBytesPerRequest owns the end-to-end number; this one owns the
// structural cause, because it fails on the line that reintroduces the field
// rather than on a statistic several layers away. reflect rather than unsafe:
// the size is all that is being asked for.
func TestH1Exchange_HasNoInlineScratchBuffer(t *testing.T) {
	// Generous: the struct is 3 words + 2 bools today. The bound is not a
	// budget to spend, it is a tripwire on "somebody inlined an array again".
	const maxSize = 128

	sz := reflect.TypeOf(h1Exchange{}).Size()

	assert.LessOrEqualf(t, sz, uintptr(maxSize),
		"h1Exchange is %d bytes, want <= %d — it is heap-allocated once per request, so "+
			"an inline buffer here is a per-request cost. Scope scratch memory to the "+
			"pooled connection or to conn's DATA-payload pool instead.", sz, maxSize)
}

// h1PatternServer is the HTTP/1.1 twin of streamPatternServer: it flushes the
// pattern in chunk-sized writes (chunked transfer-coding on the wire) so the
// client crosses many body-read boundaries and therefore many pooled buffers.
func h1PatternServer(t *testing.T, pattern []byte, chunk int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		for off := 0; off < len(pattern); off += chunk {
			end := off + chunk
			if end > len(pattern) {
				end = len(pattern)
			}
			_, _ = w.Write(pattern[off:end])
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// h1RawServer starts a listener that speaks HTTP/1.1 by hand and returns its
// address. respond is called once per request, after the request head has been
// consumed; when keepAlive is false the connection is closed as soon as it
// returns.
//
// Two things httptest cannot do for the tests below. A response body that stops
// short of its own Content-Length is not expressible through net/http's Handler
// interface at all. And net/http's server allocates per request in the SAME
// process the allocation benchmark measures — AllocedBytesPerOp is a
// process-wide TotalAlloc delta — so its bytes would land inside the assertion
// and the gate would move with the Go toolchain rather than with this client.
// Reading the head through bufio.ReadSlice returns slices into the reader's own
// buffer, and the response is precomputed by the caller, so this server's
// steady-state contribution is essentially the write syscall.
func h1RawServer(t *testing.T, respond func(net.Conn) error, keepAlive bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return // listener closed by cleanup
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				rd := bufio.NewReaderSize(c, 4096)
				for {
					// Consume the request head: lines until a bare CRLF. The tests
					// here issue only bodyless GETs.
					for {
						line, rerr := rd.ReadSlice('\n')
						if rerr != nil {
							return
						}
						if len(line) <= 2 {
							break
						}
					}
					if respond(c) != nil || !keepAlive {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func h1PoolTestClient(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH1Pool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2},
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	require.NoError(t, err, "NewClient")
	return c
}

// h1SlabTotal is large enough that a 16 KiB read buffer cannot swallow the body
// in one go, so every test here observes a sequence of distinct pooled buffers
// rather than a single trivially-balanced one.
const (
	h1SlabChunk = 8192
	h1SlabTotal = h1SlabChunk * 64 // 512 KiB
)

// TestIT_DataSlab_H1_Do_MultiChunk_PutExactlyOnce extends the DATA-slab
// ownership ledger (see dataslab_ownership_test.go) to HTTP/1.1, which only
// joined the pooled-buffer contract when h1Exchange.Recv stopped copying each
// chunk into a fresh slice. H1 reaches the pool from the OTHER side of the seam
// than H2 does — the client Gets the buffer itself instead of receiving one conn
// Got — so "exactly one Put per Get" is a claim about new code here, not a re-run
// of the H2 assertion.
//
// Only the buffered Do path is exercised because it is the only one H1 has:
// beginRespStream rejects h1Exchange with ErrStreamingUnsupported, so
// drainResponse -> handleDataEvent is the sole consumer of an H1 DataSlab.
// The ledger taps the real Get here (installSlabGetTap) rather than inferring
// one per delivery the way the H2 tests must. On the H1 path the client is the
// Getter, so the inference would be wrong in both directions — see slabLedger
// .got. That also makes the sibling truncated-body test below possible at all.
func TestIT_DataSlab_H1_Do_MultiChunk_PutExactlyOnce(t *testing.T) {
	led := newSlabLedger()
	installSlabGetTap(t, led)
	installSlabPutTap(t, led)

	pattern := nonRepeatingPattern(h1SlabTotal)
	c := h1PoolTestClient(t, h1PatternServer(t, pattern, h1SlabChunk))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response

	err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)

	require.NoError(t, err, "Do over a multi-chunk body")
	gets := led.check(t)
	// Control 1: the ledger saw a sequence of slabs. Per-pointer accounting over a
	// single Get degenerates to "one Get, one Put" and would pass against code
	// with no per-chunk discipline at all.
	require.GreaterOrEqualf(t, gets, 2,
		"observed %d DATA slab Gets, want >= 2 — the ledger cannot distinguish "+
			"per-chunk ownership from a single-chunk body", gets)
	// Control 2: the body survived being reassembled out of recycled buffers.
	// Without this the ledger would pass just as well against a Recv that handed
	// back one buffer every time and corrupted every response.
	assertPattern(t, resp.Body, pattern)
}

// TestIT_DataSlab_H1_TruncatedBody_PutExactlyOnce covers h1Exchange.Recv's ERROR
// branch, which is the one place in the client where a slab is Got and then
// returned without ever being delivered. Nothing exercised it: the other H1
// tests all drive responses that complete, so the branch shipped untested and
// its Put was invisible to a ledger that counted deliveries.
//
// The server declares a Content-Length it does not satisfy and hangs up, so
// ReadBodyChunk fails with a premature-EOF error partway through the body. By
// then several chunks have been Got, delivered and Put through the SAME pooled
// pointer, so the final undelivered Get/Put lands on a pointer with history —
// exactly the case a delivery-inferred ledger mis-scores as a double-Put.
func TestIT_DataSlab_H1_TruncatedBody_PutExactlyOnce(t *testing.T) {
	led := newSlabLedger()
	installSlabGetTap(t, led)
	installSlabPutTap(t, led)

	const (
		declared = 500000
		sent     = 6 * 8192
	)
	addr := h1RawServer(t, func(c net.Conn) error {
		hdr := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", declared)
		if _, err := c.Write([]byte(hdr)); err != nil {
			return err
		}
		_, err := c.Write(make([]byte, sent))
		return err // caller closes the conn: body ends short of Content-Length
	}, false)

	c := h1PoolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response

	err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)

	require.Error(t, err, "Do succeeded on a body truncated short of its Content-Length; "+
		"the error branch under test never ran")
	// Name the mechanism: only a premature-EOF framing error proves h1Exchange.Recv's
	// ERROR branch is what ran, rather than some earlier failure standing in for it.
	require.ErrorContainsf(t, err, "premature EOF",
		"Do error = %v, want a premature-EOF framing error — some other failure got "+
			"here first and the Recv error branch may not have run", err)
	gets := led.check(t)
	// The undelivered Get is the point of the test: without it the run is just a
	// shorter version of the multi-chunk test.
	require.GreaterOrEqualf(t, gets, 2,
		"observed %d DATA slab Gets, want >= 2 (several delivered chunks plus the "+
			"undelivered one from the failing read)", gets)
}

// TestH1_ConcurrentReuse_NoCrossRequestBleed is the dynamic twin of the ledger.
// H1's EventData.Data used to be a private copy the caller could hold
// indefinitely; it is now a pooled buffer valid only until the next Recv or
// Close. The ledger catches an early return statically, by accounting; this one
// catches what an early return DOES — the pool is process-global, so a buffer
// handed back while a consumer is still copying out of it is immediately
// available to another in-flight request, which reads its own body over the
// top.
//
// Concurrency is the whole point: sequential requests cannot expose an early
// Put, because the same goroutine finishes copying before it can possibly Get
// the buffer again. Workers therefore exceed MaxConnsPerHost so requests
// genuinely overlap, and each response carries a per-request marker byte so a
// buffer that came from a different request cannot pass as this one's. Under
// -race the same overlap is also a reportable write/read race on the buffer.
func TestH1_ConcurrentReuse_NoCrossRequestBleed(t *testing.T) {
	const (
		workers    = 8 // > MaxConnsPerHost: requests overlap and contend for the pool
		perWorker  = 12
		bodySize   = 40 * 1024 // > one 16 KiB read: several chunks per response
		markerBase = 'a'
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		marker := r.URL.Path[len("/")]
		body := make([]byte, bodySize)
		for i := range body {
			body[i] = marker
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := h1PoolTestClient(t, srv.Listener.Addr().String())
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			marker := byte(markerBase + w) // stable per worker: any foreign byte is a bleed
			for i := 0; i < perWorker; i++ {
				var resp Response
				resp.Reset()
				// assert, never require: this runs off the test goroutine, where
				// require's FailNow is illegal.
				if err := c.Do(ctx, &Request{
					Method: "GET", Path: "/" + string(marker), BodyMode: BodyBuffer,
				}, &resp); err != nil {
					assert.NoErrorf(t, err, "worker %d request %d: Do", w, i)
					return
				}
				if len(resp.Body) != bodySize {
					assert.Lenf(t, resp.Body, bodySize, "worker %d request %d: wrong body length", w, i)
					return
				}
				for j, b := range resp.Body {
					if b != marker {
						assert.Equalf(t, marker, b,
							"worker %d request %d: body[%d] = %q, want %q — a pooled buffer was "+
								"returned while still owned, so another request wrote over this "+
								"response's bytes", w, i, j, b, marker)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestH1_AllocatedBytesPerRequest is the regression test for the reported
// symptom itself: bytes allocated per request on the H1 path. It measures the
// whole Do round trip rather than any one call site, so it also fails if the
// 16 KiB comes back somewhere other than h1Exchange — a per-request make() in a
// pool, a transport, or the exchange's read path.
//
// Bytes, not allocation count: by count this bug was ~1 object/request and
// looked unremarkable, which is exactly why it survived.
//
// The peer is h1RawServer, not httptest, and that is load-bearing rather than
// incidental. AllocedBytesPerOp is a process-wide TotalAlloc delta, so an
// in-process net/http server puts its own per-request allocations inside the
// assertion: the figure stops being "what the client allocates", the budget is
// shared with the standard library, and a Go release that changes net/http's
// server profile fails this test with a message accusing h1Exchange. Against a
// precomputed-response raw server the number is the client's, and the threshold
// can be set where it actually bites.
func TestH1_AllocatedBytesPerRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation measurement needs a benchmark run")
	}
	const (
		bodySize = 2048
		// Split by build tag and set from measurement — see h1_allocgate_norace_test.go.
		// Deliberately far below the pre-fix floor rather than just below it: a limit
		// near 16 KiB would only catch a regression that reintroduced the whole scratch
		// array, and would wave through a smaller per-request buffer held behind a
		// pointer, which the struct-size test cannot see either.
		maxBytesPerOp = h1AllocGateLimit
	)
	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	// Precomputed once: the server's steady-state work is one Write.
	canned := append([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", bodySize)), body...)
	addr := h1RawServer(t, func(nc net.Conn) error {
		_, werr := nc.Write(canned)
		return werr
	}, true)

	c := h1PoolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}
	// Warm the connection pool, the DATA-slab pool and resp's own buffers, so the
	// measurement covers steady state rather than first-request setup.
	var warm Response
	for i := 0; i < 4; i++ {
		warm.Reset()
		require.NoError(t, c.Do(ctx, req, &warm), "warmup Do")
	}

	var failed error
	res := testing.Benchmark(func(b *testing.B) {
		var resp Response
		resp.Reset()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			resp.Reset()
			if err := c.Do(ctx, req, &resp); err != nil {
				failed = err
				return
			}
		}
	})
	// Every assertion is OUTSIDE the measured closure above: testify reflects and
	// allocates, and AllocedBytesPerOp is a process-wide TotalAlloc delta, so one
	// require inside the benchmark body would be charged to the client.
	require.NoError(t, failed, "Do during measurement")
	require.NotZero(t, res.N, "benchmark performed no iterations; nothing was measured")
	got := res.AllocedBytesPerOp()
	assert.LessOrEqualf(t, got, int64(maxBytesPerOp),
		"H1 allocates %d bytes/request over %d requests, want <= %d — a per-request "+
			"buffer of kilobyte scale is back on the H1 path (%d allocs/request)",
		got, res.N, maxBytesPerOp, res.AllocsPerOp())
	t.Logf("H1: %d bytes/request, %d allocs/request over %d requests", got, res.AllocsPerOp(), res.N)
}
