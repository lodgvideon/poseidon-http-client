package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

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
	if sz := reflect.TypeOf(h1Exchange{}).Size(); sz > maxSize {
		t.Fatalf("h1Exchange is %d bytes, want <= %d — it is heap-allocated once per "+
			"request, so an inline buffer here is a per-request cost. Scope scratch "+
			"memory to the pooled connection or to conn's DATA-payload pool instead.", sz, maxSize)
	}
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

func h1PoolTestClient(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportH1Pool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2},
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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
func TestIT_DataSlab_H1_Do_MultiChunk_PutExactlyOnce(t *testing.T) {
	led := newSlabLedger()
	installSlabPutTap(t, led)

	pattern := nonRepeatingPattern(h1SlabTotal)
	c := h1PoolTestClient(t, h1PatternServer(t, pattern, h1SlabChunk))
	defer c.Close()
	c.tr = &slabTapTransport{transport: c.tr, led: led}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	deliveries := led.check(t)
	// Control 1: the ledger saw a sequence of slabs. Per-pointer accounting over a
	// single delivery degenerates to "one Get, one Put" and would pass against
	// code with no per-chunk discipline at all.
	if deliveries < 2 {
		t.Fatalf("observed %d DATA slab deliveries, want >= 2 — the ledger cannot "+
			"distinguish per-chunk ownership from a single-chunk body", deliveries)
	}
	// Control 2: the body survived being reassembled out of recycled buffers.
	// Without this the ledger would pass just as well against a Recv that handed
	// back one buffer every time and corrupted every response.
	assertPattern(t, resp.Body, pattern)
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
				if err := c.Do(ctx, &Request{
					Method: "GET", Path: "/" + string(marker), BodyMode: BodyBuffer,
				}, &resp); err != nil {
					t.Errorf("worker %d request %d: Do: %v", w, i, err)
					return
				}
				if len(resp.Body) != bodySize {
					t.Errorf("worker %d request %d: body is %d bytes, want %d", w, i, len(resp.Body), bodySize)
					return
				}
				for j, b := range resp.Body {
					if b != marker {
						t.Errorf("worker %d request %d: body[%d] = %q, want %q — a pooled buffer "+
							"was returned while still owned, so another request wrote over this "+
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
// looked unremarkable, which is exactly why it survived. Allocation counting is
// deterministic, so the only noise here is the race detector's own overhead;
// the threshold clears both modes by a wide margin (measured, 2 KiB response:
// 4.3 KB plain / 9.5 KB under -race after the fix, 24.9 KB / 25.8 KB before).
// It is a tripwire on a kilobyte-scale regression, not a budget to tune against.
func TestH1_AllocatedBytesPerRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation measurement needs a benchmark run")
	}
	const (
		bodySize = 2048
		// h1BodyChunkSize is the natural bound, and says what the test means: one
		// request must not allocate as much as the scratch buffer it used to carry
		// inline. Anything at that scale is per-request memory that belongs in a
		// pool.
		maxBytesPerOp = h1BodyChunkSize
	)
	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte(i % 251)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	c := h1PoolTestClient(t, srv.Listener.Addr().String())
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}
	// Warm the connection pool, the DATA-slab pool and resp's own buffers, so the
	// measurement covers steady state rather than first-request setup.
	var warm Response
	for i := 0; i < 4; i++ {
		warm.Reset()
		if err := c.Do(ctx, req, &warm); err != nil {
			t.Fatalf("warmup Do: %v", err)
		}
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
	if failed != nil {
		t.Fatalf("Do during measurement: %v", failed)
	}
	if res.N == 0 {
		t.Fatal("benchmark performed no iterations; nothing was measured")
	}
	if got := res.AllocedBytesPerOp(); got > maxBytesPerOp {
		t.Fatalf("H1 allocates %d bytes/request over %d requests, want <= %d — "+
			"a per-request buffer of kilobyte scale is back on the H1 path "+
			"(%d allocs/request)", got, res.N, maxBytesPerOp, res.AllocsPerOp())
	} else {
		t.Logf("H1: %d bytes/request, %d allocs/request over %d requests", got, res.AllocsPerOp(), res.N)
	}
}
