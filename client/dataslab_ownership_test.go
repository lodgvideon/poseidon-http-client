package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// The DATA-slab ownership contract spans a package boundary: conn/handler.go
// OnData Gets a pooled buffer per DATA frame and transfers it to the client via
// StreamEvent.DataSlab; the client returns it through putDataSlab, from exactly
// one site per delivered slab. conn/stream.go states the invariant in prose
// ("exactly one return site per buffer ... rules out a double-Put") and nothing
// pins it. It cannot be pinned by the tools already here:
//
//   - sync.Pool accepts the same pointer twice and reports nothing;
//   - -race sees no data race, because a double-Put is not one — it just hands
//     one live buffer to two owners, and the damage lands later as one
//     request's body bytes appearing inside another's;
//   - conn/datapool_test.go deliberately withholds the Put, so it exercises the
//     conn half in isolation and can say nothing about the client's discipline.
//
// So these tests keep a per-pointer ledger across the real seam: Gets are
// observed where conn delivers them (a tap on the stream's Recv), Puts where
// the client makes them (a tap on putDataSlab), and the contract asserted is
// gets(p) == puts(p) for every slab — a leak fails it in one direction, a
// double-Put in the other.

// slabLedger records per-pointer Get/Put accounting for pooled DATA slabs.
//
// Pointers are reused by design — the whole point of the pool — so a slab can
// legitimately be Got and Put several times over a single response. Counting
// bare occurrences would therefore prove nothing; the ledger tracks the
// outstanding/returned state transition and requires the counts to balance.
type slabLedger struct {
	mu   sync.Mutex
	out  map[*[]byte]bool // delivered to us and not yet returned
	gets map[*[]byte]int
	puts map[*[]byte]int
	// foreign counts Puts of slabs this test never received. The pool is
	// process-global, so a stray goroutine left over from an earlier test could
	// in principle return a buffer while the hook is installed; those are not
	// this test's slabs and are excluded rather than mis-scored.
	foreign int
	errs    []string
}

func newSlabLedger() *slabLedger {
	return &slabLedger{
		out:  make(map[*[]byte]bool),
		gets: make(map[*[]byte]int),
		puts: make(map[*[]byte]int),
	}
}

// got records one slab taken out of the pool.
//
// It used to be called from a tap on the stream's Recv, inferring a Get from a
// DELIVERY. That inference is sound only when every Get is followed by a
// delivery, which holds for conn's OnData (it Gets once per DATA frame and
// immediately Puts back the only buffer it does not deliver, the
// push()-overflow path) but NOT for h1Exchange.Recv, which Gets its own buffer
// and, when ReadBodyChunk fails, correctly Puts it without delivering anything.
// Against a delivery-inferred ledger that Put scores as a double-Put — the pool
// hands the same pointer back, so the slab has an earlier delivery on record —
// and, in the other direction, an H1 buffer that leaked would balance and pass.
//
// Callers now tap the real Get (installSlabGetTap) so the ledger counts what
// actually happened. Delivery-side taps remain for the H2/H3 paths, where the
// Getter is on conn's side of the boundary and cannot be reached from here.
func (l *slabLedger) got(p *[]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.out[p] {
		// The pool handed out a buffer that is still outstanding — only
		// possible if it was Put while a consumer still owned it.
		l.errs = append(l.errs, fmt.Sprintf("slab %p delivered again while still outstanding (early Put)", p))
	}
	l.out[p] = true
	l.gets[p]++
}

func (l *slabLedger) put(p *[]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.out[p] {
		if l.gets[p] == 0 {
			l.foreign++
			return
		}
		l.errs = append(l.errs, fmt.Sprintf(
			"slab %p returned to the pool twice (double-Put): gets=%d, puts=%d",
			p, l.gets[p], l.puts[p]+1))
		return
	}
	delete(l.out, p)
	l.puts[p]++
}

// check asserts the ownership contract and returns the number of slab
// deliveries observed, so each test can state its own control on that count.
func (l *slabLedger) check(t *testing.T) int {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errs {
		assert.Fail(t, e)
	}
	deliveries := 0
	for p, g := range l.gets {
		deliveries += g
		assert.Equalf(t, g, l.puts[p],
			"slab %p: Got %d time(s), Put %d time(s) — want exactly one Put per Get",
			p, g, l.puts[p])
	}
	return deliveries
}

// installSlabPutTap routes every client-side DATA-slab return through led for
// the duration of the test, then restores the production closure. Safe because
// these tests are serial: a test that calls t.Parallel() parks until every
// serial test in the package has finished, so no parallel test body overlaps
// this one.
func installSlabPutTap(t *testing.T, led *slabLedger) {
	t.Helper()
	prev := putDataSlab
	putDataSlab = func(slab *[]byte) {
		led.put(slab)
		prev(slab)
	}
	t.Cleanup(func() { putDataSlab = prev })
}

// installSlabGetTap routes every client-side DATA-slab Get through led. Pair it
// with installSlabPutTap on any path where the CLIENT is the Getter — today
// that is h1Exchange.Recv — so the ledger records Gets that produce no delivery
// (the ReadBodyChunk error path) instead of inferring them from deliveries and
// mis-scoring the resulting Put. Same serial-execution safety argument as
// installSlabPutTap.
func installSlabGetTap(t *testing.T, led *slabLedger) {
	t.Helper()
	prev := getDataSlab
	getDataSlab = func() *[]byte {
		slab := prev()
		led.got(slab)
		return slab
	}
	t.Cleanup(func() { getDataSlab = prev })
}

// slabTapTransport wraps a Client's transport so the protoStream handed to
// drainResponse reports every DataSlab conn delivers. Needed for the buffered
// Do path, whose stream never escapes do().
type slabTapTransport struct {
	transport
	led *slabLedger
}

func (t *slabTapTransport) openExchange(ctx context.Context) (protoStream, pushLookuper, releaser, exchangeStats, error) {
	s, pushLookup, release, st, err := t.transport.openExchange(ctx)
	if err != nil {
		return nil, nil, nil, st, err
	}
	return slabTapStream{protoStream: s, led: t.led}, pushLookup, release, st, nil
}

type slabTapStream struct {
	protoStream
	led *slabLedger
}

func (s slabTapStream) Recv(ctx context.Context) (conn.StreamEvent, error) {
	ev, err := s.protoStream.Recv(ctx)
	if err == nil && ev.Type == conn.EventData && ev.DataSlab != nil {
		s.led.got(ev.DataSlab)
	}
	return ev, err
}

// slabTapRespStream is the respStream equivalent, installed onto
// StreamResponse.stream after DoStream returns. beginRespStream type-switches
// on the concrete *conn.Stream, so the transport tap cannot be used for the
// streaming paths — the wrapper has to go on after the switch has run.
type slabTapRespStream struct {
	respStream
	led *slabLedger
}

func (s slabTapRespStream) Recv(ctx context.Context) (conn.StreamEvent, error) {
	ev, err := s.respStream.Recv(ctx)
	if err == nil && ev.Type == conn.EventData && ev.DataSlab != nil {
		s.led.got(ev.DataSlab)
	}
	return ev, err
}

// slabFrames DATA frames of slabChunk bytes each. The count only has to exceed
// one: per-pointer accounting over a single frame degenerates to "one Get, one
// Put" and would pass against code that has no per-frame discipline at all.
// The exact number of frames on the wire is the server's choice (it may
// coalesce despite the Flush), so the tests assert a floor they set here, not
// an invented total.
const (
	slabChunk  = 8192
	slabFrames = 8
	slabTotal  = slabChunk * slabFrames
)

// TestIT_DataSlab_Do_MultiFrame_PutExactlyOnce pins the buffered Do path
// (BodyMode=BodyBuffer -> drainResponse -> handleDataEvent), the only DATA-slab
// return site that is not reachable from any exported handle: the stream lives
// and dies inside do().
func TestIT_DataSlab_Do_MultiFrame_PutExactlyOnce(t *testing.T) {
	led := newSlabLedger()
	installSlabPutTap(t, led)

	pattern := nonRepeatingPattern(slabTotal)
	c := poolTestClient(t, streamPatternServer(t, pattern, slabChunk))
	defer c.Close()
	c.tr = &slabTapTransport{transport: c.tr, led: led}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)

	require.NoError(t, err, "Do")
	deliveries := led.check(t)
	// Control 1: the response really was multi-frame, so the accounting saw a
	// sequence of slabs rather than a single trivially-balanced one.
	require.GreaterOrEqualf(t, deliveries, 2,
		"observed %d DATA slab deliveries, want >= 2 — the ledger cannot "+
			"distinguish per-frame ownership from a single-frame body", deliveries)
	// Control 2: the server actually spoke and the body survived the recycling.
	// Without this the whole test would pass just as well against a response
	// that carried no DATA at all — except that Control 1 also fails then, and
	// a corrupted-but-balanced body would slip past Control 1 alone.
	assertPattern(t, resp.Body, pattern)
}

// TestIT_DataSlab_DoStream_PutExactlyOnce pins the DoStream path
// (StreamResponse.Recv / .Close) across both terminations, which return the
// final slab from structurally different places:
//
//   - early-close: the caller abandons the body mid-stream and Close's
//     recycleData is the ONLY site that can return the last delivered slab.
//     Frames still buffered in the stream's event channel were never delivered,
//     so they are conn's to abandon (recycleStream drops them to GC by design)
//     and never enter this ledger.
//   - drain: Recv past EndStream returns the final slab, and the deferred Close
//     must not return it a second time.
//
// Both sub-tests Close twice (explicit + deferred), the shape real callers
// write, so Close idempotency is part of what is asserted.
func TestIT_DataSlab_DoStream_PutExactlyOnce(t *testing.T) {
	tests := []struct {
		name string
		// recv consumes the stream and returns how many EventData it saw.
		recv func(t *testing.T, ctx context.Context, sr *StreamResponse) int
	}{
		{
			name: "early close mid-body",
			recv: func(t *testing.T, ctx context.Context, sr *StreamResponse) int {
				const want = 3 // < slabFrames: the body is abandoned mid-stream
				for i := 0; i < want; i++ {
					ev, err := sr.Recv(ctx)

					require.NoErrorf(t, err, "Recv %d", i)
					require.Equalf(t, EventData, ev.Type, "Recv %d: type = %v, want EventData", i, ev.Type)
					require.Falsef(t, ev.EndStream, "Recv %d ended the stream early; the body is not "+
						"being abandoned mid-stream and the early-close path is untested", i)
				}

				err := sr.Close() // early close, mid-body

				require.NoError(t, err, "early Close")
				return want
			},
		},
		{
			name: "drain then close",
			recv: func(t *testing.T, ctx context.Context, sr *StreamResponse) int {
				n := 0
				for {
					ev, err := sr.Recv(ctx)
					if errors.Is(err, ErrStreamEnded) {
						return n
					}
					require.NoError(t, err, "Recv")
					if ev.Type == EventData {
						n++
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			led := newSlabLedger()
			installSlabPutTap(t, led)

			pattern := nonRepeatingPattern(slabTotal)
			c := poolTestClient(t, streamPatternServer(t, pattern, slabChunk))
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			var sr StreamResponse
			err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr)
			require.NoError(t, err, "DoStream")
			// DoStream has consumed only HEADERS (header slabs, not DataSlab) by
			// the time it returns, so no DATA delivery is missed by installing
			// the tap here.
			sr.stream = slabTapRespStream{respStream: sr.stream, led: led}
			defer sr.Close() // second Close: idempotency must not re-Put

			consumed := tc.recv(t, ctx, &sr)
			sr.Close()

			deliveries := led.check(t)
			// Control 1: the ledger saw exactly the slabs the caller saw. A tap
			// that silently missed deliveries would make "every slab Put once"
			// vacuously true.
			require.Equalf(t, consumed, deliveries,
				"ledger saw %d slab deliveries, the caller consumed %d EventData", deliveries, consumed)
			// Control 2: more than one frame, for the same reason as the Do test.
			require.GreaterOrEqualf(t, deliveries, 2,
				"observed %d DATA slab deliveries, want >= 2", deliveries)
			if led.foreign != 0 {
				t.Logf("note: %d Put(s) of slabs this test never received (cross-test pool traffic)", led.foreign)
			}
		})
	}
}
