package conn

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// TestStream_CloseDuringTerminalDelivery_NoRace drives the window between the
// reader marking a stream's remote side ended and delivering the event that
// ended it.
//
// The request half-closes with its HEADERS, so localEnded is true from the
// start and the stream becomes bothEnded the instant the reader sets
// remoteEnded. A Close() landing in that window used to observe bothEnded and
// call recycleStream, which rewrites s.Stream().events and zeroes every field — while
// the reader was still pushing into the same struct, and after the struct had
// been handed back to the pool for another request to claim.
//
// Run under -race. The failure is a data race on s.Stream().events, not a wrong result,
// so a plain run passes either way.
//
// It is a stress test on the invariant, not a reproducer: the window is narrow
// enough that this loop does not reliably trip the old code even on Linux. The
// race was caught by the grpc package's suite, whose consumer does real work
// between the reader's END_STREAM and its Close. Kept because it is cheap and
// exercises exactly the pair of operations the fix serialises.
func TestStream_CloseDuringTerminalDelivery_NoRace(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer func() { _ = c.Close() }()

	// A context per iteration, not one for the whole loop. With a shared 60 s
	// deadline the drain below parks on the first stream its own Close
	// cancelled and stays there until the deadline expires — so the test ran
	// for a full minute and the remaining 59 iterations asserted nothing,
	// having no time budget left. Per-iteration, all 60 actually execute and
	// the whole test finishes in a fraction of a second.
	for i := 0; i < 60; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s, err := c.NewStream(ctx)
		if err != nil {
			cancel()
			require.NoErrorf(t, err, "iteration %d: NewStream", i)
		}
		// endStream=true: the send side is done before any response arrives, so
		// remoteEnded alone decides bothEnded.
		if herr := s.SendHeaders(ctx, []header.Field{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true); herr != nil {
			cancel()
			require.NoErrorf(t, herr, "iteration %d: SendHeaders", i)
		}

		// Close and drain race each other, both off the main goroutine, so the
		// drain can be released as soon as Close has returned. Draining inline
		// instead — as this loop used to — parks on a stream nothing will ever
		// deliver to again and only leaves when the context expires, which is
		// what made the whole test cost its entire deadline and left the
		// remaining iterations asserting nothing. Errors are expected and
		// uninteresting: the reader touching a recycled struct is the failure
		// this test looks for, and -race is what reports it.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		drained := make(chan struct{})
		go func() {
			defer wg.Done()
			defer close(drained)
			for {
				ev, err := s.Recv(ctx)
				if err != nil || ev.EndStream {
					return
				}
			}
		}()
		select {
		case <-drained:
		case <-time.After(50 * time.Millisecond):
			// The drain is parked on a stream that is already gone; cancelling
			// is how a real caller would release it.
			cancel()
		}
		wg.Wait()
		cancel()
	}
}

// TestStream_CloseAfterDrain_StillRecycles guards the other half of the fix:
// delivering the terminal event under the same lock that sets remoteEnded must
// not make the flag arrive late. If it did, a Close() straight after reading
// END_STREAM would observe bothEnded as false and emit a pointless
// RST_STREAM(CANCEL) on a stream that ended cleanly.
func TestStream_CloseAfterDrain_StillRecycles(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")
	require.NoError(t, s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")

	for {
		ev, rerr := s.Recv(ctx)
		require.NoError(t, rerr, "Recv")
		if ev.EndStream {
			break
		}
	}
	s.Stream().mu.Lock()
	remoteEnded, localEnded := s.Stream().remoteEnded, s.Stream().localEnded
	s.Stream().mu.Unlock()
	assert.True(t, remoteEnded, "remoteEnded false after the consumer read END_STREAM — Close would send a needless CANCEL")
	assert.True(t, localEnded, "localEnded false after SendHeaders(endStream=true)")
	assert.NoError(t, s.Close(), "Close after a clean drain must be a no-op, not an error")
}

// TestStream_RecycleUnderInFlightRecv_NoRace is the deterministic counterpart
// to the stress test above, and covers the holder that the appClosed/connDone
// handshake does not know about: the application's own reader.
//
// The handshake settles which of Close and markStreamDone pools the struct, on
// the premise that once both are done nobody is left holding it. A goroutine
// sitting in Recv is holding it — Recv blocks on s.Stream().events and s.resetSignal, so
// it must read those fields outside the mutex, and recycleStream rewrites both.
// Close racing an in-flight Recv is not a caller retaining a stream "past
// Close"; it is one goroutine cancelling another's read, which is ordinary.
//
// Where the stress test hopes to hit the window, this one constructs it: the
// reader is parked in the select before the recycle is driven, so the two
// accesses are ordered and -race reports every time. Against the pre-fix code
// it failed on the first iteration with two races, at conn/stream.go:243
// (s.Stream().events) and :268 (s.resetSignal), which is exactly the pair CI reported.
func TestStream_RecycleUnderInFlightRecv_NoRace(t *testing.T) {
	// The handler sends its header block and then holds the stream open, so
	// the reader below consumes exactly one event and is then parked in the
	// select with nothing to wake it. A handler that completed the response
	// would let the reader finish before the recycle is driven, and the test
	// would assert nothing while still passing.
	handlerDone := make(chan struct{})
	defer close(handlerDone)
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-handlerDone
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")
	require.NoError(t, s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")

	// gotHeaders fires once the reader has consumed the header block, so the
	// wait below is on an observed state rather than on a duration.
	gotHeaders := make(chan struct{})
	readerOut := make(chan struct{})
	readerCtx, readerCancel := context.WithCancel(context.Background())
	defer readerCancel()
	go func() {
		defer close(readerOut)
		first := true
		for {
			ev, err := s.Recv(readerCtx)
			if first {
				first = false
				close(gotHeaders)
			}
			if err != nil || ev.EndStream {
				return
			}
		}
	}()
	select {
	case <-gotHeaders:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "reader never saw the response headers")
	}
	// One event consumed, none left: the next Recv is parked in the select.
	time.Sleep(100 * time.Millisecond)

	// Drive Close down the connDone branch, the one that pools the struct
	// immediately. Setting the flag by hand is what makes this deterministic:
	// the real handshake reaches the same state, just not on cue.
	s.Stream().mu.Lock()
	s.Stream().connDone = true
	s.Stream().closed = false
	s.Stream().mu.Unlock()
	_ = s.Close()

	// The recycle must have been DEFERRED, not merely serialised. Taking the
	// mutex around the channel snapshot is enough to silence -race, so without
	// this assertion the deferral could be deleted and the test would still
	// pass — while the struct sat in the pool, available to another request,
	// with a reader still holding it and still about to touch resetCode and
	// recvActive on whatever lifetime owned it by then.
	s.Stream().mu.Lock()
	pooled := s.Stream().w == nil
	s.Stream().mu.Unlock()
	require.False(t, pooled, "stream was reset for the pool while a reader was inside Recv")

	// Release the reader; the recycle it is owed must then run.
	readerCancel()
	select {
	case <-readerOut:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "reader never returned: the deferred recycle must not block it")
	}

	// And the deferral must not lose the recycle: the last reader out owes it.
	s.Stream().mu.Lock()
	recycled := s.Stream().w == nil && s.Stream().id == 0
	s.Stream().mu.Unlock()
	assert.True(t, recycled, "deferred recycle never ran; the stream leaked out of the pool")
}
