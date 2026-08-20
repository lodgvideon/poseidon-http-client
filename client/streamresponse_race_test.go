package client_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/stretchr/testify/require"
)

// TestStreamResponse_ConcurrentRecvCloseIsSafe is the DoStream half of #370.
//
// #393 fixed the same two defects on Response.BodyReader and deliberately left
// StreamResponse out, on the grounds that it is a concrete type with no
// io.ReadCloser convention pulling callers into a concurrent Close. #370's own
// exposure note undercuts that: DoStream IS the client streaming API, and
// closing to cancel is how a caller aborts a stream it no longer wants.
//
// StreamResponse.Close still does both of the things that were wrong there:
//
//   - it calls recycleData(), handing the pooled DATA slab back while a
//     concurrent Recv may still be holding curData or have handed the caller a
//     StreamEvent.Data that aliases it. Reported by -race at putDataSlab.
//   - it calls stream.Close(), which does not wake a Recv already parked on the
//     event channel. The abort returns promptly and the goroutine behind it does
//     not, so the caller hangs until its own deadline.
//
// Like its sibling this asserts nothing about content. It exists to fail under
// -race, and to fail by TIMEOUT if the abort does not abort.
//
// What it must NOT do is pass because the race window never opened. Recv and
// Close only collide while events are actually in flight, so both sides are
// counted: recvs is how many events the reader took before the abort landed, and
// closes is how many aborts ran. A run where the server stalled, or where Recv
// returned an error on its first call, would leave recvs at zero and otherwise
// look exactly like a clean pass — the assertion below rejects that.
func TestStreamResponse_ConcurrentRecvCloseIsSafe(t *testing.T) {
	chunk := make([]byte, 32*1024)
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		for i := 0; i < 16; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(time.Millisecond)
		}
	}))
	c := clientFor(t, addr)
	const iters = 12
	var recvs, closes atomic.Int64

	for iter := 0; iter < iters; iter++ {
		// Deliberately generous: the point is that Close ends the Recv, not the
		// deadline. If this context is what unparks the reader the test has
		// proved nothing, so the assertion below is far tighter than this.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		var sr client.StreamResponse
		err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)
		if err != nil {
			cancel()
			require.NoErrorf(t, err, "iter %d DoStream", iter)
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				if _, rerr := sr.Recv(ctx); rerr != nil {
					return
				}
				recvs.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			_ = sr.Close()
			closes.Add(1)
		}()
		go func() { wg.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(8 * time.Second):
			cancel()
			require.Failf(t, "Recv still parked 8s after Close",
				"iter %d: Close tore the stream down but never woke the reader, so an abort "+
					"through DoStream leaves the goroutine hanging until the caller's own "+
					"deadline", iter)
		}
		cancel()
	}

	t.Logf("injections: %d aborts over %d iterations, %d events received before them",
		closes.Load(), iters, recvs.Load())
	require.Equalf(t, int64(iters), closes.Load(),
		"only %d of %d aborts ran; the Close side of the race did not fire", closes.Load(), iters)
	require.Positivef(t, recvs.Load(),
		"no Recv returned an event across %d iterations, so Close never overlapped a live "+
			"reader and this test cannot have observed the race it exists for", iters)
}
