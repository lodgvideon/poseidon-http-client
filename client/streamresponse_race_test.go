package client_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
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

	for iter := 0; iter < 12; iter++ {
		// Deliberately generous: the point is that Close ends the Recv, not the
		// deadline. If this context is what unparks the reader the test has
		// proved nothing, so the assertion below is far tighter than this.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		var sr client.StreamResponse
		if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
			cancel()
			t.Fatalf("iter %d DoStream: %v", iter, err)
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				if _, err := sr.Recv(ctx); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			_ = sr.Close()
		}()
		go func() { wg.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(8 * time.Second):
			cancel()
			t.Fatalf("iter %d: Recv still parked 8s after Close;\n"+
				"Close tore the stream down but never woke the reader, so an abort "+
				"through DoStream leaves the goroutine hanging until the caller's own deadline", iter)
		}
		cancel()
	}
}
