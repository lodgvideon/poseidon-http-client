package conn

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConn_AbandonCloseDuringResponse_NoRace is a -race regression guard for a
// client abandoning a stream mid-response: the request half-closes with its
// HEADERS (localEnded true from the start), the caller reads exactly one
// event, then calls Close() WITHOUT draining to END_STREAM.
//
// This is the shape that matters: a caller that keeps Recv-ing until
// EndStream lets remoteEnded become true strictly before Close() runs, so
// Close's bothEnded branch and the reader's terminal delivery never
// overlap. A caller that gives up early — the common "cancel because I only
// wanted the headers" or "the caller errored out" pattern — races Close()
// against the reader goroutine that is still delivering DATA/trailers for
// the exact same *Stream while the connection's single reader loop keeps
// running for every other concurrently open stream. recycleStream mutates
// s.events (and every other field) with NO s.mu held; the reader's
// markStreamDone / push touch the same fields under s.mu. Two unsynchronized
// writers (recycle vs. the reader) — or a locked reader racing an unlocked
// writer — is exactly what -race is built to catch.
//
// The server writes headers, a body, an explicit Flush, and then a trailer,
// so the response arrives to the client as three distinct frames (HEADERS,
// DATA, HEADERS-trailers) instead of one coalesced write — widening the
// window between the caller's single Recv and the reader's eventual
// terminal delivery.
func TestConn_AbandonCloseDuringResponse_NoRace(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Trailer", "x-done")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("0123456789012345678901234567890123")) // 34 bytes
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		w.Header().Set("x-done", "1")
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer func() { _ = c.Close() }()

	const workers = 32
	const itersPerWorker = 60

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < itersPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

				s, err := c.NewStream(ctx)
				if err != nil {
					cancel()
					continue
				}
				if err := s.SendHeaders(ctx, []hpack.HeaderField{
					{Name: []byte(":method"), Value: []byte("GET")},
					{Name: []byte(":scheme"), Value: []byte("https")},
					{Name: []byte(":authority"), Value: []byte("example.com")},
					{Name: []byte(":path"), Value: []byte("/")},
				}, true); err != nil {
					_ = s.Close()
					cancel()
					continue
				}

				// Exactly one Recv, then abandon — no drain to END_STREAM.
				_, _ = s.Recv(ctx)
				_ = s.Close()
				cancel()
			}
		}()
	}
	wg.Wait()
}
