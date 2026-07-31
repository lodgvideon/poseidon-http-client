package conn

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestStream_CloseDuringTerminalDelivery_NoRace drives the window between the
// reader marking a stream's remote side ended and delivering the event that
// ended it.
//
// The request half-closes with its HEADERS, so localEnded is true from the
// start and the stream becomes bothEnded the instant the reader sets
// remoteEnded. A Close() landing in that window used to observe bothEnded and
// call recycleStream, which rewrites s.events and zeroes every field — while
// the reader was still pushing into the same struct, and after the struct had
// been handed back to the pool for another request to claim.
//
// Run under -race. The failure is a data race on s.events, not a wrong result,
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 60; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			t.Fatalf("iteration %d: NewStream: %v", i, err)
		}
		// endStream=true: the send side is done before any response arrives, so
		// remoteEnded alone decides bothEnded.
		if err := s.SendHeaders(ctx, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true); err != nil {
			t.Fatalf("iteration %d: SendHeaders: %v", i, err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		// Drain concurrently with the Close above. Errors are expected and
		// uninteresting — the reader touching a recycled struct is the failure
		// this test looks for, and -race is what reports it.
		for {
			ev, err := s.Recv(ctx)
			if err != nil || ev.EndStream {
				break
			}
		}
		wg.Wait()
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.EndStream {
			break
		}
	}
	s.mu.Lock()
	remoteEnded, localEnded := s.remoteEnded, s.localEnded
	s.mu.Unlock()
	if !remoteEnded {
		t.Fatal("remoteEnded false after the consumer read END_STREAM — Close would send a needless CANCEL")
	}
	if !localEnded {
		t.Fatal("localEnded false after SendHeaders(endStream=true)")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
