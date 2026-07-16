package conn

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC7540_Sec68_RealPeerGoAwayPartition pins RFC 7540 §6.8
// against a real net/http2 peer that chooses the boundary itself:
//
//	"Endpoints SHOULD always send a GOAWAY frame before closing a connection so
//	 that the remote peer can know whether a stream has been partially
//	 processed or not... streams with an identifier at or below the last stream
//	 identifier are allowed to complete."
//
// The existing §6.8 unit tests (TestOnGoAway_*) drive c.onGoAwayReceived on a
// hand-built Conn with no transport, so the test picks lastStreamID and no
// stream ever has to actually finish. Here the peer picks it: x/net's server
// sends GOAWAY(sc.maxClientStreamID) — the highest stream id it has processed
// (x/net@v0.56.0/http2/server.go:1278, fed by processHeaders at :1903) — and
// for a NO_ERROR GOAWAY defers its own shutdown timer until every open stream
// has completed (server.go:908-914). So all six of our streams land at or below
// the boundary, and all six must complete over a live transport after the
// GOAWAY.
func TestConformance_RFC7540_Sec68_RealPeerGoAwayPartition(t *testing.T) {
	const N = 6

	var started sync.WaitGroup
	started.Add(N)
	proceed := make(chan struct{})

	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Done()
		<-proceed // hold every stream open across the GOAWAY
		w.WriteHeader(204)
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Open N concurrent streams. SendHeaders is what assigns the id (ids are
	// deferred until the HEADERS write, see conn/doc.go B.2.1), so collect the
	// ids afterwards rather than assuming them.
	streams := make([]*Stream, 0, N)
	for i := 0; i < N; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			close(proceed)
			t.Fatalf("NewStream %d: %v", i, err)
		}
		if err := s.SendHeaders(ctx, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true); err != nil {
			close(proceed)
			t.Fatalf("SendHeaders %d: %v", i, err)
		}
		streams = append(streams, s)
	}

	// CONTROL: every stream must be parked in a handler before we shut down.
	// This is what proves the peer has processed all six HEADERS, so the
	// boundary it picks is above them rather than below — and it rules out a
	// pass built on streams that had already finished and were never subject
	// to the partition at all.
	allStarted := make(chan struct{})
	go func() { started.Wait(); close(allStarted) }()
	select {
	case <-allStarted:
	case <-time.After(10 * time.Second):
		close(proceed)
		t.Fatal("handlers did not all reach the server; streams were never concurrently in-flight")
	}

	// Graceful shutdown: server emits GOAWAY(maxClientStreamID) and keeps the
	// conn open for the open streams.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		_ = srv.Config.Shutdown(context.Background())
	}()

	// Wait for the GOAWAY to be observed before releasing the handlers, so the
	// responses genuinely arrive on a post-GOAWAY connection.
	deadline := time.Now().Add(10 * time.Second)
	for !c.goAwayReceived.Load() {
		if time.Now().After(deadline) {
			close(proceed)
			t.Fatal("no GOAWAY observed within 10s of srv.Shutdown")
		}
		time.Sleep(5 * time.Millisecond)
	}
	last := c.goAwayLastStreamID.Load()

	// The peer chose the boundary. Derive the expectation from what it chose
	// and from the ids it actually assigned — never from a hardcoded number.
	maxSent := uint32(0)
	for _, s := range streams {
		if s.ID() > maxSent {
			maxSent = s.ID()
		}
	}
	if last != maxSent {
		t.Fatalf("peer GOAWAY lastStreamID = %d, want %d (= highest id we sent): the peer did not process all %d streams, so the partition below is not the one this test means to pin", last, maxSent, N)
	}

	// Every stream is at or below the boundary, so every one must survive:
	// still registered, not reset, and able to complete.
	c.smu.Lock()
	registered := len(c.streams)
	c.smu.Unlock()
	if registered != N {
		t.Fatalf("registered streams = %d, want %d: GOAWAY(last=%d) evicted streams at or below its own lastStreamID, violating RFC 7540 §6.8", registered, N, last)
	}

	close(proceed) // let the handlers respond

	for i, s := range streams {
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("stream %d (id=%d, lastStreamID=%d): Recv after GOAWAY: %v; §6.8 requires it to complete", i, s.ID(), last, err)
		}
		if ev.Type == EventReset {
			t.Fatalf("stream %d (id=%d) reset with code %v after GOAWAY(last=%d); id <= lastStreamID must complete, not reset", i, s.ID(), ev.RSTCode, last)
		}
		if ev.Type != EventHeaders {
			t.Fatalf("stream %d (id=%d): first event = %v, want EventHeaders", i, s.ID(), ev.Type)
		}
		var status string
		for _, f := range ev.Headers {
			if string(f.Name) == ":status" {
				status = string(f.Value)
			}
		}
		if status != "204" {
			t.Fatalf("stream %d (id=%d): status = %q, want 204", i, s.ID(), status)
		}
	}

	<-shutdownDone
}
