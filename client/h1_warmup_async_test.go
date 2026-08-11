package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Client.Warmup, the transport interface and h1Pool.warmup's own doc all promise
// warm-up "opens up to n connections in the background, returning immediately".
// The H2 and H3 pools keep that by handing n to their actor. This one ran a
// Stats round-trip plus up to MaxConnsPerHost sequential acquires on the
// CALLER's goroutine, each bounded by the probe timeout — against a black-holed
// host at MaxConnsPerHost 64 that is over three seconds inside a call documented
// as immediate.

// TestH1Warmup_ReturnsImmediately is the gate. It measures the caller's time
// against a dialer that never completes, which is the case the contract exists
// for: a host that accepts nothing hurts most and is exactly when a blocking
// warm-up is least acceptable.
func TestH1Warmup_ReturnsImmediately(t *testing.T) {
	const conns = 16
	d := &hangingDialer{release: make(chan struct{})}
	p := newH1Pool("black.hole:0", d, PoolOptions{MaxConnsPerHost: conns}, nil, nil)
	defer func() { _ = p.Close() }()

	start := time.Now()
	p.warmup(conns)
	elapsed := time.Since(start)

	// Even one probe timeout is 50ms; the old shape paid that per connection.
	// A budget well under a single probe proves the call did not dial at all.
	if elapsed > 20*time.Millisecond {
		t.Errorf("warmup(%d) blocked the caller for %v — the contract, and this pool's own "+
			"doc, say it returns immediately; the H2 and H3 pools do", conns, elapsed)
	}
}

// TestH1Warmup_StillDials is the other half: returning immediately is worthless
// if it stopped warming. Against a live server the connections must appear.
func TestH1Warmup_StillDials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	const conns = 3
	p := newH1Pool(srv.Listener.Addr().String(), &conn.PlaintextDialer{}, PoolOptions{MaxConnsPerHost: conns}, nil, nil)
	defer func() { _ = p.Close() }()

	p.warmup(conns)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s := p.Stats(); s.ActiveConns >= conns {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	s := p.Stats()
	t.Errorf("after warmup(%d) the pool holds %d conns; the dialing goroutine did not run "+
		"or did not finish", conns, s.ActiveConns)
}

// TestH1Warmup_StopsWhenThePoolCloses guards the goroutine the fix introduces: a
// warm-up still dialing into a closed pool must give up rather than keep
// acquiring against an actor that is gone. It closes the pool while the warm-up
// is mid-flight and requires the goroutine to finish quickly.
func TestH1Warmup_StopsWhenThePoolCloses(t *testing.T) {
	d := &hangingDialer{release: make(chan struct{})}
	p := newH1Pool("black.hole:0", d, PoolOptions{MaxConnsPerHost: 32}, nil, nil)

	p.warmup(32)
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() { _ = p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return with a warm-up in flight")
	}
}
