package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
//
// The dialStarted latch is the injection counter, and it is not decoration: a
// warm-up that dialed NOTHING would return just as fast and pass the timing
// assertion for the wrong reason. Requiring the dial to have actually begun is
// what makes "fast" mean "asynchronous" rather than "absent". The measured
// elapsed time is logged either way, so a future reader can see the margin
// rather than trusting the bound.
func TestH1Warmup_ReturnsImmediately(t *testing.T) {
	const conns = 16
	d := &hangingDialer{release: make(chan struct{}), dialStarted: make(chan struct{})}
	p := newH1Pool("black.hole:0", d, PoolOptions{MaxConnsPerHost: conns}, nil, nil)
	defer func() { _ = p.Close() }()

	start := time.Now()
	p.Warmup(conns)
	elapsed := time.Since(start)

	// Even one probe timeout is 50ms; the old shape paid that per connection.
	// A budget well under a single probe proves the call did not dial at all.
	t.Logf("warmup(%d) returned in %v (budget %v, one probe timeout is %v)",
		conns, elapsed, 20*time.Millisecond, h1WarmupProbeTimeout)
	assert.LessOrEqualf(t, elapsed, 20*time.Millisecond,
		"warmup(%d) blocked the caller for %v — the contract, and this pool's own "+
			"doc, say it returns immediately; the H2 and H3 pools do", conns, elapsed)
	select {
	case <-d.dialStarted:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "warmup started no dial at all",
			"returning immediately is only the right answer if the dialing moved to "+
				"another goroutine; a warm-up that dials nothing passes the timing "+
				"assertion above for the wrong reason")
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

	p.Warmup(conns)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && p.Stats().ActiveConns < conns {
		time.Sleep(2 * time.Millisecond)
	}
	assert.GreaterOrEqualf(t, p.Stats().ActiveConns, conns,
		"after warmup(%d) the pool holds too few conns; the dialing goroutine did not run "+
			"or did not finish", conns)
}

// TestH1Warmup_StopsWhenThePoolCloses guards the goroutine the fix introduces: a
// warm-up still dialing into a closed pool must give up rather than keep
// acquiring against an actor that is gone. It closes the pool while the warm-up
// is mid-flight and requires the goroutine to finish quickly.
func TestH1Warmup_StopsWhenThePoolCloses(t *testing.T) {
	d := &hangingDialer{release: make(chan struct{}), dialStarted: make(chan struct{})}
	p := newH1Pool("black.hole:0", d, PoolOptions{MaxConnsPerHost: 32}, nil, nil)
	p.Warmup(32)
	// Close must land with the warm-up genuinely in flight, or this measures
	// nothing: wait for the first dial rather than sleeping a guessed interval.
	select {
	case <-d.dialStarted:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "warmup never started a dial",
			"the close-during-warmup race cannot be expressed if no warm-up is in flight")
	}

	done := make(chan struct{})
	go func() { _ = p.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Close did not return with a warm-up in flight",
			"the warm-up goroutine kept acquiring against an actor that is gone")
	}
}
