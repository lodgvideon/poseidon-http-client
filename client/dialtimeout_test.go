package client

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Every pooled transport bounds a dial attempt at DialTimeout. The
// single-connection transports did not: they dialed on the caller's context
// only, so a host that accepts the connection and then says nothing — or one
// that swallows SYNs — held a Do open for as long as that context allowed.
// Same lifecycle concept, a different answer depending on which transport the
// caller happened to pick.
//
// These pin the bound, and pin that a zero value means the default rather than
// an instantly-expired deadline, which is the way this fix could have made
// things worse.

// TestSingleConn_DialIsBoundedByDialTimeout is the gate. With a short
// DialTimeout the dial must give up on its own, even though the caller's
// context has a much longer deadline — that difference is the whole point.
func TestSingleConn_DialIsBoundedByDialTimeout(t *testing.T) {
	s := &singleConn{
		addr:        "black.hole:0",
		connOpts:    conn.ConnOptions{Dialer: &hangingDialer{release: make(chan struct{})}},
		metrics:     &Metrics{},
		dialTimeout: 150 * time.Millisecond,
	}

	// Far longer than the dial timeout: if the bound is missing, this is what
	// the dial waits for instead.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, _, err := s.acquireConn(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("acquireConn against a black-hole host returned no error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("the dial ran for %v against a 150ms DialTimeout — it is bounded by the "+
			"caller's context instead, so a black-hole host hangs Do for as long as the "+
			"caller allows", elapsed)
	}
	if ctx.Err() != nil {
		t.Error("the caller's context expired, so this measured the caller's deadline " +
			"rather than the dial timeout")
	}
}

// TestSingleConn_ZeroDialTimeoutMeansDefault guards the hazard the fix
// introduces. Every one of these transports is also constructed directly — the
// whole test suite does it — so a zero field must not mean "deadline already
// passed". If it did, every hand-built transport would fail its first dial.
func TestSingleConn_ZeroDialTimeoutMeansDefault(t *testing.T) {
	srv := startOneH2Server(t)
	defer srv.Close()

	s := &singleConn{
		addr:     srv.Listener.Addr().String(),
		connOpts: newConnOpts(),
		metrics:  &Metrics{},
		// dialTimeout deliberately left zero.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, _, err := s.acquireConn(ctx)
	if err != nil {
		t.Fatalf("acquireConn with a zero dialTimeout: %v — zero must mean the default, "+
			"not an expired deadline", err)
	}
	if c == nil {
		t.Fatal("acquireConn returned no connection and no error")
	}
	_ = s.close()
}

// TestDialTimeoutOrDefault pins the defaulting rule itself, including that a
// caller-supplied value survives.
func TestDialTimeoutOrDefault(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{0, defaultDialTimeout},
		{-time.Second, defaultDialTimeout},
		{time.Second, time.Second},
		{time.Hour, time.Hour},
	}
	for _, tc := range cases {
		if got := dialTimeoutOrDefault(tc.in); got != tc.want {
			t.Errorf("dialTimeoutOrDefault(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestSingleConn_DialTimeoutSurfacesAsAnError checks the failure the caller
// actually sees, so the bound does not silently become a nil conn with no error.
func TestSingleConn_DialTimeoutSurfacesAsAnError(t *testing.T) {
	s := &singleConn{
		addr:        "black.hole:0",
		connOpts:    conn.ConnOptions{Dialer: &hangingDialer{release: make(chan struct{})}},
		metrics:     &Metrics{},
		dialTimeout: 100 * time.Millisecond,
	}
	_, _, err := s.acquireConn(context.Background())
	if err == nil {
		t.Fatal("no error from a dial that timed out")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("dial error is %v", err) // wrapped in a *DialError; the type is the contract
	}
}

// The bound above is pinned for the HTTP/2 single connection only, and the fix
// it guards landed in all three single-connection transports. That asymmetry is
// how this family of divergences starts: one sibling carries the test, the other
// two carry the behaviour until somebody edits them.
//
// So the same gate, for the other two.

// TestH1SingleConn_DialIsBoundedByDialTimeout is the HTTP/1.1 sibling.
func TestH1SingleConn_DialIsBoundedByDialTimeout(t *testing.T) {
	s := &h1singleConn{
		addr:        "black.hole:0",
		dialer:      &hangingDialer{release: make(chan struct{})},
		metrics:     &Metrics{},
		dialTimeout: 150 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, _, _, err := s.openExchange(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("openExchange against a black-hole host returned no error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("the dial ran for %v against a 150ms DialTimeout — it is bounded by the "+
			"caller's context instead", elapsed)
	}
	if ctx.Err() != nil {
		t.Error("the caller's context expired, so this measured the caller's deadline " +
			"rather than the dial timeout")
	}
}

// TestSingleH3Conn_DialIsBoundedByDialTimeout is the HTTP/3 sibling. It injects
// a dialFn that blocks until its context is done, which is what a black-holed
// QUIC handshake looks like from here.
func TestSingleH3Conn_DialIsBoundedByDialTimeout(t *testing.T) {
	s := &singleH3Conn{
		addr:        "black.hole:0",
		metrics:     &Metrics{},
		dialTimeout: 150 * time.Millisecond,
		dialFn: func(ctx context.Context, _ string, _ *tls.Config) (h3Client, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	_, derr := s.dial(ctx)
	elapsed := time.Since(start)

	if derr == nil {
		t.Fatal("dial against a black-hole host returned no error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("the dial ran for %v against a 150ms DialTimeout — it is bounded by the "+
			"caller's context instead", elapsed)
	}
	if ctx.Err() != nil {
		t.Error("the caller's context expired, so this measured the caller's deadline " +
			"rather than the dial timeout")
	}
}
