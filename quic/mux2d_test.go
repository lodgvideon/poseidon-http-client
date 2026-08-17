package quic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The PR 2d mandatory QUIC-level test (docs/HTTP3_DESIGN.md §5): OpenStreamContext
// blocks on the cumulative bidi-stream limit and wakes when the peer raises it with
// MAX_STREAMS (OnMaxStreams → signalStreamCredit), and its ctx / connection-close
// escapes work. This is the transport half of the concurrency acid test; the
// http3 half (N concurrent Do over waves that exceed the limit) rides on it.

// newCreditConn builds a minimal established Conn with a bidi-stream limit of one,
// the wake latch and stream-credit channel allocated as NewConn would.
func newCreditConn(t *testing.T) *Conn {
	t.Helper()
	dcid := []byte("credtest")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	require.NoError(t, err, "NewSealer for the credit-test connection")
	return &Conn{
		pc: &capturePC{}, dcid: dcid, oneRTTSealer: sealer,
		now:          time.Now,
		peer:         TransportParams{InitialMaxStreamsBidi: 1},
		connRecvMax:  DefaultConnRecvWindow,
		done:         make(chan struct{}),
		streamCredit: make(chan struct{}, 1),
	}
}

// TestMux2d_OpenStreamWaitsOnStreamCredit proves the credit wake: a second
// OpenStreamContext parks while at the initial_max_streams_bidi limit and returns
// only once the peer's MAX_STREAMS raises it (RFC 9000 §4.6).
func TestMux2d_OpenStreamWaitsOnStreamCredit(t *testing.T) {
	c := newCreditConn(t)
	_, err := c.OpenStreamContext(context.Background()) // fills the limit of 1
	require.NoError(t, err, "first OpenStreamContext")
	opened := make(chan *Stream, 1)
	errc := make(chan error, 1)
	go func() {
		s, oerr := c.OpenStreamContext(context.Background())
		if oerr != nil {
			errc <- oerr
			return
		}
		opened <- s
	}()
	// Parked: no stream yet, no error yet, at the cumulative limit.
	select {
	case s := <-opened:
		require.Failf(t, "the second opener did not park",
			"OpenStreamContext returned stream %d before MAX_STREAMS raised the limit", s.ID())
	case perr := <-errc:
		require.NoError(t, perr, "OpenStreamContext errored while it should be parked")
	case <-time.After(50 * time.Millisecond):
	}

	// The peer raises the limit; OnMaxStreams signals the parked opener.
	c.mu.Lock()
	err = (&connFrameHandler{c: c}).OnMaxStreams(false, 4)
	c.mu.Unlock()

	require.NoError(t, err, "OnMaxStreams(bidi, 4)")
	select {
	case s := <-opened:
		assert.Equalf(t, uint64(4), s.ID(),
			"woken stream id = %d, want 4 (the next client bidi id)", s.ID())
	case werr := <-errc:
		require.NoErrorf(t, werr, "OpenStreamContext = %v, want success after MAX_STREAMS", werr)
	case <-time.After(2 * time.Second):
		require.Fail(t, "OpenStreamContext did not wake on the MAX_STREAMS credit grant")
	}
}

// TestMux2d_OpenStreamCreditWakesManyWaiters proves the baton-pass: one MAX_STREAMS
// grant that raises the limit by several slots wakes several parked openers in
// turn, not just one (openStreamLocked re-signals while credit remains).
func TestMux2d_OpenStreamCreditWakesManyWaiters(t *testing.T) {
	c := newCreditConn(t)
	_, err := c.OpenStreamContext(context.Background()) // fills the limit of 1
	require.NoError(t, err, "first OpenStreamContext")
	const waiters = 5
	got := make(chan uint64, waiters)
	errc := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			s, oerr := c.OpenStreamContext(context.Background())
			if oerr != nil {
				errc <- oerr
				return
			}
			got <- s.ID()
		}()
	}

	// One MAX_STREAMS granting exactly enough slots for every waiter (1 already
	// open + 5 waiting = 6).
	c.mu.Lock()
	err = (&connFrameHandler{c: c}).OnMaxStreams(false, 1+waiters)
	c.mu.Unlock()

	require.NoError(t, err, "OnMaxStreams(bidi, 6)")
	seen := map[uint64]bool{}
	for i := 0; i < waiters; i++ {
		select {
		case id := <-got:
			require.Falsef(t, seen[id], "duplicate stream id %d handed out", id)
			seen[id] = true
		case werr := <-errc:
			require.NoErrorf(t, werr, "a parked OpenStreamContext errored: %v", werr)
		case <-time.After(2 * time.Second):
			require.Failf(t, "the baton-pass stalled",
				"only %d of %d waiters woke from a single MAX_STREAMS grant", i, waiters)
		}
	}
}

// TestMux2d_OpenStreamContext_CtxCancelWakes proves the ctx escape: a parked
// OpenStreamContext returns ctx.Err() when its context is cancelled, so a caller
// is never wedged waiting on a peer that never raises the limit.
func TestMux2d_OpenStreamContext_CtxCancelWakes(t *testing.T) {
	c := newCreditConn(t)
	_, err := c.OpenStreamContext(context.Background())
	require.NoError(t, err, "first OpenStreamContext")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, oerr := c.OpenStreamContext(ctx)
		errc <- oerr
	}()
	time.Sleep(20 * time.Millisecond) // let it park at the limit

	cancel()

	select {
	case werr := <-errc:
		assert.Equalf(t, context.Canceled, werr,
			"OpenStreamContext = %v, want context.Canceled", werr)
	case <-time.After(2 * time.Second):
		require.Fail(t, "cancel did not wake the parked OpenStreamContext")
	}
}

// TestMux2d_OpenStreamContext_CloseWakes proves the connection-close escape: a
// parked OpenStreamContext returns the latched terminal error when the connection
// terminates (terminateLocked).
func TestMux2d_OpenStreamContext_CloseWakes(t *testing.T) {
	c := newCreditConn(t)
	_, err := c.OpenStreamContext(context.Background())
	require.NoError(t, err, "first OpenStreamContext")
	errc := make(chan error, 1)
	go func() {
		_, oerr := c.OpenStreamContext(context.Background())
		errc <- oerr
	}()
	time.Sleep(20 * time.Millisecond) // let it park at the limit

	c.mu.Lock()
	c.terminateLocked(ErrConnClosed)
	c.mu.Unlock()

	select {
	case werr := <-errc:
		assert.Equalf(t, ErrConnClosed, werr, "OpenStreamContext = %v, want ErrConnClosed", werr)
	case <-time.After(2 * time.Second):
		require.Fail(t, "connection close did not wake the parked OpenStreamContext")
	}
}
