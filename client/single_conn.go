package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// singleConn is the C.1 transport: at most one *conn.Conn per Client.
// Lazy dial on first acquire, conn-only auto-redial when the cached
// conn is no longer alive, optional backoff between failed dials.
type singleConn struct {
	addr     string
	connOpts conn.ConnOptions
	backoff  time.Duration
	// dialTimeout bounds one dial attempt (ClientOptions.DialTimeout, defaulted).
	dialTimeout time.Duration

	// hooksRef points at Client.hooks; metrics is shared with Client.
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu         sync.Mutex
	cur        *conn.Conn
	dialErr    error
	lastDialAt time.Time
	closed     bool
	// dialing is non-nil while a dial is in flight; waiters block on
	// receiving from it (closed when the dial completes) so that only
	// one goroutine dials at a time.
	dialing chan struct{}
	// warmupCancel cancels the background warmup dial context. Set
	// on first successful warmup; cleared in close/shutdown. Required
	// so that Warmup(1) → Close() does not leave the background
	// goroutine alive for the full warmup timeout.
	warmupCancel context.CancelFunc
}

// openExchange implements transport.openExchange.
func (s *singleConn) openExchange(ctx context.Context) (protoStream, pushLookuper, func(), error) {
	cn, release, err := s.acquireConn(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	stream, serr := cn.NewStream(ctx)
	if serr != nil {
		release()
		return nil, nil, nil, serr
	}
	return stream, cn, release, nil
}

// acquireConn returns a healthy *conn.Conn and a no-op release func.
// It handles lazy dial, in-flight dedup, and backoff suppression.
func (s *singleConn) acquireConn(ctx context.Context) (*conn.Conn, func(), error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, nil, ErrClosed
		}
		if s.cur != nil && s.cur.IsAlive() {
			c := s.cur
			s.mu.Unlock()
			return c, func() {}, nil
		}
		s.cur = nil
		// If a dial is already in flight, wait for it and retry.
		if s.dialing != nil {
			ch := s.dialing
			s.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		// Backoff suppression after a recent failed dial.
		if s.backoff > 0 && s.dialErr != nil &&
			time.Since(s.lastDialAt) < s.backoff {
			err := s.dialErr
			s.mu.Unlock()
			return nil, nil, &DialError{
				Addr: s.addr,
				Err:  fmt.Errorf("%w: %w", ErrRedialBackoff, err),
			}
		}
		// Become the dialer; release the lock for the long dial.
		s.dialing = make(chan struct{})
		ch := s.dialing
		s.mu.Unlock()

		dialed, dialErr := dialObserved(ctx, s.addr, s.dialTimeout, s.metrics, s.hooksRef,
			func(dctx context.Context) (*conn.Conn, error) {
				return conn.Dial(dctx, s.addr, s.connOpts)
			})

		s.mu.Lock()
		s.lastDialAt = time.Now()
		s.dialing = nil
		close(ch)
		if s.closed {
			if dialed != nil {
				_ = dialed.Close()
			}
			s.mu.Unlock()
			return nil, nil, ErrClosed
		}
		if dialErr != nil {
			s.dialErr = dialErr
			s.mu.Unlock()
			return nil, nil, &DialError{Addr: s.addr, Err: dialErr}
		}
		s.cur = dialed
		s.dialErr = nil
		c := s.cur
		s.mu.Unlock()
		return c, func() {}, nil
	}
}

// close implements transport.close. Idempotent.
func (s *singleConn) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.warmupCancel
	s.warmupCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.mu.Lock()
	cur := s.cur
	s.cur = nil
	s.mu.Unlock()
	if cur != nil {
		return cur.Close()
	}
	return nil
}

// shutdown implements transport.shutdown. Marks the transport as
// closed, then calls Shutdown on the cached conn (which sends GOAWAY
// and waits for in-flight streams within the timeout).
func (s *singleConn) shutdown(gracefulTimeout time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.warmupCancel
	s.warmupCancel = nil
	cur := s.cur
	s.cur = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if cur != nil {
		return cur.Shutdown(gracefulTimeout)
	}
	return nil
}

// warmup implements transport.warmup. For single-conn, n is capped
// at 1. Triggers a lazy dial in the background; the actual conn is
// established on the next call to acquire. The background dial
// context is cached so close()/shutdown() can cancel it promptly
// rather than waiting for the 30s timeout.
func (s *singleConn) warmup(n int) {
	if n <= 0 {
		return
	}
	// single-conn ignores n>1: at most one underlying conn is ever
	// dialled. Skip if already closed or if a warmup is already in
	// flight.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.warmupCancel != nil {
		s.mu.Unlock()
		return
	}
	// Use a fresh context with a generous timeout so the dial
	// runs even if the caller's ctx is short-lived. The cancel
	// is stashed so close()/shutdown() can interrupt promptly.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.warmupCancel = cancel
	s.mu.Unlock()
	go func() {
		_, _, _ = s.acquireConn(ctx)
		// Release the timer and clear the latch, as the H1 and H3 single-conn
		// transports do. Without this the field stays non-nil for the life of the
		// client: a warmup whose dial failed can never be retried — the guard
		// above turns every later Warmup into a no-op — and the 30-second timer
		// stays armed until it fires.
		cancel()
		s.mu.Lock()
		s.warmupCancel = nil
		s.mu.Unlock()
	}()
}
