package client

import (
	"context"
	"fmt"
	"sync"
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

	mu         sync.Mutex
	cur        *conn.Conn
	dialErr    error
	lastDialAt time.Time
	closed     bool
	// dialing is non-nil while a dial is in flight; waiters block on
	// receiving from it (closed when the dial completes) so that only
	// one goroutine dials at a time.
	dialing chan struct{}
}

// acquire implements transport.acquire.
func (s *singleConn) acquire(ctx context.Context) (*conn.Conn, func(), error) {
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

		dialed, dialErr := conn.Dial(ctx, s.addr, s.connOpts)

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
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.cur != nil {
		err := s.cur.Close()
		s.cur = nil
		return err
	}
	return nil
}
