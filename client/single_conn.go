package client

import (
	"context"
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
}

// acquire implements transport.acquire.
func (s *singleConn) acquire(ctx context.Context) (*conn.Conn, func(), error) {
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
	if s.backoff > 0 && s.dialErr != nil &&
		time.Since(s.lastDialAt) < s.backoff {
		err := s.dialErr
		s.mu.Unlock()
		return nil, nil, &DialError{Addr: s.addr, Err: err}
	}
	s.mu.Unlock()

	dialed, dialErr := conn.Dial(ctx, s.addr, s.connOpts)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDialAt = time.Now()
	if s.closed {
		if dialed != nil {
			_ = dialed.Close()
		}
		return nil, nil, ErrClosed
	}
	if dialErr != nil {
		s.dialErr = dialErr
		return nil, nil, &DialError{Addr: s.addr, Err: dialErr}
	}
	if s.cur != nil && s.cur.IsAlive() {
		_ = dialed.Close()
		return s.cur, func() {}, nil
	}
	s.cur = dialed
	s.dialErr = nil
	return s.cur, func() {}, nil
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
