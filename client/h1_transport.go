package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// h1Exchange adapts *http1.Exchange to the protoStream interface so that
// Client.sendRequest and Client.drainResponse can drive HTTP/1.1 without
// knowing the underlying protocol.
//
// State machine:
//  1. SendHeaders / SendHeadersWithPriority → WriteRequest
//  2. SendData (zero or more)               → WriteBody
//  3. Recv (first call)                     → ReadResponse → EventHeaders
//  4. Recv (body calls)                     → ReadBodyChunk → EventData
//  5. Recv (final)                          → EventData{EndStream:true}
//  6. Close                                 → drain + release
type h1Exchange struct {
	ex      *http1.Exchange
	nc      *http1.Conn
	release func(keepAlive bool) // called exactly once

	// response state
	headersSent bool // Recv has returned EventHeaders
	done        bool // EndStream delivered

	// scratch buffer for ReadBodyChunk; avoids per-Recv allocation.
	buf [16 * 1024]byte //nolint:structcheck
}

// SendHeaders on an HTTP/1.1 exchange is only ever reached via the request
// trailer path (initial headers go through SendHeadersWithPriority). HTTP/1.1
// request trailers are not supported by this fallback transport, so the
// request is rejected with ErrTrailersUnsupportedH1 rather than re-issuing
// WriteRequest, which would emit a second request line and corrupt the
// connection.
func (e *h1Exchange) SendHeaders(_ context.Context, _ []conn.HeaderField, _ bool) error {
	return ErrTrailersUnsupportedH1
}

func (e *h1Exchange) SendHeadersWithPriority(ctx context.Context, fields []conn.HeaderField, endStream bool, _ *frame.Priority) error {
	return e.ex.WriteRequest(ctx, fields, endStream)
}

func (e *h1Exchange) SendData(ctx context.Context, p []byte, endStream bool) error {
	return e.ex.WriteBody(ctx, p, endStream)
}

// Recv synthesises conn.StreamEvents from the HTTP/1.1 response.
// First call triggers ReadResponse (status + headers → EventHeaders).
// Subsequent calls read body chunks → EventData.
func (e *h1Exchange) Recv(ctx context.Context) (conn.StreamEvent, error) {
	if !e.headersSent {
		_, headers, err := e.ex.ReadResponse(ctx)
		if err != nil {
			return conn.StreamEvent{}, err
		}
		e.headersSent = true
		return conn.StreamEvent{
			Type:    conn.EventHeaders,
			Headers: headers,
		}, nil
	}

	if e.done {
		return conn.StreamEvent{}, conn.ErrStreamClosed
	}

	n, done, err := e.ex.ReadBodyChunk(e.buf[:])
	if err != nil {
		return conn.StreamEvent{}, err
	}
	e.done = done
	// Copy data into a fresh slice so the caller owns it past the next Recv.
	var data []byte
	if n > 0 {
		data = make([]byte, n)
		copy(data, e.buf[:n])
	}
	ev := conn.StreamEvent{
		Type:      conn.EventData,
		Data:      data,
		EndStream: done,
	}
	if done {
		e.release(e.ex.KeepAlive())
	}
	return ev, nil
}

// Close cancels the exchange and releases the connection.
func (e *h1Exchange) Close() error {
	if e.done {
		return nil
	}
	e.done = true
	// Not keep-alive: body may be partially read; force connection close.
	e.release(false)
	return nil
}

// ————————————————————————————————————————————————————————————————
// h1singleConn — H1.1 transport for a single persistent connection
// ————————————————————————————————————————————————————————————————

// h1singleConn is the H1.1 analogue of singleConn. It manages at most one
// *http1.Conn per transport; the connection is serialized (one in-flight
// exchange at a time) via a mutex held for the duration of the exchange.
type h1singleConn struct {
	addr    string
	dialer  conn.Dialer
	backoff time.Duration

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu           sync.Mutex
	cur          *http1.Conn
	dialErr      error
	lastDialAt   time.Time
	closed       bool
	dialing      chan struct{}
	warmupCancel context.CancelFunc

	// inFlight serializes the single exchange slot: acquired at the start of
	// openExchange, released by h1Exchange.release.
	inFlight sync.Mutex
}

// openExchange implements transport.openExchange for H1.1.
// It acquires the connection and the in-flight slot, returns an h1Exchange
// whose release function unlocks the slot and optionally recycles the conn.
func (s *h1singleConn) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	// Acquire the single in-flight slot (serializes concurrent callers).
	// We use a channel so context cancellation still works.
	acquired := make(chan struct{})
	go func() {
		s.inFlight.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-ctx.Done():
		// If we lose the race after ctx cancels, unlock immediately.
		go func() {
			<-acquired
			s.inFlight.Unlock()
		}()
		return nil, nil, nil, ctx.Err()
	}

	nc, err := s.acquireConn(ctx)
	if err != nil {
		s.inFlight.Unlock()
		return nil, nil, nil, err
	}

	ex := nc.NewExchange()
	release := func(keepAlive bool) {
		if !keepAlive {
			_ = nc.Close()
			s.mu.Lock()
			if s.cur == nc {
				s.cur = nil
			}
			s.mu.Unlock()
		}
		s.inFlight.Unlock()
	}
	h1ex := &h1Exchange{ex: ex, nc: nc, release: release}
	return h1ex, nil, func() {}, nil
}

// acquireConn returns a healthy *http1.Conn, dialling if necessary.
func (s *h1singleConn) acquireConn(ctx context.Context) (*http1.Conn, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if s.cur != nil && s.cur.IsAlive() {
			c := s.cur
			s.mu.Unlock()
			return c, nil
		}
		s.cur = nil
		if s.dialing != nil {
			ch := s.dialing
			s.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if s.backoff > 0 && s.dialErr != nil && time.Since(s.lastDialAt) < s.backoff {
			err := s.dialErr
			s.mu.Unlock()
			return nil, &DialError{Addr: s.addr, Err: fmt.Errorf("%w: %w", ErrRedialBackoff, err)}
		}
		s.dialing = make(chan struct{})
		ch := s.dialing
		s.mu.Unlock()

		dialStart := time.Now()
		s.metrics.Counters.DialsAttempted.Add(1)
		nc, dialErr := s.dialer.Dial(ctx, s.addr)
		dur := time.Since(dialStart)
		s.metrics.Latency.Dial.Observe(dur)
		if dialErr != nil {
			s.metrics.Counters.DialsFailed.Add(1)
		}
		if hr := s.hooksRef; hr != nil {
			if h := hr.Load(); h != nil && h.OnDial != nil {
				h.OnDial(DialEvent{Addr: s.addr, Err: dialErr, Duration: dur})
			}
		}

		s.mu.Lock()
		s.lastDialAt = time.Now()
		s.dialing = nil
		close(ch)
		if s.closed {
			if nc != nil {
				_ = nc.Close()
			}
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if dialErr != nil {
			s.dialErr = dialErr
			s.mu.Unlock()
			return nil, &DialError{Addr: s.addr, Err: dialErr}
		}
		hc := http1.NewConn(nc)
		s.cur = hc
		s.dialErr = nil
		c := s.cur
		s.mu.Unlock()
		return c, nil
	}
}

func (s *h1singleConn) close() error {
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
		return cur.Close()
	}
	return nil
}

func (s *h1singleConn) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return s.close()
}

func (s *h1singleConn) warmup(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	if s.closed || s.warmupCancel != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.warmupCancel = cancel
	s.mu.Unlock()
	go func() {
		_, _ = s.acquireConn(ctx)
		cancel()
		s.mu.Lock()
		s.warmupCancel = nil
		s.mu.Unlock()
	}()
}

// ————————————————————————————————————————————————————————————————
// alpnSingleConn — ALPN-detecting transport (H2 fallback to H1.1)
// ————————————————————————————————————————————————————————————————

// alpnSingleConn is the ALPN-aware transport: it dials once with a
// FlexDialer (offers "h2" + "http/1.1"), detects the negotiated protocol,
// and then permanently delegates to either a singleConn (H2) or
// h1singleConn (H1.1). Subsequent dials always use the same protocol.
type alpnSingleConn struct {
	addr     string
	connOpts conn.ConnOptions
	backoff  time.Duration

	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu       sync.Mutex
	detected string    // "" / "h2" / "http/1.1"
	h2       transport // non-nil after H2 detected
	h1       transport // non-nil after H1.1 detected
}

func (a *alpnSingleConn) delegate() transport {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.h2 != nil {
		return a.h2
	}
	return a.h1
}

func (a *alpnSingleConn) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	if d := a.delegate(); d != nil {
		return d.openExchange(ctx)
	}
	// First dial: detect protocol.
	nc, err := a.connOpts.Dialer.Dial(ctx, a.addr)
	if err != nil {
		return nil, nil, nil, &DialError{Addr: a.addr, Err: err}
	}
	proto := conn.NegotiatedProtocol(nc)

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.detected != "" {
		// Another goroutine raced us and already detected the protocol;
		// close our conn and delegate.
		_ = nc.Close()
		d := a.h2
		if d == nil {
			d = a.h1
		}
		return d.openExchange(ctx)
	}
	a.detected = proto

	switch proto {
	case "h2":
		// Build H2 conn from the already-dialled net.Conn.
		h2c, cerr := conn.NewClientConn(ctx, nc, a.connOpts)
		if cerr != nil {
			_ = nc.Close()
			return nil, nil, nil, &DialError{Addr: a.addr, Err: cerr}
		}
		sc := &singleConn{
			addr:     a.addr,
			connOpts: a.connOpts,
			backoff:  a.backoff,
			hooksRef: a.hooksRef,
			metrics:  a.metrics,
			cur:      h2c,
		}
		a.h2 = sc
		stream, serr := h2c.NewStream(ctx)
		if serr != nil {
			return nil, nil, nil, serr
		}
		return stream, h2c.LookupStream, func() {}, nil

	default: // "http/1.1" or ""
		h1c := http1.NewConn(nc)
		sc := &h1singleConn{
			addr:     a.addr,
			dialer:   a.connOpts.Dialer,
			backoff:  a.backoff,
			hooksRef: a.hooksRef,
			metrics:  a.metrics,
			cur:      h1c,
		}
		a.h1 = sc
		ex := h1c.NewExchange()
		release := func(keepAlive bool) {
			if !keepAlive {
				_ = h1c.Close()
				sc.mu.Lock()
				if sc.cur == h1c {
					sc.cur = nil
				}
				sc.mu.Unlock()
			}
			sc.inFlight.Unlock()
		}
		// Acquire the in-flight slot for this first exchange.
		sc.inFlight.Lock()
		h1ex := &h1Exchange{ex: ex, nc: h1c, release: release}
		return h1ex, nil, func() {}, nil
	}
}

func (a *alpnSingleConn) close() error {
	a.mu.Lock()
	d := a.h2
	if d == nil {
		d = a.h1
	}
	a.mu.Unlock()
	if d != nil {
		return d.close()
	}
	return nil
}

func (a *alpnSingleConn) shutdown(gracefulTimeout time.Duration) error {
	a.mu.Lock()
	d := a.h2
	if d == nil {
		d = a.h1
	}
	a.mu.Unlock()
	if d != nil {
		return d.shutdown(gracefulTimeout)
	}
	return nil
}

func (a *alpnSingleConn) warmup(n int) {
	if d := a.delegate(); d != nil {
		d.warmup(n)
	}
	// Can't warmup before first dial without knowing the protocol.
}
