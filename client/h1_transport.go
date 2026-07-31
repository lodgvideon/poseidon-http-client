package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// assertH1Conn rejects a freshly dialled connection whose ALPN protocol says the
// peer is not speaking HTTP/1.1. Every H1 dial path runs it, so a dialer that
// offers "h2" — conn.FlexDialer against an h2-capable server, or a custom dialer
// that does not implement conn.ALPNAsserter and so escaped NewClient's check —
// fails at the dial with a message naming the protocol, instead of turning every
// exchange into "http1: read status line: EOF" while the server logs a bogus
// HTTP/2 preface. A connection with no negotiated protocol (plain TCP, or TLS
// against a peer that does not speak ALPN) is fine: that is HTTP/1.1 by default.
func assertH1Conn(nc net.Conn) error {
	if p := conn.NegotiatedProtocol(nc); p != "" && p != "http/1.1" {
		return fmt.Errorf("%w: dialer negotiated %q, the HTTP/1.1 transport requires \"http/1.1\" (use conn.H1TLSDialer for HTTP/1.1 over TLS)",
			ErrALPNProtocolMismatch, p)
	}
	return nil
}

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
//
// This struct is allocated once per REQUEST — every openExchange on every H1
// transport builds one — so it must stay pointer-sized. It used to carry a
// 16 KiB inline scratch array for ReadBodyChunk, which made that per-request
// allocation 16 KiB: 69.5% of all bytes the client allocated in a 200 RPS
// profile, and the reason HTTP/1.1 spent +87% bytes/request against net/http on
// byte-identical work. The intent — do not allocate inside Recv — was right; the
// scoping was not, because an exchange is per-request while the thing that
// outlives requests is the pooled connection. Recv now takes its read buffer
// from conn's shared DATA-payload pool instead, which is the same buffer H2's
// OnData has always used and is per-connection-agnostic: nothing has to be
// scoped to the exchange at all.
type h1Exchange struct {
	ex      *http1.Exchange
	nc      *http1.Conn
	release func(keepAlive bool) // called exactly once

	// response state
	headersSent bool // Recv has returned EventHeaders
	done        bool // EndStream delivered
}

// h1BodyChunkSize is the ReadBodyChunk granularity, and the floor on the pooled
// buffer Recv reads into. It matches conn.dataBufPool's own New size (the
// default SETTINGS_MAX_FRAME_SIZE), so a buffer that has been round-tripping
// through the H2 path is already big enough and no warm-up regrow happens.
const h1BodyChunkSize = 16 * 1024

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
//
// EventData.Data aliases the pooled buffer named by EventData.DataSlab and is
// valid only until the next Recv or Close — the contract conn.StreamEvent
// already documents and that every consumer in this package (handleDataEvent,
// responseBodyReader.recycleData, StreamResponse.recycleData) already honours
// for H2. H1 used to be the one path that opted out, copying each chunk into a
// fresh make([]byte, n); that copy was 15% of the client's allocated bytes on
// top of the scratch array it copied out of. Handing the pooled buffer straight
// through removes both.
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

	// Read straight into a pooled buffer and transfer ownership to the caller via
	// DataSlab, exactly as conn's OnData does for an H2 DATA frame. The pool's
	// buffers are only ever grown, never shrunk, so cap >= h1BodyChunkSize holds
	// in practice; the guard keeps a zero-length buffer from reaching
	// ReadBodyChunk, which would read nothing and spin.
	bufPtr := getDataSlab()
	buf := *bufPtr
	if cap(buf) < h1BodyChunkSize {
		buf = make([]byte, h1BodyChunkSize)
	}
	buf = buf[:cap(buf)]

	n, done, err := e.ex.ReadBodyChunk(buf)
	if err != nil {
		// Nothing was delivered, so this is the only owner: return the buffer
		// rather than abandoning it to GC.
		*bufPtr = buf
		putDataSlab(bufPtr)
		return conn.StreamEvent{}, err
	}
	e.done = done
	*bufPtr = buf[:n]
	ev := conn.StreamEvent{
		Type:      conn.EventData,
		Data:      *bufPtr,
		DataSlab:  bufPtr,
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
			// IsAlive is a local flag — it says this end has not closed the
			// connection, and nothing about what the peer has put on it. Reusing
			// on that alone let a server append a complete unsolicited response
			// after a well-framed one and have the NEXT request read it as its
			// own (RFC 9112 §6.3). The pool grew a checkout probe for exactly this
			// in #313; this transport never got one, so the same attack simply
			// moved here.
			//
			// HasResidue rather than ProbeIdle: this is the request path of a
			// single-connection transport, where a bounded ~1ms probe would be
			// paid on every request rather than on an idle checkout.
			if !c.HasResidue() {
				s.mu.Unlock()
				return c, nil
			}
			// Unsolicited octets: this connection can no longer be framed. Drop it
			// and dial a fresh one on the next turn of the loop.
			s.cur = nil
			s.mu.Unlock()
			_ = c.Close()
			continue
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
		if dialErr == nil {
			if err := assertH1Conn(nc); err != nil {
				_ = nc.Close()
				nc, dialErr = nil, err
			}
		}
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
		// acquireConn calls HasResidue, which moves the read deadline and peeks at
		// the reader — so it may only run when no exchange is in flight.
		// openExchange takes this lock before acquireConn for exactly that reason;
		// warmup skipped it, which raced bufio state against a live reader and,
		// worse, made the in-flight response's own octets look like residue:
		// acquireConn then closed the connection out from under the request that
		// was reading it, which failed with "use of closed network connection".
		// Documented as idempotent and safe on an already-warm client, so it must
		// be exactly that.
		s.inFlight.Lock()
		_, _ = s.acquireConn(ctx)
		s.inFlight.Unlock()
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

	mu sync.Mutex
	// detecting is non-nil while the first dial + protocol detection is in
	// flight; other callers block on it (closed when detection completes) so only
	// one goroutine dials, exactly as singleConn.dialing does. The detection runs
	// with a.mu RELEASED — it dials and, for h2, performs a full SETTINGS
	// handshake, both unbounded network I/O — so that a silent peer cannot wedge
	// every other caller and Close() behind the lock.
	detecting chan struct{}
	h2        transport // non-nil after H2 detected
	h1        transport // non-nil after H1.1 detected
	closed    bool
}

func (a *alpnSingleConn) delegate() transport {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.delegateLocked()
}

func (a *alpnSingleConn) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	for {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return nil, nil, nil, ErrClosed
		}
		if d := a.delegateLocked(); d != nil {
			a.mu.Unlock()
			return d.openExchange(ctx)
		}
		// A detection is already in flight: wait for it (honouring ctx) and retry.
		// The old code held a.mu across the whole dial+handshake, so this branch
		// did not exist — every concurrent caller, and Close(), blocked on the
		// lock behind a possibly-silent peer.
		if a.detecting != nil {
			ch := a.detecting
			a.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, nil, nil, ctx.Err()
			}
		}
		// Become the detector; release the lock for the long dial + handshake.
		a.detecting = make(chan struct{})
		ch := a.detecting
		a.mu.Unlock()

		d, isH2, derr := a.detectDelegate(ctx)

		a.mu.Lock()
		a.detecting = nil
		close(ch)
		if a.closed {
			a.mu.Unlock()
			if d != nil {
				_ = d.close()
			}
			return nil, nil, nil, ErrClosed
		}
		if derr != nil {
			a.mu.Unlock()
			return nil, nil, nil, derr
		}
		// Store the delegate by the protocol that was actually negotiated. Keyed
		// on isH2, NOT on a "detected != ''" sentinel: conn.NegotiatedProtocol
		// returns "" for a PlaintextDialer, so the old sentinel never armed on the
		// plaintext path and each racer built and stored its own h1 delegate,
		// orphaning the winner's connection.
		if isH2 {
			a.h2 = d
		} else {
			a.h1 = d
		}
		a.mu.Unlock()
		// Delegate the first exchange like any other: the delegate's own
		// openExchange finds the connection we handed it as its current one, so
		// there is no separate first-exchange path to keep in step with it.
		return d.openExchange(ctx)
	}
}

// detectDelegate performs the first dial, reads the negotiated protocol, and
// builds the matching delegate around the connection — all with a.mu RELEASED,
// because every step is unbounded network I/O. The delegate is returned holding
// the dialled connection as its current one; the caller stores it under the
// lock.
func (a *alpnSingleConn) detectDelegate(ctx context.Context) (transport, bool, error) {
	nc, err := a.connOpts.Dialer.Dial(ctx, a.addr)
	if err != nil {
		return nil, false, &DialError{Addr: a.addr, Err: err}
	}
	if conn.NegotiatedProtocol(nc) == "h2" {
		h2c, cerr := conn.NewClientConn(ctx, nc, a.connOpts)
		if cerr != nil {
			_ = nc.Close()
			return nil, false, &DialError{Addr: a.addr, Err: cerr}
		}
		return &singleConn{
			addr:     a.addr,
			connOpts: a.connOpts,
			backoff:  a.backoff,
			hooksRef: a.hooksRef,
			metrics:  a.metrics,
			cur:      h2c,
		}, true, nil
	}
	return &h1singleConn{
		addr:     a.addr,
		dialer:   a.connOpts.Dialer,
		backoff:  a.backoff,
		hooksRef: a.hooksRef,
		metrics:  a.metrics,
		cur:      http1.NewConn(nc),
	}, false, nil
}

// delegateLocked returns the detected delegate, or nil if detection has not
// completed. Caller holds a.mu.
func (a *alpnSingleConn) delegateLocked() transport {
	if a.h2 != nil {
		return a.h2
	}
	return a.h1
}

func (a *alpnSingleConn) close() error {
	a.mu.Lock()
	a.closed = true
	d := a.delegateLocked()
	a.mu.Unlock()
	if d != nil {
		return d.close()
	}
	return nil
}

func (a *alpnSingleConn) shutdown(gracefulTimeout time.Duration) error {
	a.mu.Lock()
	a.closed = true
	d := a.delegateLocked()
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
