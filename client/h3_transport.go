package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/http3"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

// h3Client is the subset of *http3.Client the transport drives: a buffered Do
// (returns the whole response head + body), an incremental DoStream (returns the
// response head plus a BodyReader that pulls DATA frames on demand), Close, and a
// non-blocking Alive liveness probe (used by the connection pool to evict a dead
// QUIC connection on release). Defining it as an interface keeps the transport
// testable — a fake satisfies it without a live QUIC connection — while
// *http3.Client satisfies it directly.
type h3Client interface {
	Do(ctx context.Context, req *http3.Request) (*http3.Response, []byte, error)
	DoStream(ctx context.Context, req *http3.Request) (*http3.Response, http3.ResponseBody, error)
	// Alive reports whether the underlying QUIC connection is still usable. It
	// must not block. singleH3Conn ignores it (single-conn has no liveness
	// gate); the HTTP/3 pool uses it to eject dead connections on release.
	Alive() bool
	Close() error
}

// h3DialFn dials a live HTTP/3 client. It is the production value of
// singleH3Conn.dialFn; tests substitute a fake. The explicit nil return on
// error avoids a non-nil h3Client interface wrapping a nil *http3.Client.
func h3DialFn(ctx context.Context, addr string, tlsConfig *tls.Config) (h3Client, error) {
	cl, err := http3.Dial(ctx, addr, tlsConfig)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// makeH3DialFn returns an h3 dial function that forwards connOpts to every QUIC
// connection it dials (http3.Dial). ClientOptions.H3ConnOptions flow through here,
// so a client configured with quic.WithCongestionControl(quic.CCBBR) dials BBR
// connections. With no options it is exactly h3DialFn, so the default path (and
// the tests that pin it) are unchanged.
func makeH3DialFn(connOpts []quic.ConnOption) func(context.Context, string, *tls.Config) (h3Client, error) {
	if len(connOpts) == 0 {
		return h3DialFn
	}
	return func(ctx context.Context, addr string, tlsConfig *tls.Config) (h3Client, error) {
		cl, err := http3.Dial(ctx, addr, tlsConfig, connOpts...)
		if err != nil {
			return nil, err
		}
		return cl, nil
	}
}

// ————————————————————————————————————————————————————————————————
// h3Exchange — one buffered HTTP/3 request/response as a protoStream
// ————————————————————————————————————————————————————————————————

// h3Exchange adapts one buffered http3.Client.Do call to the protoStream
// interface, so Client.sendRequest and Client.drainResponse drive HTTP/3
// without knowing the protocol. It mirrors h1Exchange: the incremental
// protoStream Send calls are buffered into an http3.Request, the round-trip
// runs on the first Recv, and the *http3.Response is replayed to the caller as
// synthesised conn.StreamEvents.
//
// State machine:
//  1. SendHeadersWithPriority → re-split pseudo-headers into typed fields
//  2. SendData (zero or more)  → append to Request.Body
//  3. Recv (first call)        → http3.Client.Do → synthesise events
//  4. Recv (subsequent)        → replay EventHeaders / EventData / EventTrailers
type h3Exchange struct {
	client h3Client

	// req is assembled from the protoStream Send calls, then round-tripped on
	// the first Recv.
	req http3.Request

	// response replay state.
	called bool               // Do has been invoked
	events []conn.StreamEvent // synthesised from the http3.Response
	idx    int                // next event to replay
	closed bool

	// Per-request scratch, reused across pooled reuses to keep the buffered Do
	// path allocation-free (see h3ExchangePool). regularScratch backs
	// req.Headers (the non-pseudo request fields, split out in
	// SendHeadersWithPriority); hdrsScratch backs the synthesised response
	// EventHeaders slice; statusBuf backs the synthesised :status value. All
	// three are consumed synchronously — regularScratch by the buffered Do,
	// hdrsScratch + statusBuf by drainResponse (which copies the fields it keeps
	// into the caller's Response, never retaining these backing arrays) — so
	// recycling them on Close is safe.
	regularScratch []conn.HeaderField
	hdrsScratch    []conn.HeaderField
	statusBuf      [3]byte
}

// h3ExchangePool recycles h3Exchange structs (and their per-request scratch
// slices) across buffered Do calls, mirroring the H2 replyPool/hdrSlicePool
// pattern. An exchange is drawn in openExchange and returned in Close once
// drainResponse has copied every response field into the caller's Response, so
// no recycled scratch is ever aliased by a live Response. The buffered Do is
// synchronous, so this holds without further synchronisation.
var h3ExchangePool = sync.Pool{New: func() any { return new(h3Exchange) }}

// getH3Exchange draws an h3Exchange from the pool, bound to client and cleared
// of the prior request's replay state. The scratch slices retain their backing
// arrays for reuse; their contents are overwritten on next use.
func getH3Exchange(client h3Client) *h3Exchange {
	e := h3ExchangePool.Get().(*h3Exchange)
	e.client = client
	e.called = false
	e.idx = 0
	e.closed = false
	return e
}

// SendHeaders on a buffered HTTP/3 exchange is only reached via the request
// trailer path (initial headers go through SendHeadersWithPriority). http3.Request
// has no request-trailer field, so trailers are rejected rather than dropped.
func (e *h3Exchange) SendHeaders(_ context.Context, _ []conn.HeaderField, _ bool) error {
	return ErrTrailersUnsupportedH3
}

// SendHeadersWithPriority buffers the initial request headers. It re-splits the
// inline :method/:scheme/:authority/:path pseudo-headers (emitted by
// buildHeaders as ordinary HeaderField entries) back into the typed http3.Request
// fields, routing the remaining fields to Request.Headers. endStream and the H2
// priority hint are irrelevant to a buffered exchange and ignored.
func (e *h3Exchange) SendHeadersWithPriority(_ context.Context, fields []conn.HeaderField, _ bool, _ *frame.Priority) error {
	method, scheme, authority, path, regular, err := splitPseudoHeaders(fields, e.regularScratch)
	if err != nil {
		return err
	}
	e.regularScratch = regular // keep the (possibly grown) backing array for reuse
	e.req.Method = method
	e.req.Scheme = scheme
	e.req.Authority = authority
	e.req.Path = path
	e.req.Headers = regular
	return nil
}

// SendData buffers a request body chunk. The bytes are copied because the caller
// may recycle p (the upload buffer pool) before the deferred Do reads Body.
// endStream is implicit for a buffered exchange (Do is issued from Recv).
func (e *h3Exchange) SendData(_ context.Context, p []byte, _ bool) error {
	if len(p) > 0 {
		e.req.Body = append(e.req.Body, p...)
	}
	return nil
}

// Recv issues the buffered round-trip on its first call, then replays the
// synthesised events. Once the events are exhausted it returns
// conn.ErrStreamClosed, exactly like h1Exchange. A Do error (context cancel,
// http3.ErrGoAway, *http3.StreamResetError, a malformed response) is returned
// verbatim so the caller's retry/classification logic sees the typed error.
func (e *h3Exchange) Recv(ctx context.Context) (conn.StreamEvent, error) {
	if !e.called {
		e.called = true
		resp, body, err := e.client.Do(ctx, &e.req)
		if err != nil {
			return conn.StreamEvent{}, err
		}
		e.synthesize(resp, body)
	}
	if e.idx >= len(e.events) {
		return conn.StreamEvent{}, conn.ErrStreamClosed
	}
	ev := e.events[e.idx]
	e.idx++
	return ev, nil
}

// synthesize builds the conn.StreamEvent sequence a buffered *http3.Response maps
// to: one EventHeaders (:status rebuilt as an inline field, then the response
// headers), one EventData carrying the whole body, and — when the response
// carried a trailer section — one EventTrailers. The final event sets EndStream
// so drainResponse concludes.
//
// Informational 1xx responses (resp.Interim) are intentionally dropped, not
// replayed, for parity with the buffered H2 Do path — which exposes no interim
// surface either: client.Response has no Interim field, and drainResponse's
// handleHeadersEvent latches gotHeaders on the first EventHeaders and early-
// returns on every later one. Replaying each resp.Interim as a pre-final
// EventHeaders would therefore be worse than dropping it: the first replayed 1xx
// would latch as resp.Status and the real final status would be discarded.
// Dropping 1xx keeps H2 and H3 buffered Do behaviour identical.
func (e *h3Exchange) synthesize(resp *http3.Response, body []byte) {
	hdrs := append(e.hdrsScratch[:0], conn.HeaderField{Name: hdrStatus, Value: e.statusValue(resp.Status)})
	hdrs = append(hdrs, resp.Headers...)
	e.hdrsScratch = hdrs // keep the (possibly grown) backing array for reuse

	hasTrailers := len(resp.Trailers) > 0
	events := append(e.events[:0],
		conn.StreamEvent{Type: conn.EventHeaders, Headers: hdrs},
		conn.StreamEvent{Type: conn.EventData, Data: body, EndStream: !hasTrailers},
	)
	if hasTrailers {
		events = append(events, conn.StreamEvent{
			Type:      conn.EventTrailers,
			Headers:   resp.Trailers,
			EndStream: true,
		})
	}
	e.events = events
}

// statusValue renders resp.Status as its 3-digit ASCII :status value into the
// exchange's statusBuf, avoiding the per-request []byte(strconv.Itoa(...)) pair
// of allocations. The result backs only the synthesised :status field, which
// drainResponse's parseStatus reads (via parseThreeDigitInt) and then drops —
// it is never appended to the caller's Response.Headers — so reusing this
// per-exchange scratch across requests transfers no memory to the caller. A
// status outside 100–999 (which the http3 layer guarantees will not occur for a
// final response) falls back to the allocating helper so the exact
// out-of-range error behaviour of parseThreeDigitInt is preserved.
func (e *h3Exchange) statusValue(status int) []byte {
	if status < 100 || status > 999 {
		return statusValue(status)
	}
	e.statusBuf[0] = byte('0' + status/100)
	e.statusBuf[1] = byte('0' + (status/10)%10)
	e.statusBuf[2] = byte('0' + status%10)
	return e.statusBuf[:]
}

// Close marks the exchange finished and returns it to h3ExchangePool. The
// buffered Do is synchronous, so by the time drainResponse (or an error path)
// calls Close the round-trip has already completed or aborted itself — there is
// nothing to cancel, and every response field drainResponse keeps has already
// been copied into the caller's Response, so recycling the scratch is safe.
// Idempotent: a second Close is a no-op (the closed flag stays set through
// recycling and is only cleared when getH3Exchange re-issues the struct), so a
// pooled exchange is never double-Put.
func (e *h3Exchange) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.client = nil
	e.req.Method = ""
	e.req.Scheme = ""
	e.req.Authority = ""
	e.req.Path = ""
	e.req.Headers = nil
	e.req.Body = e.req.Body[:0]
	e.events = e.events[:0]
	e.regularScratch = e.regularScratch[:0]
	e.hdrsScratch = e.hdrsScratch[:0]
	h3ExchangePool.Put(e)
	return nil
}

// beginStream switches the exchange from the buffered Do to the incremental
// DoStream path: the request assembled from the protoStream Send calls is round-
// tripped up to the response HEADERS, and the returned *http3.BodyReader pulls the
// DATA frames (and trailers) on demand. It is called once per streaming request by
// client.beginRespStream, before any Recv. On a DoStream error the stream is
// already aborted by http3.Client; here we only surface the typed error.
func (e *h3Exchange) beginStream(ctx context.Context) (respStream, error) {
	resp, body, err := e.client.DoStream(ctx, &e.req)
	if err != nil {
		return nil, err
	}
	return &h3RespStream{resp: resp, body: body}, nil
}

// ————————————————————————————————————————————————————————————————
// h3RespStream — one streaming HTTP/3 response body as a respStream
// ————————————————————————————————————————————————————————————————

// h3RespStream adapts an http3.BodyReader to the respStream interface so the
// client's responseBodyReader and StreamResponse read an HTTP/3 body through the
// same Recv → conn.StreamEvent surface as an HTTP/2 *conn.Stream. The response
// head (already read by DoStream) is replayed as the first EventHeaders; each
// subsequent Recv pulls one body event from the BodyReader:
//   - a DATA chunk        → EventData (Data aliases the reader's frame buffer,
//     valid until the next Recv/Close — the same contract as an H2 EventData);
//   - the trailer section → EventTrailers (EndStream);
//   - a clean end         → a terminal empty EventData with EndStream set, so the
//     client readers observe end-of-stream exactly as they do for an H2 FIN;
//   - a server reset / protocol error → the typed error verbatim (e.g.
//     *http3.StreamResetError), preserving errors.As retry classification.
type h3RespStream struct {
	resp        *http3.Response
	body        http3.ResponseBody
	headersDone bool
}

// Recv delivers the next conn.StreamEvent for the HTTP/3 response.
func (h *h3RespStream) Recv(ctx context.Context) (conn.StreamEvent, error) {
	if !h.headersDone {
		h.headersDone = true
		hdrs := make([]conn.HeaderField, 0, 1+len(h.resp.Headers))
		hdrs = append(hdrs, conn.HeaderField{Name: hdrStatus, Value: statusValue(h.resp.Status)})
		hdrs = append(hdrs, h.resp.Headers...)
		return conn.StreamEvent{Type: conn.EventHeaders, Headers: hdrs}, nil
	}
	be, err := h.body.Next(ctx)
	if err != nil {
		return conn.StreamEvent{}, err
	}
	switch {
	case be.Trailers != nil:
		return conn.StreamEvent{Type: conn.EventTrailers, Headers: be.Trailers, EndStream: true}, nil
	case be.End:
		return conn.StreamEvent{Type: conn.EventData, EndStream: true}, nil
	default:
		return conn.StreamEvent{Type: conn.EventData, Data: be.Data}, nil
	}
}

// Close aborts the underlying HTTP/3 body reader (RST_STREAM if not fully drained).
// Idempotent.
func (h *h3RespStream) Close() error {
	return h.body.Close()
}

// splitPseudoHeaders separates the request pseudo-headers from the regular
// fields. The pseudo values are copied into strings (the caller's backing array
// is pooled and reused after SendHeaders returns) and the regular fields are
// appended into dst[:0] — pass a reusable per-exchange scratch to avoid a
// per-request allocation, or nil for a freshly-allocated slice. The returned
// regular slice must be kept (it may have grown a new backing array). An
// unrecognised pseudo-header (e.g. :protocol for extended CONNECT) is rejected —
// the buffered http3.Request models only :method/:scheme/:authority/:path.
func splitPseudoHeaders(fields, dst []conn.HeaderField) (method, scheme, authority, path string, regular []conn.HeaderField, err error) {
	regular = dst[:0]
	for i := range fields {
		f := fields[i]
		if len(f.Name) > 0 && f.Name[0] == ':' {
			switch string(f.Name) {
			case ":method":
				method = string(f.Value)
			case ":scheme":
				scheme = string(f.Value)
			case ":authority":
				authority = string(f.Value)
			case ":path":
				path = string(f.Value)
			default:
				return "", "", "", "", nil, fmt.Errorf("%w: unsupported pseudo-header %q for HTTP/3", ErrInvalidRequest, f.Name)
			}
			continue
		}
		regular = append(regular, f)
	}
	return method, scheme, authority, path, regular, nil
}

// statusValue renders an HTTP status code as its 3-digit ASCII :status value.
// The http3 layer guarantees a final response's status is in 100–599, so the
// result is always the three digits parseStatus expects.
func statusValue(status int) []byte {
	return []byte(strconv.Itoa(status))
}

// ————————————————————————————————————————————————————————————————
// singleH3Conn — HTTP/3 transport for a single QUIC connection
// ————————————————————————————————————————————————————————————————

// singleH3Conn is the HTTP/3 analogue of singleConn: at most one *http3.Client
// (one QUIC connection) per transport, lazy-dialled on first use and shared
// across concurrent exchanges (http3.Client supports concurrent Do). Unlike
// singleConn it owns UDP/QUIC dialing via http3.Dial rather than a conn.Dialer,
// so it holds a *tls.Config + addr instead of conn.ConnOptions. It serves both the
// buffered path (h3Exchange + http3.Client.Do) and the streaming path (h3RespStream
// + http3.Client.DoStream). For multiple QUIC connections to one host, use
// TransportH3Pool (h3Pool); this transport is the single-connection case.
type singleH3Conn struct {
	addr      string
	tlsConfig *tls.Config
	backoff   time.Duration

	// dialFn dials a new h3Client. Production is h3DialFn (http3.Dial); tests
	// substitute a fake so no live QUIC connection is required.
	dialFn func(ctx context.Context, addr string, tlsConfig *tls.Config) (h3Client, error)

	// hooksRef points at Client.hooks; metrics is shared with Client.
	hooksRef *atomic.Pointer[Hooks]
	metrics  *Metrics

	mu           sync.Mutex
	cur          h3Client
	dialErr      error
	lastDialAt   time.Time
	closed       bool
	dialing      chan struct{}
	warmupCancel context.CancelFunc
}

// openExchange implements transport.openExchange for HTTP/3. It acquires the
// shared client and returns a fresh h3Exchange. pushLookup is nil (HTTP/3 server
// push is disabled — the client never sends MAX_PUSH_ID) and release is a no-op
// (the client is shared, like an H2 conn; the exchange holds no per-request slot).
func (s *singleH3Conn) openExchange(ctx context.Context) (protoStream, func(uint32) (*conn.Stream, bool), func(), error) {
	cl, err := s.acquireClient(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return getH3Exchange(cl), nil, func() {}, nil
}

// acquireClient returns the cached h3Client, lazy-dialling one if needed. It
// mirrors singleConn.acquireConn: in-flight dial dedup via s.dialing, backoff
// suppression after a failed dial, and OnDial/dial-metric emission. http3.Client
// exposes no liveness probe, so (unlike singleConn) a dead connection is not
// auto-redialled here — a failed Do surfaces the error to the caller. For
// automatic replacement of a dead connection, use TransportH3Pool, whose actor
// evicts dead conns and dials fresh ones.
func (s *singleH3Conn) acquireClient(ctx context.Context) (h3Client, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if s.cur != nil {
			c := s.cur
			s.mu.Unlock()
			return c, nil
		}
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

		cl, dialErr := s.dial(ctx)

		s.mu.Lock()
		s.lastDialAt = time.Now()
		s.dialing = nil
		close(ch)
		if s.closed {
			if cl != nil {
				_ = cl.Close()
			}
			s.mu.Unlock()
			return nil, ErrClosed
		}
		if dialErr != nil {
			s.dialErr = dialErr
			s.mu.Unlock()
			return nil, &DialError{Addr: s.addr, Err: dialErr}
		}
		s.cur = cl
		s.dialErr = nil
		c := s.cur
		s.mu.Unlock()
		return c, nil
	}
}

// dial performs one dial attempt outside the lock, recording the dial metrics
// and firing the OnDial hook exactly like singleConn.
func (s *singleH3Conn) dial(ctx context.Context) (h3Client, error) {
	dialStart := time.Now()
	s.metrics.Counters.DialsAttempted.Add(1)
	cl, dialErr := s.dialFn(ctx, s.addr, s.tlsConfig)
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
	return cl, dialErr
}

// close implements transport.close. It closes the underlying HTTP/3 client
// (graceful CONNECTION_CLOSE with H3_NO_ERROR) and fires OnConnClose. Idempotent.
func (s *singleH3Conn) close() error {
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
		err := cur.Close()
		s.notifyClose()
		return err
	}
	return nil
}

// shutdown implements transport.shutdown. http3.Client has no graceful drain
// separate from Close, so — like h1singleConn — shutdown simply closes.
func (s *singleH3Conn) shutdown(gracefulTimeout time.Duration) error {
	_ = gracefulTimeout
	return s.close()
}

// warmup implements transport.warmup. n is capped at 1 (a single HTTP/3
// connection is ever dialled); it triggers a background lazy dial whose context
// close()/shutdown() can cancel promptly.
func (s *singleH3Conn) warmup(n int) {
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
		_, _ = s.acquireClient(ctx)
		cancel()
		s.mu.Lock()
		s.warmupCancel = nil
		s.mu.Unlock()
	}()
}

// notifyClose increments ConnsClosed and fires OnConnClose (CloseManual), so an
// HTTP/3 client's teardown is observable to the same hook/metric surface as a
// pooled H2 conn.
func (s *singleH3Conn) notifyClose() {
	s.metrics.Counters.ConnsClosed.Add(1)
	if hr := s.hooksRef; hr != nil {
		if h := hr.Load(); h != nil && h.OnConnClose != nil {
			h.OnConnClose(ConnCloseEvent{Addr: s.addr, Reason: CloseManual})
		}
	}
}
