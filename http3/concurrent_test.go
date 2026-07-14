package http3

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// The PR 2d acid test (docs/HTTP3_DESIGN.md §5): N concurrent Do on ONE Client,
// under -race. inFlight is gone, QPACK is per-request, OpenStream waits on stream
// credit, and streams wake independently — so these must all pass with the real
// reader goroutine driving the fake QUIC engine below.
//
// muxConn/muxStream is a multi-stream reader-model fake (the single-request fakeConn
// in client_test.go cannot host concurrent requests). It mirrors the real
// contract: one reader goroutine calls Poll in a loop while N Do goroutines
// OpenStream/Send/Recv concurrently, everything guarded by one muxConn.mu (as the
// real quic.Conn guards everything with c.mu), and the wake channels never held
// across a select. The fake models three real mechanisms the acid test needs:
//   - a cumulative initial_max_streams_bidi limit + credit wake (the "server" raises
//     the limit as each stream's response is delivered, modelling MAX_STREAMS), so a
//     wave of Do past the limit parks in OpenStream and wakes when credit frees;
//   - per-stream send backpressure (a send window the reader refills), so a request
//     body larger than the window parks in WaitSendable and is woken by the reader —
//     the unit-level stand-in for the cwnd wake under contention;
//   - per-stream response delivery on the reader, signalled via s.ready.

// muxStream is a request (or control) stream in the multi-stream fake.
type muxStream struct {
	conn      *muxConn
	id        uint64
	seq       int  // unique per request; embedded in the response for cross-talk detection
	isControl bool

	// send side (guarded by conn.mu)
	sent        []byte
	finSent     bool
	sendWindow  int  // bytes this stream may send before it must park (request streams only)
	sendBlocked bool // a Send returned 0; the reader must refill sendWindow and signal
	aborted     bool // the Do reset/stop-sent this stream; the reader skips it

	// receive side (the response the reader delivers; guarded by conn.mu)
	resp          []byte
	respFin       bool
	delivered     bool
	readOff       int
	recvReset     bool
	recvResetCode uint64

	reset     bool
	stopped   bool
	resetCode uint64
	stopCode  uint64

	ready chan struct{} // cap 1; reader→Do wake, like quic.Stream.ready
}

func (s *muxStream) ID() uint64 { return s.id }

func (s *muxStream) Send(data []byte, fin bool) (int, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.isControl {
		s.sent = append(s.sent, data...) // the control stream is never flow-control blocked
		if fin {
			s.finSent = true
		}
		return len(data), nil
	}
	n := len(data)
	if n > s.sendWindow {
		n = s.sendWindow
	}
	if len(data) > 0 && n == 0 {
		s.sendBlocked = true
		s.conn.kickWakeLocked() // ask the reader to refill this stream's send window
		return 0, nil
	}
	s.sendWindow -= n
	s.sent = append(s.sent, data[:n]...)
	if fin && n == len(data) {
		s.finSent = true
	}
	s.conn.kickWakeLocked() // finSent → deliver the response; progress → re-evaluate
	return n, nil
}

func (s *muxStream) Recv() []byte {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.delivered && s.readOff < len(s.resp) {
		out := s.resp[s.readOff:]
		s.readOff = len(s.resp)
		return out
	}
	return nil
}

func (s *muxStream) finishedLocked() bool {
	return s.recvReset || (s.delivered && s.respFin && s.readOff >= len(s.resp))
}

func (s *muxStream) Finished() bool {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.finishedLocked()
}

func (s *muxStream) RecvState() (finished, reset bool, code uint64) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.finishedLocked(), s.recvReset, s.recvResetCode
}

func (s *muxStream) WaitReadable(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.done:
		return s.conn.closeErr
	}
}

func (s *muxStream) WaitSendable(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.done:
		return s.conn.closeErr
	}
}

func (s *muxStream) Reset(code uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.reset, s.resetCode, s.aborted = true, code, true
	return nil
}

func (s *muxStream) StopSending(code uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.stopped, s.stopCode, s.aborted = true, code, true
	return nil
}

// muxConn is the multi-stream fake QUIC connection.
type muxConn struct {
	mu sync.Mutex

	bidiLimit  uint64 // cumulative initial_max_streams_bidi (raised as responses deliver)
	openedBidi uint64
	nextID     uint64 // next client bidi id (0, 4, 8, …)
	seqCounter int

	streams []*muxStream // active request streams the reader scans
	control *muxStream

	sendWindow0 int  // initial per-request send window
	grant       int  // send-credit refill per reader pass
	deliverOpen bool // when false the reader withholds responses (for cancel/close tests)

	credit chan struct{} // cap 1; stream-credit wake (models c.streamCredit)
	wake   chan struct{} // cap 1; reader wake
	done   chan struct{} // single-close latch (models c.done)

	closeErr  error
	closed    bool
	closeApp  bool
	closeCode uint64
}

func newMuxConn(bidiLimit uint64) *muxConn {
	return &muxConn{
		bidiLimit:   bidiLimit,
		sendWindow0: 8192,
		grant:       8192,
		deliverOpen: true,
		credit:      make(chan struct{}, 1),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

func (c *muxConn) kickWakeLocked() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *muxConn) signalCreditLocked() {
	select {
	case c.credit <- struct{}{}:
	default:
	}
}

func signalReady(s *muxStream) {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

func (c *muxConn) OpenStream(ctx context.Context) (quicStream, error) {
	for {
		c.mu.Lock()
		if c.closed {
			err := c.closeErr
			c.mu.Unlock()
			return nil, err
		}
		if c.openedBidi < c.bidiLimit {
			id := c.nextID
			c.nextID += 4
			c.openedBidi++
			c.seqCounter++
			s := c.newRequestStreamLocked(id, c.seqCounter)
			c.streams = append(c.streams, s)
			// Baton-pass, like the real openStreamLocked: if credit remains, wake the
			// next parked OpenStream so one MAX_STREAMS grant wakes a chain of waiters.
			if c.openedBidi < c.bidiLimit {
				c.signalCreditLocked()
			}
			c.mu.Unlock()
			return s, nil
		}
		c.mu.Unlock()
		select {
		case <-c.credit: // the limit rose; retry under the lock
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			c.mu.Lock()
			err := c.closeErr
			c.mu.Unlock()
			return nil, err
		}
	}
}

// newRequestStreamLocked builds a request stream whose canned response embeds seq
// both in an x-seq header and in the body, so a Do can prove it received its OWN
// response and QPACK did not cross-decode another request's headers.
func (c *muxConn) newRequestStreamLocked(id uint64, seq int) *muxStream {
	seqStr := strconv.Itoa(seq)
	resp := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("x-seq", seqStr)))
	resp = append(resp, AppendData(nil, []byte("seq="+seqStr))...)
	return &muxStream{
		conn:       c,
		id:         id,
		seq:        seq,
		resp:       resp,
		respFin:    true,
		sendWindow: c.sendWindow0,
		ready:      make(chan struct{}, 1),
	}
}

// OpenUniStream hands out the three client unidirectional streams (control, QPACK
// encoder, QPACK decoder). All accept sends whole (isControl); only the first is
// retained as the control stream. INERT: at capacity 0 the QPACK streams carry
// nothing beyond their type byte.
func (c *muxConn) OpenUniStream() (quicStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &muxStream{conn: c, isControl: true, ready: make(chan struct{}, 1)}
	if c.control == nil {
		c.control = s
	}
	return s, nil
}

func (c *muxConn) AcceptUniStream() quicStream { return nil } // no server-initiated uni streams

func (c *muxConn) Poll(ctx context.Context) error {
	c.mu.Lock()
	progressed := c.processBurstLocked()
	closed, closeErr := c.closed, c.closeErr
	c.mu.Unlock()
	if closed {
		return closeErr
	}
	if progressed {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	case <-c.wake:
		return nil // a Do made progress; loop to deliver / grant
	}
}

// processBurstLocked is the reader step: refill the send window of any send-blocked
// stream, and deliver the response of any stream whose request is fully sent. Each
// delivery raises the cumulative stream limit and signals stream credit, modelling
// the server issuing MAX_STREAMS as it finishes each exchange. Assumes c.mu held.
func (c *muxConn) processBurstLocked() bool {
	progressed := false
	for _, s := range c.streams {
		if s.aborted || s.recvReset {
			continue
		}
		if !s.finSent {
			if s.sendBlocked {
				s.sendWindow += c.grant
				s.sendBlocked = false
				signalReady(s)
				progressed = true
			}
			continue
		}
		if !s.delivered && c.deliverOpen {
			s.delivered = true
			signalReady(s)
			c.bidiLimit++           // the server closes the stream → grants a MAX_STREAMS credit
			c.signalCreditLocked()  // wake an OpenStream parked on the limit
			progressed = true
		}
	}
	return progressed
}

func (c *muxConn) CloseWithError(app bool, code uint64, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil // idempotent: first close wins (mirrors quic.Conn)
	}
	c.closed, c.closeApp, c.closeCode = true, app, code
	if c.closeErr == nil {
		c.closeErr = quic.ErrConnClosed
	}
	close(c.done)
	return nil
}

// openDelivery lets withheld responses flow (cancel/close tests start with the gate
// shut), waking the reader.
func (c *muxConn) openDelivery() {
	c.mu.Lock()
	c.deliverOpen = true
	c.kickWakeLocked()
	c.mu.Unlock()
}

// sentCount reports how many request streams have their FIN sent (their whole
// request is on the wire and the Do is in its response loop).
func (c *muxConn) sentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.streams {
		if s.finSent {
			n++
		}
	}
	return n
}

// waitSentN spins until at least n request streams are fully sent, so a test acts on
// genuinely in-flight Do goroutines. Bounded so a stall fails loudly.
func waitSentN(t *testing.T, c *muxConn, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.sentCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d requests were sent", c.sentCount(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

type doResult struct {
	resp *Response
	body []byte
	err  error
}

func doRequest(ctx context.Context, client *Client, path string) doResult {
	resp, body, err := client.Do(ctx, &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: path,
	})
	return doResult{resp, body, err}
}

func doRequestBody(ctx context.Context, client *Client, path string, body []byte) doResult {
	resp, rbody, err := client.Do(ctx, &Request{
		Method: "POST", Scheme: "https", Authority: "example.com", Path: path, Body: body,
	})
	return doResult{resp, rbody, err}
}

// headerValue returns the value of the first header named name, or "".
func headerValue(resp *Response, name string) string {
	for _, h := range resp.Headers {
		if string(h.Name) == name {
			return string(h.Value)
		}
	}
	return ""
}

// checkResponse asserts a Do returned its OWN response: status 200 and the x-seq
// header equals the body's "seq=" token. A shared/corrupted QPACK decoder would
// mismatch these (and trip -race first).
func checkResponse(t *testing.T, r doResult) {
	t.Helper()
	if r.err != nil {
		t.Errorf("Do: %v", r.err)
		return
	}
	if r.resp == nil || r.resp.Status != 200 {
		t.Errorf("resp = %+v, want status 200", r.resp)
		return
	}
	seq := headerValue(r.resp, "x-seq")
	if seq == "" {
		t.Errorf("response missing x-seq header: %+v", r.resp.Headers)
		return
	}
	if want := []byte("seq=" + seq); !bytes.Equal(r.body, want) {
		t.Errorf("body = %q, want %q (QPACK cross-talk between concurrent Do?)", r.body, want)
	}
}

// TestConcurrent_ManyStreamsUnderRace is the acid test: 64 concurrent Do on one
// Client, each with a request body larger than the 8 KiB send window (so it parks
// in WaitSendable and is woken by the reader), against a peer whose bidi-stream
// limit starts at 8 (so most Do park on stream credit and wake as the limit rises).
// Every response must be its own, and nothing may race or hang.
func TestConcurrent_ManyStreamsUnderRace(t *testing.T) {
	conn := newMuxConn(8) // small initial stream limit forces the credit wake
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 64
	body := bytes.Repeat([]byte("x"), 13000) // > 12 KiB: exercises the send/cwnd wake under contention
	results := make([]doResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = doRequestBody(context.Background(), client, "/"+strconv.Itoa(i), body)
		}(i)
	}
	wg.Wait()

	for i := range results {
		checkResponse(t, results[i])
	}
}

// TestConcurrent_WavesExceedStreamLimit validates the OnMaxStreams credit wake
// directly: three waves of eight Do (24 total) all launched concurrently against a
// peer bidi limit of 8, so 16 of them must park on stream credit and wake only as
// the limit rises with each delivered response. All 24 must complete.
func TestConcurrent_WavesExceedStreamLimit(t *testing.T) {
	conn := newMuxConn(8) // 8 slots; 24 requests => 16 must wait for credit
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 24
	results := make([]doResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = doRequest(context.Background(), client, "/"+strconv.Itoa(i))
		}(i)
	}
	wg.Wait()

	for i := range results {
		checkResponse(t, results[i])
	}
}

// TestConcurrent_PerRequestCancel checks that cancelling ONE Do among many aborts
// only its own stream and leaves the rest to complete (docs/HTTP3_DESIGN.md §5).
// Responses are withheld until after the cancel, so the cancelled Do is genuinely
// parked in WaitReadable when its context fires.
func TestConcurrent_PerRequestCancel(t *testing.T) {
	conn := newMuxConn(100) // ample stream credit; this test is about per-request cancel
	conn.deliverOpen = false // hold every response until after the cancel
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const others = 7
	results := make([]doResult, others)
	var wg sync.WaitGroup
	wg.Add(others)

	// The Do we cancel reports to its own channel so we can await it without racing
	// the other goroutines' result slice; deliverOpen is false, so a cancelled Do
	// can only wake via its context, never via a delivered response.
	cancelCtx, cancel := context.WithCancel(context.Background())
	res0 := make(chan doResult, 1)
	go func() { res0 <- doRequest(cancelCtx, client, "/cancel") }()
	for i := 0; i < others; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = doRequest(context.Background(), client, "/"+strconv.Itoa(i))
		}(i)
	}

	waitSentN(t, conn, others+1) // every Do is parked in WaitReadable (no response delivered)
	cancel()                     // cancel only the dedicated Do

	select {
	case r := <-res0:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("cancelled Do err = %v, want context.Canceled", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled Do did not return")
	}

	conn.openDelivery() // let the remaining seven complete
	wg.Wait()
	for i := range results {
		checkResponse(t, results[i])
	}
	// The connection survived a per-request cancel.
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Fatal("a per-request cancel must not close the connection")
	}
}

// TestConcurrent_CloseMidFlight checks that Close while many Do are in flight fails
// every one of them cleanly with the graceful ErrConnClosed, not context.Canceled
// (docs/HTTP3_DESIGN.md §5, PR 2c/2d Close latch).
func TestConcurrent_CloseMidFlight(t *testing.T) {
	conn := newMuxConn(100)
	conn.deliverOpen = false // nothing ever completes; every Do is left parked
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}

	const n = 32
	results := make([]doResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = doRequest(context.Background(), client, "/"+strconv.Itoa(i))
		}(i)
	}
	waitSentN(t, conn, n) // all 32 parked in WaitReadable

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	for i := range results {
		if !errors.Is(results[i].err, quic.ErrConnClosed) {
			t.Fatalf("in-flight Do %d woke with %v, want quic.ErrConnClosed (graceful)", i, results[i].err)
		}
	}
}
