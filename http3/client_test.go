package http3

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// fakeStream models a QUIC stream under the reader-goroutine architecture
// (docs/HTTP3_DESIGN.md §3). A request stream's response chunks in recvChunks are
// DELIVERED by the reader — the fake conn's Poll advances deliverIdx and signals
// ready — and read by Do via Recv, which returns the whole delivered-but-unread
// prefix (like quic.Stream.Recv returning all contiguous bytes). A server
// unidirectional stream (directRead, set by AcceptUniStream) is read straight off
// recvChunks by serviceControl, one chunk per Recv, with no reader delivery.
type fakeStream struct {
	id      uint64
	conn    *fakeConn
	sent    []byte
	finSent bool

	recvChunks    [][]byte
	deliverIdx    int    // chunks the reader (Poll) has delivered — request streams
	readIdx       int    // chunks Recv has consumed
	directRead    bool   // server uni stream: Recv reads recvChunks directly
	fin           bool   // stream carries FIN after the last chunk
	recvReset     bool   // the peer reset its send side (RESET_STREAM received)
	recvResetCode uint64 // the application error code carried by that RESET_STREAM
	sendCap       int    // max bytes accepted per Send (0 = unlimited); models flow control
	sendResetErr  bool   // Send returns quic.ErrStreamReset (models a received STOP_SENDING)

	reset     bool // Reset was called
	stopped   bool // StopSending was called
	resetCode uint64
	stopCode  uint64

	ready chan struct{} // cap 1; reader→Do wake, like quic.Stream.ready
}

func (s *fakeStream) ID() uint64 { return s.id }

func (s *fakeStream) Send(data []byte, fin bool) (int, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.sendResetErr {
		return 0, quic.ErrStreamReset // the peer reset our send side (STOP_SENDING)
	}
	n := len(data)
	if s.sendCap > 0 && n > s.sendCap {
		n = s.sendCap
	}
	s.sent = append(s.sent, data[:n]...)
	if fin && n == len(data) {
		s.finSent = true
	}
	return n, nil
}

func (s *fakeStream) Recv() []byte {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.directRead {
		// Server uni stream: one chunk per call (a stream-type varint or a control
		// frame may arrive across service calls).
		if s.readIdx >= len(s.recvChunks) {
			return nil
		}
		c := s.recvChunks[s.readIdx]
		s.readIdx++
		return c
	}
	// Request stream: return all delivered-but-unread bytes at once.
	var out []byte
	for s.readIdx < s.deliverIdx {
		out = append(out, s.recvChunks[s.readIdx]...)
		s.readIdx++
	}
	return out
}

// resetActive reports whether the peer's RESET_STREAM has surfaced. A real
// stream returns its buffered response bytes before reporting the terminal
// reset, so the reset only becomes visible once every buffered chunk has been
// read (readIdx past the last chunk). Reporting it earlier races the response
// header read and makes DoStream return the reset instead of the head.
func (s *fakeStream) resetActive() bool {
	return s.recvReset && s.readIdx >= len(s.recvChunks)
}

func (s *fakeStream) finishedLocked() bool {
	if s.resetActive() {
		return true
	}
	if s.directRead {
		return s.fin && s.readIdx >= len(s.recvChunks)
	}
	return s.fin && s.deliverIdx >= len(s.recvChunks)
}

// Finished reports end-of-stream. Used by checkCriticalStreams on the reader.
func (s *fakeStream) Finished() bool {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.finishedLocked()
}

// RecvState is the locked snapshot the response loop reads (docs/HTTP3_DESIGN.md §3.4).
func (s *fakeStream) RecvState() (finished, reset bool, code uint64) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	return s.finishedLocked(), s.resetActive(), s.recvResetCode
}

// WaitReadable blocks until the reader signals progress, the request ctx is
// cancelled, or the connection terminates (docs/HTTP3_DESIGN.md §3.3).
func (s *fakeStream) WaitReadable(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.done:
		return s.conn.closeErr
	}
}

// WaitSendable mirrors WaitReadable: the fake Send always makes progress when
// there is data, so a park here only happens in the never-completing tests.
func (s *fakeStream) WaitSendable(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.done:
		return s.conn.closeErr
	}
}

func (s *fakeStream) Reset(code uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.resetCode = code
	s.reset = true
	return nil
}
func (s *fakeStream) StopSending(code uint64) error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.stopCode = code
	s.stopped = true
	return nil
}

type fakeConn struct {
	mu sync.Mutex

	control    *fakeStream
	clientQEnc *fakeStream // the client's own QPACK encoder stream (2nd uni opened)
	clientQDec *fakeStream // the client's own QPACK decoder stream (3rd uni opened)
	uniOpened  int         // count of OpenUniStream calls, to route the three client uni streams
	req        *fakeStream
	polls      atomic.Int64
	uniSendCap int                         // send cap applied to the control stream
	pollHook   func(context.Context) error // overrides Poll when set (for special tests)
	acceptQ    []quicStream                // server uni streams handed out by AcceptUniStream
	closeApp   bool                        // captured CloseWithError arguments
	closeCode  uint64
	closed     bool

	done     chan struct{} // closed once by CloseWithError; models quic.Conn.done
	closeErr error         // published before done is closed; models quic.Conn.closeErr
	wake     chan struct{} // cap 1; wakes a parked Poll to re-deliver fed data
}

// ensureInitLocked lazily allocates the wake/close channels so tests can build a
// fakeConn with a struct literal.
func (c *fakeConn) ensureInitLocked() {
	if c.done == nil {
		c.done = make(chan struct{})
	}
	if c.wake == nil {
		c.wake = make(chan struct{}, 1)
	}
}

func (c *fakeConn) AcceptUniStream() quicStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitLocked()
	if len(c.acceptQ) == 0 {
		return nil
	}
	s := c.acceptQ[0]
	c.acceptQ = c.acceptQ[1:]
	if fs, ok := s.(*fakeStream); ok {
		fs.conn = c
		fs.directRead = true
		if fs.ready == nil {
			fs.ready = make(chan struct{}, 1)
		}
	}
	return s
}

// OpenUniStream hands out the three client unidirectional streams newClient opens
// in order: control (0x00), then the client QPACK encoder (0x02) and decoder (0x03)
// streams. Only the control stream carries uniSendCap (the flow-control-drain
// test); the QPACK streams accept sends whole.
func (c *fakeConn) OpenUniStream() (quicStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitLocked()
	s := &fakeStream{conn: c, ready: make(chan struct{}, 1)}
	switch c.uniOpened {
	case 0:
		s.sendCap = c.uniSendCap
		c.control = s
	case 1:
		c.clientQEnc = s
	case 2:
		c.clientQDec = s
	}
	c.uniOpened++
	return s, nil
}

func (c *fakeConn) OpenStream(_ context.Context) (quicStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitLocked()
	if c.req == nil {
		c.req = &fakeStream{}
	}
	c.req.conn = c
	if c.req.ready == nil {
		c.req.ready = make(chan struct{}, 1)
	}
	return c.req, nil
}

// Poll models the reader step: it delivers the request stream's pending response
// chunks in one burst (advancing deliverIdx and signalling ready), and otherwise
// parks until data is fed (via wake), the ctx is cancelled, or the connection is
// closed — so the reader never busy-loops and never auto-services the control
// stream for a request-less test (control_test.go drives serviceControl itself).
func (c *fakeConn) Poll(ctx context.Context) error {
	c.mu.Lock()
	c.ensureInitLocked()
	c.polls.Add(1)
	delivered := false
	if r := c.req; r != nil && !r.directRead && r.deliverIdx < len(r.recvChunks) {
		r.deliverIdx = len(r.recvChunks) // deliver the whole available burst at once
		delivered = true
		if r.ready != nil {
			select {
			case r.ready <- struct{}{}:
			default:
			}
		}
	}
	hook, done, wake := c.pollHook, c.done, c.wake
	c.mu.Unlock()
	if delivered {
		return nil
	}
	if hook != nil {
		return hook(ctx)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		c.mu.Lock()
		e := c.closeErr
		c.mu.Unlock()
		return e
	case <-wake:
		return nil // fed more data; loop to deliver
	}
}

func (c *fakeConn) CloseWithError(app bool, code uint64, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitLocked()
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

// TestClient_RequestResponse drives a full request/response through the HTTP/3
// client against a fake QUIC transport: the control stream gets SETTINGS first,
// the request is a well-formed HEADERS frame, and the response HEADERS + DATA
// (delivered across two Poll iterations) decode to a Response and body.
func TestClient_RequestResponse(t *testing.T) {
	// Server-side response bytes, split so the client must Poll for the body.
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-type", "text/plain")))
	dataFrame := AppendData(nil, []byte("hello world"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headersFrame, dataFrame}, fin: true}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}

	resp, body, err := client.Do(context.Background(), req)

	require.NoError(t, err, "Do against a well-formed HEADERS+DATA response")
	// Control stream: type 0x00 then a SETTINGS frame (§6.2.1).
	styp, n, serr := ReadStreamType(conn.control.sent)
	require.NoError(t, serr, "the control stream must open with a stream-type varint")
	require.Equalf(t, StreamTypeControl, styp, "control stream type = %#x, want 0x00", styp)
	var cfr FrameReader
	cfr.Feed(conn.control.sent[n:])
	ftyp, _, ferr := cfr.ReadFrame()
	require.NoError(t, ferr, "the control stream's first frame must be complete")
	assert.Equalf(t, FrameSettings, ftyp,
		"first control frame = %#x, want SETTINGS — §6.2.1 requires it first", ftyp)
	// The request stream carried a single HEADERS frame, sent with FIN.
	assert.True(t, conn.req.finSent, "request must be sent with FIN")
	rtyp, rlen, rn, perr := ParseFrameHeader(conn.req.sent)
	require.NoError(t, perr, "the request stream must carry a parseable frame header")
	require.Equalf(t, FrameHeaders, rtyp, "request frame = %#x, want HEADERS", rtyp)
	reqFields := decodeAll(t, conn.req.sent[rn:rn+int(rlen)])
	require.GreaterOrEqualf(t, len(reqFields), 4,
		"request must carry all four pseudo-headers, got %+v", reqFields)
	assert.Equalf(t, ":method", string(reqFields[0].Name),
		"the first request field must be :method, got %+v", reqFields)
	assert.Equalf(t, "GET", string(reqFields[0].Value),
		"the :method value must be the request's method, got %+v", reqFields)
	// The response decoded correctly, and the body required a Poll.
	assert.Equal(t, 200, resp.Status, "status decoded off the response HEADERS frame")
	assert.Truef(t, bytes.Equal(body, []byte("hello world")),
		"body = %q, want %q", body, "hello world")
	assert.NotZero(t, conn.polls.Load(),
		"expected at least one Poll to receive the body — a body delivered without one "+
			"means the fixture handed it over before Do could park")
}

// TestConformance_RFC9114_Sec41_ResponseReadAfterStopSending checks that when the
// server aborts reading the request with STOP_SENDING (surfaced as ErrStreamReset
// on the send path), the client stops sending but still reads the response on the
// stream's independent receive side (RFC 9114 §4.1) rather than discarding it.
func TestConformance_RFC9114_Sec41_ResponseReadAfterStopSending(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headersFrame}, fin: true, sendResetErr: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	req := &Request{Method: "POST", Scheme: "https", Authority: "h", Path: "/", Body: []byte("aborted request body")}

	resp, _, doErr := client.Do(context.Background(), req)

	require.NoErrorf(t, doErr,
		"Do after STOP_SENDING = %v, want the response still delivered — §4.1 makes the "+
			"receive side independent of the aborted send side", doErr)
	assert.Equal(t, 200, resp.Status,
		"the response the server sent must survive its own STOP_SENDING on the request")
}

// TestClient_RequestWithBody verifies a request body is sent as a DATA frame
// after the HEADERS frame, with the FIN on the DATA (RFC 9114 §4.1).
func TestClient_RequestWithBody(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headersFrame}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	body := []byte("field=value&x=1")

	resp, _, doErr := client.Do(context.Background(), &Request{Method: "POST", Scheme: "https", Authority: "h", Path: "/", Body: body})

	require.NoError(t, doErr, "Do for a POST carrying a request body")
	assert.Equal(t, 200, resp.Status, "status decoded off the response HEADERS frame")
	assert.True(t, conn.req.finSent, "FIN must be sent on the DATA frame")
	// The request stream carried a HEADERS frame then a DATA frame with the body.
	htyp, hlen, hn, herr := ParseFrameHeader(conn.req.sent)
	require.NoError(t, herr, "the request stream must open with a parseable frame header")
	require.Equalf(t, FrameHeaders, htyp, "first frame = %#x, want HEADERS", htyp)
	rest := conn.req.sent[hn+int(hlen):]
	dtyp, dlen, dn, derr := ParseFrameHeader(rest)
	require.NoError(t, derr, "the body must follow in a parseable second frame")
	require.Equalf(t, FrameData, dtyp, "second frame = %#x, want DATA", dtyp)
	got := rest[dn : dn+int(dlen)]
	assert.Truef(t, bytes.Equal(got, body), "DATA payload = %q, want %q", got, body)
}

// TestClient_SendDrainsUnderFlowControl forces both the control and request
// streams to accept only a few bytes per Send, verifying the client drains the
// whole SETTINGS and HEADERS frames (and the FIN) instead of truncating them —
// the flow-control-block case the happy-path fake cannot exercise.
func TestClient_SendDrainsUnderFlowControl(t *testing.T) {
	settings := []Setting{{SettingQPACKMaxTableCapacity, 0}, {SettingMaxFieldSectionSize, 65536}}
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "204")))
	conn := &fakeConn{
		uniSendCap: 3,
		req:        &fakeStream{sendCap: 3, recvChunks: [][]byte{headersFrame}, fin: true},
	}
	client, err := NewClientFake(conn, settings)
	require.NoError(t, err, "NewClientFake over a transport that accepts 3 bytes per Send")

	resp, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	require.NoError(t, doErr, "Do over a transport that accepts 3 bytes per Send")
	wantControl := AppendClientControlStream(nil, settings)
	assert.Truef(t, bytes.Equal(conn.control.sent, wantControl),
		"control stream truncated: got %d bytes, want %d — a partial Send must be "+
			"retried until the whole SETTINGS frame is on the wire",
		len(conn.control.sent), len(wantControl))
	assert.Equal(t, 204, resp.Status, "status decoded off the response HEADERS frame")
	assert.True(t, conn.req.finSent, "FIN must be sent once the whole HEADERS frame is drained")
	rtyp, rlen, rn, perr := ParseFrameHeader(conn.req.sent)
	require.NoError(t, perr, "the request stream must carry a parseable frame header")
	assert.Equalf(t, FrameHeaders, rtyp, "request frame = %#x, want HEADERS", rtyp)
	assert.Equalf(t, len(conn.req.sent), rn+int(rlen),
		"request HEADERS frame truncated: %d bytes on the wire, frame declares %d",
		len(conn.req.sent), rn+int(rlen))
}

// TestClient_DataBeforeHeaders rejects a response that sends DATA before any
// HEADERS frame — an invalid frame sequence, so a H3_FRAME_UNEXPECTED connection
// error (RFC 9114 §4.1).
func TestClient_DataBeforeHeaders(t *testing.T) {
	dataFrame := AppendData(nil, []byte("body"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{dataFrame}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control (connection error) — a DATA frame before any "+
			"HEADERS is an invalid frame sequence, not a per-stream fault", doErr)
	assert.Equalf(t, H3FrameUnexpected, conn.closeCode,
		"close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
}

// TestConformance_RFC9114_Sec724_SettingsOnRequestStream checks that a SETTINGS
// frame on a request stream is a H3_FRAME_UNEXPECTED connection error (§7.2.4).
func TestConformance_RFC9114_Sec724_SettingsOnRequestStream(t *testing.T) {
	settings := AppendFrameHeader(nil, FrameSettings, 0) // empty SETTINGS
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{settings}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control — a control-stream-only frame on a request "+
			"stream is a connection error, not a per-stream fault", doErr)
	assert.Equalf(t, H3FrameUnexpected, conn.closeCode,
		"close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
}

// TestConformance_RFC9114_Sec728_ReservedFrameOnRequestStream checks that a
// reserved HTTP/2-carryover frame type (0x02) on a request stream is a
// H3_FRAME_UNEXPECTED connection error (§7.2.8).
func TestConformance_RFC9114_Sec728_ReservedFrameOnRequestStream(t *testing.T) {
	reserved := AppendFrameHeader(nil, 0x02, 0)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{reserved}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control — a reserved HTTP/2-carryover frame type must "+
			"be a connection error, never silently ignored as GREASE is", doErr)
	assert.Equalf(t, H3FrameUnexpected, conn.closeCode,
		"close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
}

// TestConformance_RFC9114_Sec725_PushPromiseOnRequestStream checks that a
// PUSH_PROMISE on a request stream, when the client never sent MAX_PUSH_ID, is a
// H3_ID_ERROR connection error (§4.6, §7.2.5).
func TestConformance_RFC9114_Sec725_PushPromiseOnRequestStream(t *testing.T) {
	pp := AppendFrameHeader(nil, FramePushPromise, 0)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{pp}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control — a push the client never enabled is a "+
			"connection error, not something to ignore", doErr)
	assert.Equalf(t, H3IDError, conn.closeCode,
		"close code = %#x, want H3_ID_ERROR — the distinct code is what tells the "+
			"server it exceeded a push id we never granted", conn.closeCode)
}

// TestConformance_RFC9114_Sec71_TruncatedFrameAtStreamEnd checks that a stream
// ending in the middle of a frame payload (a truncated final frame after a valid
// response) is a H3_FRAME_ERROR connection error.
func TestConformance_RFC9114_Sec71_TruncatedFrameAtStreamEnd(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	truncated := AppendFrameHeader(nil, FrameData, 10) // declares 10 payload bytes
	truncated = append(truncated, []byte("abc")...)    // but only 3 arrive, then FIN
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, truncated...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control — a FIN mid-payload must not read back as a "+
			"short but successful body", doErr)
	assert.Equalf(t, H3FrameError, conn.closeCode,
		"close code = %#x, want H3_FRAME_ERROR", conn.closeCode)
}

// TestConformance_RFC9114_Sec71_TruncatedHeaderAtStreamEnd checks that a stream
// ending in the middle of a frame header (an incomplete frame) is a H3_FRAME_ERROR
// connection error.
func TestConformance_RFC9114_Sec71_TruncatedHeaderAtStreamEnd(t *testing.T) {
	// One byte: a frame type (HEADERS) with no length varint, then FIN.
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{{byte(FrameHeaders)}}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control — a FIN inside a frame header must not read "+
			"back as a clean end of stream", doErr)
	assert.Equalf(t, H3FrameError, conn.closeCode,
		"close code = %#x, want H3_FRAME_ERROR", conn.closeCode)
}

// TestConformance_RFC9114_Sec411_RequestReset checks that a server RESET_STREAM
// aborting the response surfaces as a StreamResetError carrying the application
// error code — not the §7.1 truncation frame error and not a truncated-body
// success — and that H3_REQUEST_REJECTED is reported retryable (§4.1.1). The
// connection is not torn down.
func TestConformance_RFC9114_Sec411_RequestReset(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	partial := AppendFrameHeader(nil, FrameData, 10) // declares 10 bytes
	partial = append(partial, []byte("abc")...)      // only 3 arrive, then a reset
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, partial...)}, recvReset: true, recvResetCode: H3RequestRejected}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	var rst *StreamResetError
	require.Truef(t, errors.As(doErr, &rst),
		"err = %v, want *StreamResetError — a peer reset must not be reported as "+
			"the §7.1 truncation error nor as a short success", doErr)
	assert.Equalf(t, H3RequestRejected, rst.Code,
		"reset code = %#x, want H3_REQUEST_REJECTED", rst.Code)
	assert.True(t, rst.Retryable(),
		"H3_REQUEST_REJECTED should be reported retryable — §4.1.1 makes it the one "+
			"code that guarantees the request was not processed")
	assert.NotEqualf(t, H3FrameError, conn.closeCode,
		"connection torn down with H3_FRAME_ERROR on a reset (%#x) — a stream reset "+
			"must not kill the connection", conn.closeCode)
}

// TestClient_RequestReset_NonRetryable checks that a reset with a code other than
// H3_REQUEST_REJECTED is surfaced but not reported retryable.
func TestClient_RequestReset_NonRetryable(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{recvReset: true, recvResetCode: H3RequestCancelled}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	var rst *StreamResetError
	require.Truef(t, errors.As(doErr, &rst),
		"err = %v, want *StreamResetError{H3_REQUEST_CANCELLED}", doErr)
	require.Equalf(t, H3RequestCancelled, rst.Code,
		"err = %v, want *StreamResetError{H3_REQUEST_CANCELLED}", doErr)
	assert.False(t, rst.Retryable(),
		"H3_REQUEST_CANCELLED must not be reported retryable — replaying a request "+
			"the server may already have processed is not safe")
}

// TestConformance_RFC9114_Sec412_ContentLengthMismatch checks that a response
// whose Content-Length does not equal the sum of DATA payloads is malformed
// (ErrH3Message) and the stream is aborted with H3_MESSAGE_ERROR.
func TestConformance_RFC9114_Sec412_ContentLengthMismatch(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "5")))
	data := AppendData(nil, []byte("abc")) // 3 bytes ≠ declared 5
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.Equalf(t, ErrH3Message, doErr,
		"err = %v, want ErrH3Message — a body shorter than the declared "+
			"Content-Length must not be handed to the caller as complete", doErr)
	assert.Equalf(t, H3MessageError, conn.req.resetCode,
		"abort code = %#x, want H3_MESSAGE_ERROR — a malformed message is a stream "+
			"error, so the connection stays up", conn.req.resetCode)
}

// TestConformance_RFC9114_Sec412_ContentLengthMatch checks that a Content-Length
// equal to the received body is accepted.
func TestConformance_RFC9114_Sec412_ContentLengthMatch(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "3")))
	data := AppendData(nil, []byte("abc"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	resp, body, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	require.NoErrorf(t, doErr,
		"Do = %v; a Content-Length equal to the received body is well-formed and "+
			"must not be rejected", doErr)
	assert.Equalf(t, 200, resp.Status, "status=%d body=%q", resp.Status, body)
	assert.Equalf(t, "abc", string(body), "status=%d body=%q", resp.Status, body)
}

// TestClient_ContentLength_NoContentStatusExempt checks that a 204 with a
// non-zero Content-Length and no DATA is not malformed — the anticipatory
// Content-Length exception (RFC 9114 §4.1.2).
func TestClient_ContentLength_NoContentStatusExempt(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "204"), hf("content-length", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	resp, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	require.NoErrorf(t, doErr,
		"204 with anticipatory content-length must not be malformed: %v", doErr)
	assert.Equalf(t, 204, resp.Status, "status = %d, want 204", resp.Status)
}

// TestClient_ContentLength_OverflowMalformed checks that a Content-Length value
// past the int64 range does not wrap to a small number that spuriously matches
// the body — it is rejected as malformed.
func TestClient_ContentLength_OverflowMalformed(t *testing.T) {
	// 2^64 + 3 — a valid digit string that, if parsed with a post-multiply guard,
	// would wrap to 3 and falsely match a 3-byte body.
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "18446744073709551619")))
	data := AppendData(nil, []byte("abc"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.Equalf(t, ErrH3Message, doErr,
		"overflowing content-length: err = %v, want ErrH3Message — a value that "+
			"wrapped to 3 would spuriously match the 3-byte body", doErr)
}

// TestClient_ContentLength_ConflictingMalformed checks that two Content-Length
// fields with different values are malformed (RFC 9110 §8.6).
func TestClient_ContentLength_ConflictingMalformed(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "3"), hf("content-length", "5")))
	data := AppendData(nil, []byte("abc"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.Equalf(t, ErrH3Message, doErr,
		"conflicting content-length: err = %v, want ErrH3Message — picking either "+
			"value lets a peer disagree with an intermediary about the body length", doErr)
}

// TestConformance_RFC9204_Sec22_DecompressionFailedClosesConn checks that a QPACK
// field section the decoder cannot decode — here a reference to the dynamic table,
// which the static-only decoder does not support — is a QPACK_DECOMPRESSION_FAILED
// connection error, not a per-stream reset (RFC 9204 §2.2, §6).
func TestConformance_RFC9204_Sec22_DecompressionFailedClosesConn(t *testing.T) {
	// Field-section prefix (Required Insert Count 0, Base 0) then an Indexed Field
	// Line referencing the dynamic table (T=0, byte 0x80) — unresolvable at cap 0.
	headers := AppendHeaders(nil, []byte{0x00, 0x00, 0x80})
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrH3Control,
		"err = %v, want ErrH3Control (connection closed) — a failed decode leaves "+
			"the shared table state ambiguous, so it cannot be a per-stream error", doErr)
	assert.Equalf(t, H3QpackDecompressionFailed, conn.closeCode,
		"close code = %#x, want QPACK_DECOMPRESSION_FAILED %#x", conn.closeCode, H3QpackDecompressionFailed)
	assert.True(t, conn.closeApp,
		"a QPACK error is an application-layer (HTTP/3) CONNECTION_CLOSE")
}

// TestClient_Close checks that Close sends an application CONNECTION_CLOSE with
// H3_NO_ERROR (RFC 9114 §8.1) through the QUIC connection.
func TestClient_Close(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	closeErr := client.Close()

	require.NoErrorf(t, closeErr, "Close: %v", closeErr)
	assert.Truef(t, conn.closed, "Close: closed=%v, want true", conn.closed)
	assert.Truef(t, conn.closeApp,
		"Close: app=%v, want true — §8.1 makes this an application CONNECTION_CLOSE, "+
			"not a transport one", conn.closeApp)
	assert.Equalf(t, H3NoError, conn.closeCode,
		"Close: code=%#x, want %#x", conn.closeCode, H3NoError)
}

// NewClientFake constructs a Client over a fake quicConn (test-only shim around
// the unexported newClient).
func NewClientFake(conn quicConn, settings []Setting) (*Client, error) {
	return newClient(conn, settings)
}
