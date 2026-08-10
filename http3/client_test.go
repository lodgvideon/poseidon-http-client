package http3

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

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
	if err != nil {
		t.Fatal(err)
	}

	// Control stream: type 0x00 then a SETTINGS frame (§6.2.1).
	styp, n, err := ReadStreamType(conn.control.sent)
	if err != nil || styp != StreamTypeControl {
		t.Fatalf("control stream type = (%#x,%v)", styp, err)
	}
	var cfr FrameReader
	cfr.Feed(conn.control.sent[n:])
	if ftyp, _, ferr := cfr.ReadFrame(); ferr != nil || ftyp != FrameSettings {
		t.Fatalf("first control frame = (%#x,%v), want SETTINGS", ftyp, ferr)
	}

	req := &Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}
	resp, body, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	// The request stream carried a single HEADERS frame, sent with FIN.
	if !conn.req.finSent {
		t.Fatal("request must be sent with FIN")
	}
	rtyp, rlen, rn, err := ParseFrameHeader(conn.req.sent)
	if err != nil || rtyp != FrameHeaders {
		t.Fatalf("request frame = (%#x,%v), want HEADERS", rtyp, err)
	}
	reqFields := decodeAll(t, conn.req.sent[rn:rn+int(rlen)])
	if len(reqFields) < 4 || string(reqFields[0].Name) != ":method" || string(reqFields[0].Value) != "GET" {
		t.Fatalf("request fields = %+v", reqFields)
	}

	// The response decoded correctly, and the body required a Poll.
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	if !bytes.Equal(body, []byte("hello world")) {
		t.Fatalf("body = %q, want %q", body, "hello world")
	}
	if conn.polls.Load() == 0 {
		t.Fatal("expected at least one Poll to receive the body")
	}
}

// TestConformance_RFC9114_Sec41_ResponseReadAfterStopSending checks that when the
// server aborts reading the request with STOP_SENDING (surfaced as ErrStreamReset
// on the send path), the client stops sending but still reads the response on the
// stream's independent receive side (RFC 9114 §4.1) rather than discarding it.
func TestConformance_RFC9114_Sec41_ResponseReadAfterStopSending(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headersFrame}, fin: true, sendResetErr: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, _, err := client.Do(context.Background(), &Request{Method: "POST", Scheme: "https", Authority: "h", Path: "/", Body: []byte("aborted request body")})
	if err != nil {
		t.Fatalf("Do after STOP_SENDING = %v, want the response still delivered", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
}

// TestClient_RequestWithBody verifies a request body is sent as a DATA frame
// after the HEADERS frame, with the FIN on the DATA (RFC 9114 §4.1).
func TestClient_RequestWithBody(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headersFrame}, fin: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("field=value&x=1")
	resp, _, err := client.Do(context.Background(), &Request{Method: "POST", Scheme: "https", Authority: "h", Path: "/", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	if !conn.req.finSent {
		t.Fatal("FIN must be sent on the DATA frame")
	}
	// The request stream carried a HEADERS frame then a DATA frame with the body.
	htyp, hlen, hn, err := ParseFrameHeader(conn.req.sent)
	if err != nil || htyp != FrameHeaders {
		t.Fatalf("first frame = (%#x, %v), want HEADERS", htyp, err)
	}
	rest := conn.req.sent[hn+int(hlen):]
	dtyp, dlen, dn, err := ParseFrameHeader(rest)
	if err != nil || dtyp != FrameData {
		t.Fatalf("second frame = (%#x, %v), want DATA", dtyp, err)
	}
	if got := rest[dn : dn+int(dlen)]; !bytes.Equal(got, body) {
		t.Fatalf("DATA payload = %q, want %q", got, body)
	}
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
	if err != nil {
		t.Fatal(err)
	}

	if want := AppendClientControlStream(nil, settings); !bytes.Equal(conn.control.sent, want) {
		t.Fatalf("control stream truncated: got %d bytes, want %d", len(conn.control.sent), len(want))
	}

	resp, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
	if !conn.req.finSent {
		t.Fatal("FIN must be sent once the whole HEADERS frame is drained")
	}
	rtyp, rlen, rn, _ := ParseFrameHeader(conn.req.sent)
	if rtyp != FrameHeaders || rn+int(rlen) != len(conn.req.sent) {
		t.Fatalf("request HEADERS frame truncated: %d bytes on the wire", len(conn.req.sent))
	}
}

// TestClient_DataBeforeHeaders rejects a response that sends DATA before any
// HEADERS frame — an invalid frame sequence, so a H3_FRAME_UNEXPECTED connection
// error (RFC 9114 §4.1).
func TestClient_DataBeforeHeaders(t *testing.T) {
	dataFrame := AppendData(nil, []byte("body"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{dataFrame}, fin: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control (connection error)", err)
	}
	if conn.closeCode != H3FrameUnexpected {
		t.Fatalf("close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec724_SettingsOnRequestStream checks that a SETTINGS
// frame on a request stream is a H3_FRAME_UNEXPECTED connection error (§7.2.4).
func TestConformance_RFC9114_Sec724_SettingsOnRequestStream(t *testing.T) {
	settings := AppendFrameHeader(nil, FrameSettings, 0) // empty SETTINGS
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{settings}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameUnexpected {
		t.Fatalf("close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec728_ReservedFrameOnRequestStream checks that a
// reserved HTTP/2-carryover frame type (0x02) on a request stream is a
// H3_FRAME_UNEXPECTED connection error (§7.2.8).
func TestConformance_RFC9114_Sec728_ReservedFrameOnRequestStream(t *testing.T) {
	reserved := AppendFrameHeader(nil, 0x02, 0)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{reserved}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameUnexpected {
		t.Fatalf("close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec725_PushPromiseOnRequestStream checks that a
// PUSH_PROMISE on a request stream, when the client never sent MAX_PUSH_ID, is a
// H3_ID_ERROR connection error (§4.6, §7.2.5).
func TestConformance_RFC9114_Sec725_PushPromiseOnRequestStream(t *testing.T) {
	pp := AppendFrameHeader(nil, FramePushPromise, 0)
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{pp}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3IDError {
		t.Fatalf("close code = %#x, want H3_ID_ERROR", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec71_TruncatedFrameAtStreamEnd checks that a stream
// ending in the middle of a frame payload (a truncated final frame after a valid
// response) is a H3_FRAME_ERROR connection error.
func TestConformance_RFC9114_Sec71_TruncatedFrameAtStreamEnd(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	truncated := AppendFrameHeader(nil, FrameData, 10) // declares 10 payload bytes
	truncated = append(truncated, []byte("abc")...)    // but only 3 arrive, then FIN
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, truncated...)}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameError {
		t.Fatalf("close code = %#x, want H3_FRAME_ERROR", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec71_TruncatedHeaderAtStreamEnd checks that a stream
// ending in the middle of a frame header (an incomplete frame) is a H3_FRAME_ERROR
// connection error.
func TestConformance_RFC9114_Sec71_TruncatedHeaderAtStreamEnd(t *testing.T) {
	// One byte: a frame type (HEADERS) with no length varint, then FIN.
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{{byte(FrameHeaders)}}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameError {
		t.Fatalf("close code = %#x, want H3_FRAME_ERROR", conn.closeCode)
	}
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
	client, _ := NewClientFake(conn, nil)
	_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	var rst *StreamResetError
	if !errors.As(err, &rst) {
		t.Fatalf("err = %v, want *StreamResetError", err)
	}
	if rst.Code != H3RequestRejected {
		t.Fatalf("reset code = %#x, want H3_REQUEST_REJECTED", rst.Code)
	}
	if !rst.Retryable() {
		t.Fatal("H3_REQUEST_REJECTED should be reported retryable")
	}
	if conn.closeCode == H3FrameError {
		t.Fatalf("connection torn down with H3_FRAME_ERROR on a reset (%#x)", conn.closeCode)
	}
}

// TestClient_RequestReset_NonRetryable checks that a reset with a code other than
// H3_REQUEST_REJECTED is surfaced but not reported retryable.
func TestClient_RequestReset_NonRetryable(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{recvReset: true, recvResetCode: H3RequestCancelled}}
	client, _ := NewClientFake(conn, nil)
	_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	var rst *StreamResetError
	if !errors.As(err, &rst) || rst.Code != H3RequestCancelled {
		t.Fatalf("err = %v, want *StreamResetError{H3_REQUEST_CANCELLED}", err)
	}
	if rst.Retryable() {
		t.Fatal("H3_REQUEST_CANCELLED must not be reported retryable")
	}
}

// TestConformance_RFC9114_Sec412_ContentLengthMismatch checks that a response
// whose Content-Length does not equal the sum of DATA payloads is malformed
// (ErrH3Message) and the stream is aborted with H3_MESSAGE_ERROR.
func TestConformance_RFC9114_Sec412_ContentLengthMismatch(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "5")))
	data := AppendData(nil, []byte("abc")) // 3 bytes ≠ declared 5
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Message {
		t.Fatalf("err = %v, want ErrH3Message", err)
	}
	if conn.req.resetCode != H3MessageError {
		t.Fatalf("abort code = %#x, want H3_MESSAGE_ERROR", conn.req.resetCode)
	}
}

// TestConformance_RFC9114_Sec412_ContentLengthMatch checks that a Content-Length
// equal to the received body is accepted.
func TestConformance_RFC9114_Sec412_ContentLengthMatch(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "3")))
	data := AppendData(nil, []byte("abc"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	resp, body, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 || string(body) != "abc" {
		t.Fatalf("status=%d body=%q", resp.Status, body)
	}
}

// TestClient_ContentLength_NoContentStatusExempt checks that a 204 with a
// non-zero Content-Length and no DATA is not malformed — the anticipatory
// Content-Length exception (RFC 9114 §4.1.2).
func TestClient_ContentLength_NoContentStatusExempt(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "204"), hf("content-length", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{headers}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	resp, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	if err != nil {
		t.Fatalf("204 with anticipatory content-length must not be malformed: %v", err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
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
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Message {
		t.Fatalf("overflowing content-length: err = %v, want ErrH3Message", err)
	}
}

// TestClient_ContentLength_ConflictingMalformed checks that two Content-Length
// fields with different values are malformed (RFC 9110 §8.6).
func TestClient_ContentLength_ConflictingMalformed(t *testing.T) {
	headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-length", "3"), hf("content-length", "5")))
	data := AppendData(nil, []byte("abc"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Message {
		t.Fatalf("conflicting content-length: err = %v, want ErrH3Message", err)
	}
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
	client, _ := NewClientFake(conn, nil)
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3Control {
		t.Fatalf("err = %v, want ErrH3Control (connection closed)", err)
	}
	if conn.closeCode != H3QpackDecompressionFailed {
		t.Fatalf("close code = %#x, want QPACK_DECOMPRESSION_FAILED %#x", conn.closeCode, H3QpackDecompressionFailed)
	}
	if !conn.closeApp {
		t.Fatal("a QPACK error is an application-layer (HTTP/3) CONNECTION_CLOSE")
	}
}

// TestClient_Close checks that Close sends an application CONNECTION_CLOSE with
// H3_NO_ERROR (RFC 9114 §8.1) through the QUIC connection.
func TestClient_Close(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !conn.closed || !conn.closeApp || conn.closeCode != H3NoError {
		t.Fatalf("Close: closed=%v app=%v code=%#x, want true/true/%#x",
			conn.closed, conn.closeApp, conn.closeCode, H3NoError)
	}
}

// NewClientFake constructs a Client over a fake quicConn (test-only shim around
// the unexported newClient).
func NewClientFake(conn quicConn, settings []Setting) (*Client, error) {
	return newClient(conn, settings)
}
