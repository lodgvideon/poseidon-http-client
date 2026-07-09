package http3

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// fakeStream records what the client sends and hands back queued response chunks
// (one per Recv), modeling stream data arriving across several Poll calls.
type fakeStream struct {
	id         uint64
	sent       []byte
	finSent    bool
	recvChunks    [][]byte
	fin           bool
	recvReset     bool   // the peer reset its send side (RESET_STREAM received)
	recvResetCode uint64 // the application error code carried by that RESET_STREAM
	sendCap       int    // max bytes accepted per Send (0 = unlimited); models flow control
	reset      bool // Reset was called
	stopped    bool // StopSending was called
	resetCode  uint64
	stopCode   uint64
}

func (s *fakeStream) ID() uint64 { return s.id }

func (s *fakeStream) Send(data []byte, fin bool) (int, error) {
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
	if len(s.recvChunks) == 0 {
		return nil
	}
	c := s.recvChunks[0]
	s.recvChunks = s.recvChunks[1:]
	return c
}

// Finished reports end-of-stream once the FIN is set and every queued chunk has
// been handed out, or once the peer has reset the stream.
func (s *fakeStream) Finished() bool { return s.recvReset || (s.fin && len(s.recvChunks) == 0) }

func (s *fakeStream) ResetReceived() bool { return s.recvReset }
func (s *fakeStream) ResetCode() uint64   { return s.recvResetCode }

func (s *fakeStream) Reset(code uint64) error       { s.resetCode = code; s.reset = true; return nil }
func (s *fakeStream) StopSending(code uint64) error { s.stopCode = code; s.stopped = true; return nil }

type fakeConn struct {
	control    *fakeStream
	req        *fakeStream
	polls      int
	uniSendCap int                         // send cap applied to the control stream
	pollHook   func(context.Context) error // overrides Poll when set (for ctx tests)
	acceptQ    []quicStream                // server uni streams handed out by AcceptUniStream
	closeApp   bool                        // captured CloseWithError arguments
	closeCode  uint64
	closed     bool
}

func (c *fakeConn) AcceptUniStream() quicStream {
	if len(c.acceptQ) == 0 {
		return nil
	}
	s := c.acceptQ[0]
	c.acceptQ = c.acceptQ[1:]
	return s
}

func (c *fakeConn) OpenUniStream() (quicStream, error) {
	c.control = &fakeStream{sendCap: c.uniSendCap}
	return c.control, nil
}
func (c *fakeConn) OpenStream() (quicStream, error) { return c.req, nil }
func (c *fakeConn) Poll(ctx context.Context) error {
	if c.pollHook != nil {
		return c.pollHook(ctx)
	}
	c.polls++
	return nil
}
func (c *fakeConn) CloseWithError(app bool, code uint64, _ string) error {
	c.closed, c.closeApp, c.closeCode = true, app, code
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
	if conn.polls == 0 {
		t.Fatal("expected at least one Poll to receive the body")
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
	truncated := AppendFrameHeader(nil, FrameData, 10)      // declares 10 payload bytes
	truncated = append(truncated, []byte("abc")...)         // but only 3 arrive, then FIN
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
	return newClient(context.Background(), conn, settings)
}
