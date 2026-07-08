package http3

import (
	"bytes"
	"testing"
)

// fakeStream records what the client sends and hands back queued response chunks
// (one per Recv), modeling stream data arriving across several Poll calls.
type fakeStream struct {
	sent       []byte
	finSent    bool
	recvChunks [][]byte
	fin        bool
	sendCap    int // max bytes accepted per Send (0 = unlimited); models flow control
}

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
// been handed out.
func (s *fakeStream) Finished() bool { return s.fin && len(s.recvChunks) == 0 }

type fakeConn struct {
	control    *fakeStream
	req        *fakeStream
	polls      int
	uniSendCap int // send cap applied to the control stream
}

func (c *fakeConn) OpenUniStream() (quicStream, error) {
	c.control = &fakeStream{sendCap: c.uniSendCap}
	return c.control, nil
}
func (c *fakeConn) OpenStream() (quicStream, error) { return c.req, nil }
func (c *fakeConn) Poll() error                     { c.polls++; return nil }

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
	resp, body, err := client.Do(req)
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

	resp, _, err := client.Do(&Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
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
// HEADERS frame (RFC 9114 §4.1 → H3_FRAME_UNEXPECTED).
func TestClient_DataBeforeHeaders(t *testing.T) {
	dataFrame := AppendData(nil, []byte("body"))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{dataFrame}, fin: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Do(&Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}); err != ErrH3FrameUnexpected {
		t.Fatalf("err = %v, want ErrH3FrameUnexpected", err)
	}
}

// NewClientFake constructs a Client over a fake quicConn (test-only shim around
// the unexported newClient).
func NewClientFake(conn quicConn, settings []Setting) (*Client, error) {
	return newClient(conn, settings)
}
