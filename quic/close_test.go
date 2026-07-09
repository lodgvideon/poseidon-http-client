package quic

import (
	"bytes"
	"crypto/tls"
	"io"
	"testing"
	"time"
)

// closePC records written datagrams and whether Close was called.
type closePC struct {
	writes [][]byte
	closed bool
}

func (p *closePC) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}
func (p *closePC) Read([]byte) (int, error) { return 0, errPipeTimeout }
func (p *closePC) Close() error             { p.closed = true; return nil }

// closeCapture records a parsed CONNECTION_CLOSE frame.
type closeCapture struct {
	nopFrameHandler
	got    bool
	app    bool
	code   uint64
	reason []byte
}

func (h *closeCapture) OnConnectionClose(app bool, code, _ uint64, reason []byte) error {
	h.got, h.app, h.code = true, app, code
	h.reason = append([]byte(nil), reason...)
	return nil
}

func closeTestSealerOpener(t *testing.T, seed byte) (*Sealer, *Opener) {
	t.Helper()
	k, err := KeysFromSecret(tls.TLS_AES_128_GCM_SHA256, bytes.Repeat([]byte{seed}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSealer(k)
	if err != nil {
		t.Fatal(err)
	}
	o, err := NewOpener(k)
	if err != nil {
		t.Fatal(err)
	}
	return s, o
}

// TestConn_CloseWithError_SendsAppConnectionClose checks that CloseWithError
// emits one application CONNECTION_CLOSE (RFC 9000 §10.2 / §19.19) with the given
// code and reason on the 1-RTT space, and closes the transport.
func TestConn_CloseWithError_SendsAppConnectionClose(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x11)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("closetst"), oneRTTSealer: sealer}

	if err := c.CloseWithError(true, 0x0100, "bye"); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	if !c.closed || !pc.closed {
		t.Fatalf("closed flags: conn=%v transport=%v, want both true", c.closed, pc.closed)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1", len(pc.writes))
	}
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	if !h.got || !h.app || h.code != 0x0100 || string(h.reason) != "bye" {
		t.Fatalf("CONNECTION_CLOSE = got=%v app=%v code=%#x reason=%q, want true/true/0x0100/\"bye\"",
			h.got, h.app, h.code, h.reason)
	}
}

// TestConn_Close_Idempotent checks that a second Close (or a Close after the peer
// already closed) does not send another CONNECTION_CLOSE.
func TestConn_Close_Idempotent(t *testing.T) {
	sealer, _ := closeTestSealerOpener(t, 0x22)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("closetst"), oneRTTSealer: sealer}
	_ = c.Close()
	_ = c.Close()
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets across two Close calls, want 1", len(pc.writes))
	}
}

// TestConn_CloseWithError_DowngradesAppBeforeOneRTT checks RFC 9000 §10.2.3: an
// application close requested before 1-RTT keys exist is sent as a transport
// CONNECTION_CLOSE with APPLICATION_ERROR in the highest available space.
func TestConn_CloseWithError_DowngradesAppBeforeOneRTT(t *testing.T) {
	hsSealer, opener := closeTestSealerOpener(t, 0x33)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("closetst"), handshakeSealer: hsSealer}

	if err := c.CloseWithError(true, 0x0100, "bye"); err != nil {
		t.Fatalf("CloseWithError: %v", err)
	}
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	if !h.got || h.app || h.code != ErrCodeApplicationError {
		t.Fatalf("downgrade = got=%v app=%v code=%#x, want true/false/APPLICATION_ERROR(0x0c)", h.got, h.app, h.code)
	}
}

// TestConn_Fail_SendsCloseForProtocolError checks that fail signals a local
// protocol violation with a CONNECTION_CLOSE carrying the mapped transport error
// code (RFC 9000 §10.2), and returns the original error.
func TestConn_Fail_SendsCloseForProtocolError(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x44)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("closetst"), oneRTTSealer: sealer}

	if err := c.fail(ErrFrameEncoding); err != ErrFrameEncoding {
		t.Fatalf("fail returned %v, want ErrFrameEncoding", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1 CONNECTION_CLOSE", len(pc.writes))
	}
	h := parseSealedClose(t, opener, c.dcid, pc.writes[0])
	if !h.got || h.app || h.code != ErrCodeFrameEncodingError {
		t.Fatalf("close = got=%v app=%v code=%#x, want true/false/FRAME_ENCODING_ERROR", h.got, h.app, h.code)
	}
}

// TestConn_Fail_NoCloseForIOError checks that a non-protocol error (I/O failure,
// timeout, peer close) is returned as-is without sending a CONNECTION_CLOSE.
func TestConn_Fail_NoCloseForIOError(t *testing.T) {
	sealer, _ := closeTestSealerOpener(t, 0x55)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("closetst"), oneRTTSealer: sealer}
	if err := c.fail(io.EOF); err != io.EOF {
		t.Fatalf("fail returned %v, want io.EOF", err)
	}
	if len(pc.writes) != 0 || pc.closed {
		t.Fatalf("I/O error must not send a close or close the transport (writes=%d closed=%v)", len(pc.writes), pc.closed)
	}
}

// oneShotPC yields one datagram then times out; writes and Close are captured.
type oneShotPC struct {
	pkt    []byte
	read   bool
	writes [][]byte
	closed bool
}

func (p *oneShotPC) Read(b []byte) (int, error) {
	if p.read {
		return 0, timeoutError{}
	}
	p.read = true
	return copy(b, p.pkt), nil
}
func (p *oneShotPC) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}
func (p *oneShotPC) Close() error                    { p.closed = true; return nil }
func (p *oneShotPC) SetReadDeadline(time.Time) error { return nil }

// TestConn_Poll_MalformedFrame_SendsConnectionClose drives the whole path: a
// received 1-RTT packet with a malformed frame makes Poll return the frame error
// and emit a CONNECTION_CLOSE with FRAME_ENCODING_ERROR (RFC 9000 §10.2).
func TestConn_Poll_MalformedFrame_SendsConnectionClose(t *testing.T) {
	k, err := KeysFromSecret(tls.TLS_AES_128_GCM_SHA256, bytes.Repeat([]byte{0x66}, 32))
	if err != nil {
		t.Fatal(err)
	}
	serverSealer, _ := NewSealer(k) // seals the server→client packet
	clientOpener, _ := NewOpener(k) // client opens it
	sendSealer, sendOpener := closeTestSealerOpener(t, 0x77)

	// A CRYPTO frame whose declared length runs past the packet → ErrFrameEncoding.
	badFrames := []byte{0x06, 0x00, 0x40, 0xFF}
	badPkt := sealKP(t, serverSealer, nil, 0, false, badFrames)

	pc := &oneShotPC{pkt: badPkt}
	c := &Conn{
		pc: pc, dcid: []byte("closetst"), oneRTTSealer: sendSealer,
		handshakeComplete: true, handshakeConfirmed: true,
	}
	c.keys.OneRTT = clientOpener

	if err := c.Poll(); err != ErrFrameEncoding {
		t.Fatalf("Poll = %v, want ErrFrameEncoding", err)
	}
	if !pc.closed {
		t.Fatal("transport should be closed after a protocol violation")
	}
	// The last written datagram is the CONNECTION_CLOSE (an ACK may precede it).
	last := pc.writes[len(pc.writes)-1]
	h := parseSealedClose(t, sendOpener, c.dcid, last)
	if !h.got || h.code != ErrCodeFrameEncodingError {
		t.Fatalf("expected FRAME_ENCODING_ERROR CONNECTION_CLOSE, got=%v code=%#x", h.got, h.code)
	}
}

// TestConn_Poll_MalformedHeader_Discarded checks that an unparseable packet
// header is silently discarded, not treated as a connection error (RFC 9000
// §5.2/§12.2): a spoofed datagram must not tear down a healthy connection.
func TestConn_Poll_MalformedHeader_Discarded(t *testing.T) {
	// Short-header first byte with the fixed bit clear → ParseHeader rejects it.
	pc := &oneShotPC{pkt: []byte{0x00, 0x01, 0x02, 0x03}}
	c := &Conn{pc: pc, dcid: []byte("closetst"), handshakeComplete: true, handshakeConfirmed: true}
	if err := c.Poll(); err != nil {
		t.Fatalf("Poll on a malformed header = %v, want nil (packet discarded)", err)
	}
	if pc.closed {
		t.Fatal("a malformed packet must not close the connection")
	}
}

func parseSealedClose(t *testing.T, opener *Opener, dcid, pkt []byte) closeCapture {
	t.Helper()
	hdr, err := ParseHeader(pkt, len(dcid))
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var h closeCapture
	if err := ParseFrames(payload, &h); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	return h
}
