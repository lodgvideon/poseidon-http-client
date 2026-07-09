package quic

import (
	"bytes"
	"crypto/tls"
	"testing"
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
