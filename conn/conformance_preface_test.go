package conn

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Batch 3 — server connection preface validation (RFC 9113 §3.4). The server's
// first frame MUST be a (non-ACK) SETTINGS frame; the client must treat any other
// first frame as a connection error of type PROTOCOL_ERROR. The handshake
// recorder silently ignored non-SETTINGS frames, so a server that led with a
// PING, HEADERS, WINDOW_UPDATE — or a SETTINGS ACK before its own SETTINGS — was
// tolerated, and the handshake only failed later (or hung) on an unrelated cause.

// badPrefaceServer completes the client handshake up to reading the client's
// SETTINGS, then sends `send` as its FIRST frame instead of a server SETTINGS.
func badPrefaceServer(t *testing.T, srv net.Conn, send func(*frame.Framer) error) {
	t.Helper()
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("preface read: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("read client settings: %v", err)
		return
	}
	<-asyncWrite(func() error { return send(srvFr) })
}

// assertPrefaceRefused runs a handshake against a server that leads with a bad
// first frame and asserts NewClientConn fails with ConnError PROTOCOL_ERROR.
func assertPrefaceRefused(t *testing.T, send func(*frame.Framer) error) {
	t.Helper()
	cli, srv := net.Pipe()
	defer cli.Close()
	go badPrefaceServer(t, srv, send)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err == nil {
		_ = c.Close()
		t.Fatal("NewClientConn accepted a server whose first frame was not a non-ACK SETTINGS")
	}
	var ce *ConnError
	if !errors.As(err, &ce) || ce.Code != frame.ErrCodeProtocolError {
		t.Errorf("err = %v, want ConnError PROTOCOL_ERROR", err)
	}
}

// TestConformance_RFC9113_Sec3_4_NonSettingsFirstFrame_Refused pins that a
// server whose connection preface is not a SETTINGS frame is refused.
func TestConformance_RFC9113_Sec3_4_NonSettingsFirstFrame_Refused(t *testing.T) {
	t.Run("ping", func(t *testing.T) {
		assertPrefaceRefused(t, func(fr *frame.Framer) error {
			return fr.WritePing(false, [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
		})
	})
	t.Run("window_update", func(t *testing.T) {
		assertPrefaceRefused(t, func(fr *frame.Framer) error {
			return fr.WriteWindowUpdate(0, 100)
		})
	})
	t.Run("settings ack before server settings", func(t *testing.T) {
		assertPrefaceRefused(t, func(fr *frame.Framer) error {
			return fr.WriteSettingsAck()
		})
	})
}

// TestConformance_RFC9113_Sec3_4_SettingsFirstFrame_Accepted is the
// over-rejection guard: a conformant server preface (SETTINGS first) still
// handshakes successfully.
func TestConformance_RFC9113_Sec3_4_SettingsFirstFrame_Accepted(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn refused a conformant SETTINGS-first preface: %v", err)
	}
	if !c.IsAlive() {
		t.Error("connection not alive after a conformant handshake")
	}
	_ = c.Close()
}
