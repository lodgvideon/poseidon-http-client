package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"testing"
)

// capturePacketConn records datagrams written to it; Read returns EOF.
type capturePacketConn struct {
	written [][]byte
}

func (c *capturePacketConn) Write(p []byte) (int, error) {
	c.written = append(c.written, append([]byte(nil), p...))
	return len(p), nil
}
func (c *capturePacketConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *capturePacketConn) Close() error             { return nil }

// TestConn_SendInitialFlight checks that a new Conn drives the TLS handshake to
// a ClientHello and sends it in a single padded (>=1200-byte) Initial datagram
// that decrypts, with the Conn's own Initial keys, back to the ClientHello.
func TestConn_SendInitialFlight(t *testing.T) {
	_, pool := genServerCert(t)
	pc := &capturePacketConn{}
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.sendInitialFlight(context.Background()); err != nil {
		t.Fatalf("sendInitialFlight: %v", err)
	}

	if len(pc.written) != 1 {
		t.Fatalf("wrote %d datagrams, want 1", len(pc.written))
	}
	dg := pc.written[0]
	if len(dg) < InitialDatagramMinSize {
		t.Fatalf("Initial datagram = %d bytes, want >= %d", len(dg), InitialDatagramMinSize)
	}
	if c.sendPN[spaceInitial] != 1 {
		t.Fatalf("next Initial PN = %d, want 1", c.sendPN[spaceInitial])
	}

	// Decrypt as the server would: the client seals its Initial with the client
	// keys derived from the DCID the Conn chose.
	client, _ := InitialKeys(c.dcid)
	opener, err := NewOpener(client)
	if err != nil {
		t.Fatal(err)
	}
	h, err := ParseHeader(dg, 0)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if h.Type != PacketInitial || !bytes.Equal(h.DCID, c.dcid) {
		t.Fatalf("header type=%d dcid=%x", h.Type, h.DCID)
	}
	_, _, payload, err := opener.Open(dg, h.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	sink := &cryptoFrameSink{}
	if err := ParseFrames(payload, sink); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	if len(sink.data) == 0 {
		t.Fatal("no CRYPTO (ClientHello) recovered from the Initial packet")
	}
	// The recovered CRYPTO must be a valid TLS handshake message start
	// (ClientHello = handshake type 0x01).
	if sink.data[0] != 0x01 {
		t.Fatalf("first CRYPTO byte = %#x, want 0x01 (ClientHello)", sink.data[0])
	}
}
