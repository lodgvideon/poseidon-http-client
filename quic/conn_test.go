package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	err = c.sendInitialFlight(context.Background())

	require.NoErrorf(t, err, "sendInitialFlight: %v", err)
	require.Lenf(t, pc.written, 1, "wrote %d datagrams, want 1", len(pc.written))
	dg := pc.written[0]
	assert.GreaterOrEqualf(t, len(dg), InitialDatagramMinSize,
		"Initial datagram = %d bytes, want >= %d", len(dg), InitialDatagramMinSize)
	assert.EqualValuesf(t, 1, c.sendPN[spaceInitial],
		"next Initial PN = %d, want 1", c.sendPN[spaceInitial])

	// Decrypt as the server would: the client seals its Initial with the client
	// keys derived from the DCID the Conn chose.
	client, _ := InitialKeys(c.dcid)
	opener, err := NewOpener(client)
	require.NoError(t, err)
	h, err := ParseHeader(dg, 0)
	require.NoErrorf(t, err, "ParseHeader: %v", err)
	require.Equalf(t, PacketInitial, h.Type, "header type=%d dcid=%x", h.Type, h.DCID)
	require.Truef(t, bytes.Equal(h.DCID, c.dcid), "header type=%d dcid=%x", h.Type, h.DCID)
	_, _, payload, err := opener.Open(dg, h.PNOffset, 0)
	require.NoErrorf(t, err, "Open: %v", err)
	sink := &cryptoFrameSink{}
	require.NoErrorf(t, ParseFrames(payload, sink), "ParseFrames")
	require.NotEmpty(t, sink.data, "no CRYPTO (ClientHello) recovered from the Initial packet")
	// The recovered CRYPTO must be a valid TLS handshake message start
	// (ClientHello = handshake type 0x01).
	assert.EqualValuesf(t, 0x01, sink.data[0],
		"first CRYPTO byte = %#x, want 0x01 (ClientHello)", sink.data[0])
}

// TestConn_SendInitialFlight_Epilogue pins the bookkeeping the first Initial
// flight performs alongside the write, which TestConn_SendInitialFlight above
// does not observe: the packet is recorded ack-eliciting with a retransmittable
// private copy of its CRYPTO bytes (RFC 9000 §13.3), it is charged to
// bytes_in_flight at its on-wire size (RFC 9002 §7), the CRYPTO offset advances
// past the ClientHello, and the pending-CRYPTO buffer is drained so the
// ClientHello is not sent a second time. The wire datagram is one half of the
// flight; this is the half the Conn remembers.
func TestConn_SendInitialFlight_Epilogue(t *testing.T) {
	_, pool := genServerCert(t)
	pc := &capturePacketConn{}
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	require.NoError(t, err)

	err = c.sendInitialFlight(context.Background())

	require.NoErrorf(t, err, "sendInitialFlight: %v", err)
	require.Lenf(t, pc.written, 1, "wrote %d datagrams, want 1", len(pc.written))
	dg := pc.written[0]
	rec, ok := c.sent[spaceInitial].packets[0]
	require.True(t, ok, "Initial packet 0 not recorded as sent; loss detection cannot see it")
	assert.True(t, rec.ackEliciting,
		"Initial recorded as not ack-eliciting; a lost ClientHello would never be probed")
	require.True(t, rec.hasFrame,
		"Initial recorded with no retransmittable frame; a lost ClientHello could not be resent")
	assert.Equalf(t, retransCrypto, rec.frame.kind,
		"retransmit descriptor kind=%v offset=%d, want retransCrypto at offset 0",
		rec.frame.kind, rec.frame.offset)
	assert.Zerof(t, rec.frame.offset,
		"retransmit descriptor kind=%v offset=%d, want retransCrypto at offset 0",
		rec.frame.kind, rec.frame.offset)
	require.NotEmpty(t, rec.frame.data, "retransmit descriptor retained no CRYPTO bytes")
	assert.EqualValuesf(t, 0x01, rec.frame.data[0],
		"retained CRYPTO starts %#x, want 0x01 (ClientHello)", rec.frame.data[0])
	assert.Equalf(t, len(dg), rec.size,
		"in-flight accounting: rec.size=%d bytesInFlight=%d, want %d for both",
		rec.size, c.bytesInFlight, len(dg))
	assert.EqualValuesf(t, len(dg), c.bytesInFlight,
		"in-flight accounting: rec.size=%d bytesInFlight=%d, want %d for both",
		rec.size, c.bytesInFlight, len(dg))
	assert.EqualValuesf(t, len(rec.frame.data), c.cryptoOffset[spaceInitial],
		"cryptoOffset = %d, want %d (one ClientHello past the start)",
		c.cryptoOffset[spaceInitial], len(rec.frame.data))
	assert.Emptyf(t, c.pendingCrypto[spaceInitial],
		"pendingCrypto = %d bytes, want drained; the ClientHello would be sent twice",
		len(c.pendingCrypto[spaceInitial]))
	assert.EqualValuesf(t, 1, c.sendPN[spaceInitial],
		"next Initial PN = %d, want 1", c.sendPN[spaceInitial])

	// The retained CRYPTO must be a private copy, not an alias of the drained
	// pendingCrypto backing array: the next TLS pump appends into that array, and
	// a retransmission of this packet must still carry the original ClientHello.
	want := append([]byte(nil), rec.frame.data...)

	c.pendingCrypto[spaceInitial] = append(c.pendingCrypto[spaceInitial], bytes.Repeat([]byte{0xff}, len(want))...)

	assert.True(t, bytes.Equal(rec.frame.data, want),
		"retransmit CRYPTO aliases the pendingCrypto buffer; a resend would carry garbage")
}
