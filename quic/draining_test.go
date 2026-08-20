package quic

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec1022_NoSendAfterConnectionClose checks that once a
// CONNECTION_CLOSE has been received the connection enters the draining state and
// flush sends nothing further — no ACK, no PATH_RESPONSE — even with such frames
// queued (RFC 9000 §10.2.2 MUST NOT send packets in the draining state).
func TestConformance_RFC9000_Sec1022_NoSendAfterConnectionClose(t *testing.T) {
	sealer, _ := closeTestSealerOpener(t, 0x83)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("draintst"), oneRTTSealer: sealer}
	h := &connFrameHandler{c: c}
	// Queue work that flush would normally send: a PATH_RESPONSE and a pending ACK.
	require.NoError(t, h.OnPathChallenge(&[8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	c.acks[spaceApp].receive(5, true)
	// The peer's CONNECTION_CLOSE puts the connection into the draining state.
	require.NoError(t, h.OnConnectionClose(false, ErrCodeNoError, 0, nil))
	require.True(t, c.closed, "a received CONNECTION_CLOSE should mark the connection closed")

	err := c.flush()

	require.NoErrorf(t, err, "flush: %v", err)
	assert.Emptyf(t, pc.writes,
		"flush wrote %d packets in the draining state, want 0 (§10.2.2)", len(pc.writes))
}

// TestConformance_RFC9000_Sec1022_NoAppSendAfterClose checks that the application
// send path also refuses to transmit once the connection is closed — flush is not
// the only send path (RFC 9000 §10.2.2).
func TestConformance_RFC9000_Sec1022_NoAppSendAfterClose(t *testing.T) {
	sealer, _ := closeTestSealerOpener(t, 0x84)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: []byte("draintst"), oneRTTSealer: sealer, closed: true}

	err := c.writeAppFrames(appendPing(nil), nil)

	assert.ErrorIsf(t, err, ErrConnClosed, "writeAppFrames after close = %v, want ErrConnClosed", err)
	assert.Emptyf(t, pc.writes,
		"wrote %d application packets after close, want 0 (§10.2.2)", len(pc.writes))
}

// TestConformance_RFC9000_Sec1022_PollSurfacesPeerClose checks that Poll, on
// processing a received CONNECTION_CLOSE, returns a *PeerClosedError carrying the
// peer's error code and sends nothing back (RFC 9000 §10.2.2).
func TestConformance_RFC9000_Sec1022_PollSurfacesPeerClose(t *testing.T) {
	dcid := []byte("polltst2")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	require.NoError(t, err)
	opener, err := NewOpener(keys)
	require.NoError(t, err)
	// A server 1-RTT packet carrying a transport CONNECTION_CLOSE (CONNECTION_REFUSED).
	frames := AppendConnectionClose(nil, false, ErrCodeConnectionRefused, 0, []byte("bye"))
	pkt := sealServerPacket(t, sealer, PacketShort, nil, nil, 0, frames)
	pc := &onePacketPC{pkt: pkt}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT = opener

	err = c.Poll(context.Background())

	var pce *PeerClosedError
	require.Truef(t, errors.As(err, &pce), "Poll = %v, want *PeerClosedError", err)
	assert.Falsef(t, pce.App || pce.Code != ErrCodeConnectionRefused,
		"PeerClosedError = %+v, want transport code %#x", pce, ErrCodeConnectionRefused)
	assert.True(t, c.closed,
		"the connection should be marked closed after a received CONNECTION_CLOSE")
	assert.Emptyf(t, pc.written,
		"Poll wrote %d packets after the peer close, want 0 (draining)", len(pc.written))
}
