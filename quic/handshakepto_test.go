package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9002_Sec6221_PTOArmedWithEmptyFlight checks that before the
// handshake completes the probe timer is armed even with no ack-eliciting packet
// in flight, and that this no longer holds once the handshake completes (RFC 9002
// §6.2.2.1).
func TestConformance_RFC9002_Sec6221_PTOArmedWithEmptyFlight(t *testing.T) {
	base := time.Unix(4000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.smoothedRTT, c.rtt.rttvar, c.rtt.haveSample = 40*time.Millisecond, 5*time.Millisecond, true

	armedBefore := c.handshakeAntiDeadlock()
	dl, isLoss := c.lossDetectionDeadline()
	c.handshakeComplete = true
	armedAfter := c.handshakeAntiDeadlock()

	assert.True(t, armedBefore, "handshakeAntiDeadlock should hold before the handshake completes")
	assert.False(t, isLoss, "no time-threshold loss is pending")
	assert.Truef(t, dl.Equal(base.Add(c.ptoPeriod())),
		"deadline = %v, want the PTO %v (armed with an empty flight)", dl, base.Add(c.ptoPeriod()))
	assert.False(t, armedAfter, "handshakeAntiDeadlock should be false once the handshake completes")
}

// TestConformance_RFC9002_Sec6221_HandshakeProbeSendsPing checks that a PTO during
// the handshake with an empty flight sends a Handshake-space PING probe (not an
// application-space probe), to unblock an anti-amplification-limited server
// (RFC 9002 §6.2.2.1).
func TestConformance_RFC9002_Sec6221_HandshakeProbeSendsPing(t *testing.T) {
	dcid := []byte("hspto000")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	// Handshake keys present, handshake not complete, nothing in flight.
	c := &Conn{pc: pc, dcid: dcid, handshakeSealer: sealer, now: func() time.Time { return time.Unix(4, 0) }}

	c.onPTO()
	markedProbe, usedAppProbe := c.handshakeProbe, c.probePending
	err := c.flush()

	require.NoError(t, err, "flush the anti-deadlock probe")
	assert.True(t, markedProbe,
		"a PTO during the handshake with empty flight should mark a handshake probe")
	assert.False(t, usedAppProbe, "the application-space probe must not be used during the handshake")
	require.Lenf(t, pc.writes, 1, "wrote %d packets, want 1 handshake PING probe", len(pc.writes))
	assert.False(t, c.handshakeProbe, "handshakeProbe should be cleared once the PING is sent")

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	require.NoError(t, err, "ParseHeader on the probe packet")
	assert.Equalf(t, PacketHandshake, hdr.Type,
		"probe packet type = %v, want Handshake (§6.2.2.1)", hdr.Type)
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	require.NoError(t, err, "Open the probe packet")
	var h pingCapture
	require.NoError(t, ParseFrames(payload, &h), "ParseFrames on the probe payload")
	assert.True(t, h.got, "the handshake probe packet does not carry a PING")
}

// TestConformance_RFC9002_Sec6221_InitialProbePadded checks that when the client
// holds only Initial keys, the anti-deadlock probe is an Initial PING in a
// datagram padded to at least 1200 bytes (RFC 9002 §6.2.2.1, RFC 9000 §14.1).
func TestConformance_RFC9002_Sec6221_InitialProbePadded(t *testing.T) {
	dcid := []byte("inpto000")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	// Only Initial keys (no Handshake keys yet), handshake not complete.
	c := &Conn{pc: pc, dcid: dcid, initialSealer: sealer, now: func() time.Time { return time.Unix(4, 0) }}

	c.onPTO()
	markedProbe := c.handshakeProbe
	err := c.flush()

	require.NoError(t, err, "flush the anti-deadlock probe")
	assert.True(t, markedProbe,
		"a PTO during the handshake with empty flight should mark a handshake probe")
	require.Lenf(t, pc.writes, 1, "wrote %d packets, want 1 Initial PING probe", len(pc.writes))
	assert.GreaterOrEqualf(t, len(pc.writes[0]), InitialDatagramMinSize,
		"Initial probe datagram = %d bytes, want ≥ %d (§14.1)", len(pc.writes[0]), InitialDatagramMinSize)

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	require.NoError(t, err, "ParseHeader on the probe packet")
	assert.Equalf(t, PacketInitial, hdr.Type, "probe packet type = %v, want Initial", hdr.Type)
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	require.NoError(t, err, "Open the probe packet")
	var h pingCapture
	require.NoError(t, ParseFrames(payload, &h), "ParseFrames on the probe payload")
	assert.True(t, h.got, "the Initial probe packet does not carry a PING")
}
