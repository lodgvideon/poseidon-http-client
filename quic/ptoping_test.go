package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pingCapture records whether a PING frame was parsed.
type pingCapture struct {
	nopFrameHandler
	got bool
}

func (h *pingCapture) OnPing() error { h.got = true; return nil }

// TestConformance_RFC9002_Sec624_PTOSendsPingWhenNoData checks that a probe
// timeout with only a frameless ack-eliciting packet in flight (e.g. a lone
// *_BLOCKED) still sends an ack-eliciting PING probe (RFC 9002 §6.2.4).
func TestConformance_RFC9002_Sec624_PTOSendsPingWhenNoData(t *testing.T) {
	dcid := []byte("ptoping0")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	pc := &closePC{}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true, handshakeConfirmed: true, now: func() time.Time { return time.Unix(3, 0) }}
	c.keys.OneRTT = opener
	c.sent[spaceApp].onSent(0, c.clock(), true, nil) // ack-eliciting, no retransmittable frames

	c.onPTO()
	markedPending := c.probePending
	flushErr := c.flush()

	require.True(t, markedPending,
		"a PTO with only a frameless ack-eliciting packet in flight should mark a probe pending")
	require.NoError(t, flushErr, "flush the pending PING probe")
	require.Lenf(t, pc.writes, 1, "wrote %d packets, want 1 PING probe", len(pc.writes))
	assert.False(t, c.probePending, "probePending should be cleared once the PING is sent")

	hdr, err := ParseHeader(pc.writes[0], len(c.dcid))
	require.NoError(t, err, "the probe packet must be a parseable QUIC packet")
	_, _, payload, err := opener.Open(pc.writes[0], hdr.PNOffset, 0)
	require.NoError(t, err, "the probe packet must authenticate under the 1-RTT keys")
	var h pingCapture
	require.NoError(t, ParseFrames(payload, &h), "the probe payload must be parseable frames")
	assert.True(t, h.got, "the probe packet does not carry a PING")
}

// TestConn_PTO_NoPingWhenDataToResend checks that a PTO resends the oldest
// unacknowledged frames when there are some — and does not also mark a bare PING.
func TestConn_PTO_NoPingWhenDataToResend(t *testing.T) {
	c := &Conn{now: func() time.Time { return time.Unix(3, 0) }, handshakeConfirmed: true}
	c.sent[spaceApp].onSent(0, c.clock(), true, &retransFrame{kind: retransStream, streamID: 0, data: []byte("x")})

	c.onPTO()

	require.False(t, c.probePending,
		"with retransmittable data in flight, the probe resends it — no bare PING")
	require.NotEmpty(t, c.retransQueue[spaceApp],
		"the oldest packet's frames should be queued for resend")
}

// TestConformance_RFC9002_Sec621_NoAppPTOBeforeConfirmed pins "An endpoint MUST NOT
// set a timer for the Application Data packet number space until the handshake is
// confirmed." Between TLS completion and HANDSHAKE_DONE the server may still lack
// 1-RTT read keys, so probing 1-RTT there is guaranteed-spurious retransmission.
func TestConformance_RFC9002_Sec621_NoAppPTOBeforeConfirmed(t *testing.T) {
	base := time.Unix(5, 0)
	newConn := func(confirmed bool) *Conn {
		c := &Conn{now: func() time.Time { return base }, handshakeComplete: true, handshakeConfirmed: confirmed}
		c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "oneRTT"))
		return c
	}
	unconfirmed, control := newConn(false), newConn(true)

	unconfirmedArmed := unconfirmed.hasInFlight()
	unconfirmed.onPTO()
	controlArmed := control.hasInFlight()
	control.onPTO()

	require.False(t, unconfirmedArmed,
		"1-RTT data in flight must not arm the probe timer before the handshake is confirmed")
	require.Emptyf(t, unconfirmed.retransQueue[spaceApp],
		"probe queued in the Application space before confirmation: %+v", unconfirmed.retransQueue[spaceApp])
	assert.Contains(t, unconfirmed.sent[spaceApp].packets, uint64(0),
		"the unconfirmed Application packet must stay in flight, not be probed")
	// Control: once confirmed the very same state does arm and probe.
	assert.True(t, controlArmed,
		"control: confirmed 1-RTT data in flight should arm the probe timer")
	assert.Lenf(t, control.retransQueue[spaceApp], 1,
		"control: probe queue = %+v, want the oldest packet's frame", control.retransQueue[spaceApp])
}
