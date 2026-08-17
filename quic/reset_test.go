package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetCtl captures decoded RESET_STREAM / STOP_SENDING / STREAM frames.
type resetCtl struct {
	nopFrameHandler
	reset, stop, stream bool
	id, code, finalSize uint64
}

func (h *resetCtl) OnResetStream(id, code, fs uint64) error {
	h.reset, h.id, h.code, h.finalSize = true, id, code, fs
	return nil
}
func (h *resetCtl) OnStopSending(id, code uint64) error {
	h.stop, h.id, h.code = true, id, code
	return nil
}
func (h *resetCtl) OnStream(_, _ uint64, _ bool, _ []byte) error {
	h.stream = true
	return nil
}

func newResetTestConn(t *testing.T) (*Conn, *Stream, *Opener, *closePC) {
	t.Helper()
	sealer, opener := closeTestSealerOpener(t, 0x88)
	pc := &closePC{}
	c := &Conn{
		pc: pc, dcid: []byte("resettst"), oneRTTSealer: sealer, connMax: 1 << 20,
		peer: TransportParams{InitialMaxStreamsBidi: 1, InitialMaxStreamDataBidiRemote: 1000},
	}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the reset frames are sent on")
	return c, s, opener, pc
}

func decodeStreamCtl(t *testing.T, opener *Opener, dcid, pkt []byte) resetCtl {
	t.Helper()
	hdr, err := ParseHeader(pkt, len(dcid))
	require.NoErrorf(t, err, "ParseHeader: %v", err)
	_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
	require.NoErrorf(t, err, "Open: %v", err)
	var h resetCtl
	require.NoErrorf(t, ParseFrames(payload, &h), "ParseFrames")
	return h
}

// TestConformance_RFC9000_Sec194_ResetStream checks that Stream.Reset sends a
// RESET_STREAM frame with the final size and blocks further sends.
func TestConformance_RFC9000_Sec194_ResetStream(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)

	resetErr := s.Reset(0x0108)
	_, sendErr := s.Send([]byte("x"), false)
	writesAfterFirst := len(pc.writes)
	secondResetErr := s.Reset(0x0108) // idempotent

	require.NoErrorf(t, resetErr, "Reset: %v", resetErr)
	assert.True(t, s.sendReset, "sendReset should be set")
	assert.ErrorIsf(t, sendErr, ErrStreamReset, "Send after Reset = %v, want ErrStreamReset", sendErr)
	require.Equalf(t, 1, writesAfterFirst, "wrote %d packets, want 1", writesAfterFirst)
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	assert.Truef(t, fr.reset && fr.id == s.id && fr.code == 0x0108,
		"RESET_STREAM = %+v, want reset id=%d code=0x0108", fr, s.id)
	assert.NoError(t, secondResetErr, "a second Reset must be a no-op, not an error")
	assert.Lenf(t, pc.writes, 1,
		"a second Reset wrote another packet (total %d), want 1", len(pc.writes))
}

// TestConformance_RFC9000_Sec195_StopSending checks that Stream.StopSending sends
// a STOP_SENDING frame.
func TestConformance_RFC9000_Sec195_StopSending(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)

	stopErr := s.StopSending(0x010c)

	require.NoErrorf(t, stopErr, "StopSending: %v", stopErr)
	require.NotEmpty(t, pc.writes, "StopSending must put a packet on the wire")
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	assert.Truef(t, fr.stop && fr.id == s.id && fr.code == 0x010c,
		"STOP_SENDING = %+v, want stop id=%d code=0x010c", fr, s.id)
}

// TestConformance_RFC9000_Sec35_StopSendingTriggersReset checks that receiving a
// STOP_SENDING resets our send side with the same code (RFC 9000 §3.5).
func TestConformance_RFC9000_Sec35_StopSendingTriggersReset(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	h := &connFrameHandler{c: c}

	stopErr := h.OnStopSending(s.id, 0x0107)

	require.NoErrorf(t, stopErr, "OnStopSending: %v", stopErr)
	assert.True(t, s.sendReset, "a received STOP_SENDING must reset our send side")
	require.NotEmpty(t, pc.writes, "the automatic RESET_STREAM must reach the wire")
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	assert.Truef(t, fr.reset && fr.code == 0x0107,
		"auto RESET_STREAM = %+v, want reset code=0x0107", fr)
}

// TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv checks that a received
// RESET_STREAM finishes the receive side.
func TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv(t *testing.T) {
	c, s, _, _ := newResetTestConn(t)
	h := &connFrameHandler{c: c}

	resetErr := h.OnResetStream(s.id, 0x0107, 0)

	require.NoErrorf(t, resetErr, "OnResetStream: %v", resetErr)
	assert.True(t, s.Finished(), "a received RESET_STREAM must finish the receive side")
}

// TestStream_Reset_NotEstablished checks that Reset/StopSending error rather than
// panic when the 1-RTT keys are not yet installed.
func TestStream_Reset_NotEstablished(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}} // no oneRTTSealer, no pc
	s, err := c.OpenStream()
	require.NoError(t, err, "open a stream on a connection with no 1-RTT keys")

	resetErr := s.Reset(0x0108)
	stopErr := s.StopSending(0x0108)

	assert.ErrorIsf(t, resetErr, ErrNotEstablished,
		"Reset without keys = %v, want ErrNotEstablished", resetErr)
	assert.ErrorIsf(t, stopErr, ErrNotEstablished,
		"StopSending without keys = %v, want ErrNotEstablished", stopErr)
}

// TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss checks that a lost
// RESET_STREAM is retransmitted (§13.3).
func TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	require.NoError(t, s.Reset(0x0108), "reset the stream so a RESET_STREAM is in flight")

	// Force the RESET_STREAM packet (pn 0) lost by acking a much later packet.
	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp)
	flushErr := c.flush()

	require.NoError(t, flushErr, "flush the retransmitted RESET_STREAM")
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[len(pc.writes)-1])
	assert.Truef(t, fr.reset && fr.code == 0x0108,
		"RESET_STREAM not retransmitted on loss: %+v", fr)
}

// TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset checks that a reset
// stream's STREAM data is not retransmitted (§13.3).
func TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	n, sendErr := s.Send([]byte("hello"), false) // pn 0: STREAM
	require.NoErrorf(t, sendErr, "Send = %d,%v, want 5,nil (STREAM data must actually go out)", n, sendErr)
	require.Equal(t, 5, n, "the whole payload must go out before the reset")
	require.NoError(t, s.Reset(0x0108), "reset the stream") // pn 1: RESET_STREAM
	writesBefore := len(pc.writes)

	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp) // both the STREAM (pn 0) and RESET (pn 1) packets are lost
	flushErr := c.flush()

	require.NoError(t, flushErr, "flush the retransmissions")
	for _, w := range pc.writes[writesBefore:] {
		assert.False(t, decodeStreamCtl(t, opener, c.dcid, w).stream,
			"STREAM data must not be retransmitted after RESET_STREAM (§13.3)")
	}
}

// TestConformance_RFC9000_Sec133_NoRetransmitAfterResetAndEvict checks that a
// reset stream's STREAM data is not retransmitted even after the stream has been
// retired from the routing map: Reset scrubs the data from the retransmit
// sources, so §13.3 does not depend on the flush-time suppression check finding
// the stream by id.
func TestConformance_RFC9000_Sec133_NoRetransmitAfterResetAndEvict(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	h := &connFrameHandler{c: c}
	n, sendErr := s.Send([]byte("hello"), false) // pn 0: STREAM
	require.NoErrorf(t, sendErr, "Send = %d,%v, want 5,nil (STREAM data must actually go out)", n, sendErr)
	require.Equal(t, 5, n, "the whole payload must go out before the reset")
	require.NoError(t, s.Reset(0x0108), "reset the send side; this scrubs the STREAM data")
	require.NoError(t, h.OnResetStream(s.id, 0x0108, 5), "make the receive side terminal too")
	require.NotContains(t, c.streams, s.id,
		"stream should be retired after send reset + receive reset")
	writesBefore := len(pc.writes)

	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp) // both the STREAM (pn 0) and RESET (pn 1) packets are lost
	flushErr := c.flush()

	require.NoError(t, flushErr, "flush the retransmissions")
	sawReset := false
	for _, w := range pc.writes[writesBefore:] {
		fr := decodeStreamCtl(t, opener, c.dcid, w)
		assert.False(t, fr.stream,
			"STREAM data must not be retransmitted after RESET_STREAM, even once retired (§13.3)")
		sawReset = sawReset || fr.reset
	}
	assert.True(t, sawReset, "the lost RESET_STREAM must still be retransmitted (§13.3)")
}
