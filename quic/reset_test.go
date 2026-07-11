package quic

import "testing"

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
	if err != nil {
		t.Fatal(err)
	}
	return c, s, opener, pc
}

func decodeStreamCtl(t *testing.T, opener *Opener, dcid, pkt []byte) resetCtl {
	t.Helper()
	hdr, err := ParseHeader(pkt, len(dcid))
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var h resetCtl
	if err := ParseFrames(payload, &h); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
	return h
}

// TestConformance_RFC9000_Sec194_ResetStream checks that Stream.Reset sends a
// RESET_STREAM frame with the final size and blocks further sends.
func TestConformance_RFC9000_Sec194_ResetStream(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	if err := s.Reset(0x0108); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if !s.sendReset {
		t.Fatal("sendReset should be set")
	}
	if _, err := s.Send([]byte("x"), false); err != ErrStreamReset {
		t.Fatalf("Send after Reset = %v, want ErrStreamReset", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d packets, want 1", len(pc.writes))
	}
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	if !fr.reset || fr.id != s.id || fr.code != 0x0108 {
		t.Fatalf("RESET_STREAM = %+v, want reset id=%d code=0x0108", fr, s.id)
	}
	// Idempotent.
	if err := s.Reset(0x0108); err != nil || len(pc.writes) != 1 {
		t.Fatalf("second Reset should be a no-op (err=%v writes=%d)", err, len(pc.writes))
	}
}

// TestConformance_RFC9000_Sec195_StopSending checks that Stream.StopSending sends
// a STOP_SENDING frame.
func TestConformance_RFC9000_Sec195_StopSending(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	if err := s.StopSending(0x010c); err != nil {
		t.Fatalf("StopSending: %v", err)
	}
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	if !fr.stop || fr.id != s.id || fr.code != 0x010c {
		t.Fatalf("STOP_SENDING = %+v, want stop id=%d code=0x010c", fr, s.id)
	}
}

// TestConformance_RFC9000_Sec35_StopSendingTriggersReset checks that receiving a
// STOP_SENDING resets our send side with the same code (RFC 9000 §3.5).
func TestConformance_RFC9000_Sec35_StopSendingTriggersReset(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	h := &connFrameHandler{c: c}
	if err := h.OnStopSending(s.id, 0x0107); err != nil {
		t.Fatalf("OnStopSending: %v", err)
	}
	if !s.sendReset {
		t.Fatal("a received STOP_SENDING must reset our send side")
	}
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[0])
	if !fr.reset || fr.code != 0x0107 {
		t.Fatalf("auto RESET_STREAM = %+v, want reset code=0x0107", fr)
	}
}

// TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv checks that a received
// RESET_STREAM finishes the receive side.
func TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv(t *testing.T) {
	c, s, _, _ := newResetTestConn(t)
	h := &connFrameHandler{c: c}
	if err := h.OnResetStream(s.id, 0x0107, 0); err != nil {
		t.Fatalf("OnResetStream: %v", err)
	}
	if !s.Finished() {
		t.Fatal("a received RESET_STREAM must finish the receive side")
	}
}

// TestStream_Reset_NotEstablished checks that Reset/StopSending error rather than
// panic when the 1-RTT keys are not yet installed.
func TestStream_Reset_NotEstablished(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}} // no oneRTTSealer, no pc
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(0x0108); err != ErrNotEstablished {
		t.Fatalf("Reset without keys = %v, want ErrNotEstablished", err)
	}
	if err := s.StopSending(0x0108); err != ErrNotEstablished {
		t.Fatalf("StopSending without keys = %v, want ErrNotEstablished", err)
	}
}

// TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss checks that a lost
// RESET_STREAM is retransmitted (§13.3).
func TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	if err := s.Reset(0x0108); err != nil {
		t.Fatal(err)
	}
	// Force the RESET_STREAM packet (pn 0) lost by acking a much later packet.
	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	fr := decodeStreamCtl(t, opener, c.dcid, pc.writes[len(pc.writes)-1])
	if !fr.reset || fr.code != 0x0108 {
		t.Fatalf("RESET_STREAM not retransmitted on loss: %+v", fr)
	}
}

// TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset checks that a reset
// stream's STREAM data is not retransmitted (§13.3).
func TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset(t *testing.T) {
	c, s, opener, pc := newResetTestConn(t)
	if n, err := s.Send([]byte("hello"), false); err != nil || n != 5 { // pn 0: STREAM
		t.Fatalf("Send = %d,%v, want 5,nil (STREAM data must actually go out)", n, err)
	}
	if err := s.Reset(0x0108); err != nil { // pn 1: RESET_STREAM
		t.Fatal(err)
	}
	writesBefore := len(pc.writes)
	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp) // both the STREAM (pn 0) and RESET (pn 1) packets are lost
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	for _, w := range pc.writes[writesBefore:] {
		if fr := decodeStreamCtl(t, opener, c.dcid, w); fr.stream {
			t.Fatal("STREAM data must not be retransmitted after RESET_STREAM (§13.3)")
		}
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
	if n, err := s.Send([]byte("hello"), false); err != nil || n != 5 { // pn 0: STREAM
		t.Fatalf("Send = %d,%v, want 5,nil (STREAM data must actually go out)", n, err)
	}
	if err := s.Reset(0x0108); err != nil { // pn 1: RESET_STREAM; scrubs the STREAM data
		t.Fatal(err)
	}
	if err := h.OnResetStream(s.id, 0x0108, 5); err != nil { // receive side terminal → stream retired
		t.Fatal(err)
	}
	if _, ok := c.streams[s.id]; ok {
		t.Fatal("stream should be retired after send reset + receive reset")
	}
	writesBefore := len(pc.writes)
	c.sent[spaceApp].largestAckedPN, c.sent[spaceApp].haveLargestAcked = 5, true
	c.detectLost(spaceApp) // both the STREAM (pn 0) and RESET (pn 1) packets are lost
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	sawReset := false
	for _, w := range pc.writes[writesBefore:] {
		fr := decodeStreamCtl(t, opener, c.dcid, w)
		if fr.stream {
			t.Fatal("STREAM data must not be retransmitted after RESET_STREAM, even once retired (§13.3)")
		}
		if fr.reset {
			sawReset = true
		}
	}
	if !sawReset {
		t.Fatal("the lost RESET_STREAM must still be retransmitted (§13.3)")
	}
}
