package quic

import (
	"context"
	"testing"
	"time"
)

// The bounded ACK-deferral suite (RFC 9000 §13.2.1). A 1-RTT ACK may be delayed up
// to our advertised max_ack_delay so it can piggyback the next outbound STREAM
// packet — halving send syscalls on the GET request/response path — EXCEPT when the
// received ack-eliciting packet is out of order, or is the 2nd since the last ACK,
// which are acknowledged immediately to drive the peer's loss detection.

// frameSpy records whether an ACK and/or a STREAM frame decoded from a packet.
type frameSpy struct {
	nopFrameHandler
	sawAck     bool
	sawStream  bool
	streamData []byte
}

func (h *frameSpy) OnAck(_, _, _ uint64) error { h.sawAck = true; return nil }
func (h *frameSpy) OnStream(_, _ uint64, _ bool, data []byte) error {
	h.sawStream = true
	h.streamData = append(h.streamData, data...)
	return nil
}

// decodeClientApp opens a client-sealed 1-RTT datagram (sealed with the symmetric
// test key, so the same-key opener reads it back) and parses its frames into spy.
func decodeClientApp(t *testing.T, opener *Opener, dcid, pkt []byte, spy *frameSpy) {
	t.Helper()
	hdr, err := ParseHeader(pkt, len(dcid))
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ParseFrames(payload, spy); err != nil {
		t.Fatalf("ParseFrames: %v", err)
	}
}

// ackDeferConn builds a post-handshake client Conn over pc with a fake clock and a
// symmetric 1-RTT key, seeded so an opened stream can both receive and send. It
// returns the Conn and the opener that reads back what the Conn seals.
func ackDeferConn(t *testing.T, pc PacketConn, dcid []byte, clock func() time.Time) (*Conn, *Opener) {
	t.Helper()
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{
		pc:                pc,
		dcid:              dcid,
		oneRTTSealer:      sealer,
		now:               clock,
		handshakeComplete: true,
		connMax:           1 << 20,
		connRecvMax:       DefaultConnRecvWindow,
		peer: TransportParams{
			InitialMaxStreamsBidi:          4,
			InitialMaxStreamDataBidiRemote: 1 << 20,
			MaxAckDelay:                    defaultMaxAckDelay,
		},
	}
	c.keys.OneRTT = opener
	return c, opener
}

// TestAckTracker_ImmediateTriggers isolates the two §13.2.1 immediate-ACK triggers
// at the tracker level: the 2nd ack-eliciting packet since the last ACK, and an
// out-of-order arrival (a gap above the top, or a reorder below it). A lone
// in-order ack-eliciting packet does NOT trigger immediate — it may be deferred.
func TestAckTracker_ImmediateTriggers(t *testing.T) {
	t.Run("in-order-single-defers", func(t *testing.T) {
		var a ackTracker
		a.receive(0, true)
		if a.immediate {
			t.Fatal("a lone in-order ack-eliciting packet must not force an immediate ACK")
		}
	})
	t.Run("second-ack-eliciting", func(t *testing.T) {
		var a ackTracker
		a.receive(0, true)
		a.receive(1, true) // 2nd ack-eliciting since the last ACK
		if !a.immediate {
			t.Fatal("the 2nd ack-eliciting packet must force an immediate ACK (§13.2.1)")
		}
	})
	t.Run("gap-above-top", func(t *testing.T) {
		var a ackTracker
		a.receive(0, true)
		a.acked() // simulate a prior ACK: reset the deferral counters
		a.receive(3, true)
		if !a.immediate {
			t.Fatal("an ack-eliciting packet leaving a gap above the top must ACK immediately (§13.2.1)")
		}
	})
	t.Run("reorder-below-top", func(t *testing.T) {
		var a ackTracker
		for _, pn := range []uint64{0, 1, 3, 4} { // gap at 2
			a.receive(pn, true)
		}
		a.acked() // reset the deferral counters
		a.receive(2, true)
		if !a.immediate {
			t.Fatal("an ack-eliciting packet reordered below the top must ACK immediately (§13.2.1)")
		}
	})
	t.Run("acked-resets-triggers", func(t *testing.T) {
		var a ackTracker
		a.receive(0, true)
		a.receive(1, true)
		a.acked()
		if a.immediate || a.elicitCount != 0 || a.pending {
			t.Fatalf("acked must clear pending/immediate/elicitCount, got %+v", a)
		}
	})
}

// TestConn_AckDefer_ImmediateOnOutOfOrder drives the receive path with a gapped
// burst (packet numbers 0 and 2, missing 1): the out-of-order arrival forces the
// ACK out in the same Poll rather than being deferred (RFC 9000 §13.2.1).
func TestConn_AckDefer_ImmediateOnOutOfOrder(t *testing.T) {
	dcid := []byte("ooorder1")
	base := time.Unix(50000, 0)
	pc := &drainingPC{}
	c, opener := ackDeferConn(t, pc, dcid, func() time.Time { return base })

	sealer := c.oneRTTSealer
	// Two server 1-RTT STREAM packets on stream 0 with a gap at packet number 1.
	pc.pkts = [][]byte{
		sealServerPacket(t, sealer, PacketShort, nil, nil, 0, AppendStream(nil, 0, 0, false, []byte("a"))),
		sealServerPacket(t, sealer, PacketShort, nil, nil, 2, AppendStream(nil, 0, 4, false, []byte("c"))),
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatal(err)
	}

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(pc.written) != 1 {
		t.Fatalf("wrote %d datagrams, want 1 immediate ACK for the out-of-order burst", len(pc.written))
	}
	spy := &frameSpy{}
	decodeClientApp(t, opener, dcid, pc.written[0], spy)
	if !spy.sawAck {
		t.Fatal("the immediate datagram must carry an ACK frame")
	}
	if !c.ackDeadline.IsZero() || c.acks[spaceApp].pending {
		t.Fatalf("an immediate ACK must leave no deferral armed: deadline=%v pending=%v", c.ackDeadline, c.acks[spaceApp].pending)
	}
}

// TestConn_AckDefer_ImmediateOnSecondAckEliciting drives the receive path with two
// in-order ack-eliciting packets: the 2nd forces the ACK out immediately, so the
// whole burst is acknowledged in the same Poll (RFC 9000 §13.2.1).
func TestConn_AckDefer_ImmediateOnSecondAckEliciting(t *testing.T) {
	dcid := []byte("second01")
	base := time.Unix(51000, 0)
	pc := &drainingPC{}
	c, opener := ackDeferConn(t, pc, dcid, func() time.Time { return base })

	sealer := c.oneRTTSealer
	pc.pkts = [][]byte{
		sealServerPacket(t, sealer, PacketShort, nil, nil, 0, AppendStream(nil, 0, 0, false, []byte("a"))),
		sealServerPacket(t, sealer, PacketShort, nil, nil, 1, AppendStream(nil, 0, 1, false, []byte("b"))),
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatal(err)
	}

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(pc.written) != 1 {
		t.Fatalf("wrote %d datagrams, want 1 immediate ACK on the 2nd ack-eliciting packet", len(pc.written))
	}
	spy := &frameSpy{}
	decodeClientApp(t, opener, dcid, pc.written[0], spy)
	if !spy.sawAck {
		t.Fatal("the immediate datagram must carry an ACK frame")
	}
}

// TestConn_AckDefer_DefersLoneInOrder proves the core deferral: a single in-order
// ack-eliciting packet does NOT flush an ACK inside the receive hold — the ACK is
// held pending with a max_ack_delay deadline, to ride the next outbound packet
// (RFC 9000 §13.2.1).
func TestConn_AckDefer_DefersLoneInOrder(t *testing.T) {
	dcid := []byte("defer001")
	base := time.Unix(52000, 0)
	pc := &drainingPC{}
	c, _ := ackDeferConn(t, pc, dcid, func() time.Time { return base })

	sealer := c.oneRTTSealer
	pc.pkts = [][]byte{
		sealServerPacket(t, sealer, PacketShort, nil, nil, 0, AppendStream(nil, 0, 0, false, []byte("resp"))),
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatal(err)
	}

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(pc.written) != 0 {
		t.Fatalf("wrote %d datagrams, want 0 — a lone in-order ACK must be deferred, not flushed in the recv hold", len(pc.written))
	}
	if !c.acks[spaceApp].pending {
		t.Fatal("the ACK must remain owed (pending) after deferral")
	}
	if want := base.Add(defaultMaxAckDelay); !c.ackDeadline.Equal(want) {
		t.Fatalf("ackDeadline = %v, want recv + max_ack_delay = %v", c.ackDeadline, want)
	}
}

// TestConn_AckDefer_PiggybackOnStream is the syscall-saving win and the datagrams/
// req 2->1 proof: after a lone in-order ack-eliciting packet defers its ACK, a Do
// that sends a STREAM carries the owed ACK in that same packet — so the recv-then-
// send sequence emits ONE datagram (STREAM+ACK) instead of two (RFC 9000 §13.2.1).
func TestConn_AckDefer_PiggybackOnStream(t *testing.T) {
	dcid := []byte("piggy001")
	base := time.Unix(53000, 0)
	pc := &drainingPC{}
	c, opener := ackDeferConn(t, pc, dcid, func() time.Time { return base })

	sealer := c.oneRTTSealer
	pc.pkts = [][]byte{
		sealServerPacket(t, sealer, PacketShort, nil, nil, 0, AppendStream(nil, 0, 0, false, []byte("resp"))),
	}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	// Receive: the response defers an ACK (no datagram out yet).
	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(pc.written) != 0 {
		t.Fatalf("recv wrote %d datagrams, want 0 (ACK deferred)", len(pc.written))
	}

	// Send: a request STREAM must carry the deferred ACK — one datagram, not two.
	if _, err := s.Send([]byte("GET / HTTP/3"), true); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(pc.written) != 1 {
		t.Fatalf("recv-then-send emitted %d datagrams, want 1 (ACK piggybacked on the STREAM packet)", len(pc.written))
	}
	spy := &frameSpy{}
	decodeClientApp(t, opener, dcid, pc.written[0], spy)
	if !spy.sawAck || !spy.sawStream {
		t.Fatalf("the STREAM datagram must carry BOTH a STREAM and the piggybacked ACK: ack=%v stream=%v", spy.sawAck, spy.sawStream)
	}
	if c.acks[spaceApp].pending || !c.ackDeadline.IsZero() {
		t.Fatalf("piggyback must clear the owed ACK + deferral: pending=%v deadline=%v", c.acks[spaceApp].pending, c.ackDeadline)
	}
}

// TestConn_AckDefer_TimerFallback proves the deferred ACK still fires within
// max_ack_delay when no outbound packet carries it: before the deadline the reader
// wakes but sends nothing; at the deadline it flushes the owed ACK — and the wake
// never provokes a spurious PTO probe (RFC 9000 §13.2.1). Driven through the read-
// deadline expiry path (handleExpiry) with the deferral state seeded.
func TestConn_AckDefer_TimerFallback(t *testing.T) {
	dcid := []byte("timer001")
	base := time.Unix(54000, 0)
	clk := base
	pc := &expiryPC{}
	c, opener := ackDeferConn(t, pc, dcid, func() time.Time { return clk })

	// Seed a deferred ACK for a lone in-order ack-eliciting packet with nothing in
	// flight, so the loss/idle deadline is the far give-up bound and the ACK deadline
	// is the binding constraint on the read.
	c.acks[spaceApp].receive(0, true)
	c.ackDeadline = base.Add(defaultMaxAckDelay)
	c.armedLossDeadline = base.Add(idleTimeout)
	c.armedForAck = true // the reader armed its read deadline for the ACK deadline

	// Before the deadline: an early wake must send nothing and must not probe.
	clk = base.Add(10 * time.Millisecond)
	if err := c.handleExpiry(clk, timeoutError{}); err != nil {
		t.Fatalf("handleExpiry (pre-deadline) = %v, want nil", err)
	}
	if len(pc.writes) != 0 {
		t.Fatalf("wrote %d datagrams before max_ack_delay, want 0", len(pc.writes))
	}
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d, want 0 — an ACK-only wake must not provoke a PTO probe", c.ptoCount)
	}

	// At the deadline: the owed ACK must go out.
	clk = base.Add(defaultMaxAckDelay)
	if err := c.handleExpiry(clk, timeoutError{}); err != nil {
		t.Fatalf("handleExpiry (at deadline) = %v, want nil (the ACK fires, connection stays up)", err)
	}
	if len(pc.writes) != 1 {
		t.Fatalf("wrote %d datagrams at max_ack_delay, want 1 ACK", len(pc.writes))
	}
	spy := &frameSpy{}
	decodeClientApp(t, opener, dcid, pc.writes[0], spy)
	if !spy.sawAck {
		t.Fatal("the timer-fired datagram must carry an ACK frame")
	}
	if c.acks[spaceApp].pending || !c.ackDeadline.IsZero() {
		t.Fatalf("firing the ACK must clear the owed state: pending=%v deadline=%v", c.acks[spaceApp].pending, c.ackDeadline)
	}
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d, want 0 after the ACK fired", c.ptoCount)
	}
}

// TestConn_AckDefer_NotDeferredWithoutTimer checks the safety fallback: on a
// transport that cannot arm a read deadline, the deferred-ACK timer cannot fire, so
// the ACK is never deferred — it is sent immediately, exactly as before deferral,
// so it can never stall (RFC 9000 §13.2.1, conservative).
func TestConn_AckDefer_NotDeferredWithoutTimer(t *testing.T) {
	dcid := []byte("notimer1")
	base := time.Unix(55000, 0)
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	// onePacketPC has no SetReadDeadline, so canScheduleAckTimer is false.
	frames := AppendStream(nil, 0, 0, false, []byte("resp"))
	pkt := sealServerPacket(t, sealer, PacketShort, nil, nil, 0, frames)
	pc := &onePacketPC{pkt: pkt}
	c := &Conn{
		pc: pc, dcid: dcid, oneRTTSealer: sealer,
		now: func() time.Time { return base }, handshakeComplete: true,
		connRecvMax: DefaultConnRecvWindow,
		peer:        TransportParams{InitialMaxStreamsBidi: 1},
	}
	c.keys.OneRTT = opener
	if _, err := c.OpenStream(); err != nil {
		t.Fatal(err)
	}

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(pc.written) != 1 {
		t.Fatalf("wrote %d datagrams, want 1 — without a timer the ACK must be immediate, never deferred", len(pc.written))
	}
	if !c.ackDeadline.IsZero() {
		t.Fatalf("no deferral must be armed on a deadline-less transport, got %v", c.ackDeadline)
	}
}
