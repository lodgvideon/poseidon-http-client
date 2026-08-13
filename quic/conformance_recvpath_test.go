package quic

import (
	"bytes"
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

// newInitialConn builds a client Conn that can open server Initial packets keyed
// on origDCID, as the tests below receive them.
func newInitialConn(t *testing.T, origDCID []byte) *Conn {
	t.Helper()
	_, serverKeys := InitialKeys(origDCID)
	c := &Conn{pc: &closePC{}, dcid: append([]byte(nil), origDCID...), handshakeComplete: true}
	op, err := NewOpener(serverKeys)
	if err != nil {
		t.Fatal(err)
	}
	c.keys.Initial = op
	return c
}

// TestConformance_RFC9000_Sec52_ForeignVersionDiscarded pins "If a client receives
// a packet that uses a different version than it initially selected, it MUST
// discard that packet." The packet must be dropped BEFORE decryption: Initial keys
// derive from the observable connection ID with a public salt, so an on-path forger
// can seal a perfectly authenticating v1 Initial while stamping another version in
// the header. Discarding is silent — never a connection error, since the header is
// unauthenticated at that point.
func TestConformance_RFC9000_Sec52_ForeignVersionDiscarded(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	forgedSCID := []byte{0x11, 0x22, 0x33, 0x44}

	for _, version := range []uint32{0x00000002, 0x6b3343cf /* a GREASE version */, QUICVersion1 + 1} {
		c := newInitialConn(t, origDCID)
		pkt := craftServerInitialVersion(t, serverKeys, nil, forgedSCID, 0,
			appendPing(nil), version)
		if err := c.recvDatagram(pkt); err != nil {
			t.Fatalf("version %#x: recvDatagram = %v, want nil (silently discarded)", version, err)
		}
		if c.gotServerCID {
			t.Fatalf("version %#x: adopted the server connection ID from a foreign-version packet", version)
		}
		if !bytes.Equal(c.dcid, origDCID) {
			t.Fatalf("version %#x: DCID poisoned: %x, want %x", version, c.dcid, origDCID)
		}
		if c.haveRecv[spaceInitial] {
			t.Fatalf("version %#x: a foreign-version packet was processed", version)
		}
	}

	// Control: the same packet at version 1 IS processed, so the test is not
	// passing merely because the fixture never decrypts.
	c := newInitialConn(t, origDCID)
	ok := craftServerInitial(t, serverKeys, nil, forgedSCID, 0, appendPing(nil))
	if err := c.recvDatagram(ok); err != nil {
		t.Fatalf("control: recvDatagram = %v, want nil", err)
	}
	if !c.gotServerCID || !c.haveRecv[spaceInitial] {
		t.Fatal("control: a valid v1 Initial was not processed, so the fixture proves nothing")
	}
}

// TestConformance_RFC9000_Sec122_CoalescedForeignDCIDIgnored pins "Receivers SHOULD
// ignore any subsequent packets with a different Destination Connection ID than the
// first packet in the datagram." A forger can append a packet to a genuine
// datagram; only the first packet's DCID is the one the connection answers to.
func TestConformance_RFC9000_Sec122_CoalescedForeignDCIDIgnored(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	scid := []byte{0xaa, 0xbb, 0xcc, 0xdd}

	// Packet 1 addresses the client's (empty) connection ID; packet 2 carries a
	// different DCID and must be ignored even though it authenticates.
	p1 := craftServerInitial(t, serverKeys, nil, scid, 1, appendPing(nil))
	p2 := craftServerInitial(t, serverKeys, []byte{0x09, 0x09, 0x09, 0x09}, scid, 2, appendPing(nil))

	c := newInitialConn(t, origDCID)
	if err := c.recvDatagram(append(append([]byte{}, p1...), p2...)); err != nil {
		t.Fatalf("recvDatagram = %v, want nil", err)
	}
	if !c.haveRecv[spaceInitial] {
		t.Fatal("the first packet was not processed, so the test proves nothing")
	}
	if got := c.largestRecv[spaceInitial]; got != 1 {
		t.Fatalf("largestRecv = %d, want 1 — the foreign-DCID packet was processed", got)
	}
}

// TestConformance_RFC9000_Sec124_EmptyPacketIsProtocolViolation pins "An endpoint
// MUST treat receipt of a packet containing no frames as a connection error of type
// PROTOCOL_VIOLATION." Unlike the discards above this packet authenticated, so it
// is the peer's own violation, not something anyone can inject.
func TestConformance_RFC9000_Sec124_EmptyPacketIsProtocolViolation(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)

	c := newInitialConn(t, origDCID)
	empty := craftServerInitial(t, serverKeys, nil, []byte{0xaa}, 0, nil)
	if err := c.recvDatagram(empty); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("recvDatagram(zero-frame packet) = %v, want ErrProtocolViolation", err)
	}
}

// TestConformance_RFC9001_Sec57_ZeroRTTDiscardedNotDecrypted pins "a client MUST NOT
// attempt to decrypt 0-RTT packets it receives and instead MUST discard them"
// (RFC 9001 §5.7). Without the discard, packetSpace's default arm routes a 0-RTT
// packet into the Initial space and hands it to the Initial Opener — and Initial
// keys derive from the observable connection ID with a public salt, so an on-path
// forger can seal a 0-RTT-typed packet that genuinely authenticates.
func TestConformance_RFC9001_Sec57_ZeroRTTDiscardedNotDecrypted(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)
	forgedSCID := []byte{0x11, 0x22, 0x33, 0x44}

	sealer, err := NewSealer(serverKeys)
	if err != nil {
		t.Fatal(err)
	}
	payload := appendPing(make([]byte, 0, 32))
	pnLen := 4
	hdr, pnOff := AppendLongHeader(nil, PacketZeroRTT, QUICVersion1, nil, forgedSCID, nil, pnLen, uint64(pnLen+len(payload)+16))
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, 0)
	}
	pkt, err := sealer.Seal(nil, hdr, pnOff, pnLen, 0, payload)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	c := newInitialConn(t, origDCID)
	if err := c.recvDatagram(pkt); err != nil {
		t.Fatalf("recvDatagram(0-RTT) = %v, want nil (discarded)", err)
	}
	if c.gotServerCID {
		t.Fatal("a 0-RTT packet must not be decrypted, let alone have its SCID adopted")
	}
	if c.haveRecv[spaceInitial] {
		t.Fatal("a 0-RTT packet was processed in the Initial packet-number space")
	}
}

// TestConformance_RFC9000_Sec123_ReplayedPacketDiscarded pins "A receiver MUST
// discard a newly unprotected packet unless it is certain that it has not processed
// another packet with the same packet number from the same packet number space." A
// replayed datagram authenticates — the AEAD nonce derives from the packet number —
// so the duplicate must be dropped before its frames reach the handlers.
//
// Every delivery gets a FRESH COPY of the captured bytes, and that copy is the
// whole test. recvDatagram removes header protection IN PLACE — byte 0 of an
// Initial goes 0xca -> 0xc3 — so handing it the same slice a second time replays a
// packet that no longer authenticates. That one is dropped at the "no keys, or
// authentication failed" skip in the datagram loop, long before the dedup check,
// and leaves no ACK owed for entirely the wrong reason. This test did exactly that
// and passed with the dedup check disabled (#636); an off-path attacker replays
// the bytes as they went over the wire, which is what the copy reproduces.
//
// ALL THREE packet-number spaces, because c.acks is per-space and so is any
// defect in a per-space check: narrowing the dedup to `sp != spaceApp` left the
// whole quic suite green while 1-RTT replays sailed through, and `sp !=
// spaceHandshake` did the same until the Handshake arm below existed.
func TestConformance_RFC9000_Sec123_ReplayedPacketDiscarded(t *testing.T) {
	for _, arm := range replayLongHeaderArms() {
		t.Run(arm.name, func(t *testing.T) { testReplayDiscarded(t, arm) })
	}
	t.Run("app_frame_dispatch", testReplayAppFramesNotDispatched)
}

// replayArm is one long-header packet-number space's fixture for
// testReplayDiscarded: a Conn already holding that space's read keys, and a way
// to seal a server packet into that space carrying one ack-eliciting PING.
// Initial and Handshake differ in nothing else, so they share the arm body rather
// than duplicating it — and running that one body per space is what makes a dedup
// check narrowed to skip a single space fail on exactly that space's run.
type replayArm struct {
	name  string
	space int
	setup func(t *testing.T) (c *Conn, craft func(pn uint64) []byte)
}

// replayLongHeaderArms builds the Initial and Handshake fixtures.
//
// The Handshake arm's starting state is one the connection really passes
// through, not a convenience: c.keys.Handshake goes in at
// SetReadKeys(QUICEncryptionLevelHandshake) and the only thing that takes it away
// is discardSpace(spaceHandshake), whose sole non-test caller is OnHandshakeDone
// — the server's HANDSHAKE_DONE (RFC 9001 §4.9.2). handshakeComplete is already
// true because TLS completion sets it (Conn.HandshakeComplete) without discarding
// the space. So the replay window spans the whole server Handshake flight, and
// every packet of it stays replayable until HANDSHAKE_DONE lands.
//
// The payload is a PING because §12.4 Table 3 permits only PADDING, PING, ACK,
// CRYPTO and a transport CONNECTION_CLOSE in these two spaces — the 1-RTT arm's
// PATH_CHALLENGE would be a PROTOCOL_VIOLATION here, which is why frame dispatch
// is observed there and the per-packet costs are observed here.
func replayLongHeaderArms() []replayArm {
	return []replayArm{{
		name:  "initial_ack_and_idle_timer",
		space: spaceInitial,
		setup: func(t *testing.T) (*Conn, func(uint64) []byte) {
			origDCID := []byte("origdcid")
			_, serverKeys := InitialKeys(origDCID)
			return newInitialConn(t, origDCID), func(pn uint64) []byte {
				return craftServerInitial(t, serverKeys, nil, []byte{0xaa}, pn, appendPing(nil))
			}
		},
	}, {
		name:  "handshake_ack_and_idle_timer",
		space: spaceHandshake,
		setup: func(t *testing.T) (*Conn, func(uint64) []byte) {
			scid := []byte{0xaa}
			sealer, opener := closeTestSealerOpener(t, 0x5c)
			c := &Conn{
				pc: &closePC{}, dcid: []byte("replayhs"), handshakeComplete: true,
				// The server's Initial arrived long before its Handshake flight,
				// so its connection ID is already adopted (§7.2).
				gotServerCID: true, serverSCID: scid,
			}
			c.keys.Handshake = opener
			return c, func(pn uint64) []byte {
				return sealServerPacket(t, sealer, PacketHandshake, nil, scid, pn, appendPing(nil))
			}
		},
	}}
}

// testReplayDiscarded is the long-header arm, run once per space: the two costs a
// duplicate that reaches the handlers imposes on this end regardless of what it
// carries — the ACK owed (§13.2.1, one ACK per replay is attacker-driven work)
// and the idle timer (§10.1, a captured datagram replayed forever would postpone
// the idle timeout without the peer ever sending anything new).
func testReplayDiscarded(t *testing.T, arm replayArm) {
	sp := arm.space
	c, craft := arm.setup(t)
	start := time.Unix(1700000000, 0)
	now := start
	c.now = func() time.Time { return now }
	c.lastActivity = start.Add(-time.Hour)

	captured := craft(7)
	deliver := func(pkt []byte) error { return c.recvDatagram(append([]byte(nil), pkt...)) }

	if err := deliver(captured); err != nil {
		t.Fatalf("first recvDatagram = %v, want nil", err)
	}
	if !c.acks[sp].ackPending() {
		t.Fatal("an ack-eliciting packet should leave an ACK owed")
	}
	if !c.lastActivity.Equal(start) {
		t.Fatalf("a processed packet must reset the idle timer: lastActivity = %v, want %v",
			c.lastActivity, start)
	}
	c.acks[sp].acked()           // the ACK went out
	now = start.Add(time.Minute) // time passes before the replay arrives

	if err := deliver(captured); err != nil {
		t.Fatalf("replayed recvDatagram = %v, want nil (discarded)", err)
	}
	if c.acks[sp].ackPending() {
		t.Error("a replayed packet was processed again: its PING re-owed an ACK")
	}
	if !c.lastActivity.Equal(start) {
		t.Errorf("a replayed packet advanced the idle timer to %v, want it left at %v",
			c.lastActivity, start)
	}

	// Control: a genuinely new packet number is still processed, and still does
	// both of the things the replay must not.
	if err := deliver(craft(8)); err != nil {
		t.Fatalf("fresh recvDatagram = %v, want nil", err)
	}
	if !c.acks[sp].ackPending() {
		t.Fatal("control: a new packet number must still be processed")
	}
	if !c.lastActivity.Equal(now) {
		t.Fatalf("control: a new packet must reset the idle timer: lastActivity = %v, want %v",
			c.lastActivity, now)
	}
}

// testReplayAppFramesNotDispatched is the 1-RTT arm, and it observes FRAME
// DISPATCH itself — which neither the owed ACK nor the idle timer does, since both
// are bookkeeping that processPacket runs after ParseFrames returns. Moving the
// dedup check to just below ParseFrames therefore left the arms above green while
// every replayed frame ran a second time.
//
// A PATH_CHALLENGE is the observable: the handler answers it by queueing a
// PATH_RESPONSE in pendingCtrl (§8.2.2), so a replay whose frames run appends nine
// bytes the peer never asked for — echo traffic an off-path attacker gets to
// generate from one captured datagram. pendingCtrl is the harness's existing view
// of queued outbound bytes; the flushed datagram is not, since §8.2.2 pads it to
// 1200 whether it carries one PATH_RESPONSE or two.
//
// The replayed number is deliberately NOT the largest received, so a check keyed on
// largestRecv rather than on the tracker's per-number ranges fails here as well.
func testReplayAppFramesNotDispatched(t *testing.T) {
	sealer, opener := closeTestSealerOpener(t, 0x9d)
	c := &Conn{pc: &closePC{}, dcid: []byte("replay1r"), oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT = opener

	craft := func(pn uint64, data [8]byte) []byte {
		return sealServerPacket(t, sealer, PacketShort, nil, nil, pn,
			append([]byte{byte(FramePathChallenge)}, data[:]...))
	}
	deliver := func(pkt []byte) error { return c.recvDatagram(append([]byte(nil), pkt...)) }
	var want []byte // the PATH_RESPONSEs dispatch is allowed to have queued so far
	check := func(stage string) {
		t.Helper()
		if !bytes.Equal(c.pendingCtrl, want) {
			t.Errorf("%s: pendingCtrl = %x, want %x (%d PATH_RESPONSEs)",
				stage, c.pendingCtrl, want, len(want)/9)
		}
	}

	lowData := [8]byte{'l', 'o', 'w', 'c', 'h', 'a', 'l', 'l'}
	low := craft(5, lowData)
	if err := deliver(low); err != nil {
		t.Fatalf("recvDatagram(pn=5) = %v, want nil", err)
	}
	want = appendPathResponse(want, lowData)
	check("first delivery")

	// A higher number lands next, so the replay below is no longer the largest
	// received and a largest-only dedup would wave it through.
	highData := [8]byte{'h', 'i', 'g', 'h', 'c', 'h', 'a', 'l'}
	if err := deliver(craft(9, highData)); err != nil {
		t.Fatalf("recvDatagram(pn=9) = %v, want nil", err)
	}
	want = appendPathResponse(want, highData)
	check("higher packet number")
	c.acks[spaceApp].acked() // both ACKs went out

	if err := deliver(low); err != nil {
		t.Fatalf("replayed recvDatagram = %v, want nil (discarded)", err)
	}
	check("after the replay: its PATH_CHALLENGE was dispatched a second time")
	if c.acks[spaceApp].ackPending() {
		t.Error("a replayed packet was processed again: it re-owed an ACK")
	}

	// Control: a genuinely new packet number in the same space still dispatches,
	// so the arm is not passing merely because nothing ever reaches the handlers.
	freshData := [8]byte{'f', 'r', 'e', 's', 'h', 'c', 'h', 'l'}
	if err := deliver(craft(10, freshData)); err != nil {
		t.Fatalf("fresh recvDatagram = %v, want nil", err)
	}
	want = appendPathResponse(want, freshData)
	check("control")
	if !c.acks[spaceApp].ackPending() {
		t.Fatal("control: a new packet number must still be processed")
	}
}

// TestConformance_RFC9000_Sec174_SpinBitRandomPerConnection pins §17.4's
// RECOMMENDED "set the spin bit to a random value either chosen independently for
// each packet or chosen independently for each connection ID" for an endpoint that
// does not implement latency spin. A constant 0 is a passive fingerprint. Checks
// both that the drawn bit reaches the wire and that it varies across connections.
func TestConformance_RFC9000_Sec174_SpinBitRandomPerConnection(t *testing.T) {
	seal := func(spin bool) byte {
		dcid := []byte("spinbit0")
		keys, _ := InitialKeys(dcid)
		sealer, err := NewSealer(keys)
		if err != nil {
			t.Fatal(err)
		}
		c := &Conn{pc: &closePC{}, dcid: dcid, oneRTTSealer: sealer, spin: spin, now: func() time.Time { return time.Unix(9, 0) }}
		pkt, err := c.sealPacket(spaceApp, appendPing(nil), true, nil, false)
		if err != nil {
			t.Fatalf("sealPacket: %v", err)
		}
		return pkt[0]
	}
	if got := seal(true) & 0x20; got == 0 {
		t.Fatal("spin=true must set bit 0x20 on the short header")
	}
	if got := seal(false) & 0x20; got != 0 {
		t.Fatal("spin=false must leave bit 0x20 clear")
	}

	// The value is actually drawn per connection, not hard-coded: over enough
	// NewConn calls both values must appear.
	tlsCfg := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}
	var seenTrue, seenFalse bool
	for i := 0; i < 64 && (!seenTrue || !seenFalse); i++ {
		c, err := NewConn(&closePC{}, tlsCfg, nil)
		if err != nil {
			t.Fatalf("NewConn: %v", err)
		}
		if c.spin {
			seenTrue = true
		} else {
			seenFalse = true
		}
	}
	if !seenTrue || !seenFalse {
		t.Fatalf("spin bit is not random per connection: sawTrue=%v sawFalse=%v", seenTrue, seenFalse)
	}
}

// TestConformance_RFC9000_Sec123_ReorderedPacketNotDiscarded is the false-reject
// guard on the §12.3 duplicate check: "unless it is certain" licenses discarding
// only what the receiver cannot decide. A packet numbered below one already
// received is provably new while the tracker still holds every range, so ordinary
// reordering must be processed. Only real maxAckRanges truncation makes the region
// below the retained window undecidable.
func TestConformance_RFC9000_Sec123_ReorderedPacketNotDiscarded(t *testing.T) {
	origDCID := []byte("origdcid")
	_, serverKeys := InitialKeys(origDCID)

	c := newInitialConn(t, origDCID)
	// pn 1 overtakes pn 0 on the wire.
	if err := c.recvDatagram(craftServerInitial(t, serverKeys, nil, []byte{0xaa}, 1, appendPing(nil))); err != nil {
		t.Fatalf("recvDatagram(pn=1) = %v", err)
	}
	c.acks[spaceInitial].acked()
	if err := c.recvDatagram(craftServerInitial(t, serverKeys, nil, []byte{0xaa}, 0, appendPing(nil))); err != nil {
		t.Fatalf("recvDatagram(pn=0) = %v", err)
	}
	if !c.acks[spaceInitial].ackPending() {
		t.Fatal("a reordered lower packet number was discarded as a duplicate")
	}

	// Only actual truncation closes the floor. Force >maxAckRanges gaps, then a
	// number under the retained window becomes undecidable and is discarded.
	var a ackTracker
	for i := 0; i <= maxAckRanges; i++ {
		a.receive(uint64(100+2*i), true) // permanent gaps: one range per packet
	}
	if !a.truncated {
		t.Fatalf("expected truncation after %d gapped packets", maxAckRanges+1)
	}
	// 100 is inside the range that truncation dropped: genuinely undecidable.
	if !a.seen(100) {
		t.Fatal("a number inside a dropped range must count as seen")
	}
	// 101 was never received and sits above the dropped range, so it is still
	// provably new — the floor must not be raised to the last retained range's lo.
	if a.seen(101) {
		t.Fatalf("lowWater = %d over-discards: 101 was never received and is provably new", a.lowWater)
	}
	// And a gap inside the retained window stays new.
	if a.seen(103) {
		t.Fatal("a gap inside the retained window is still decidably new")
	}
}
