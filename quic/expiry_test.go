package quic

import (
	"context"
	"errors"
	"testing"
	"time"
)

// expiryPC is a PacketConn whose Read always reports a timeout, simulating a
// read-deadline expiry with no datagram available — the trigger for the
// PTO / loss / idle-close branch of Poll. Writes (probes, ACKs) are captured and
// Close is recorded so an idle close can be observed.
type expiryPC struct {
	writes [][]byte
	closed bool
}

func (p *expiryPC) Read([]byte) (int, error) { return 0, timeoutError{} }
func (p *expiryPC) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}
func (p *expiryPC) Close() error                    { p.closed = true; return nil }
func (p *expiryPC) SetReadDeadline(time.Time) error { return nil }

// TestConn_Poll_IdleTimeoutCloses drives Poll into the read-timeout expiry branch
// with the idle deadline already elapsed: handleExpiry must silently idle-close
// the connection and surface ErrIdleTimeout (RFC 9000 §10.1). This is the
// afterReadLocked isTimeout -> handleExpiry -> idleClose path, previously covered
// only through the loss-relay interop harness.
func TestConn_Poll_IdleTimeoutCloses(t *testing.T) {
	base := time.Unix(10000, 0)
	pc := &expiryPC{}
	c := &Conn{
		pc:                pc,
		now:               func() time.Time { return base },
		handshakeComplete: true,
		localMaxIdle:      30 * time.Second,
		lastActivity:      base.Add(-time.Hour), // long past the idle deadline
	}

	if err := c.Poll(context.Background()); err != ErrIdleTimeout {
		t.Fatalf("Poll = %v, want ErrIdleTimeout on an elapsed idle deadline", err)
	}
	if !c.closed {
		t.Fatal("the connection should be marked closed after an idle timeout")
	}
	if !pc.closed {
		t.Fatal("idleClose should close the transport")
	}
}

// TestConn_Poll_PTOProbeOnTimeout drives Poll into the expiry branch with an
// ack-eliciting packet in flight and no idle timeout in effect: handleExpiry must
// take a PTO step — re-queue the oldest packet as a probe, back off ptoCount, and
// flush the probe — while keeping the connection up (RFC 9002 §6.2).
func TestConn_Poll_PTOProbeOnTimeout(t *testing.T) {
	base := time.Unix(20000, 0)
	dcid := []byte("expirypt")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	pc := &expiryPC{}
	c := &Conn{
		pc:                pc,
		dcid:              dcid,
		oneRTTSealer:      sealer,
		now:               func() time.Time { return base },
		handshakeComplete: true, // no anti-deadlock probe; no idle timeout advertised
		// The Application-space probe timer is armed only once the handshake is
		// confirmed (RFC 9002 §6.2.1), which is also when 1-RTT data can be in flight.
		handshakeConfirmed: true,
	}
	// One ack-eliciting packet (pn 0) in flight, so the probe timer runs and has
	// something to re-queue on expiry. sendPN is advanced past it so the resent
	// probe takes a fresh packet number rather than reusing 0 — otherwise the
	// re-add would mask the removal of the probed packet.
	c.sendPN[spaceApp] = 10
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "req"))

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll = %v, want nil (a PTO probe keeps the connection up)", err)
	}
	if c.ptoCount != 1 {
		t.Fatalf("ptoCount = %d, want 1 after a PTO expiry", c.ptoCount)
	}
	if _, ok := c.sent[spaceApp].packets[0]; ok {
		t.Fatal("the probed packet should be re-queued and removed from flight")
	}
	if len(pc.writes) == 0 {
		t.Fatal("a probe packet should have been written on the PTO expiry")
	}
}

// TestConn_HandleExpiry_DeclaresLostOnLossTimer covers the loss-detection branch
// of handleExpiry directly: with a packet eligible for time-threshold loss (an
// older packet below an acknowledged one), an expiry must declare it lost and
// re-queue its frames, not close the connection (RFC 9002 §6.1.2).
func TestConn_HandleExpiry_DeclaresLostOnLossTimer(t *testing.T) {
	base := time.Unix(30000, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.rtt.update(20*time.Millisecond, 0) // establishes a ~22.5ms loss delay
	// pn 0 sent a second ago; pn 1 acknowledged -> pn 0 is past the time threshold.
	c.sent[spaceApp].onSent(0, base.Add(-time.Second), true, streamFrame(0, 0, "lost"))
	c.sent[spaceApp].onSent(1, base.Add(-time.Second), true, nil)
	c.sent[spaceApp].ack(c, 1, 1) // largestAckedPN=1, haveLargestAcked=true, removes pn 1

	if err := c.handleExpiry(base, timeoutError{}); err != nil {
		t.Fatalf("handleExpiry = %v, want nil (loss detected, connection stays up)", err)
	}
	if len(c.retransQueue[spaceApp]) != 1 || string(c.retransQueue[spaceApp][0].data) != "lost" {
		t.Fatalf("retransQueue = %+v, want pn 0's frame re-queued as a retransmit", c.retransQueue[spaceApp])
	}
	if _, ok := c.sent[spaceApp].packets[0]; ok {
		t.Fatal("the lost packet should be removed from flight")
	}
}

// TestConn_HandleExpiry_GivesUpWhenNothingToProbe covers the default branch: with
// the handshake complete, no idle timeout, nothing in flight, and no pending
// loss, an expiry has nothing to probe and must surface the timeout to end the
// connection rather than re-arm forever (RFC 9002 §6.2).
//
// This asserted identity with the read error until #717: the give-up branch now
// reports ErrNoProgress instead, because a caller receiving *net.OpError out of
// Do could not tell "the peer went quiet" from "the socket broke". The three
// checks below are what identity used to stand for, and are strictly stronger —
// they pin the sentinel, the preserved cause, and the timeout surface separately,
// so dropping any one of the three fails here.
func TestConn_HandleExpiry_GivesUpWhenNothingToProbe(t *testing.T) {
	base := time.Unix(40000, 0)
	sentinel := timeoutError{}
	c := &Conn{now: func() time.Time { return base }, handshakeComplete: true}

	err := c.handleExpiry(base, sentinel)
	if err == nil {
		t.Fatal("handleExpiry = nil, want the give-up error that ends the connection")
	}
	if !errors.Is(err, ErrNoProgress) {
		t.Fatalf("handleExpiry = %v, want an error matching ErrNoProgress", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("handleExpiry = %v, dropped the underlying read error (%v) a caller needs for diagnosis", err, sentinel)
	}
	if !isTimeout(err) {
		t.Fatalf("handleExpiry = %v, which isTimeout rejects; the engine and any caller "+
			"asserting net.Error classify with a direct type assertion, so this must keep reporting Timeout()", err)
	}
}

// TestConn_Poll_NoProgressReachesTheCaller is the end-to-end half: the give-up
// error must survive Poll's teardown path, since that is where a parked Do wakes
// (Poll latches it through terminateLocked). The white-box test above pins what
// handleExpiry builds; this pins that nothing between there and the caller
// replaces it — which is the surface #717 was actually reported against.
func TestConn_Poll_NoProgressReachesTheCaller(t *testing.T) {
	base := time.Unix(50000, 0)
	pc := &expiryPC{}
	// No localMaxIdle, so no idle deadline is in effect and the expiry reaches the
	// give-up branch instead of idleClose — the arm TestConn_Poll_IdleTimeoutCloses
	// covers, and the one that must NOT be reported as ErrIdleTimeout.
	c := &Conn{
		pc:                pc,
		now:               func() time.Time { return base },
		handshakeComplete: true,
	}

	err := c.Poll(context.Background())
	if !errors.Is(err, ErrNoProgress) {
		t.Fatalf("Poll = %v, want an error matching ErrNoProgress", err)
	}
	if errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("Poll = %v, reported as an idle timeout; no idle timeout was negotiated and none elapsed", err)
	}
	if !isTimeout(err) {
		t.Fatalf("Poll = %v, which isTimeout rejects", err)
	}
}
