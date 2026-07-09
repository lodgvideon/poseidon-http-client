package quic

import (
	"context"
	"testing"
	"time"
)

func TestConn_PTOPeriod(t *testing.T) {
	c := &Conn{}
	if got := c.ptoPeriod(); got != 2*kInitialRtt {
		t.Fatalf("pre-sample ptoPeriod = %v, want %v", got, 2*kInitialRtt)
	}
	c.rtt.update(40*ms, 0) // smoothed=40ms, rttvar=20ms -> base = 40 + max(80,1) = 120ms
	if got := c.ptoPeriod(); got != 120*ms {
		t.Fatalf("ptoPeriod = %v, want 120ms", got)
	}
	c.ptoCount = 2
	if got, want := c.ptoPeriod(), 120*ms<<2; got != want {
		t.Fatalf("backoff ptoPeriod = %v, want %v", got, want)
	}
}

// TestConn_OnPTO_QueuesProbe checks a probe timeout re-queues the oldest
// unacknowledged packet's frames and backs off (RFC 9002 §6.2.4).
func TestConn_OnPTO_QueuesProbe(t *testing.T) {
	base := time.Unix(1, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "oldest"))
	c.sent[spaceApp].onSent(1, base.Add(ms), true, streamFrame(0, 6, "newer"))

	c.onPTO()

	if c.ptoCount != 1 {
		t.Fatalf("ptoCount = %d, want 1", c.ptoCount)
	}
	if len(c.retransQueue[spaceApp]) != 1 || string(c.retransQueue[spaceApp][0].data) != "oldest" {
		t.Fatalf("probe queue = %+v, want the oldest packet's frame", c.retransQueue[spaceApp])
	}
	if _, ok := c.sent[spaceApp].packets[0]; ok {
		t.Fatal("the probed (oldest) packet should be removed from flight")
	}
	if _, ok := c.sent[spaceApp].packets[1]; !ok {
		t.Fatal("the newer packet should remain in flight")
	}
}

func TestConn_PTOCount_ResetOnAck(t *testing.T) {
	base := time.Unix(2, 0)
	c := &Conn{now: func() time.Time { return base }}
	c.ptoCount = 3
	c.sent[spaceApp].onSent(5, base, true, nil)
	c.onAckRange(spaceApp, 5, 5, 0) // acknowledges an ack-eliciting packet
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d, want 0 after acknowledgement", c.ptoCount)
	}
}

// TestConformance_RFC9002_Sec622_PTOCountResetOnKeyDiscard checks that discarding
// a packet-number space (Initial or Handshake keys) resets the PTO backoff count,
// since the discard is forward progress (RFC 9002 §6.2.2, App. A.4).
func TestConformance_RFC9002_Sec622_PTOCountResetOnKeyDiscard(t *testing.T) {
	c := &Conn{ptoCount: 3}
	c.discardSpace(spaceHandshake)
	if c.ptoCount != 0 {
		t.Fatalf("ptoCount = %d after discarding Handshake keys, want 0", c.ptoCount)
	}
}

// deadlinePC times out on the first read (a simulated PTO expiry) then returns a
// canned datagram, recording writes (probes) and that a deadline was set.
type deadlinePC struct {
	reads       int
	writes      int
	deadlineSet bool
	datagram    []byte
}

func (p *deadlinePC) SetReadDeadline(time.Time) error { p.deadlineSet = true; return nil }
func (p *deadlinePC) Read(b []byte) (int, error) {
	p.reads++
	if p.reads == 1 {
		return 0, timeoutError{}
	}
	return copy(b, p.datagram), nil
}
func (p *deadlinePC) Write(b []byte) (int, error) { p.writes++; return len(b), nil }
func (p *deadlinePC) Close() error                { return nil }

type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }
func (timeoutError) Timeout() bool { return true }

// TestConn_ReadWithPTO_ProbesOnTimeout drives the receive loop's probe path:
// with data in flight, a read timeout triggers a probe (written) and a retry
// that then succeeds.
func TestConn_ReadWithPTO_ProbesOnTimeout(t *testing.T) {
	base := time.Unix(3, 0)
	ck, _ := InitialKeys([]byte("ptotest0"))
	sealer, err := NewSealer(ck)
	if err != nil {
		t.Fatal(err)
	}
	pc := &deadlinePC{datagram: []byte("later")}
	c := &Conn{pc: pc, dcid: []byte("ptotest0"), oneRTTSealer: sealer, now: func() time.Time { return base }}
	c.sent[spaceApp].onSent(0, base, true, streamFrame(0, 0, "req")) // in flight, probeable

	buf := make([]byte, 64)
	n, err := c.readWithPTO(context.Background(), buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "later" {
		t.Fatalf("read %q, want later", buf[:n])
	}
	if !pc.deadlineSet {
		t.Fatal("a read deadline should have been set")
	}
	if pc.writes == 0 {
		t.Fatal("a probe should have been written on the PTO timeout")
	}
	if c.ptoCount != 1 {
		t.Fatalf("ptoCount = %d, want 1", c.ptoCount)
	}
}
