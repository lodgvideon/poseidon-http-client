package quic

import (
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// In-process datagram loss, so loss recovery is an ordinary unit test instead of
// a Docker compose cycle (#553).
//
// Two things here are load-bearing and neither is obvious.
//
// First, this is NOT a wrapper around chanPC. The probe timeout only runs when
// the transport exposes SetReadDeadline: readWithPTO type-asserts for it
// (pto.go:222) and without it takes the plain-Read path, where a timeout goes
// straight back to the caller and no probe is ever sent. A fault injector
// layered over a deadline-less pipe would turn every dropped datagram into a
// hang rather than a retransmission, so the resulting "loss test" could only
// ever time out. faultPC therefore implements the deadline itself, and
// TestFaultPC_ReadDeadlineIsHonoured gates that property directly.
//
// Second, these run inside a synctest bubble. That is not only for speed: it
// makes time exact. The backoff ladder below is asserted as an equality against
// kInitialRtt rather than as "finished under N seconds", so the tests measure
// RFC 9002 §6.2.2 behaviour instead of the speed of the machine running them —
// and cannot flake on a loaded CI runner. Real elapsed time for the whole file
// is a few milliseconds.

// faultPC is an in-memory, deadline-capable PacketConn that can discard
// datagrams on the write path. Drops are chosen by a predicate over the
// datagram index rather than by a probability: a loss-recovery assertion wants
// "the first Initial is lost", not "some datagram is lost about this often",
// and an index predicate reproduces exactly without needing a seed.
type faultPC struct {
	rx <-chan []byte
	tx chan<- []byte

	// dropWrite reports whether the n-th datagram written (1-based) is
	// discarded. A nil predicate is the fault-free path.
	dropWrite func(n int) bool

	mu           sync.Mutex
	readDeadline time.Time
	writes       int
	drops        int
}

// Write accounts the datagram, then either forwards it or silently discards it.
// A dropped datagram is reported to the caller as fully written, because that is
// what losing it in the network looks like from the sender.
func (p *faultPC) Write(b []byte) (int, error) {
	p.mu.Lock()
	p.writes++
	drop := p.dropWrite != nil && p.dropWrite(p.writes)
	if drop {
		p.drops++
	}
	p.mu.Unlock()

	if drop {
		return len(b), nil
	}
	p.tx <- append([]byte(nil), b...)
	return len(b), nil
}

// Read delivers one datagram, bounded by the deadline last set. It returns
// os.ErrDeadlineExceeded on expiry because isTimeout classifies by the
// Timeout() bool method, and that classification is what makes the engine send
// a probe and back off rather than fail the handshake.
func (p *faultPC) Read(b []byte) (int, error) {
	p.mu.Lock()
	dl := p.readDeadline
	p.mu.Unlock()

	var expiry <-chan time.Time
	if !dl.IsZero() {
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		expiry = t.C
	}
	select {
	case dg := <-p.rx:
		return copy(b, dg), nil
	case <-expiry:
		return 0, os.ErrDeadlineExceeded
	}
}

// SetReadDeadline is what makes the PTO path reachable; see the file comment.
func (p *faultPC) SetReadDeadline(t time.Time) error {
	p.mu.Lock()
	p.readDeadline = t
	p.mu.Unlock()
	return nil
}

// Close satisfies PacketConn; the channels belong to the test.
func (p *faultPC) Close() error { return nil }

// counts reports datagrams offered to Write and datagrams discarded. A loss
// test that injected nothing passes for the wrong reason and looks identical to
// one that worked, so every test below asserts on this.
func (p *faultPC) counts() (writes, drops int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writes, p.drops
}

// dropNth returns a predicate discarding exactly the listed 1-based write
// indexes.
func dropNth(ns ...int) func(int) bool {
	want := make(map[int]bool, len(ns))
	for _, n := range ns {
		want[n] = true
	}
	return func(n int) bool { return want[n] }
}

// withFaults runs a handshake against the in-process server peer with faultPC
// on the client's datagram path, and returns the client and the injector.
func withFaults(t *testing.T, drop func(n int) bool) (*Conn, *faultPC) {
	t.Helper()
	var fp *faultPC
	client, _, _, _ := setupServerConnWith(t, func(inner PacketConn) PacketConn {
		cp := inner.(*chanPC)
		fp = &faultPC{rx: cp.rx, tx: cp.tx, dropWrite: drop}
		return fp
	})
	return client, fp
}

// TestFaultPC_NoLoss_IsTheControl pins that the injector is transparent when it
// injects nothing. Without this control, a green loss test would prove only
// that the handshake works, not that it survived a drop.
func TestFaultPC_NoLoss_IsTheControl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		client, fp := withFaults(t, nil)
		elapsed := time.Since(start)

		if !client.handshakeComplete {
			t.Fatal("handshake did not complete on a clean path")
		}
		writes, drops := fp.counts()
		if writes == 0 {
			t.Fatal("the injector saw no writes — it is not on the client's datagram path")
		}
		if drops != 0 {
			t.Fatalf("discarded %d datagrams with no predicate set", drops)
		}
		if elapsed != 0 {
			t.Errorf("clean handshake consumed %v; with nothing lost there is no probe "+
				"timeout to wait out, so it should cost no time at all", elapsed)
		}
	})
}

// TestConformance_RFC9002_Sec622_PTOBackoffFirstProbe loses exactly the client's
// first Initial and nothing else — the case the Docker loss suite cannot stage,
// because its relay rolls one uniform die per datagram and cannot target a
// flight. The handshake must still complete, and it can only do so by
// retransmitting when the probe timeout expires. §6.2.2 fixes that first
// expiry at 2*kInitialRtt, since no RTT sample exists yet.
func TestConformance_RFC9002_Sec622_PTOBackoffFirstProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		client, fp := withFaults(t, dropNth(1))
		elapsed := time.Since(start)

		if !client.handshakeComplete {
			t.Fatal("handshake did not complete after losing the first Initial")
		}
		writes, drops := fp.counts()
		if drops != 1 {
			t.Fatalf("discarded %d datagrams, want exactly 1 — the injection did not "+
				"happen as specified, so a pass here would mean nothing", drops)
		}
		if writes < 2 {
			t.Fatalf("client wrote %d datagrams; recovering a lost Initial requires at "+
				"least one retransmission", writes)
		}
		if want := 2 * kInitialRtt; elapsed != want {
			t.Errorf("recovered after %v, want exactly %v (§6.2.2: the first probe "+
				"timeout is 2*kInitialRtt before any RTT sample)", elapsed, want)
		}
	})
}

// TestConformance_RFC9002_Sec622_PTOBackoffDoubles loses the first two Initials
// and walks one rung up the ladder. ptoPeriod is ptoBase<<ptoCount, so the
// second wait is twice the first and the total is 2*kInitialRtt +
// 4*kInitialRtt. Asserting the sum, rather than merely "slower than one loss",
// is what makes this a test of the doubling instead of a test that time passes.
func TestConformance_RFC9002_Sec622_PTOBackoffDoubles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		client, fp := withFaults(t, dropNth(1, 2))
		elapsed := time.Since(start)

		if !client.handshakeComplete {
			t.Fatal("handshake did not complete after losing two Initials")
		}
		writes, drops := fp.counts()
		if drops != 2 {
			t.Fatalf("discarded %d datagrams, want exactly 2", drops)
		}
		if writes < 3 {
			t.Fatalf("client wrote %d datagrams, want at least 3", writes)
		}
		if want := 6 * kInitialRtt; elapsed != want {
			t.Errorf("recovered after %v, want exactly %v — one probe at 2*kInitialRtt "+
				"plus a doubled second at 4*kInitialRtt", elapsed, want)
		}
	})
}

// TestFaultPC_ReadDeadlineIsHonoured gates the property the whole file depends
// on: without a working deadline the engine never probes. Asserted directly so
// a regression names its cause instead of surfacing as an unexplained hang.
func TestFaultPC_ReadDeadlineIsHonoured(t *testing.T) {
	rx := make(chan []byte) // never written to
	fp := &faultPC{rx: rx, tx: make(chan []byte, 1)}

	if _, ok := PacketConn(fp).(interface {
		SetReadDeadline(time.Time) error
	}); !ok {
		t.Fatal("faultPC does not expose SetReadDeadline, so readWithPTO would take " +
			"the plain-Read path and never send a probe")
	}

	if err := fp.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := fp.Read(make([]byte, 16)); !isTimeout(err) {
		t.Fatalf("Read past the deadline returned %v, which isTimeout rejects; the "+
			"engine would treat it as a hard error instead of a probe trigger", err)
	}
}
