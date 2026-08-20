package quic

import "testing"

// sendAllocsPerDatagram is what one datagram costs in heap allocations when
// nothing has been acknowledged yet: the RFC 9000 §13.3 retransmit copy of the
// chunk, and nothing else.
//
// This is the cold case, not the steady state. §13.3 requires the data be
// available to resend until the packet is acknowledged, so with no ACKs in sight
// there is nothing to recycle and every datagram buys a fresh buffer. Once
// acknowledgements start arriving the copies come off the connection's free list
// instead — see sendAllocsPerDatagramSteadyState, which is what a real
// connection pays.
//
// The second allocation that used to sit beside this one was a one-element
// []retransFrame per packet, which existed only because sentPacket kept its
// frames in a slice while every send site in the package builds a packet around
// exactly one.
const sendAllocsPerDatagram = 1

// sendAllocsPerDatagramSteadyState is what one datagram costs on a connection
// whose packets are being acknowledged — every real connection. The §13.3 copy
// comes off the free list a previous ACK returned it to, so the acknowledged
// send path allocates nothing at all.
//
// It stood at 1 for a while, on the strength of the sent-packet map churning as
// this loop deletes and re-inserts the same packet number. That allocation is no
// longer observed: 0.00 per datagram, read off the gate itself by forcing its
// limit to -1, on go1.26.6 linux/amd64 without -race. Leaving the constant at 1
// left this gate tolerating a full allocation per datagram of regression before
// it could fire — which is how it drifted in the first place, and exactly what
// the two-sided form below prevents. Its cold sibling has been two-sided all
// along, which is why the cold figure is still accurate (#856).
//
// Without the free list this is 2, so the gate below still catches the pooling
// being removed. Measured directly when it was added: pool on, 1 alloc / 160 B;
// pool off, 2 allocs / 208 B — the 48-byte difference is the retransmit copy.
const sendAllocsPerDatagramSteadyState = 0

// TestSendPath_AllocsPerDatagram pins the send path's allocation count.
//
// The send benchmarks that measure the same thing are gated behind
// POSEIDON_BENCH_SEND and excluded from the zero-alloc bench-gate — deliberately,
// since this path is not zero-alloc and never can be while §13.3 stands. That
// leaves the count ungated, which is how a per-packet allocation gets reintroduced
// without anyone noticing. This test is the gate.
func TestSendPath_AllocsPerDatagram(t *testing.T) {
	c, s, pc := benchSendConn(t)
	req := []byte("GET / HTTP/3\r\nhost: h3.example\r\naccept: */*\r\n\r\n")

	// Warm up: the first send grows the sent-packet map and the seal scratch,
	// one-time costs that are not what this measures.
	resetSend(c, s)
	if _, err := s.Send(req, false); err != nil {
		t.Fatalf("warmup Send: %v", err)
	}

	before := pc.datagrams.Load()
	const runs = 200
	got := testing.AllocsPerRun(runs, func() {
		resetSend(c, s)
		if _, err := s.Send(req, false); err != nil {
			t.Fatalf("Send: %v", err)
		}
	})
	// AllocsPerRun runs the closure runs+1 times. Confirm the request really did
	// fit one datagram, or the per-datagram figure below means something else.
	if sent := pc.datagrams.Load() - before; sent != runs+1 {
		t.Fatalf("request took %d datagrams over %d runs, want 1 each — "+
			"the allocation figure is per datagram and this no longer measures that",
			sent, runs+1)
	}

	if got > sendAllocsPerDatagram {
		t.Errorf("send path allocates %.2f times per datagram, want at most %d: "+
			"something on the packet path started allocating again",
			got, sendAllocsPerDatagram)
	}
	if got < sendAllocsPerDatagram {
		t.Errorf("send path allocates %.2f times per datagram, fewer than the recorded %d: "+
			"the path improved — lower sendAllocsPerDatagram to %.0f to lock the win in",
			got, sendAllocsPerDatagram, got)
	}
}

// TestSendPath_AllocsPerDatagramSteadyState is the figure that describes a live
// connection: send, acknowledge, repeat.
//
// TestSendPath_AllocsPerDatagram deliberately never acknowledges anything, so it
// can only ever see the cold cost — and that is exactly why pooling the §13.3
// retransmit copy did not move it by a single allocation. Measuring the pool
// needs the ACK that returns a buffer to the free list, which is the step that
// benchmark leaves out and every real connection performs.
func TestSendPath_AllocsPerDatagramSteadyState(t *testing.T) {
	c, s, pc := benchSendConn(t)
	req := []byte("GET / HTTP/3\r\nhost: h3.example\r\naccept: */*\r\n\r\n")

	// Warm up past the one-time growth: the sent-packet map, the seal scratch,
	// and the free list's own backing array.
	for i := 0; i < 4; i++ {
		resetSend(c, s)
		if _, err := s.Send(req, false); err != nil {
			t.Fatalf("warmup Send: %v", err)
		}
		c.sent[spaceApp].ack(c, 0, 0)
	}

	before := pc.datagrams.Load()
	const runs = 200
	got := testing.AllocsPerRun(runs, func() {
		resetSend(c, s)
		if _, err := s.Send(req, false); err != nil {
			t.Fatalf("Send: %v", err)
		}
		// The acknowledgement a real peer would return, which is what hands the
		// retransmit copy back for the next send to reuse.
		c.sent[spaceApp].ack(c, 0, 0)
	})
	if sent := pc.datagrams.Load() - before; sent != runs+1 {
		t.Fatalf("request took %d datagrams over %d runs, want 1 each", sent, runs+1)
	}

	if got > sendAllocsPerDatagramSteadyState {
		t.Errorf("an acknowledged send path allocates %.2f times per datagram, want at most %d: "+
			"the §13.3 retransmit copy is no longer coming off the free list",
			got, sendAllocsPerDatagramSteadyState)
	}
	// The lower arm the cold gate has. It is unreachable while the constant is 0 —
	// an allocation count cannot go below zero — and it is here so the pair stays
	// symmetric: if the constant is ever raised to absorb a regression, this is
	// what announces the day the regression is fixed instead of letting the slack
	// be quietly re-absorbed. That is the drift this gate just came out of.
	if got < sendAllocsPerDatagramSteadyState {
		t.Errorf("an acknowledged send path allocates %.2f times per datagram, fewer than the "+
			"recorded %d: the path improved — lower sendAllocsPerDatagramSteadyState to %.0f "+
			"to lock the win in", got, sendAllocsPerDatagramSteadyState, got)
	}
}
