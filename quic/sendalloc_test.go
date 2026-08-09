package quic

import "testing"

// sendAllocsPerDatagram is what one datagram on the ordinary send path costs in
// heap allocations: the retransmit copy of the chunk, and nothing else.
//
// RFC 9000 §13.3 requires the data be available to resend until the packet is
// acknowledged, so that copy is a protocol obligation rather than an oversight
// — it is the one allocation this path is allowed. The second allocation that
// used to sit beside it was a one-element []retransFrame per packet, which
// existed only because sentPacket kept its frames in a slice while every send
// site in the package builds a packet around exactly one.
//
// Lower it if the retransmit copy is ever drawn from a pool; raise it only with
// a reason written down here.
const sendAllocsPerDatagram = 1

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
