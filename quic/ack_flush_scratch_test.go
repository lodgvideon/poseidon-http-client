//go:build !race

package quic

import (
	"bytes"
	"testing"
)

// flush built the frame payload for a STREAM-less packet from a nil slice, so the
// standalone-ACK path allocated once per ACK even on a lossless in-order connection,
// and up to four times as ACK ranges accumulate under loss (#689).
//
// That is a different site from the one ack_scratch_test.go guards. There the scratch
// is the ACK *Range* section (a.extra), which is empty when a single contiguous range
// is being acknowledged — which is why its fix measured 0.00 -> 0.00 on a clean path
// and only paid under loss. Here the scratch is the frame *payload buffer*, which has
// to exist for every ACK regardless of how many ranges it carries. Measured on main
// before the fix, buildACK with a nil destination:
//
//	1 range    1.00 allocs/op        (a reused destination: 0.00)
//	2 ranges   1.00 allocs/op
//	6 ranges   2.00 allocs/op
//	20 ranges  4.00 allocs/op
//
// The existing gate does not see it because it hands buildACK a pre-allocated
// destination, so it is measuring a.extra and not the caller's nil.
//
// Behind !race for the reason the other allocation gates are: the detector allocates
// as it instruments.

// The transport here is bench_throughput_test.go's countingPC, which drops each
// datagram instead of keeping it. That is not incidental: capturePC retains every
// packet with append([]byte(nil), b...), which is an allocation per packet, and a
// memory profile of an earlier version of the gate below attributed 95% of its
// objects to that Write and none to the code under test. AllocsPerRun measures the
// whole process, so the fixture has to be quieter than the thing being measured.

// TestFlush_StandaloneACKDoesNotAllocate is the gate. It drives the real flush path
// on a connection that owes an ACK and has nothing else to send, which is exactly the
// standalone case, and asserts the steady state allocates nothing.
//
// The first call is excluded from the measurement on purpose: it is the one that
// grows the scratch, and charging a per-connection warmup to a per-ACK gate would
// make the test fail for the thing it is trying to permit.
func TestFlush_StandaloneACKDoesNotAllocate(t *testing.T) {
	c, _ := drainConn()
	pc := &countingPC{}
	c.pc = pc

	var pn uint64
	owe := func() {
		// An ack-eliciting packet arrives, so an ACK is owed and immediately due. The
		// numbers advance rather than repeat, because a duplicate is not new and would
		// leave nothing owed.
		pn += 2
		c.acks[spaceApp].receive(pn, true)
		c.acks[spaceApp].immediate = true
	}

	owe()
	if err := c.flush(); err != nil { // warmup: grows the scratch
		t.Fatal(err)
	}
	before := pc.datagrams.Load()

	n := testing.AllocsPerRun(200, func() {
		owe()
		_ = c.flush()
	})

	if pc.datagrams.Load() <= before {
		t.Fatalf("flush wrote no further packets (%d then %d) — nothing was measured, "+
			"so a zero here would say only that the path never ran", before, pc.datagrams.Load())
	}
	if n != 0 {
		t.Errorf("the standalone-ACK flush allocates %.2f objects per ACK, want 0 — the "+
			"frame payload buffer is being grown from nil on every call", n)
	}
}

// TestFlush_ScratchDoesNotCarryStaleFrames is the hazard the reuse buys, and the
// reason the gate above is not the only test. A payload buffer truncated with [:0]
// and refilled must not let a later, shorter packet emit bytes left by an earlier,
// longer one — the failure would be a peer parsing a frame this connection did not
// mean to send, on a path where nothing else would notice.
//
// It compares the frame payload, not the datagram. The sealed bytes cannot be
// compared across connections at all: the packet number is part of the AEAD nonce and
// of the header, so a connection that has already sent one packet encrypts its next
// one differently from a fresh connection's first — an earlier version of this test
// compared datagrams and could never have passed, for a reason that had nothing to do
// with the scratch.
//
// The expected bytes still come from the same code rather than a hand-encoded
// constant, so a mistake in the encoder cannot make both sides agree on the wrong
// answer.
func TestFlush_ScratchDoesNotCarryStaleFrames(t *testing.T) {
	// A connection whose first flush emits a large payload: many ACK ranges plus a
	// credit grant.
	warm, warmPC := drainConn()
	for i := 0; i < 12; i++ {
		warm.acks[spaceApp].receive(uint64(i*2), true) // gaps -> many ACK ranges
	}
	warm.acks[spaceApp].immediate = true
	warm.pendingCtrl = AppendMaxData(nil, 1<<40) // a wide varint
	if err := warm.flush(); err != nil {
		t.Fatal(err)
	}
	if len(warmPC.pkts) != 1 {
		t.Fatalf("warmup wrote %d packets, want 1", len(warmPC.pkts))
	}
	longLen := len(warm.ackScratch)

	// The same connection now sends the smallest thing it can: one contiguous ACK.
	warm.acks[spaceApp] = ackTracker{}
	warm.acks[spaceApp].receive(100, true)
	warm.acks[spaceApp].immediate = true
	if err := warm.flush(); err != nil {
		t.Fatal(err)
	}
	got := append([]byte(nil), warm.ackScratch...)

	// A fresh connection sending only that small packet.
	fresh, _ := drainConn()
	fresh.acks[spaceApp].receive(100, true)
	fresh.acks[spaceApp].immediate = true
	if err := fresh.flush(); err != nil {
		t.Fatal(err)
	}
	want := fresh.ackScratch

	if longLen <= len(want) {
		t.Fatalf("the warmup payload was %d bytes and the short one is %d — the first has "+
			"to be longer or there are no stale bytes for the second to expose", longLen, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Errorf("the frame payload built after a larger one differs from the same payload "+
			"built fresh:\n got %x\nwant %x\nthe reused scratch leaked the earlier packet's "+
			"frames", got, want)
	}
}
