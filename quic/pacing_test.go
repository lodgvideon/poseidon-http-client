package quic

import (
	"testing"
	"time"
)

// pacingConn returns a sealable connection wired for pacing: a large congestion
// window, a 100 ms smoothed RTT, and a frozen clock the caller advances by hand.
func pacingConn(t *testing.T) (*Conn, *Stream, *time.Time) {
	t.Helper()
	c, s, _, _ := sendTestConn(t, 1<<24, 1<<24) // generous flow-control credit
	base := time.Unix(100, 0)
	clk := &base
	c.now = func() time.Time { return *clk }
	c.cwnd = 200000 // a congestion window far above the burst limit
	c.ssthresh = ^uint64(0)
	c.rtt.smoothedRTT = 100 * time.Millisecond
	return c, s, clk
}

// TestConformance_RFC9002_Sec77_Pacing checks that a bulk send is paced rather than
// dumped back-to-back: the first burst is limited to about the initial congestion
// window (not the whole, larger congestion window), an empty bucket admits nothing
// until time passes, and a smoothed-RTT of elapsed time refills the bucket.
func TestConformance_RFC9002_Sec77_Pacing(t *testing.T) {
	c, s, clk := pacingConn(t)

	// A single send of far more than the congestion window: pacing admits only an
	// initial-window burst of on-wire bytes, never the whole window back-to-back.
	n, err := s.Send(make([]byte, 200000), false)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(n) >= c.cwnd {
		t.Fatalf("burst admitted %d payload bytes >= cwnd %d — pacing did not limit the burst", n, c.cwnd)
	}
	// The burst is bounded in on-wire bytes to the initial window; the admitted
	// payload is that minus a datagram of framing/AEAD overhead per packet.
	if lo := int(kInitialWindow) - 2*maxDatagramSize; n < lo || uint64(n) > kInitialWindow {
		t.Fatalf("burst = %d payload bytes, want ≈ initial window %d (§7.7 burst limit)", n, kInitialWindow)
	}

	// The bucket is empty and the clock has not moved: nothing more is admitted.
	if n2, _ := s.Send(make([]byte, 200000), false); n2 != 0 {
		t.Fatalf("second immediate send admitted %d bytes, want 0 (empty pacing bucket)", n2)
	}

	// A full smoothed-RTT of wall-clock refills the bucket for another burst.
	*clk = clk.Add(100 * time.Millisecond)
	n3, err := s.Send(make([]byte, 200000), false)
	if err != nil {
		t.Fatal(err)
	}
	if n3 == 0 || uint64(n3) > kInitialWindow {
		t.Fatalf("post-refill send = %d bytes, want a fresh ≈initial-window burst", n3)
	}
}

// TestConformance_RFC9002_Sec77_PacingRate checks the refill rate is exactly the
// §7.7 rate N·congestion_window/smoothed_rtt with N=1.25: over 1 ms with cwnd=200000
// and srtt=100 ms the bucket gains 1.25·200000·(1/100) = 2500 bytes.
func TestConformance_RFC9002_Sec77_PacingRate(t *testing.T) {
	c, _, clk := pacingConn(t)
	c.pacingBudget, c.pacingLast = 0, *clk

	*clk = clk.Add(time.Millisecond)
	if got := c.pacingCredit(); got != 2500 {
		t.Fatalf("pacingCredit after 1 ms = %d, want 2500 (rate 1.25·cwnd/srtt)", got)
	}
}

// TestConformance_RFC9002_Sec77_PacingSubQuantumNoStarve is the regression for a
// fast retry loop: many refills each shorter than one byte's worth of time must
// still accumulate into budget, not be discarded — otherwise the send path
// livelocks at zero bytes per second.
func TestConformance_RFC9002_Sec77_PacingSubQuantumNoStarve(t *testing.T) {
	c, _, clk := pacingConn(t)
	c.pacingBudget, c.pacingLast = 0, *clk

	// The rate is 2.5 bytes/µs, so a single 100 ns step is 0.25 byte and truncates
	// to 0; only the carried remainder mints budget. 5000 steps = 500 µs ≈ 1250 bytes.
	for i := 0; i < 5000; i++ {
		*clk = clk.Add(100 * time.Nanosecond)
		c.pacingCredit()
	}
	if c.pacingBudget < 1000 {
		t.Fatalf("500 µs of sub-quantum steps minted %d bytes, want ≈1250 — time was discarded (livelock)", c.pacingBudget)
	}
}

// TestConformance_RFC9002_Sec77_PacingBoundsSmallWrites checks that the burst
// limit is counted in on-wire bytes, not payload: many one-byte writes are bounded
// to about an initial window of wire bytes. Debiting the 1-byte payload instead
// would admit ~kInitialWindow one-byte datagrams (a >40x overshoot of the limit).
func TestConformance_RFC9002_Sec77_PacingBoundsSmallWrites(t *testing.T) {
	c, s, pc, _ := sendTestConn(t, 1<<24, 1<<24)
	base := time.Unix(100, 0)
	c.now = func() time.Time { return base } // frozen clock: no refill during the burst
	c.cwnd = 200000
	c.ssthresh = ^uint64(0)
	c.rtt.smoothedRTT = 100 * time.Millisecond

	count := 0
	for {
		n, err := s.Send([]byte{0xAA}, false)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break // pacing bucket empty
		}
		count++
		if count > 5000 {
			t.Fatal("one-byte writes not bounded — pacing debit is not counting wire bytes")
		}
	}

	wire := 0
	for _, p := range pc.pkts {
		wire += len(p)
	}
	if uint64(wire) > kInitialWindow+maxDatagramSize || uint64(wire) < kInitialWindow-maxDatagramSize {
		t.Fatalf("one-byte-write burst = %d wire bytes over %d packets, want ≈ initial window %d", wire, count, kInitialWindow)
	}
}

// TestConformance_RFC9002_Sec77_NoPacingWithoutRTT checks that before any RTT
// sample (smoothed_rtt == 0) sending is not paced: more than the initial window may
// be sent, bounded only by the congestion window (§7.7/§7.2 initial-window burst).
func TestConformance_RFC9002_Sec77_NoPacingWithoutRTT(t *testing.T) {
	c, s, _ := pacingConn(t)
	c.rtt.smoothedRTT = 0 // no RTT sample yet

	n, err := s.Send(make([]byte, 200000), false)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(n) <= kInitialWindow {
		t.Fatalf("without an RTT sample the send should not be paced; got %d bytes", n)
	}
}

// TestConformance_RFC9002_Sec77_PacingCarriesTheWholeByteRemainder is the other
// half of the carry, and the half nothing covered.
//
// PacingSubQuantumNoStarve pins the case where a step earns NO whole byte. This
// pins the case where it earns some: the leftover fraction has to survive too, or
// the long-run rate drifts below the one 7.7 asks for. The two are different
// branches -- one returns before touching pacingLast, the other advances it by
// the time that minted the whole bytes rather than by the whole step.
//
// At 2.5 bytes/microsecond a 600 ns step earns exactly 1.5 bytes. Truncating each
// step to 1 byte and discarding the rest costs a third of the rate: 1000 bytes
// where 7.7 wants 1500. That is the gap this asserts, and it is what a
// refillPacingBucket advancing pacingLast by the full elapsed produces.
func TestConformance_RFC9002_Sec77_PacingCarriesTheWholeByteRemainder(t *testing.T) {
	c, _, clk := pacingConn(t)
	c.pacingBudget, c.pacingLast = 0, *clk

	const steps = 1000
	for i := 0; i < steps; i++ {
		*clk = clk.Add(600 * time.Nanosecond)
		c.pacingCredit()
	}

	// 600 microseconds at 2.5 bytes/microsecond. Well under kInitialWindow, so the
	// budget cap is not what this measures.
	const want = 1500
	if c.pacingBudget < want-50 {
		t.Fatalf("1000 steps of 1.5 bytes each minted %d bytes, want about %d: the "+
			"sub-byte remainder is being discarded, so the long-run pacing rate sits "+
			"below N*cwnd/smoothed_rtt (RFC 9002 §7.7)", c.pacingBudget, want)
	}
	if c.pacingBudget > want+50 {
		t.Fatalf("1000 steps of 1.5 bytes each minted %d bytes, want about %d: more "+
			"time was credited than elapsed", c.pacingBudget, want)
	}
}
