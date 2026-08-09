package client

import (
	"context"
	"math/rand"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// pickLeastLoaded returns early on the first idle connection instead of scanning
// the whole pool. The claim is that this is EXACTLY what a full scan returns:
// zero is the smallest possible active count, and the comparison is strict, so
// ties already go to the earliest connection. A claim of exact equivalence has to
// be checked against a reference rather than asserted — which is what these do.

// pickFakeH3 is a minimal h3Client whose only job is to answer the two liveness
// predicates the pick loop calls.
type pickFakeH3 struct {
	alive  bool
	goaway bool
}

func (f *pickFakeH3) Do(context.Context, *http3.Request) (*http3.Response, []byte, error) {
	return nil, nil, nil
}

func (f *pickFakeH3) DoStream(context.Context, *http3.Request) (*http3.Response, http3.ResponseBody, error) {
	return nil, nil, nil
}
func (f *pickFakeH3) Alive() bool     { return f.alive }
func (f *pickFakeH3) GoingAway() bool { return f.goaway }
func (f *pickFakeH3) Close() error    { return nil }

// refPickH3 is the pre-optimisation loop, kept verbatim as the oracle.
func refPickH3(conns []*h3ManagedConn) *h3ManagedConn {
	var best *h3ManagedConn
	for _, mc := range conns {
		if !mc.cl.Alive() || mc.cl.GoingAway() {
			continue
		}
		if mc.active >= mc.streamCap {
			continue
		}
		if best == nil || mc.active < best.active {
			best = mc
		}
	}
	return best
}

// refPickH2 is the same oracle for the HTTP/2 pool.
func refPickH2(conns []*managedConn) *managedConn {
	var best *managedConn
	for _, mc := range conns {
		if !mc.c.IsAlive() {
			continue
		}
		if mc.active >= mc.streamCap {
			continue
		}
		if best == nil || mc.active < best.active {
			best = mc
		}
	}
	return best
}

// TestPickLeastLoaded_EarlyReturnMatchesFullScan drives randomised pools through
// both implementations and requires the SAME pointer back, not merely an equally
// loaded one: identity is what the caller sees, so returning a different
// connection with the same active count would still be a behaviour change.
func TestPickLeastLoaded_EarlyReturnMatchesFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test input, not crypto
	p := &h3Pool{}

	for trial := 0; trial < 3000; trial++ {
		n := rng.Intn(12)
		conns := make([]*h3ManagedConn, 0, n)
		for i := 0; i < n; i++ {
			conns = append(conns, &h3ManagedConn{
				// A dead or GOAWAY'd conn is skipped, and streamCap 0 forces the
				// over-cap branch — all three skip paths get exercised.
				cl:        &pickFakeH3{alive: rng.Intn(6) != 0, goaway: rng.Intn(8) == 0},
				active:    rng.Intn(4),
				streamCap: rng.Intn(4),
			})
		}
		if got, want := p.pickLeastLoaded(conns), refPickH3(conns); got != want {
			t.Fatalf("trial %d: early return picked %p, full scan picked %p", trial, got, want)
		}
	}
}

// TestPickLeastLoaded_H2EarlyReturnMatchesFullScan is the same property for the
// HTTP/2 pool, whose loop changed identically. conn.Conn's liveness flags are
// unexported, so every connection here is alive and the randomisation covers the
// load dimension; the H3 case above covers the liveness dimension.
func TestPickLeastLoaded_H2EarlyReturnMatchesFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic test input, not crypto
	p := &Pool{}

	for trial := 0; trial < 3000; trial++ {
		n := rng.Intn(12)
		conns := make([]*managedConn, 0, n)
		for i := 0; i < n; i++ {
			conns = append(conns, &managedConn{
				c:         &conn.Conn{}, // hand-built: reads alive
				active:    rng.Intn(4),
				streamCap: rng.Intn(4),
			})
		}
		if got, want := p.pickLeastLoaded(conns), refPickH2(conns); got != want {
			t.Fatalf("trial %d: early return picked %p, full scan picked %p", trial, got, want)
		}
	}
}

// TestPickLeastLoaded_IdleWinsAndTiesGoToTheEarliest pins the two facts the
// equivalence argument rests on, so a future edit that breaks either is caught
// here rather than as a load-distribution change nobody notices.
func TestPickLeastLoaded_IdleWinsAndTiesGoToTheEarliest(t *testing.T) {
	p := &h3Pool{}
	live := func(active, capn int) *h3ManagedConn {
		return &h3ManagedConn{cl: &pickFakeH3{alive: true}, active: active, streamCap: capn}
	}

	busy, idle1, idle2 := live(2, 8), live(0, 8), live(0, 8)
	if got := p.pickLeastLoaded([]*h3ManagedConn{busy, idle1, idle2}); got != idle1 {
		t.Errorf("picked %p, want the FIRST idle connection %p — ties go to the earliest", got, idle1)
	}

	// With nothing idle the full scan still has to run to the end.
	all := []*h3ManagedConn{live(3, 8), live(1, 8), live(2, 8)}
	if got := p.pickLeastLoaded(all); got != all[1] {
		t.Errorf("with no idle connection, picked %p, want the least loaded %p", got, all[1])
	}

	// An idle but over-cap connection is not eligible, so the early return must
	// not fire on it: streamCap 0 means even zero active is at the cap.
	overCap := live(0, 0)
	busy2 := live(1, 8)
	if got := p.pickLeastLoaded([]*h3ManagedConn{overCap, busy2}); got != busy2 {
		t.Errorf("picked %p, want %p — an at-cap connection is not eligible however idle", got, busy2)
	}
}
