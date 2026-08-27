package client

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// PickLeastLoaded no longer scans the whole pool: it starts at a rotating cursor
// and returns the first idle connection it meets, falling back to a full sweep
// for the true minimum when nothing is idle.
//
// That is a DELIBERATE change of selection order, so it cannot be tested against
// the old loop for pointer equality — the old one always returned the earliest
// tie, this one spreads. What must still hold is the contract, and these check it
// as properties over randomised pools rather than on a handful of examples.

// pickFakeH3 is a minimal h3Client that answers the two liveness predicates the
// pick loop calls, and nothing else.
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

func h3Eligible(mc *h3ManagedConn) bool {
	return mc.cl.Alive() && !mc.cl.GoingAway() && mc.active < mc.streamCap
}

func h2Eligible(mc *managedConn) bool {
	return mc.C.IsAlive() && mc.Active < mc.StreamCap
}

// TestPickLeastLoaded_ContractHolds is the property set, checked on 3,000
// randomised pools:
//
//  1. it never returns an ineligible connection — dead, GOAWAY'd or at its cap;
//  2. it returns nil only when nothing is eligible;
//  3. when any eligible connection is idle, the one returned is idle;
//  4. when none is idle, the one returned carries the minimum active count.
//
// Together these say "least loaded" is still honoured. What is deliberately NOT
// asserted is which of several equally loaded connections comes back — that is
// the freedom the rotation uses.
func TestPickLeastLoaded_ContractHolds(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	p := &h3Pool{}

	for trial := 0; trial < 3000; trial++ {
		n := rng.Intn(12)
		conns := make([]*h3ManagedConn, 0, n)
		for i := 0; i < n; i++ {
			conns = append(conns, &h3ManagedConn{
				// streamCap 0 forces the at-cap branch even for an idle conn,
				// which is the case a naive early return gets wrong.
				cl:        &pickFakeH3{alive: rng.Intn(6) != 0, goaway: rng.Intn(8) == 0},
				active:    rng.Intn(4),
				streamCap: rng.Intn(4),
			})
		}
		anyEligible, anyIdle, minActive := false, false, 1<<30
		for _, mc := range conns {
			if !h3Eligible(mc) {
				continue
			}
			anyEligible = true
			if mc.active == 0 {
				anyIdle = true
			}
			if mc.active < minActive {
				minActive = mc.active
			}
		}

		got := p.pickLeastLoaded(conns)

		if got == nil {
			require.Falsef(t, anyEligible,
				"trial %d: returned nil with an eligible connection present", trial)
			continue
		}
		require.Truef(t, h3Eligible(got), "trial %d: returned an ineligible connection", trial)
		if anyIdle {
			require.Zerof(t, got.active,
				"trial %d: an idle connection existed but a busy one (active=%d) was returned",
				trial, got.active)
		} else {
			require.Equalf(t, minActive, got.active,
				"trial %d: returned active=%d, want the minimum %d", trial, got.active, minActive)
		}
	}
}

// TestPickLeastLoaded_H2ContractHolds is the same property set for the HTTP/2
// pool, whose loop changed identically. conn.Conn's liveness flags are
// unexported, so every connection here is alive and the randomisation covers the
// load dimension; the H3 case covers liveness.
func TestPickLeastLoaded_H2ContractHolds(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	p := &Pool{}

	for trial := 0; trial < 3000; trial++ {
		n := rng.Intn(12)
		conns := make([]*managedConn, 0, n)
		for i := 0; i < n; i++ {
			conns = append(conns, &managedConn{
				C:         &conn.Conn{}, // hand-built: reads alive
				Active:    rng.Intn(4),
				StreamCap: rng.Intn(4),
			})
		}
		anyEligible, anyIdle, minActive := false, false, 1<<30
		for _, mc := range conns {
			if !h2Eligible(mc) {
				continue
			}
			anyEligible = true
			if mc.Active == 0 {
				anyIdle = true
			}
			if mc.Active < minActive {
				minActive = mc.Active
			}
		}

		got := p.PickLeastLoaded(conns)

		if got == nil {
			require.Falsef(t, anyEligible,
				"trial %d: returned nil with an eligible connection present", trial)
			continue
		}
		require.Truef(t, h2Eligible(got), "trial %d: returned an ineligible connection", trial)
		if anyIdle {
			require.Zerof(t, got.Active,
				"trial %d: an idle connection existed but a busy one was returned", trial)
		} else {
			require.Equalf(t, minActive, got.Active,
				"trial %d: returned Active=%d, want the minimum %d", trial, got.Active, minActive)
		}
	}
}

// TestPickLeastLoaded_RotatesAcrossIdleConns is the point of the cursor: with a
// pool of idle connections, consecutive picks must spread instead of returning
// the same one every time. Without rotation this test sees connection 0 four
// times; the old full scan did exactly that, because its strict comparison gave
// every tie to the earliest connection.
func TestPickLeastLoaded_RotatesAcrossIdleConns(t *testing.T) {
	p := &h3Pool{}
	conns := make([]*h3ManagedConn, 4)
	for i := range conns {
		conns[i] = &h3ManagedConn{cl: &pickFakeH3{alive: true}, streamCap: 8}
	}

	seen := map[*h3ManagedConn]int{}
	for i := 0; i < len(conns); i++ {
		got := p.pickLeastLoaded(conns)
		require.NotNilf(t, got, "pick %d returned nil", i)
		seen[got]++
	}

	assert.Lenf(t, seen, len(conns),
		"four picks over four idle connections touched %d of them, want all %d — "+
			"the cursor is not rotating", len(seen), len(conns))
}

// TestPickLeastLoaded_H2RotatesAcrossIdleConns is the same property for the
// HTTP/2 pool, which had no rotation test at all: pinning the cursor assignment
// in Pool.PickLeastLoaded left the whole client package green, while the same
// mutation in h3Pool.PickLeastLoaded failed the test above (#655).
//
// A frozen cursor still satisfies "return an idle connection when one exists",
// which is all TestPickLeastLoaded_H2ContractHolds asserts, so the contract test
// stays green while every request piles onto conns[0] and the rest of the pool
// goes cold — the whole-pool scan that #448 removed, minus its spread.
//
// Only the fixture differs from the H3 twin: h3ManagedConn holds a mockable
// h3Client, managedConn holds a concrete *conn.Conn whose liveness flags are
// unexported, so a hand-built one reads alive — the same trick the H2 contract
// test uses. What is asserted is what PickLeastLoaded's caller sees, the
// connections it hands back, not p.pickCursor: the cursor is how the spread is
// implemented, the spread is what is promised.
func TestPickLeastLoaded_H2RotatesAcrossIdleConns(t *testing.T) {
	p := &Pool{}
	conns := make([]*managedConn, 4)
	for i := range conns {
		conns[i] = &managedConn{C: &conn.Conn{}, StreamCap: 8}
	}

	seen := map[*managedConn]int{}
	for i := 0; i < len(conns); i++ {
		got := p.PickLeastLoaded(conns)
		require.NotNilf(t, got, "pick %d returned nil", i)
		seen[got]++
	}

	assert.Lenf(t, seen, len(conns),
		"four picks over four idle connections touched %d of them, want all %d — "+
			"the cursor is not rotating", len(seen), len(conns))
}

// TestPickLeastLoaded_CursorSurvivesAShrinkingPool pins the modulus: the cursor
// persists across calls while the slice length changes as connections are
// retired, so a stale cursor must not index out of range.
func TestPickLeastLoaded_CursorSurvivesAShrinkingPool(t *testing.T) {
	p := &h3Pool{}
	big := make([]*h3ManagedConn, 8)
	for i := range big {
		big[i] = &h3ManagedConn{cl: &pickFakeH3{alive: true}, streamCap: 8}
	}
	for i := 0; i < 8; i++ {
		p.pickLeastLoaded(big) // drive the cursor up
	}

	// The pool shrinks to one connection; the cursor is now larger than the slice.
	small := big[:1]
	gotSmall := p.pickLeastLoaded(small)
	gotEmpty := p.pickLeastLoaded(nil)

	require.Samef(t, small[0], gotSmall,
		"after the pool shrank, pick returned %p, want the only connection %p", gotSmall, small[0])
	assert.Truef(t, gotEmpty == nil, "pick over an empty pool returned %p, want nil", gotEmpty)
}
