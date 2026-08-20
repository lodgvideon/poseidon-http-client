package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// Two H3-pool rules the suite documents at length and never defends (#885,
// #887). Both are survivors across the whole H3 filter, and both fail in the
// direction that looks like health: the pool quietly stops dialling.
// ————————————————————————————————————————————————————————————————

// TestH3SpareStreamCapacity_ClassifiesEveryConnState is the decision table
// h3SpareStreamCapacity's own doc comment describes (#885).
//
// #885 reported the GoingAway term as unpinned, and it is — under the filter the
// issue used. Widened to the whole H3 surface, the existing
// TestH3Pool_DialForWaiters_ADrainedConnIsNotCoverage
// (pool_batch_redial_test.go) kills that mutation 2/2, so no new test is needed
// for the drained arm and none is written here.
//
// The other three arms of the same predicate were genuinely undefended: making
// the helper return 0 for every conn, counting a DEAD conn's slots, and ignoring
// mc.active so a conn at its cap still counts, all survive the whole H3 surface
// 2/2. This is the decision table over the four states an mc can be in as far as
// this predicate cares — usable, drained, dead, at cap — which is what the
// helper's caller subtracts from the queued waiter count.
func TestH3SpareStreamCapacity_ClassifiesEveryConnState(t *testing.T) {
	cases := []struct {
		name string
		mc   *h3ManagedConn
		want int
	}{
		{"usable conn contributes its idle slots",
			&h3ManagedConn{cl: &pickFakeH3{alive: true}, active: 1, streamCap: 10}, 9},
		{"drained conn contributes nothing though it is alive and idle",
			&h3ManagedConn{cl: &pickFakeH3{alive: true, goaway: true}, active: 1, streamCap: 10}, 0},
		{"dead conn contributes nothing",
			&h3ManagedConn{cl: &pickFakeH3{}, active: 1, streamCap: 10}, 0},
		{"conn at its stream cap contributes nothing",
			&h3ManagedConn{cl: &pickFakeH3{alive: true}, active: 10, streamCap: 10}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := h3SpareStreamCapacity([]*h3ManagedConn{tc.mc})

			assert.Equalf(t, tc.want, got,
				"spare capacity %d, want %d — this number is subtracted from the queued "+
					"waiter count in dialForWaiters, so capacity counted here that "+
					"pickLeastLoaded would then refuse to use is a dial that never happens",
				got, tc.want)
		})
	}
}

// TestH3Pool_DialForWaiters_UsableConnIsCoverage is the arm at the call site: a
// helper that returned 0 for everything makes the pool dial for waiters an
// existing connection can already serve, and nothing on the H3 surface noticed —
// every dialForWaiters test either has no conns at all or conns already at their
// cap, so the suppressing direction was never exercised.
func TestH3Pool_DialForWaiters_UsableConnIsCoverage(t *testing.T) {
	p := inertH3Pool(PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10}, nil)
	usable := &h3ManagedConn{cl: &pickFakeH3{alive: true}, active: 1, streamCap: 10}
	rs := &h3RunState{
		conns:   []*h3ManagedConn{usable},
		waiters: []h3AcquireReq{{ctx: context.Background(), reply: make(chan h3AcquireResp, 1)}},
	}

	p.dialForWaiters(rs)

	assert.Zerof(t, rs.inFlightDials,
		"the pool dialled %d extra connections for one waiter that the live conn's nine "+
			"idle stream slots already cover; over-dialling is how a load generator ends up "+
			"with a socket per request", rs.inFlightDials)
}

// TestH3PruneExpiredWaiters_RepliesToTheExpiredAndOnlyTheExpired pins both
// halves of the prune contract (#887).
//
// Making the function a no-op left 43 tests green. Two things break when it
// regresses, and the second is the one that bites: expired waiters accumulate
// so dialForWaiters' uncovered count is inflated by callers that are already
// gone, and — because the doc promises "each dropped waiter is still sent the
// one reply it is owed" — a waiter the actor later SERVES has the conn's active
// count incremented for a reply nobody reads, leaking a stream slot for the life
// of the connection.
func TestH3PruneExpiredWaiters_RepliesToTheExpiredAndOnlyTheExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // dead's ctx is already done before the prune runs
	dead := h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)}
	live := h3AcquireReq{ctx: context.Background(), reply: make(chan h3AcquireResp, 1)}

	out := h3PruneExpiredWaiters([]h3AcquireReq{live, dead})

	require.Lenf(t, out, 1,
		"prune kept %d of two waiters, want only the live one — an expired waiter left in "+
			"the queue inflates dialForWaiters' uncovered count, so the pool dials sockets "+
			"for callers that are already gone", len(out))
	assert.Truef(t, out[0].reply == live.reply, "prune dropped the LIVE waiter and kept the expired one")
	select {
	case resp := <-dead.reply:
		assert.ErrorIsf(t, resp.err, context.Canceled,
			"the dropped waiter was answered %v, want its own ctx error; its reclaim "+
				"goroutine is blocked on exactly one value", resp.err)
		assert.Truef(t, resp.mc == nil,
			"a dropped waiter was also handed a connection (%v), whose active count is now "+
				"charged for a reply nobody reads", resp.mc)
	default:
		require.FailNow(t, "the expired waiter was dropped without its reply",
			"its caller's reclaim goroutine waits for exactly one value and now waits forever")
	}
	select {
	case resp := <-live.reply:
		require.FailNowf(t, "a waiter whose ctx is still live was answered by prune",
			"got %+v; prune must drop only the expired, or a caller is refused while its "+
				"deadline still has room", resp)
	default:
	}
}

// TestH1PruneExpiredWaiters_RepliesToTheExpiredAndOnlyTheExpired is the h1 twin
// (#887 names it). The two pools carry the same function under two names, and a
// divergence between them is exactly the shape this repo keeps producing.
func TestH1PruneExpiredWaiters_RepliesToTheExpiredAndOnlyTheExpired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dead := h1SettleWaiter(ctx)
	live := h1SettleWaiter(context.Background())

	out := h1PruneExpiredWaiters([]h1AcquireReq{live, dead})

	require.Lenf(t, out, 1,
		"prune kept %d of two waiters, want only the live one", len(out))
	assert.Truef(t, out[0].reply == live.reply, "prune dropped the LIVE waiter and kept the expired one")
	resp, got := h1SettleAnswer(dead)
	require.Truef(t, got, "the expired waiter was dropped without the one reply it is owed")
	assert.ErrorIsf(t, resp.err, context.Canceled,
		"the dropped waiter was answered %v, want its own ctx error", resp.err)
	assert.Truef(t, resp.mc == nil, "a dropped waiter was also handed a connection: %v", resp.mc)
	_, gotLive := h1SettleAnswer(live)
	assert.Falsef(t, gotLive, "a waiter whose ctx is still live was answered by prune")
}
