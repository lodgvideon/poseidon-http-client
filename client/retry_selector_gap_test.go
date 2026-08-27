package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// Three one-sided properties: a jitter nothing distinguishes from a constant,
// a five-member hard-stop list of which one member is pinned, and two
// load-balancing selectors whose tests a constant pick satisfies (#876, #877,
// #878). All three fail toward "send everything to one place at one instant",
// which is the shape a load generator can least afford.
// ————————————————————————————————————————————————————————————————

// TestDefaultBackoff_JitterSpreadsAroundTheNominalDelay pins the jitter itself
// (#876).
//
// defaultBackoff documents "truncated exponential backoff with ±25% uniform
// jitter", and the jitter is the thundering-herd defence: without it every
// client that took the same failure retries at exactly the same instant.
// TestDefaultBackoff_Bounds only checks the band [75ms,125ms], and the
// unjittered value is exactly 100ms — comfortably inside it — so replacing the
// whole jitter expression with `return d` survives.
//
// Two properties a constant cannot satisfy: more than one distinct value, and
// values on BOTH sides of the nominal delay. The second matters on its own — a
// jitter that only ever subtracts would still spread the herd but would also
// quietly shorten every backoff.
func TestDefaultBackoff_JitterSpreadsAroundTheNominalDelay(t *testing.T) {
	t.Parallel()
	const nominal = 100 * time.Millisecond // attempt=1: base << 0
	rng := rand.New(rand.NewSource(42))    // seeded: the run is deterministic

	distinct := map[time.Duration]int{}
	below, above := 0, 0
	for range 500 {
		d := defaultBackoff(1, rng)
		distinct[d]++
		switch {
		case d < nominal:
			below++
		case d > nominal:
			above++
		}
	}

	assert.Greaterf(t, len(distinct), 1,
		"500 samples of defaultBackoff(1) produced %d distinct value(s); a constant delay "+
			"means every client that took the same failure retries at the same instant, "+
			"which is the herd the jitter exists to break up", len(distinct))
	assert.Positivef(t, below,
		"no sample fell below the nominal %v — jitter that only ever adds is not the "+
			"±25%% the doc comment promises", nominal)
	assert.Positivef(t, above,
		"no sample rose above the nominal %v — jitter that only ever subtracts silently "+
			"shortens every backoff and makes the truncation cap a lie", nominal)
}

// TestRetryer_Do_HardStopsOutrankAnOverEagerPredicate is the decision table
// isHardStop deserves (#877).
//
// Five errors must never be retried however loudly a caller's IsRetryable says
// otherwise. Only ErrPoolClosed was pinned against such a predicate:
// dropping ErrInvalidRequest or context.DeadlineExceeded from the list survives
// the whole retry filter 2/2, and context.Canceled is only reached indirectly
// through sleepBackoff returning ctx.Err() — a different mechanism from the
// classification under test.
//
// Each row scripts a SECOND success the loop must never reach, so the assertion
// is a call count rather than the returned error: an error can arrive by the
// hard stop or by exhaustion, and only the count tells them apart.
func TestRetryer_Do_HardStopsOutrankAnOverEagerPredicate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"ErrPoolClosed", ErrPoolClosed},
		{"ErrClosed", ErrClosed},
		{"ErrInvalidRequest", ErrInvalidRequest},
		{"wrapped ErrInvalidRequest", fmt.Errorf("building headers: %w", ErrInvalidRequest)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeDoer{t: t, results: []doResult{
				{nil, c.err},
				{&Response{Status: 200}, nil}, // must never be reached
			}}
			r := newFakeRetryer(f, RetryOptions{
				MaxAttempts: 3,
				IsRetryable: func(error, *Response) bool { return true },
			})
			var res Response

			err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

			assert.Equalf(t, 1, f.calls,
				"%v was attempted %d times with an always-yes IsRetryable, want 1 — a hard "+
					"stop must outrank the caller's predicate, or a cancelled context and a "+
					"malformed request both spin the loop to exhaustion", c.err, f.calls)
			assert.ErrorIsf(t, err, c.err,
				"err = %v, want the hard-stop error itself so a caller can classify it", err)
		})
	}
}

// TestRetryer_Do_ANonHardStopStillHonoursThePredicate is the control arm: the
// table above is satisfied by a Retryer that never retries anything. This is the
// same fixture with an error that is NOT on the hard-stop list, where the
// always-yes predicate must be obeyed.
func TestRetryer_Do_ANonHardStopStillHonoursThePredicate(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, errors.New("some transport hiccup")},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		// Always yes for an ERROR, never for a response: doLoop also consults the
		// predicate on success, and an unconditional yes would replay the 200 too.
		IsRetryable: func(err error, _ *Response) bool { return err != nil },
	})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "the second attempt succeeded, so Do must report success")
	assert.Equalf(t, 2, f.calls,
		"calls = %d, want 2 — an error outside the hard-stop list must still be retried "+
			"when the caller's predicate says so, or isHardStop has quietly become "+
			"\"never retry anything\"", f.calls)
}

// TestRandom_SpreadsAcrossTheSet is the direction TestRandom_PicksFromSet leaves
// out (#878).
//
// "Every pick is a member of the set" is satisfied perfectly by a selector that
// returns set[0] every time — and replacing rng.Intn(len(set)) with 0 survives
// the selector suite 2/2. This is a load balancer: a constant pick sends the
// whole load to one backend, which is not a cosmetic defect.
func TestRandom_SpreadsAcrossTheSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := Random(rand.New(rand.NewSource(1))) // seeded: deterministic run

	seen := map[string]int{}
	for i := range 200 {
		a, err := s.Pick(set, PickContext{})
		require.NoErrorf(t, err, "Pick %d", i)
		seen[a.Host]++
	}

	assert.Lenf(t, seen, len(set),
		"200 random picks over three addresses touched %d of them (%v), want all 3 — a "+
			"selector that always answers the same address sends the whole load to one "+
			"backend while reporting itself as random", len(seen), seen)
}

// TestHash_DifferentKeysReachDifferentAddresses is the other half of the hash
// contract (#878).
//
// TestHash_Deterministic asserts that the SAME key agrees with itself, which
// set[0]-for-everything satisfies perfectly. Determinism is only half of
// consistent hashing; the other half is that the key actually selects.
func TestHash_DifferentKeysReachDifferentAddresses(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	var key string
	s, err := Hash(func(PickContext) string { return key })
	require.NoError(t, err, "Hash with a non-nil key function")

	seen := map[string]int{}
	for i := range 200 {
		key = fmt.Sprintf("/key/%d", i)
		a, perr := s.Pick(set, PickContext{})
		require.NoErrorf(t, perr, "Pick %d", i)
		seen[a.Host]++
	}

	assert.Lenf(t, seen, len(set),
		"200 distinct keys landed on %d of three addresses (%v), want all 3 — a hash "+
			"selector whose output does not depend on the key is deterministic and useless: "+
			"every session pins to one backend", len(seen), seen)
}
