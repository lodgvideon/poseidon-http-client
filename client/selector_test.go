package client

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundRobin_RotatesSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := RoundRobin()

	got := make([]string, 6)
	for i := range got {
		a, err := s.Pick(set, PickContext{})
		require.NoErrorf(t, err, "Pick %d err = %v", i, err)
		got[i] = a.Host
	}

	want := []string{"a", "b", "c", "a", "b", "c"}
	assert.Equalf(t, want, got,
		"Pick sequence = %v, want %v — round robin must visit every address and wrap, "+
			"not stick to one", got, want)
}

func TestRoundRobin_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()

	_, err := RoundRobin().Pick(nil, PickContext{})

	assert.ErrorIsf(t, err, ErrNoAddresses,
		"Pick(nil) err = %v, want ErrNoAddresses — a nil error here hands the caller a "+
			"zero Address it would then dial", err)
}

func TestRoundRobin_Concurrent_FairBalance(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}, {Host: "d"}}
	s := RoundRobin()
	const total = 4000
	var counts [4]atomic.Int32
	var pickErr atomic.Value
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total/8; i++ {
				a, err := s.Pick(set, PickContext{})
				if err != nil {
					pickErr.Store(err)
					return
				}
				switch a.Host {
				case "a":
					counts[0].Add(1)
				case "b":
					counts[1].Add(1)
				case "c":
					counts[2].Add(1)
				case "d":
					counts[3].Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Nil(t, pickErr.Load(), "a concurrent Pick failed: %v", pickErr.Load())
	for i := range counts {
		got := counts[i].Load()
		assert.Equalf(t, int32(total/4), got,
			"addr %d count = %d, want %d — round-robin must be EXACT under atomic.Add; an "+
				"approximate split means the counter is being read and written non-atomically",
			i, got, total/4)
	}
}

func TestRandom_PicksFromSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}}
	s := Random(rand.New(rand.NewSource(1)))

	got := make([]string, 100)
	for i := range got {
		a, err := s.Pick(set, PickContext{})
		require.NoErrorf(t, err, "Pick %d err = %v", i, err)
		got[i] = a.Host
	}

	for i, h := range got {
		assert.Containsf(t, []string{"a", "b"}, h,
			"Pick[%d] = %v, want a or b — a pick outside the supplied set would be dialled "+
				"as a live backend", i, h)
	}
}

func TestRandom_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()

	_, err := Random(nil).Pick(nil, PickContext{})

	assert.ErrorIsf(t, err, ErrNoAddresses, "Pick(nil) err = %v, want ErrNoAddresses", err)
}

func TestHash_Deterministic(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s, err := Hash(func(pc PickContext) string { return pc.Request.Path })
	require.NoError(t, err, "Hash with a non-nil key function")

	first, err1 := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})
	second, err2 := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})

	require.NoError(t, err1, "first Pick")
	require.NoError(t, err2, "second Pick")
	assert.Equalf(t, first.Host, second.Host,
		"Hash not deterministic: %v vs %v — session affinity depends on the same key always "+
			"landing on the same backend", first.Host, second.Host)
}

func TestHash_EmptyKey_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}}
	s, err := Hash(func(_ PickContext) string { return "" })
	require.NoError(t, err, "Hash with a non-nil key function")

	_, err = s.Pick(set, PickContext{})

	assert.ErrorIsf(t, err, ErrNoAddresses,
		"Pick err = %v, want ErrNoAddresses on an empty key — silently hashing the empty "+
			"string would pin every keyless request onto one backend", err)
}

func TestHash_NilKeyFn_ReturnsError(t *testing.T) {
	t.Parallel()

	_, err := Hash(nil)

	assert.ErrorIsf(t, err, ErrNilKeyFn,
		"Hash(nil) err = %v, want ErrNilKeyFn — a nil key function must be refused at "+
			"construction, not panic on the first Pick", err)
}
