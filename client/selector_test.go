package client

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRoundRobin_RotatesSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := RoundRobin()
	got := make([]string, 6)
	for i := 0; i < 6; i++ {
		a, err := s.Pick(set, PickContext{})
		if err != nil {
			t.Fatalf("Pick err = %v", err)
		}
		got[i] = a.Host
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Pick[%d] = %s, want %s (full = %v)", i, got[i], want[i], got)
		}
	}
}

func TestRoundRobin_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	if _, err := RoundRobin().Pick(nil, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick(nil) err = %v, want ErrNoAddresses", err)
	}
}

func TestRoundRobin_Concurrent_FairBalance(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}, {Host: "d"}}
	s := RoundRobin()
	const total = 4000
	var counts [4]atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total/8; i++ {
				a, err := s.Pick(set, PickContext{})
				if err != nil {
					t.Errorf("Pick err = %v", err)
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
	for i := range counts {
		got := counts[i].Load()
		if got != int32(total/4) {
			t.Errorf("addr %d count = %d, want %d (round-robin must be exact under atomic.Add)", i, got, total/4)
		}
	}
}

func TestRandom_PicksFromSet(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}}
	s := Random(rand.New(rand.NewSource(1)))
	for i := 0; i < 100; i++ {
		a, err := s.Pick(set, PickContext{})
		if err != nil {
			t.Fatalf("Pick err = %v", err)
		}
		if a.Host != "a" && a.Host != "b" {
			t.Errorf("Pick = %v, want a or b", a)
		}
	}
}

func TestRandom_EmptySet_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	if _, err := Random(nil).Pick(nil, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick(nil) err = %v, want ErrNoAddresses", err)
	}
}

func TestHash_Deterministic(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}, {Host: "b"}, {Host: "c"}}
	s := Hash(func(pc PickContext) string {
		return pc.Request.Path
	})
	first, _ := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})
	second, _ := s.Pick(set, PickContext{Request: &Request{Path: "/x"}})
	if first.Host != second.Host {
		t.Errorf("Hash not deterministic: %v vs %v", first.Host, second.Host)
	}
}

func TestHash_EmptyKey_ErrNoAddresses(t *testing.T) {
	t.Parallel()
	set := []Address{{Host: "a"}}
	s := Hash(func(_ PickContext) string { return "" })
	if _, err := s.Pick(set, PickContext{}); err != ErrNoAddresses {
		t.Errorf("Pick err = %v, want ErrNoAddresses on empty key", err)
	}
}

func TestHash_NilKeyFn_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("Hash(nil) did not panic")
		}
	}()
	Hash(nil)
}
