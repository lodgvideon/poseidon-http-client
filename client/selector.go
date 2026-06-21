package client

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Selector picks one address from a candidate set for the next dial.
// Implementations must be goroutine-safe.
type Selector interface {
	Pick(set []Address, pc PickContext) (Address, error)
}

// PickContext carries optional hints to the Selector. All fields are
// optional; the zero value is valid.
type PickContext struct {
	// Request is the in-flight request if Pick is called from the
	// Acquire path. May be nil.
	Request *Request
}

// roundRobin rotates through the set via an atomic counter.
type roundRobin struct {
	c atomic.Uint64
}

// RoundRobin returns a stateful Selector that rotates through the
// candidate set in order. The counter is shared across all calls;
// concurrent Pick is exact (atomic.Add).
func RoundRobin() Selector { return &roundRobin{} }

func (r *roundRobin) Pick(set []Address, _ PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	idx := r.c.Add(1) - 1
	return set[int(idx%uint64(len(set)))], nil
}

// randomSel picks uniformly at random. The supplied *rand.Rand (or
// the default time-seeded one) is serialized via mu — math/rand.Rand
// is not goroutine-safe.
type randomSel struct {
	rng *rand.Rand
	mu  sync.Mutex
}

// Random returns a Selector that picks uniformly at random.
// nil rng → a time-seeded *rand.Rand owned by the Selector.
func Random(rng *rand.Rand) Selector {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &randomSel{rng: rng}
}

func (r *randomSel) Pick(set []Address, _ PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	r.mu.Lock()
	idx := r.rng.Intn(len(set))
	r.mu.Unlock()
	return set[idx], nil
}

// hashSel picks deterministically by hash(keyFn(pc)).
type hashSel struct {
	keyFn func(PickContext) string
	hash  hash.Hash64
	mu    sync.Mutex
}

// Hash returns a Selector that picks by FNV-1a hash of keyFn(pc) %
// len(set). keyFn returning "" returns ErrNoAddresses (caller hint
// insufficient for deterministic selection). A nil keyFn returns an
// error; library callers should check it.
func Hash(keyFn func(PickContext) string) (Selector, error) {
	if keyFn == nil {
		return nil, ErrNilKeyFn
	}
	return &hashSel{keyFn: keyFn, hash: fnv.New64a()}, nil
}

func (h *hashSel) Pick(set []Address, pc PickContext) (Address, error) {
	if len(set) == 0 {
		return Address{}, ErrNoAddresses
	}
	k := h.keyFn(pc)
	if k == "" {
		return Address{}, ErrNoAddresses
	}
	h.mu.Lock()
	h.hash.Reset()
	_, _ = h.hash.Write([]byte(k))
	idx := int(h.hash.Sum64() % uint64(len(set)))
	h.mu.Unlock()
	return set[idx], nil
}
