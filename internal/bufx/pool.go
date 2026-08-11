package bufx

import "sync"

// defaultReadBufSize is what the pool hands out when it has nothing recycled.
//
// The 9 is not padding. This pool's one production consumer is frame.NewFramer,
// which asks for a whole maximum-size HTTP/2 frame — 16384 bytes of payload plus
// the 9-byte frame header. A round 16 KiB is nine bytes short of that, so New
// could never satisfy the only caller it has: every cold Get allocated the
// pooled buffer, rejected it as too small, and allocated again. Two allocations
// per cold NewFramer, and the pool only started paying once PutReadBuf had
// recycled enough big-enough buffers from elsewhere.
//
// Sized to the real demand rather than to a round number. A different consumer
// asking for more still works — GetReadBuf allocates to fit — it just does not
// get the pool's help.
const defaultReadBufSize = 16<<10 + 9

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, defaultReadBufSize)
		return &b
	},
}

// GetReadBuf returns a pooled byte slice with cap >= min. The returned slice
// has length 0; caller is responsible for re-slicing as needed.
func GetReadBuf(min int) *[]byte {
	p := readBufPool.Get().(*[]byte)
	if cap(*p) < min {
		// Too small for this caller, but still a perfectly good buffer for the
		// next one that asks for less. Dropping it here — as this used to —
		// threw away a live allocation and left the pool no fuller than before,
		// so a run of oversized asks emptied it one buffer at a time.
		readBufPool.Put(p)
		newBuf := make([]byte, 0, min)
		return &newBuf
	}
	*p = (*p)[:0]
	return p
}

// PutReadBuf returns the slice to the pool. Caller must not retain references.
func PutReadBuf(p *[]byte) {
	if p == nil {
		return
	}
	readBufPool.Put(p)
}
