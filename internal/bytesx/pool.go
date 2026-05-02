package bytesx

import "sync"

const defaultReadBufSize = 16 << 10 // 16 KiB

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
		newBuf := make([]byte, 0, min)
		p = &newBuf
		return p
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
