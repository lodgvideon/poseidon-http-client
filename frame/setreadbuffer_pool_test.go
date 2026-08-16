package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NewFramer draws its read buffer from a shared pool and remembers the handle;
// Close writes whatever readBuf currently is back through that handle and returns
// it to the pool. SetReadBuffer replaced readBuf and left the handle alone, so
// after it the two no longer described the same buffer (#515).
//
// The consequence is not the leak the issue leads with. The pooled buffer being
// dropped costs one allocation later; donating the CALLER's buffer to a shared
// pool hands a second owner a slice the caller is still writing into, and nothing
// anywhere reports it.

// sameArray reports whether two slices share a backing array.
func sameArray(a, b []byte) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	return &a[:1][0] == &b[:1][0]
}

// TestSetReadBuffer_DoesNotDonateTheCallersBufferToThePool is the gate.
//
// It inspects the pool handle rather than drawing from the pool afterwards: a
// sync.Pool may drop or reorder what it holds, so "did my buffer come back out"
// is not a decidable question. What the handle points at after Close is.
func TestSetReadBuffer_DoesNotDonateTheCallersBufferToThePool(t *testing.T) {
	fr := NewFramer(nil, nil)
	handle := fr.readBufPtr
	require.NotNil(t, handle,
		"NewFramer did not take a pooled buffer; the test cannot observe the donation")
	pooled := *handle
	// GetReadBuf hands back a slice of LENGTH ZERO with capacity to spare, so
	// sizing the caller's buffer from len(pooled) makes it cap-0 — and sameArray
	// then answers false whatever happened, which is how the first version of
	// this test passed against the unfixed code.
	require.NotZero(t, cap(pooled),
		"the pooled buffer has no capacity; nothing to distinguish it from the caller's")
	mine := make([]byte, 0, cap(pooled))
	require.False(t, sameArray(mine, pooled),
		"the caller's buffer and the pooled one are the same array; the test proves nothing")

	fr.SetReadBuffer(mine)
	fr.Close()

	assert.False(t, sameArray(*handle, mine),
		"after SetReadBuffer, Close wrote the CALLER's buffer into the pooled "+
			"handle and returned it to the shared pool — the next Framer to draw from "+
			"that pool shares an array with a caller that still owns it")
}

// TestSetReadBuffer_TheFramerActuallyUsesIt is the control: not donating the
// caller's buffer must not mean ignoring it. Without this, a SetReadBuffer that
// did nothing at all would pass the gate above.
func TestSetReadBuffer_TheFramerActuallyUsesIt(t *testing.T) {
	fr := NewFramer(nil, nil)
	mine := make([]byte, 8192)

	fr.SetReadBuffer(mine)

	assert.True(t, sameArray(fr.readBuf, mine), "SetReadBuffer did not install the caller's buffer")
}

// TestClose_StillReturnsThePooledBufferWhenUntouched is the other control: the
// ordinary path — no SetReadBuffer — must still return its buffer to the pool,
// which is the whole reason Close exists.
func TestClose_StillReturnsThePooledBufferWhenUntouched(t *testing.T) {
	fr := NewFramer(nil, nil)
	handle := fr.readBufPtr
	original := fr.readBuf

	fr.Close()

	assert.Nil(t, fr.readBufPtr, "Close did not release the pool handle")
	assert.True(t, sameArray(*handle, original),
		"Close did not write the framer's own buffer back through the handle")
}
