package http3

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// The QPACK encoder and decoder instruction streams are ordered and stateful:
// each instruction mutates the peer's view of a dynamic table, so a byte dropped
// or reordered desynchronises that table for the rest of the connection. Every
// later field section then decodes against the wrong entries — wrong header
// values, or a decompression failure that kills the connection (RFC 9204 §2.2).
//
// The writer behind both streams therefore has three properties, and before
// these tests none of them was pinned: mutating any one of them left the whole
// http3 package green, verified twice.
//
//	partial write  -> the unsent tail is kept, in order, for the next flush
//	write error    -> the buffer is kept whole for a later retry
//	residue flag   -> set exactly when bytes remain, since it is what makes the
//	                  caller come back

func instrStreamPair(sendCap int) (*fakeStream, *atomic.Bool) {
	c := &fakeConn{}
	return &fakeStream{conn: c, sendCap: sendCap}, new(atomic.Bool)
}

// TestFlushInstrStream_PartialWriteKeepsTheTailInOrder is the QPACK ordering
// guard. A stream that accepts three bytes at a time must receive the whole
// instruction sequence, unbroken and in order, across the flushes it takes.
func TestFlushInstrStream_PartialWriteKeepsTheTailInOrder(t *testing.T) {
	st, residue := instrStreamPair(3)
	buf := []byte("abcdefgh")

	buf = flushInstrStream(st, buf, residue)
	afterFirstFlush, flaggedAfterFirstFlush := string(buf), residue.Load()
	buf = flushInstrStream(st, buf, residue)
	buf = flushInstrStream(st, buf, residue)

	assert.Equalf(t, "defgh", afterFirstFlush,
		"residue after a 3-byte write = %q, want %q: the unsent tail was "+
			"dropped, so the peer's dynamic table loses those instructions",
		afterFirstFlush, "defgh")
	assert.True(t, flaggedAfterFirstFlush,
		"residue flag false with 5 bytes still pending: nothing will re-flush them")
	assert.Equalf(t, "abcdefgh", string(st.sent),
		"stream received %q, want %q: QPACK instructions are ordered, and "+
			"this is the order the peer applies them in", string(st.sent), "abcdefgh")
	assert.Emptyf(t, buf, "buffer = %q after draining, want empty", buf)
	assert.False(t, residue.Load(),
		"residue flag still set with nothing pending: the caller will keep re-flushing")
}

// TestFlushInstrStream_WriteErrorKeepsTheBuffer pins that a failed write does not
// consume the instructions. They are the only copy: dropping them on an error
// would leave the peer's table permanently behind ours even if the stream
// recovers.
func TestFlushInstrStream_WriteErrorKeepsTheBuffer(t *testing.T) {
	st, residue := instrStreamPair(0)
	st.sendResetErr = true // models a received STOP_SENDING

	buf := flushInstrStream(st, []byte("abc"), residue)

	assert.Equalf(t, "abc", string(buf),
		"buffer = %q after a failed write, want %q kept for a retry", string(buf), "abc")
	assert.Emptyf(t, st.sent, "stream recorded %q despite the error", st.sent)
}

// TestFlushInstrStream_NilStreamAndEmptyBufferAreNoOps covers the two early
// returns: before the uni streams are opened the writer is nil, and the flush is
// called speculatively on paths that may have nothing queued.
func TestFlushInstrStream_NilStreamAndEmptyBufferAreNoOps(t *testing.T) {
	var nilStreamResidue atomic.Bool
	nilStreamResidue.Store(true)
	st, emptyBufResidue := instrStreamPair(0)

	nilStreamBuf := flushInstrStream(nil, []byte("abc"), &nilStreamResidue)
	emptyBuf := flushInstrStream(st, nil, emptyBufResidue)

	assert.Equalf(t, "abc", string(nilStreamBuf),
		"nil stream: buffer = %q, want it untouched", nilStreamBuf)
	assert.True(t, nilStreamResidue.Load(),
		"nil stream cleared the residue flag, hiding bytes that were never sent")
	assert.Emptyf(t, emptyBuf, "empty buffer: got %q", emptyBuf)
	assert.Emptyf(t, st.sent, "empty buffer still wrote %q to the stream", st.sent)
}

var _ = quic.ErrStreamReset // the error fakeStream returns for sendResetErr
