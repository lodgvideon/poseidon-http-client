package http3

import (
	"sync/atomic"
	"testing"

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
	if got, want := string(buf), "defgh"; got != want {
		t.Fatalf("residue after a 3-byte write = %q, want %q: the unsent tail was "+
			"dropped, so the peer's dynamic table loses those instructions", got, want)
	}
	if !residue.Load() {
		t.Error("residue flag false with 5 bytes still pending: nothing will re-flush them")
	}

	buf = flushInstrStream(st, buf, residue)
	buf = flushInstrStream(st, buf, residue)

	if got, want := string(st.sent), "abcdefgh"; got != want {
		t.Fatalf("stream received %q, want %q: QPACK instructions are ordered, and "+
			"this is the order the peer applies them in", got, want)
	}
	if len(buf) != 0 {
		t.Errorf("buffer = %q after draining, want empty", buf)
	}
	if residue.Load() {
		t.Error("residue flag still set with nothing pending: the caller will keep re-flushing")
	}
}

// TestFlushInstrStream_WriteErrorKeepsTheBuffer pins that a failed write does not
// consume the instructions. They are the only copy: dropping them on an error
// would leave the peer's table permanently behind ours even if the stream
// recovers.
func TestFlushInstrStream_WriteErrorKeepsTheBuffer(t *testing.T) {
	st, residue := instrStreamPair(0)
	st.sendResetErr = true // models a received STOP_SENDING

	buf := flushInstrStream(st, []byte("abc"), residue)
	if got, want := string(buf), "abc"; got != want {
		t.Fatalf("buffer = %q after a failed write, want %q kept for a retry", got, want)
	}
	if len(st.sent) != 0 {
		t.Errorf("stream recorded %q despite the error", st.sent)
	}
}

// TestFlushInstrStream_NilStreamAndEmptyBufferAreNoOps covers the two early
// returns: before the uni streams are opened the writer is nil, and the flush is
// called speculatively on paths that may have nothing queued.
func TestFlushInstrStream_NilStreamAndEmptyBufferAreNoOps(t *testing.T) {
	var residue atomic.Bool
	residue.Store(true)

	if got := flushInstrStream(nil, []byte("abc"), &residue); string(got) != "abc" {
		t.Errorf("nil stream: buffer = %q, want it untouched", got)
	}
	if !residue.Load() {
		t.Error("nil stream cleared the residue flag, hiding bytes that were never sent")
	}

	st, r2 := instrStreamPair(0)
	if got := flushInstrStream(st, nil, r2); len(got) != 0 {
		t.Errorf("empty buffer: got %q", got)
	}
	if len(st.sent) != 0 {
		t.Errorf("empty buffer still wrote %q to the stream", st.sent)
	}
}

var _ = quic.ErrStreamReset // the error fakeStream returns for sendResetErr
