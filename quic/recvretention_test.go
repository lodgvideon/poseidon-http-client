package quic

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retentionChunk is the STREAM frame size the retention tests deliver. It divides
// DefaultStreamRecvWindow evenly, so a no-consume peer fills the window exactly.
const retentionChunk = 16 << 10

// patternAt returns n bytes of a position-dependent pattern starting at absolute
// stream offset off, so a reassembly bug that misplaces bytes is caught rather
// than hidden by uniform zero-filled payloads.
func patternAt(off uint64, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((off + uint64(i)) * 31 % 251)
	}
	return b
}

// newRecvFCConn builds a Conn with receive flow control ENABLED (connRecvMax != 0
// is the live gate; 0 is the disabled sentinel checked in OnStream and
// onStreamConsumed) plus one open bidirectional stream and the real peer-side
// frame handler. Every retention test drives bytes in through
// connFrameHandler.OnStream — the actual entry point a server's packets reach —
// and out through Stream.Recv, never through recvStream internals.
func newRecvFCConn(t *testing.T) (*Conn, *Stream, *connFrameHandler) {
	t.Helper()
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream every retention test drives bytes through")
	return c, s, &connFrameHandler{c: c}
}

// TestConn_RecvFlowControl_NoConsumeStopsAtWindow is the CONTROL for the
// retention tests: a caller that never consumes must be stopped by receive flow
// control at exactly DefaultStreamRecvWindow (RFC 9000 §4.1). If this fails, flow
// control is not actually enforced in the harness (connRecvMax == 0 disables it)
// and every retention number measured alongside it is meaningless.
func TestConn_RecvFlowControl_NoConsumeStopsAtWindow(t *testing.T) {
	_, s, h := newRecvFCConn(t)

	var accepted uint64
	var stopErr error
	for {
		stopErr = h.OnStream(s.ID(), accepted, false, patternAt(accepted, retentionChunk))
		if stopErr != nil {
			break
		}
		accepted += retentionChunk
		if accepted > 4*DefaultStreamRecvWindow {
			break
		}
	}

	require.ErrorIsf(t, stopErr, ErrFlowControl,
		"flow control never rejected a non-consuming peer (accepted %d bytes, last err %v) — "+
			"FC is disabled in this harness and retention numbers here are worthless",
		accepted, stopErr)
	assert.Equalf(t, DefaultStreamRecvWindow, accepted,
		"flow control stopped a non-consuming peer at %d bytes, want exactly %d",
		accepted, DefaultStreamRecvWindow)
	// The window bounds IN-FLIGHT bytes, which is what retention should track.
	assert.EqualValuesf(t, DefaultStreamRecvWindow, len(s.recv.data),
		"buffered = %d, want %d", len(s.recv.data), DefaultStreamRecvWindow)
}

// TestConn_RecvStream_RetentionBoundedByWindow pins the leak: recvStream.read
// advanced a cursor but never released consumed bytes, so a streaming caller —
// one that consumes after every frame, exactly what DoStream exists to allow —
// still retained every byte the stream ever carried. Retention was linear in
// total bytes received (1:1 with what the peer sent) rather than bounded by the
// receive window. Flow control does not save it: recvflow.go slides the window
// forward as the app consumes, so consuming to free window is precisely what grew
// the buffer.
//
// No testify inside the ReadMemStats window: it reflects and allocates, and the
// measurement is of this process's live heap.
func TestConn_RecvStream_RetentionBoundedByWindow(t *testing.T) {
	_, s, h := newRecvFCConn(t)
	const total = 8 << 20 // 8 MiB, 32x the 256 KiB stream window

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	var off uint64
	var deliverErr error
	mismatchAt := int64(-1)
	for off < total {
		want := patternAt(off, retentionChunk)
		if deliverErr = h.OnStream(s.ID(), off, false, want); deliverErr != nil {
			break
		}
		if got := s.Recv(); !bytes.Equal(got, want) { // consume immediately, like a streaming caller
			mismatchAt = int64(off)
			break
		}
		off += retentionChunk
	}
	runtime.GC()
	runtime.ReadMemStats(&m1)
	heapDelta := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)

	require.NoErrorf(t, deliverErr, "OnStream at %d: %v", off, deliverErr)
	require.EqualValuesf(t, -1, mismatchAt, "Recv at offset %d returned wrong bytes", mismatchAt)
	t.Logf("received=%d consumed=%d | len(r.data)=%d cap(r.data)=%d | live heap delta=%d bytes | window=%d",
		total, off, len(s.recv.data), cap(s.recv.data), heapDelta, DefaultStreamRecvWindow)
	// The buffer must hold only what has not been consumed — nothing, here.
	assert.Emptyf(t, s.recv.data,
		"len(r.data) = %d after consuming everything, want 0", len(s.recv.data))
	// Allow generous slack for append's growth headroom, but nothing that scales
	// with total bytes received.
	limit := 4 * int(DefaultStreamRecvWindow)
	assert.LessOrEqualf(t, cap(s.recv.data), limit,
		"cap(r.data) = %d after receiving+consuming %d bytes, want <= %d (O(window), not O(total))",
		cap(s.recv.data), total, limit)
	assert.LessOrEqualf(t, heapDelta, int64(limit),
		"live heap retention = %d bytes after consuming %d, want <= %d", heapDelta, total, limit)
}

// TestConn_RecvStream_RetentionBoundedWithGaps drives the same retention bound
// through the out-of-order path, where an offset-base bug bites: bufferGap merges
// chunks by ABSOLUTE offset while the contiguous prefix is indexed relative to a
// trimmed buffer. Each round delivers its two frames tail-first so the tail is
// buffered as a gap and only the head makes it contiguous, and every consumed byte
// is checked against the pattern — a silently misplaced byte here is worse than the
// leak.
func TestConn_RecvStream_RetentionBoundedWithGaps(t *testing.T) {
	_, s, h := newRecvFCConn(t)
	const rounds = 128
	const half = retentionChunk / 2

	var off, consumed uint64
	var got []byte
	for i := 0; i < rounds; i++ {
		head, tail := off, off+half
		// Tail first: it lands in pending as a gap. Head second: it makes the prefix
		// contiguous and absorb folds the tail back in.
		require.NoErrorf(t, h.OnStream(s.ID(), tail, false, patternAt(tail, half)),
			"OnStream tail at %d", tail)
		require.NoErrorf(t, h.OnStream(s.ID(), head, false, patternAt(head, half)),
			"OnStream head at %d", head)
		// A duplicate of the tail, and an overlap spanning both halves, must not grow
		// anything (recvHighest does not advance, so FC never rejects them).
		require.NoErrorf(t, h.OnStream(s.ID(), tail, false, patternAt(tail, half)),
			"OnStream dup tail at %d", tail)
		require.NoErrorf(t, h.OnStream(s.ID(), head+4, false, patternAt(head+4, half)),
			"OnStream overlap at %d", head+4)
		got = append(got[:0], s.Recv()...)
		require.Truef(t, bytes.Equal(got, patternAt(consumed, len(got))),
			"round %d: Recv from offset %d returned wrong bytes", i, consumed)
		consumed += uint64(len(got))
		off += retentionChunk
	}

	require.Equalf(t, off, consumed, "consumed %d of %d delivered bytes", consumed, off)
	t.Logf("gapped: received=%d consumed=%d | len=%d cap=%d pending=%d",
		off, consumed, len(s.recv.data), cap(s.recv.data), len(s.recv.pending))
	limit := 4 * int(DefaultStreamRecvWindow)
	assert.LessOrEqualf(t, cap(s.recv.data), limit,
		"cap(r.data) = %d after %d gapped bytes, want <= %d", cap(s.recv.data), off, limit)
	assert.Emptyf(t, s.recv.pending,
		"pending = %d chunks, want 0 (all gaps filled)", len(s.recv.pending))
}

// TestConn_RecvStream_GapFilledAfterConsumption is the sharpest offset-base test:
// consumption advances the read cursor past earlier data, THEN a gap below the
// highest received offset is filled. Every absolute-offset computation in insert /
// absorb / complete must be measured from the trimmed buffer's base, not from
// len(r.data). Getting this wrong silently returns the wrong bytes.
func TestConn_RecvStream_GapFilledAfterConsumption(t *testing.T) {
	_, s, h := newRecvFCConn(t)

	// 1. Deliver and fully consume [0, 4096) so the read cursor is well past 0.
	require.NoError(t, h.OnStream(s.ID(), 0, false, patternAt(0, 4096)), "deliver [0,4096)")
	firstRecv := s.Recv()
	// 2. A frame at 8192 arrives with [4096, 8192) still missing: it must buffer as
	//    a gap and stay unreadable.
	require.NoError(t, h.OnStream(s.ID(), 8192, false, patternAt(8192, 4096)), "deliver [8192,12288)")
	gappedRecv := s.Recv()
	// 3. Fill the gap. Both the filler and the buffered chunk beyond it must now
	//    surface, in order, with the right bytes — measured from the base, not from
	//    a buffer whose consumed prefix was trimmed away.
	// No FIN here: this frame ends at 8192, below the 12288 already received, and a
	// FIN below data already received is a final-size error (RFC 9000 §4.5).
	require.NoError(t, h.OnStream(s.ID(), 4096, false, patternAt(4096, 4096)), "fill the gap [4096,8192)")
	filledRecv := s.Recv()

	require.Truef(t, bytes.Equal(firstRecv, patternAt(0, 4096)), "first Recv returned wrong bytes")
	require.Emptyf(t, gappedRecv, "gapped data must not be readable, got %d bytes", len(gappedRecv))
	require.Truef(t, bytes.Equal(filledRecv, patternAt(4096, 8192)),
		"after gap fill got %d bytes, want %d matching the pattern from 4096",
		len(filledRecv), len(patternAt(4096, 8192)))
	assert.Emptyf(t, s.recv.data,
		"len(r.data) = %d after consuming everything, want 0", len(s.recv.data))
	assert.Emptyf(t, s.recv.pending, "pending = %d, want 0", len(s.recv.pending))
}

// TestConn_RecvStream_CompleteAfterConsumption checks that FIN/final-size
// accounting still works once the consumed prefix is released: complete() compared
// len(r.data) against finalSize, which only held while nothing was ever trimmed.
func TestConn_RecvStream_CompleteAfterConsumption(t *testing.T) {
	_, s, h := newRecvFCConn(t)

	require.NoError(t, h.OnStream(s.ID(), 0, false, patternAt(0, 2048)), "deliver [0,2048)")
	prefix := s.Recv()
	completeBeforeFin := s.recv.complete()
	require.NoError(t, h.OnStream(s.ID(), 2048, true, patternAt(2048, 1024)), "deliver the FIN frame")
	completeAtFin := s.recv.complete()
	tail := s.Recv()

	require.Lenf(t, prefix, 2048, "Recv = %d bytes, want 2048", len(prefix))
	assert.False(t, completeBeforeFin, "must not be complete before the FIN")
	assert.True(t, completeAtFin,
		"must be complete once the FIN arrives, even though the prefix was consumed")
	assert.True(t, bytes.Equal(tail, patternAt(2048, 1024)), "final Recv returned wrong bytes")
	assert.True(t, s.recv.complete(), "must stay complete after the tail is consumed")
	assert.True(t, s.Finished(), "Finished must report a complete receive side")
}

// TestConnFrameHandler_OnCrypto_BufferCapTracksConsumed checks that the CRYPTO
// buffer cap (RFC 9000 §7.5) stays measured from the CONSUMED prefix once read
// releases bytes, which is what its doc promises: "maxCryptoBuffer past the read
// cursor". cryptoRecv shares recvStream with the request streams but has no
// flow-control window, so this cap is its only bound — and the existing
// TestConnFrameHandler_OnCrypto_BufferCap never consumes, leaving the cap's
// dependence on the read cursor untested. A handshake flight longer than
// maxCryptoBuffer, fed to TLS incrementally as it arrives (conn_recv.go drives
// cryptoRecv.read every packet), must keep being accepted.
func TestConnFrameHandler_OnCrypto_BufferCapTracksConsumed(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c, space: spaceInitial}
	cr := &c.cryptoRecv[spaceInitial]
	const step = 8 << 10

	// Deliver and consume a full maxCryptoBuffer in order, as conn_recv.go does
	// when it hands each contiguous run to TLS.
	var off uint64
	for off < maxCryptoBuffer {
		require.NoErrorf(t, h.OnCrypto(off, patternAt(off, step)), "OnCrypto at %d", off)
		require.Truef(t, bytes.Equal(cr.read(), patternAt(off, step)),
			"crypto read at %d returned wrong bytes", off)
		off += step
	}
	// The cap is relative to the consumed prefix, so the very next byte — far past
	// maxCryptoBuffer in absolute terms — must still be accepted; while one reaching
	// more than maxCryptoBuffer PAST that prefix is still refused, so the bound is
	// preserved and not merely widened.
	consumedPrefix := cr.base
	pastAbsolute := h.OnCrypto(off, []byte("x"))
	pastCursor := h.OnCrypto(cr.base+maxCryptoBuffer, []byte("x"))

	require.Equalf(t, maxCryptoBuffer, consumedPrefix,
		"consumed prefix = %d, want %d", consumedPrefix, maxCryptoBuffer)
	assert.NoErrorf(t, pastAbsolute, "in-order CRYPTO past maxCryptoBuffer absolute = %v, want nil "+
		"(the cap must track the consumed prefix, not offset 0)", pastAbsolute)
	assert.ErrorIsf(t, pastCursor, ErrCryptoBufferExceeded,
		"CRYPTO past the consumed prefix + cap = %v, want ErrCryptoBufferExceeded", pastCursor)
}

// TestConn_RecvStream_RecvAliasingSurvivesNextFrames pins Stream.Recv's documented
// aliasing contract (quic/stream.go: "The result aliases the stream's buffer and
// should be consumed before the next Recv"). Releasing the consumed prefix must
// re-slice past the returned bytes, never truncate the buffer back over them: a
// [:0] reset would let the next STREAM frame's append overwrite bytes the caller
// still holds.
func TestConn_RecvStream_RecvAliasingSurvivesNextFrames(t *testing.T) {
	_, s, h := newRecvFCConn(t)
	require.NoError(t, h.OnStream(s.ID(), 0, false, patternAt(0, 1024)), "deliver [0,1024)")
	held := s.Recv() // the caller holds this slice, unconsumed
	require.Lenf(t, held, 1024, "Recv = %d bytes, want 1024", len(held))

	// More frames arrive and are absorbed before the caller looks at held.
	for i := 0; i < 8; i++ {
		off := uint64(1024 + i*1024)
		require.NoErrorf(t, h.OnStream(s.ID(), off, false, patternAt(off, 1024)),
			"OnStream at %d", off)
	}

	assert.True(t, bytes.Equal(held, patternAt(0, 1024)),
		"bytes returned by Recv were clobbered by later STREAM frames — "+
			"the consumed prefix must be released by re-slicing past them, not truncating over them")
}
