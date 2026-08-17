package quic

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// feed applies a sequence of (offset, data, fin) frames to a fresh recvStream
// and returns it.
func feed(frames ...streamFrameInput) *recvStream {
	r := &recvStream{}
	for _, f := range frames {
		_ = r.receive(f.offset, f.data, f.fin)
	}
	return r
}

type streamFrameInput struct {
	offset uint64
	data   []byte
	fin    bool
}

func TestRecvStream_InOrder(t *testing.T) {
	frames := []streamFrameInput{
		{0, []byte("hello "), false},
		{6, []byte("world"), true},
	}

	r := feed(frames...)

	assert.Equal(t, "hello world", string(r.bytes()),
		"in-order frames must reassemble to the sent byte stream")
	assert.True(t, r.complete(), "expected complete after FIN")
}

// TestConformance_RFC9000_Sec2_StreamReassembly verifies that STREAM data
// arriving out of order is reassembled into the correct byte stream, and that
// the stream is only complete once the FIN and every preceding byte is present
// (RFC 9000 §2.2).
func TestConformance_RFC9000_Sec2_StreamReassembly(t *testing.T) {
	// FIN arrives (offset 6) before the bytes that precede it (offset 0).
	frames := []streamFrameInput{
		{6, []byte("world"), true},
		{0, []byte("hello "), false},
	}

	r := feed(frames...)

	assert.EqualValuesf(t, 11, r.finalSize, "finalSize = %d, want 11", r.finalSize)
	assert.Equalf(t, "hello world", string(r.bytes()),
		"bytes = %q, want %q", r.bytes(), "hello world")
	assert.True(t, r.complete(), "expected complete once the gap before the FIN is filled")
}

func TestRecvStream_GapNotComplete(t *testing.T) {
	// Tail arrives with FIN but the middle is still missing.
	r := feed(
		streamFrameInput{0, []byte("AB"), false},
		streamFrameInput{4, []byte("EF"), true},
	)
	gappedComplete := r.complete()
	gappedBytes := string(r.bytes())

	// Fill the gap; now the buffered tail should fold in and complete.
	_ = r.receive(2, []byte("CD"), false)

	assert.False(t, gappedComplete, "must not be complete while [2,4) is missing")
	assert.Equalf(t, "AB", gappedBytes, "bytes = %q, want %q", gappedBytes, "AB")
	assert.Equalf(t, "ABCDEF", string(r.bytes()), "bytes = %q, want %q", r.bytes(), "ABCDEF")
	assert.True(t, r.complete(), "expected complete after gap filled")
}

func TestRecvStream_DuplicateAndOverlap(t *testing.T) {
	frames := []streamFrameInput{
		{0, []byte("hello"), false},
		{0, []byte("hel"), false},      // wholly duplicate
		{3, []byte("lo world"), false}, // overlaps [3,5) then extends
	}

	r := feed(frames...)

	assert.Equalf(t, "hello world", string(r.bytes()),
		"bytes = %q, want %q", r.bytes(), "hello world")
}

func TestRecvStream_MultipleBufferedChunks(t *testing.T) {
	// Deliver strictly in reverse; only the last frame unblocks all of them.
	frames := []streamFrameInput{
		{8, []byte("IJKL"), true},
		{4, []byte("EFGH"), false},
		{0, []byte("ABCD"), false},
	}

	r := feed(frames...)

	assert.Equalf(t, "ABCDEFGHIJKL", string(r.bytes()),
		"bytes = %q, want %q", r.bytes(), "ABCDEFGHIJKL")
	assert.True(t, r.complete(), "expected complete")
}

// TestRecvStream_GappedResendDoesNotGrow: a peer resending the same gapped frame
// (or overlapping gapped frames) must not grow the pending buffer without bound.
// Because recvHighest does not advance on a resend, receive-flow control never
// rejects it, so the reassembly buffer itself must dedup.
func TestRecvStream_GappedResendDoesNotGrow(t *testing.T) {
	r := &recvStream{}

	for i := 0; i < 1000; i++ {
		_ = r.receive(100, []byte("PAYLOAD"), false) // same gap, 1000 times
	}
	chunksAfterDuplicates, bufferedAfterDuplicates := len(r.pending), bufferedBytes(r)
	// Overlapping resends that each extend by one byte also stay a single chunk.
	for i := 1; i <= 50; i++ {
		_ = r.receive(200, bytes.Repeat([]byte("x"), i), false)
	}

	assert.Equalf(t, 1, chunksAfterDuplicates,
		"pending chunks = %d, want 1 (duplicate gaps must merge)", chunksAfterDuplicates)
	assert.Equalf(t, len("PAYLOAD"), bufferedAfterDuplicates,
		"buffered bytes = %d, want %d (no duplicate storage)",
		bufferedAfterDuplicates, len("PAYLOAD"))
	for _, c := range r.pending {
		if c.offset == 200 {
			assert.Lenf(t, c.data, 50,
				"overlapping chunk at 200 = %d bytes, want 50 (merged, not accumulated)", len(c.data))
		}
	}
	assert.Lenf(t, r.pending, 2,
		"pending chunks = %d, want 2 (one per distinct gap)", len(r.pending))
}

// bufferedBytes totals the bytes held in a recvStream's pending gap buffer.
func bufferedBytes(r *recvStream) int {
	var n int
	for _, c := range r.pending {
		n += len(c.data)
	}
	return n
}

// TestRecvStream_OverlappingGapsMerge: chunks that overlap or abut in the pending
// buffer fuse into one non-overlapping run, and the correct bytes surface once the
// preceding gap is filled (already-held bytes win on overlap, RFC 9000 §2.2).
func TestRecvStream_OverlappingGapsMerge(t *testing.T) {
	r := &recvStream{}

	_ = r.receive(4, []byte("EFGH"), false) // [4,8)
	_ = r.receive(6, []byte("GHIJ"), false) // [6,10) overlaps [4,8), extends to 10
	_ = r.receive(2, []byte("CD"), false)   // [2,4) abuts [4,10)
	fusedChunks := len(r.pending)
	_ = r.receive(0, []byte("AB"), false) // fills the gap to offset 0

	assert.Equalf(t, 1, fusedChunks, "pending chunks = %d, want 1 (all fused)", fusedChunks)
	assert.Equalf(t, "ABCDEFGHIJ", string(r.bytes()),
		"bytes = %q, want %q", r.bytes(), "ABCDEFGHIJ")
}

func TestConn_OnStream_DeliversToOpenStream(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}}
	s, err := c.OpenStream() // stream 0
	require.NoError(t, err, "OpenStream")
	h := &connFrameHandler{c: c}

	errDeliver := h.OnStream(s.ID(), 0, true, []byte("response body"))
	// A STREAM for a locally initiated stream we have not yet created (id&3==0, and
	// stream 8 is above our high-water mark of 4) is a STREAM_STATE_ERROR (§19.8).
	// Server-initiated streams are classified separately (see accept_uni_test.go).
	errNotCreated := h.OnStream(8, 0, false, []byte("x"))

	require.NoError(t, errDeliver, "OnStream for the open stream")
	assert.True(t, h.ackEliciting, "STREAM frame must mark the packet ack-eliciting")
	assert.Equalf(t, "response body", string(s.recv.bytes()),
		"delivered %q, want %q", s.recv.bytes(), "response body")
	assert.True(t, s.recv.complete(), "stream should be complete after FIN")
	assert.Truef(t, errNotCreated == ErrStreamState,
		"STREAM on a not-yet-created client stream = %v, want ErrStreamState", errNotCreated)
}

// TestConnFrameHandler_OnCrypto_ReassemblesByOffset pins the fix for the real
// bug that broke live interop: a server's handshake CRYPTO stream spans many
// frames that can arrive out of order, and must be reassembled by offset before
// being fed to TLS (RFC 9000 §19.6). Appending in arrival order garbled the
// server's ServerHello/Certificate flight.
func TestConnFrameHandler_OnCrypto_ReassemblesByOffset(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c, space: spaceInitial}

	// A later CRYPTO frame arrives before the bytes preceding it.
	errLate := h.OnCrypto(6, []byte("world"))
	gapped := c.cryptoRecv[spaceInitial].read()
	// The gap fills; the whole prefix becomes available in order.
	errFill := h.OnCrypto(0, []byte("hello "))

	require.NoError(t, errLate, "OnCrypto for the later fragment")
	assert.Emptyf(t, gapped, "gapped CRYPTO must not be readable yet, got %q", gapped)
	require.NoError(t, errFill, "OnCrypto for the fragment that fills the gap")
	assert.Equalf(t, "hello world", string(c.cryptoRecv[spaceInitial].read()),
		"reassembled CRYPTO = %q, want %q", c.cryptoRecv[spaceInitial].read(), "hello world")
}

// TestConnFrameHandler_OnCrypto_BufferCap: CRYPTO has no flow-control window, so
// a gapped frame reaching too far past the consumed prefix must be refused with
// CRYPTO_BUFFER_EXCEEDED (RFC 9000 §7.5) rather than buffered without bound.
func TestConnFrameHandler_OnCrypto_BufferCap(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c, space: spaceInitial}

	// A gapped frame within the cap is buffered; one byte past the cap is refused.
	errWithin := h.OnCrypto(maxCryptoBuffer-1, []byte("x"))
	errPast := h.OnCrypto(maxCryptoBuffer, []byte("x"))
	code, ok := closeCodeFor(ErrCryptoBufferExceeded)

	assert.NoErrorf(t, errWithin, "CRYPTO within the cap = %v, want nil", errWithin)
	assert.Truef(t, errPast == ErrCryptoBufferExceeded,
		"CRYPTO past the cap = %v, want ErrCryptoBufferExceeded", errPast)
	assert.Truef(t, ok && code == ErrCodeCryptoBufferExceeded,
		"closeCodeFor = %#x,%v, want CRYPTO_BUFFER_EXCEEDED", code, ok)
}

func TestConn_OpenUniStream_IDs(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsUni: 3, InitialMaxStreamDataUni: 5000}}

	for _, want := range []uint64{2, 6, 10} {
		s, err := c.OpenUniStream()

		require.NoError(t, err, "OpenUniStream")
		assert.Equalf(t, want, s.ID(), "uni stream ID = %d, want %d", s.ID(), want)
		assert.EqualValuesf(t, 5000, s.sendMax,
			"uni sendMax = %d, want 5000 (initial_max_stream_data_uni)", s.sendMax)
	}
	_, err := c.OpenUniStream()
	assert.Truef(t, err == ErrTooManyStreams,
		"4th uni stream err = %v, want ErrTooManyStreams", err)
}

// TestStream_RecvAndFinished feeds a stream in two chunks (the second with FIN)
// and checks Recv returns only the newly contiguous bytes each call, and
// Finished flips once the FIN and all bytes are present.
func TestStream_RecvAndFinished(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream")
	h := &connFrameHandler{c: c}

	errFirst := h.OnStream(s.ID(), 0, false, []byte("hello "))
	firstRecv := string(s.Recv())
	emptyRecv := string(s.Recv())
	finishedBeforeFin := s.Finished()
	errSecond := h.OnStream(s.ID(), 6, true, []byte("world"))
	secondRecv := string(s.Recv())

	require.NoError(t, errFirst, "the first STREAM frame")
	assert.Equalf(t, "hello ", firstRecv, "Recv 1 = %q, want %q", firstRecv, "hello ")
	assert.Equalf(t, "", emptyRecv, "Recv with nothing new = %q, want empty", emptyRecv)
	assert.False(t, finishedBeforeFin, "stream must not be finished before FIN")
	require.NoError(t, errSecond, "the second STREAM frame carrying the FIN")
	assert.Equalf(t, "world", secondRecv, "Recv 2 = %q, want %q", secondRecv, "world")
	assert.True(t, s.Finished(), "stream should be finished after FIN with all bytes present")
}

func TestConn_OpenStream_IDs(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}}

	for want := uint64(0); want < 16; want += 4 {
		s, err := c.OpenStream()

		require.NoError(t, err, "OpenStream")
		assert.Equalf(t, want, s.ID(), "stream ID = %d, want %d", s.ID(), want)
		assert.Samef(t, s, c.streams[want], "stream %d not registered", want)
	}
}

// TestConformance_RFC9000_Sec46_StreamLimit verifies that OpenStream refuses to
// open more bidirectional streams than the peer's advertised
// initial_max_streams_bidi limit (RFC 9000 §4.6): the (limit+1)th open returns
// ErrTooManyStreams.
func TestConformance_RFC9000_Sec46_StreamLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 2}}
	zero := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 0}}

	_, err1 := c.OpenStream()
	_, err2 := c.OpenStream()
	_, err3 := c.OpenStream()
	// A peer that advertises no bidi streams forbids even the first (§18.2).
	_, errZero := zero.OpenStream()

	require.NoError(t, err1, "stream 1")
	require.NoError(t, err2, "stream 2")
	assert.Truef(t, err3 == ErrTooManyStreams,
		"stream 3 err = %v, want ErrTooManyStreams", err3)
	assert.Truef(t, errZero == ErrTooManyStreams,
		"zero-limit err = %v, want ErrTooManyStreams", errZero)
}
