package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec11_1_TooManyGaps_ProtocolViolation pins the cap on
// retained out-of-order ranges. A peer that sends many tiny, non-adjacent STREAM
// frames forces one buffered chunk per gap; the receive-flow-control window
// bounds the bytes buffered but not the range count, and bufferGap is O(ranges)
// per frame, so an unbounded range count is a quadratic-CPU denial of service.
// RFC 9000 §11.1 permits treating resource-exhausting behaviour as a connection
// error; the stream stops reassembling and returns ErrProtocolViolation.
func TestConformance_RFC9000_Sec11_1_TooManyGaps_ProtocolViolation(t *testing.T) {
	r := &recvStream{}
	var err error

	// 1-byte frames at increasing even offsets; offset 0 is never sent, so
	// nothing absorbs and every frame adds a fresh, non-adjacent range.
	for i := 0; i <= maxRecvGapChunks+2; i++ {
		if err = r.receive(uint64(2*i+2), []byte{'x'}, false); err != nil {
			break
		}
	}

	assert.ErrorIsf(t, err, ErrProtocolViolation,
		"err = %v, want ErrProtocolViolation once retained ranges exceed %d", err, maxRecvGapChunks)
}

// TestConformance_RFC9000_Sec11_1_NormalReorderingAccepted is the over-rejection
// guard: the handful of gaps genuine packet reordering leaves outstanding is well
// under the cap and must be accepted.
func TestConformance_RFC9000_Sec11_1_NormalReorderingAccepted(t *testing.T) {
	r := &recvStream{}

	for i := 0; i < 64; i++ {
		err := r.receive(uint64(2*i+2), []byte{'x'}, false)

		assert.NoErrorf(t, err, "receive gap %d: %v, want nil — normal reordering must be accepted", i, err)
	}
}

// TestConformance_RFC9000_Sec11_1_GapCapAtTheLimit pins maxRecvGapChunks AT its
// value, in both directions, and records WHICH frame first errored.
//
// The two tests above bracket the cap from far away: one accepts 64 retained
// ranges, the other pushes maxRecvGapChunks+3 and asserts only that SOME frame in
// the loop returned ErrProtocolViolation without recording which. Between 64 and
// ~515 the constant could move by 8x in either direction with nothing going red —
// so neither the denial-of-service bound nor the over-rejection risk to genuine
// packet reordering was actually held anywhere. #828.
//
// The guard is `len(r.pending) > maxRecvGapChunks`, so the N'th gapped frame
// leaves N retained ranges: exactly maxRecvGapChunks of them is the last legal
// state and one more is the connection error.
func TestConformance_RFC9000_Sec11_1_GapCapAtTheLimit(t *testing.T) {
	r := &recvStream{}

	// 1-byte frames at increasing even offsets; offset 0 is never sent, so nothing
	// absorbs and every frame adds a fresh, non-adjacent range.
	firstErrAt, firstErr := -1, error(nil)
	for i := 0; i < maxRecvGapChunks+1; i++ {
		if err := r.receive(uint64(2*i+2), []byte{'x'}, false); err != nil {
			firstErrAt, firstErr = i, err
			break
		}
	}

	require.Equalf(t, maxRecvGapChunks, firstErrAt,
		"the first refused frame was #%d (err %v), want #%d — the cap has moved: "+
			"lower rejects reordering a real path produces, higher widens the "+
			"quadratic-CPU window §11.1 exists to close",
		firstErrAt, firstErr, maxRecvGapChunks)
	assert.ErrorIsf(t, firstErr, ErrProtocolViolation,
		"frame #%d = %v, want ErrProtocolViolation", firstErrAt, firstErr)
	// receive() inserts first and checks the cap afterwards, so the refused frame
	// is itself retained: the peak the peer can force is one range past the cap,
	// not the cap. That is the number the memory bound is actually worth.
	assert.Equalf(t, maxRecvGapChunks+1, len(r.pending),
		"retained ranges at the refusal = %d, want %d — the range that trips the cap "+
			"is buffered before the check, so this is the true peak a peer can force",
		len(r.pending), maxRecvGapChunks+1)
}
