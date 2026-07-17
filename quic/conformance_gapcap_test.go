package quic

import (
	"errors"
	"testing"
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
	if !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("err = %v, want ErrProtocolViolation once retained ranges exceed %d",
			err, maxRecvGapChunks)
	}
}

// TestConformance_RFC9000_Sec11_1_NormalReorderingAccepted is the over-rejection
// guard: the handful of gaps genuine packet reordering leaves outstanding is well
// under the cap and must be accepted.
func TestConformance_RFC9000_Sec11_1_NormalReorderingAccepted(t *testing.T) {
	r := &recvStream{}
	for i := 0; i < 64; i++ {
		if err := r.receive(uint64(2*i+2), []byte{'x'}, false); err != nil {
			t.Fatalf("receive gap %d: %v, want nil — normal reordering must be accepted", i, err)
		}
	}
}
