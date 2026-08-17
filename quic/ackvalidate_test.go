package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec1931_AckFirstRangeNegative checks that a First ACK
// Range larger than Largest Acknowledged — which makes the smallest packet number
// in the range negative — is a FRAME_ENCODING_ERROR (RFC 9000 §19.3.1).
func TestConformance_RFC9000_Sec1931_AckFirstRangeNegative(t *testing.T) {
	h := &connFrameHandler{c: &Conn{}}

	err := h.OnAck(3, 0, 10)

	assert.ErrorIsf(t, err, ErrFrameEncoding,
		"OnAck(largest=3, firstRange=10) = %v, want ErrFrameEncoding", err)
}

// TestConformance_RFC9000_Sec1931_AckRangeNegative checks that an additional ACK
// range whose Gap or Length underflows the running lower bound — a negative
// computed packet number — is a FRAME_ENCODING_ERROR (RFC 9000 §19.3.1).
func TestConformance_RFC9000_Sec1931_AckRangeNegative(t *testing.T) {
	// Gap underflow: after acking [8,10] (ackLow=8), a Gap of 7 makes the next
	// range's highest packet 8-7-2 = -1.
	h := &connFrameHandler{c: ackConn()}
	require.NoError(t, h.OnAck(10, 0, 2), "first range")
	// Length underflow: ackLow=8, Gap 0 → highest 6, then a Length of 7 makes the
	// lowest packet 6-7 = -1.
	h2 := &connFrameHandler{c: ackConn()}
	require.NoError(t, h2.OnAck(10, 0, 2), "first range")

	gapUnderflow := h.OnAckRange(7, 0)
	lengthUnderflow := h2.OnAckRange(0, 7)

	assert.ErrorIsf(t, gapUnderflow, ErrFrameEncoding,
		"OnAckRange(gap=7) with ackLow=8 = %v, want ErrFrameEncoding", gapUnderflow)
	assert.ErrorIsf(t, lengthUnderflow, ErrFrameEncoding,
		"OnAckRange(length=7) with high=6 = %v, want ErrFrameEncoding", lengthUnderflow)
}

// TestConformance_RFC9000_Sec1931_AckNegativeViaParser checks the malformed ACK is
// rejected end to end through the frame parser (RFC 9000 §19.3.1), and that a
// well-formed multi-range ACK is still accepted.
func TestConformance_RFC9000_Sec1931_AckNegativeViaParser(t *testing.T) {
	// Largest 3, First ACK Range 10 → negative smallest packet number.
	bad := AppendAck(nil, 3, 0, 10, nil)
	// A valid ACK: acks [8,10], then Gap 0 / Length 1 acks [5,6].
	good := AppendAck(nil, 10, 0, 2, []AckRange{{Gap: 0, Length: 1}})

	badErr := ParseFrames(bad, &connFrameHandler{c: &Conn{}})
	goodErr := ParseFrames(good, &connFrameHandler{c: ackConn()})

	assert.ErrorIsf(t, badErr, ErrFrameEncoding, "ParseFrames(bad ack) = %v, want ErrFrameEncoding", badErr)
	assert.NoErrorf(t, goodErr, "ParseFrames(valid multi-range ack) = %v, want nil", goodErr)
}

// ackConn returns a Conn whose Initial-space send counter is high enough that the
// ACKs these tests feed are for packets it could have sent (RFC 9000 §13.1), so the
// tests exercise the §19.3.1 range validation rather than the never-sent check.
func ackConn() *Conn {
	c := &Conn{}
	c.sendPN[spaceInitial] = 100
	return c
}

// TestConformance_RFC9000_Sec1931_AckBoundariesAccepted checks that the exact
// boundaries — a smallest packet number of 0 in the first range or in an
// additional range — are accepted, not rejected as negative (RFC 9000 §19.3.1).
func TestConformance_RFC9000_Sec1931_AckBoundariesAccepted(t *testing.T) {
	cases := []struct {
		name string
		ack  []byte
	}{
		// First ACK Range == Largest: acks [0, 5], smallest packet number exactly 0.
		{"first-range-to-zero", AppendAck(nil, 5, 0, 5, nil)},
		// Additional range highest == 0: ack [3,5], then Gap 1 → highest 3-1-2 = 0.
		{"range-highest-zero", AppendAck(nil, 5, 0, 2, []AckRange{{Gap: 1, Length: 0}})},
		// Additional range lowest == 0: ack [3,5], Gap 0 → highest 1, Length 1 → lowest 0.
		{"range-lowest-zero", AppendAck(nil, 5, 0, 2, []AckRange{{Gap: 0, Length: 1}})},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ParseFrames(c.ack, &connFrameHandler{c: ackConn()})

			assert.NoErrorf(t, err, "a valid ACK reaching packet 0 was rejected: %v", err)
		})
	}
}
