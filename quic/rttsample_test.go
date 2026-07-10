package quic

import (
	"testing"
	"time"
)

// TestConformance_RFC9002_Sec51_RTTSampleWhenLargestNonEliciting checks that an
// RTT sample is generated when the largest newly acknowledged packet is not itself
// ack-eliciting but a lower newly acknowledged packet is — using the largest
// packet's send time (RFC 9002 §5.1).
func TestConformance_RFC9002_Sec51_RTTSampleWhenLargestNonEliciting(t *testing.T) {
	var s sentSpace
	base := time.Unix(1000, 0)
	s.onSent(5, base.Add(-20*time.Millisecond), true, nil)  // ack-eliciting
	s.onSent(6, base.Add(-10*time.Millisecond), false, nil) // pure ACK, the largest

	sendTime, hasRTT := s.ack(5, 6)
	if !hasRTT {
		t.Fatal("an RTT sample must be generated when a newly-acked packet is ack-eliciting (§5.1)")
	}
	if !sendTime.Equal(base.Add(-10 * time.Millisecond)) {
		t.Fatalf("sample send time = %v, want the largest acked (pn 6) send time", sendTime)
	}
}

// TestConformance_RFC9002_Sec51_NoRTTSampleWithoutAckEliciting checks that no RTT
// sample is generated when none of the newly acknowledged packets is ack-eliciting
// (RFC 9002 §5.1).
func TestConformance_RFC9002_Sec51_NoRTTSampleWithoutAckEliciting(t *testing.T) {
	var s sentSpace
	base := time.Unix(1000, 0)
	s.onSent(5, base, false, nil) // pure ACK
	s.onSent(6, base, false, nil) // pure ACK, the largest

	if _, hasRTT := s.ack(5, 6); hasRTT {
		t.Fatal("no RTT sample when no newly-acked packet is ack-eliciting (§5.1)")
	}
}
