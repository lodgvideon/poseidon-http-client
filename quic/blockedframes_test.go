package quic

import "testing"

// TestConformance_RFC9000_Sec1321_BlockedFramesAckEliciting checks that
// DATA_BLOCKED, STREAM_DATA_BLOCKED, and STREAMS_BLOCKED are ack-eliciting, so a
// packet carrying only such a frame is acknowledged rather than left unacked and
// retransmitted by the peer (RFC 9000 §13.2.1; only PADDING, ACK, and
// CONNECTION_CLOSE are non-ack-eliciting, §12.4).
func TestConformance_RFC9000_Sec1321_BlockedFramesAckEliciting(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
	}{
		{"data-blocked", AppendDataBlocked(nil, 100)},
		{"stream-data-blocked", AppendStreamDataBlocked(nil, 0, 100)},
		{"streams-blocked-bidi", AppendStreamsBlocked(nil, false, 5)},
		{"streams-blocked-uni", AppendStreamsBlocked(nil, true, 5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &connFrameHandler{c: &Conn{}}
			if err := ParseFrames(tc.frame, h); err != nil {
				t.Fatalf("ParseFrames: %v", err)
			}
			if !h.ackEliciting {
				t.Fatal("a BLOCKED frame must be ack-eliciting (§13.2.1)")
			}
		})
	}
}
