package quic

import "testing"

// TestConformance_RFC9000_Sec1321_NewTokenPathResponseAckEliciting checks that a
// received NEW_TOKEN (§19.7) or PATH_RESPONSE (§19.18) is ack-eliciting, so a packet
// carrying only one is acknowledged rather than left unacked and retransmitted by
// the peer (RFC 9000 §13.2.1 — only ACK, PADDING, and CONNECTION_CLOSE are not).
func TestConformance_RFC9000_Sec1321_NewTokenPathResponseAckEliciting(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
	}{
		{"new-token", AppendNewToken(nil, []byte("tok"))},
		{"path-response", AppendPathResponse(nil, [8]byte{1, 2, 3, 4, 5, 6, 7, 8})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &connFrameHandler{c: &Conn{}, space: spaceApp} // both ride 1-RTT
			if err := ParseFrames(tc.frame, h); err != nil {
				t.Fatalf("ParseFrames: %v", err)
			}
			if !h.ackEliciting {
				t.Fatalf("%s must be ack-eliciting (§13.2.1)", tc.name)
			}
		})
	}
}
