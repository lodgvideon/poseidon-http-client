package conn

import (
	"context"
	"testing"
)

// TestConformance_RFC7540_Sec6_1_PaddedDataDebitsPaddingOverhead pins that a
// padded DATA frame debits its data bytes PLUS the pad-length octet and the
// padding against both send windows. RFC 7540 §6.1: "The entire DATA frame
// payload is included in flow control, including the Pad Length and Padding
// fields if present." acquireSendCredits used to debit only the data bytes, so
// with padding enabled our send window drifted above the peer's and a later frame
// overran the peer's window (FLOW_CONTROL_ERROR).
func TestConformance_RFC7540_Sec6_1_PaddedDataDebitsPaddingOverhead(t *testing.T) {
	t.Run("full frame fits", func(t *testing.T) {
		c := newOutFCConn(0, 1000)
		s := newStream(1, 8, c, 65535)
		c.streams[1] = s
		s.sendWindow = 1000

		// 100 data bytes with a 10-byte padding overhead (1 pad-length octet + 9
		// padding). The frame's flow-controlled cost is 110.
		n, err := c.acquireSendCredits(context.Background(), s, s.gen.Load(), 100, 10)
		if err != nil {
			t.Fatalf("acquireSendCredits: %v", err)
		}
		if n != 100 {
			t.Fatalf("n = %d, want 100 data bytes", n)
		}
		if c.peerConnSendWindow != 1000-110 {
			t.Fatalf("conn send window = %d, want %d — a padded frame debits data + pad "+
				"octet + padding (RFC 7540 §6.9.1)", c.peerConnSendWindow, 1000-110)
		}
		if s.sendWindow != 1000-110 {
			t.Fatalf("stream send window = %d, want %d", s.sendWindow, 1000-110)
		}
	})

	t.Run("data capped by window minus padding", func(t *testing.T) {
		c := newOutFCConn(0, 50)
		s := newStream(1, 8, c, 65535)
		c.streams[1] = s
		s.sendWindow = 50

		// Window 50, padding 10 → at most 40 data bytes fit (40+10=50).
		n, err := c.acquireSendCredits(context.Background(), s, s.gen.Load(), 100, 10)
		if err != nil {
			t.Fatalf("acquireSendCredits: %v", err)
		}
		if n != 40 {
			t.Fatalf("n = %d, want 40 — data is capped so data+padding fits the window", n)
		}
		if c.peerConnSendWindow != 0 || s.sendWindow != 0 {
			t.Fatalf("windows = (%d,%d), want (0,0) — the whole 50-byte frame is debited",
				c.peerConnSendWindow, s.sendWindow)
		}
	})
}

// TestConformance_RFC7540_Sec6_9_PushedStreamRecvWindow_FromInitialWindowSize
// pins that a server-pushed stream's per-stream recv window is seeded from the
// value we advertised as SETTINGS_INITIAL_WINDOW_SIZE — the same seed NewStream
// uses — not from the fluctuating connection window. Seeding from connRecvWindow
// under-credited the push, so a push within the window we advertised overran a
// smaller one and was reset with FLOW_CONTROL_ERROR.
func TestConformance_RFC7540_Sec6_9_PushedStreamRecvWindow_FromInitialWindowSize(t *testing.T) {
	c := newOutFCConn(0, 0)
	c.opts.Settings.InitialWindowSize = 100000 // advertised per-stream window
	c.connRecvWindow = 5000                    // connection window, debited low — must NOT be the seed

	pushed, err := c.reservePushedStream(2)
	if err != nil {
		t.Fatalf("reservePushedStream: %v", err)
	}
	if pushed.recvWindow != 100000 {
		t.Fatalf("pushed recvWindow = %d, want 100000 (SETTINGS_INITIAL_WINDOW_SIZE), "+
			"not the connection window %d", pushed.recvWindow, c.connRecvWindow)
	}
}
