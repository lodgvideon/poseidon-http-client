package quic

import (
	"errors"
	"testing"
)

// TestConformance_RFC9000_Sec124_FrameTypePermittedBySpace checks that a frame
// carried in a packet-number space that does not permit its type is a
// PROTOCOL_VIOLATION (RFC 9000 §12.4 Table 3 / §12.5). Initial and Handshake
// packets permit only PADDING, PING, ACK, CRYPTO, and a transport CONNECTION_CLOSE;
// the 1-RTT space permits any frame. The sharp case is HANDSHAKE_DONE: without the
// gate a forged Initial carrying it would fake handshake completion.
func TestConformance_RFC9000_Sec124_FrameTypePermittedBySpace(t *testing.T) {
	handshakeDone := []byte{0x1e}
	stream := []byte{0x08, 0x00}             // STREAM, stream 0
	maxData := []byte{0x10, 0x40, 0x64}      // MAX_DATA 100
	appClose := []byte{0x1d, 0x00, 0x00}     // application CONNECTION_CLOSE (0x1d)
	ping := []byte{0x01}                     // PING
	crypto := []byte{0x06, 0x00, 0x00}       // CRYPTO offset 0 length 0
	ack := []byte{0x02, 0x00, 0x00, 0x00, 0x00} // ACK largest 0, no ranges
	transportClose := []byte{0x1c, 0x00, 0x00, 0x00} // transport CONNECTION_CLOSE

	// Frames NOT permitted in Initial/Handshake → PROTOCOL_VIOLATION.
	for _, sp := range []int{spaceInitial, spaceHandshake} {
		for _, tc := range []struct {
			name  string
			frame []byte
		}{
			{"handshake-done", handshakeDone},
			{"stream", stream},
			{"max-data", maxData},
			{"app-connection-close", appClose},
		} {
			h := &connFrameHandler{c: &Conn{}, space: sp}
			if err := ParseFrames(tc.frame, h); !errors.Is(err, ErrProtocolViolation) {
				t.Fatalf("space %d %s: ParseFrames = %v, want ErrProtocolViolation", sp, tc.name, err)
			}
		}
	}

	// Frames permitted in Initial are accepted there.
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"padding", []byte{0x00}},
		{"ping", ping},
		{"ack", ack},
		{"crypto", crypto},
		{"transport-connection-close", transportClose},
	} {
		c := &Conn{}
		c.sendPN[spaceInitial] = 1 // so the ACK of packet 0 is not a §13.1 never-sent
		h := &connFrameHandler{c: c, space: spaceInitial}
		if err := ParseFrames(tc.frame, h); err != nil {
			t.Fatalf("Initial %s: ParseFrames = %v, want nil (permitted §12.4)", tc.name, err)
		}
	}

	// An UNKNOWN frame type in Initial/Handshake is a FRAME_ENCODING_ERROR, not a
	// PROTOCOL_VIOLATION (RFC 9000 §12.4 unknown-type rule takes precedence).
	unknown := []byte{0x30} // 0x30 is not a QUIC v1 frame type
	if err := ParseFrames(unknown, &connFrameHandler{c: &Conn{}, space: spaceInitial}); !errors.Is(err, ErrFrameEncoding) {
		t.Fatalf("unknown frame type in Initial: ParseFrames = %v, want ErrFrameEncoding", err)
	}

	// HANDSHAKE_DONE is permitted in the 1-RTT (application) space.
	h := &connFrameHandler{c: &Conn{}, space: spaceApp}
	if err := ParseFrames(handshakeDone, h); err != nil {
		t.Fatalf("app-space HANDSHAKE_DONE: ParseFrames = %v, want nil", err)
	}
	if !h.c.handshakeConfirmed {
		t.Fatal("HANDSHAKE_DONE in the 1-RTT space should still confirm the handshake")
	}
}
