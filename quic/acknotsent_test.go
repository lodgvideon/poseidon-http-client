package quic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec131_AckForNeverSentPacket checks that an ACK whose
// Largest Acknowledged names a packet number the client never sent is a
// PROTOCOL_VIOLATION (RFC 9000 §13.1), while an ACK reaching only sent packets is
// accepted. sendPN is the next number to send, so anything at or above it is unsent.
func TestConformance_RFC9000_Sec131_AckForNeverSentPacket(t *testing.T) {
	base := time.Unix(1, 0)
	// The client has sent packets 0 and 1 in the application space (sendPN == 2).
	newConn := func() *Conn {
		c := &Conn{now: func() time.Time { return base }}
		c.sendPN[spaceApp] = 2
		c.sent[spaceApp].onSent(0, base, true, nil)
		c.sent[spaceApp].onSent(1, base, true, nil)
		return c
	}

	// Largest Acknowledged == sendPN (the next, not-yet-sent number), a far-future
	// one, and the exact boundary Largest == sendPN-1 (the highest we sent).
	atSendPN := (&connFrameHandler{c: newConn(), space: spaceApp}).OnAck(2, 0, 0)
	farFuture := (&connFrameHandler{c: newConn(), space: spaceApp}).OnAck(999, 0, 0)
	highestSent := (&connFrameHandler{c: newConn(), space: spaceApp}).OnAck(1, 0, 1)
	code, ok := closeCodeFor(ErrProtocolViolation)

	assert.ErrorIsf(t, atSendPN, ErrProtocolViolation,
		"OnAck(largest=2) with sendPN=2 = %v, want ErrProtocolViolation", atSendPN)
	assert.ErrorIsf(t, farFuture, ErrProtocolViolation,
		"OnAck(largest=999) = %v, want ErrProtocolViolation", farFuture)
	assert.NoErrorf(t, highestSent, "OnAck(largest=1) with sendPN=2 = %v, want nil", highestSent)
	// ErrProtocolViolation maps to the PROTOCOL_VIOLATION transport code.
	require.Truef(t, ok, "closeCodeFor(ErrProtocolViolation) = (%#x, %v), want (%#x, true)",
		code, ok, ErrCodeProtocolViolation)
	assert.Equalf(t, ErrCodeProtocolViolation, code,
		"closeCodeFor(ErrProtocolViolation) = (%#x, %v), want (%#x, true)",
		code, ok, ErrCodeProtocolViolation)
}
