package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeHeadersAndData fuses HEADERS and the body into one transport write, but
// only when the send windows can cover the body without waiting. That decision
// rests entirely on tryAcquireSendCreditsAll, which is what these test.
//
// The fallback is what makes the fusion a pure optimisation, so refusing
// correctly matters more than granting: a grant that overshoots the peer's
// window puts bytes on the wire it never allowed, and a version that blocked
// instead of refusing would hold the connection's write lock while waiting —
// stalling every other stream, and deadlocking against a peer that will not
// send credit until it sees the frame still sitting in our buffer.
//
// The end-to-end shape (one HEADERS, body, END_STREAM on the last DATA) is
// covered in grpc/: TestUnaryTransportWriteCount pins the write count at 1 and
// the integration suite reads the responses back.

// oneShotConn is the minimum a credit check needs: no framer, no writer.
func oneShotConn(connWindow int32) *Conn {
	return &Conn{peerConnSendWindow: connWindow}
}

func oneShotStream(c *Conn, streamWindow int32) *Stream {
	s := &Stream{w: c, sendWindow: streamWindow}
	s.gen.Store(1)
	return s
}

// TestOneShotCredit_RefusesWhenShort is the decision that keeps the fused path
// honest: with less credit than the body needs, it must refuse rather than grant
// a partial amount, because a partial write needs a second flush — the thing the
// caller is trying not to do.
func TestOneShotCredit_RefusesWhenShort(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stream, connWn int32
	}{
		{"stream window short", 1, 1000},
		{"conn window short", 1000, 1},
		{"both short", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := oneShotConn(tc.connWn)
			s := oneShotStream(c, tc.stream)

			ok, err := c.tryAcquireSendCreditsAll(s, s.gen.Load(), 64, 0)

			require.NoErrorf(t, err, "err = %v, want nil", err)
			require.False(t, ok, "granted 64 bytes against a smaller window — the fused path would "+
				"emit more than the peer allows")
			s.mu.Lock()
			got := s.sendWindow
			s.mu.Unlock()
			assert.Equalf(t, tc.stream, got,
				"a refused acquire debited the windows: stream=%d conn=%d, want %d and %d",
				got, c.peerConnSendWindow, tc.stream, tc.connWn)
			assert.Equalf(t, tc.connWn, c.peerConnSendWindow,
				"a refused acquire debited the windows: stream=%d conn=%d, want %d and %d",
				got, c.peerConnSendWindow, tc.stream, tc.connWn)
		})
	}
}

// TestOneShotCredit_GrantsTheWholeAmount pins the other half: a grant debits the
// full cost from both windows, so every frame the caller is about to write is
// already paid for and it never has to stop partway.
func TestOneShotCredit_GrantsTheWholeAmount(t *testing.T) {
	c := oneShotConn(100)
	s := oneShotStream(c, 100)

	ok, err := c.tryAcquireSendCreditsAll(s, s.gen.Load(), 40, 0)

	require.NoErrorf(t, err, "tryAcquireSendCreditsAll = (%v, %v), want (true, nil)", ok, err)
	require.Truef(t, ok, "tryAcquireSendCreditsAll = (%v, %v), want (true, nil)", ok, err)
	s.mu.Lock()
	got := s.sendWindow
	s.mu.Unlock()
	assert.EqualValuesf(t, 60, got,
		"after granting 40: stream=%d conn=%d, want 60 and 60", got, c.peerConnSendWindow)
	assert.EqualValuesf(t, 60, c.peerConnSendWindow,
		"after granting 40: stream=%d conn=%d, want 60 and 60", got, c.peerConnSendWindow)
}

// TestOneShotCredit_ChargesPadding pins that the padding overhead is part of the
// cost. A padded DATA frame puts its data bytes PLUS the pad-length octet and the
// padding on the wire, all of which the peer debits (RFC 7540 §6.9.1), so a
// version that charged only the data would drift our windows above the peer's.
func TestOneShotCredit_ChargesPadding(t *testing.T) {
	c := oneShotConn(50)
	s := oneShotStream(c, 50)

	// 40 data + 10 overhead exactly fills a 50-byte window.
	ok, err := c.tryAcquireSendCreditsAll(s, s.gen.Load(), 40, 10)

	require.NoErrorf(t, err, "exact fit = (%v, %v), want (true, nil)", ok, err)
	require.Truef(t, ok, "exact fit = (%v, %v), want (true, nil)", ok, err)
	s.mu.Lock()
	got := s.sendWindow
	s.mu.Unlock()
	assert.EqualValuesf(t, 0, got, "after 40+10: stream=%d conn=%d, want 0 and 0", got, c.peerConnSendWindow)
	assert.EqualValuesf(t, 0, c.peerConnSendWindow,
		"after 40+10: stream=%d conn=%d, want 0 and 0", got, c.peerConnSendWindow)
}

// TestOneShotCredit_RefusesOnePastTheWindow is the other side of the exact fit
// above: one byte more than the window, counting overhead, must be refused.
func TestOneShotCredit_RefusesOnePastTheWindow(t *testing.T) {
	c := oneShotConn(50)
	s := oneShotStream(c, 50)

	ok, err := c.tryAcquireSendCreditsAll(s, s.gen.Load(), 41, 10)

	assert.NoErrorf(t, err, "41+10 against a 50-byte window = (%v, %v), want (false, nil) — "+
		"padding is not being charged", ok, err)
	assert.Falsef(t, ok, "41+10 against a 50-byte window = (%v, %v), want (false, nil) — "+
		"padding is not being charged", ok, err)
}

// TestOneShotCredit_RefusesAStaleLifetime pins that the fused path honours the
// generation guard the rest of the send side is built on: a reference held from a
// finished request must not spend the live stream's credit.
func TestOneShotCredit_RefusesAStaleLifetime(t *testing.T) {
	c := oneShotConn(1000)
	s := oneShotStream(c, 1000)
	stale := s.gen.Load()
	s.gen.Add(1) // the struct now belongs to a later request

	ok, err := c.tryAcquireSendCreditsAll(s, stale, 10, 0)

	require.ErrorIsf(t, err, ErrStaleStream, "err = %v, want ErrStaleStream", err)
	assert.False(t, ok, "a stale lifetime was granted credit")
	assert.EqualValuesf(t, 1000, c.peerConnSendWindow,
		"a stale acquire debited the connection window: %d, want 1000", c.peerConnSendWindow)
}

// TestOneShotCredit_RefusesAClosedStream pins RFC 9113 §6.4: once a stream is
// reset or closed, no further DATA may go out on it. The blocking path re-checks
// this on every wake; the fused path has one chance, so it checks up front.
func TestOneShotCredit_RefusesAClosedStream(t *testing.T) {
	c := oneShotConn(1000)
	s := oneShotStream(c, 1000)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	ok, err := c.tryAcquireSendCreditsAll(s, s.gen.Load(), 10, 0)

	require.ErrorIsf(t, err, ErrStreamClosed, "err = %v, want ErrStreamClosed", err)
	assert.False(t, ok, "a closed stream was granted credit")
}
