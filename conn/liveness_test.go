package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// IsAlive reads atomics rather than selecting on readerDone, because the pool
// calls it once per connection per request. See http3/liveness_test.go for the
// same pair on the H3 side and the measurement that motivated both.

// TestIsAlive_HandBuiltConnWithNoReader pins the case the old implementation
// handled by accident: a Conn assembled in a test never starts a reader, and its
// readerDone is nil. A receive on a nil channel blocks, so the old select fell
// through to the flags; the flag version must reach the same answer, or a large
// number of unit tests silently start seeing their Conn as dead.
func TestIsAlive_HandBuiltConnWithNoReader(t *testing.T) {
	c := &Conn{}

	got := c.IsAlive()

	assert.True(t, got, "a hand-built Conn with no reader reports dead; it must fall through "+
		"to the closed/goaway flags exactly as it did with a nil readerDone")
}

// TestIsAlive_ReaderExitMarksDead is the invariant tying the flag to the channel:
// readerLoop publishes readerExited and closes readerDone together, so a
// connection whose reader has gone must never read as alive.
func TestIsAlive_ReaderExitMarksDead(t *testing.T) {
	c := &Conn{readerDone: make(chan struct{})}
	require.True(t, c.IsAlive(), "a fresh Conn reports dead")

	// Exactly what readerLoop's deferred exit does.
	c.readerExited.Store(true)
	close(c.readerDone)

	assert.False(t, c.IsAlive(), "the reader has exited but IsAlive still reports true — the pool would "+
		"keep handing out this connection")
}

// TestIsAlive_FlagsStillIndependentlyKill pins that adding readerExited did not
// swallow the other two conditions: Close and a peer GOAWAY each still make a
// connection unusable on their own, with the reader still running.
func TestIsAlive_FlagsStillIndependentlyKill(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		c := &Conn{readerDone: make(chan struct{})}

		c.closed.Store(true)

		assert.False(t, c.IsAlive(), "a closed Conn reports alive")
	})
	t.Run("goaway", func(t *testing.T) {
		c := &Conn{readerDone: make(chan struct{})}

		c.goAwayReceived.Store(true)

		assert.False(t, c.IsAlive(), "a Conn that received GOAWAY reports alive")
	})
}
