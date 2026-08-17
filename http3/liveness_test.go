package http3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Alive reads an atomic flag rather than selecting on readerDone, because the
// pool calls it once per connection per request. The flag and the channel must
// therefore agree, and nothing in the type system enforces that: a future raw
// `close(c.readerDone)` would leave Alive reporting true forever and the pool
// handing out a dead connection, with no test failing. These pin it.

// TestAlive_FlagAgreesWithReaderDone is the invariant: once readerDone is
// closed, Alive must be false — on every path that can close it.
func TestAlive_FlagAgreesWithReaderDone(t *testing.T) {
	c := &Client{readerDone: make(chan struct{})}
	require.True(t, c.Alive(),
		"a fresh client reports dead, so nothing markDead does below could be told apart from the fixture")

	c.markDead()

	closed := false
	select {
	case <-c.readerDone:
		closed = true
	default:
	}
	assert.True(t, closed, "markDead did not close readerDone")
	assert.False(t, c.Alive(), "readerDone is closed but Alive still reports true — the pool would "+
		"keep handing out this connection")
}

// TestAlive_FlagIsVisibleBeforeTheChannelCloses pins the ordering, which is the
// half that is easy to get backwards. A goroutine woken by readerDone must never
// then observe the connection as alive; storing after the close would allow
// exactly that. Reporting death slightly EARLY is safe and documented, late is
// the bug.
func TestAlive_FlagIsVisibleBeforeTheChannelCloses(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := &Client{readerDone: make(chan struct{})}
		observed := make(chan bool, 1)
		go func() {
			<-c.readerDone
			observed <- c.Alive()
		}()

		c.markDead()

		select {
		case alive := <-observed:
			require.Falsef(t, alive,
				"iteration %d: a waiter woken by readerDone saw Alive() == true", i)
		case <-time.After(2 * time.Second):
			require.FailNowf(t, "waiter never woke", "iteration %d", i)
		}
	}
}
