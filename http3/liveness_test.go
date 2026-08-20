package http3

import (
	"sync/atomic"
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
//
// The observer SPINS on a non-blocking receive rather than parking on the
// channel. That is the whole test: the window the wrong order opens is two
// instructions wide, and waking a PARKED goroutine takes microseconds, so 200
// iterations of the parked form never landed inside it and the inverted order
// passed 2/2 (#807). Spinning sees the close within nanoseconds, and the swap is
// then caught on the first run.
func TestAlive_FlagIsVisibleBeforeTheChannelCloses(t *testing.T) {
	const iterations = 5000
	var polls atomic.Int64 // evidence the observer really spun rather than parking
	for i := 0; i < iterations; i++ {
		c := &Client{readerDone: make(chan struct{})}
		started := make(chan struct{})
		observed := make(chan bool, 1)
		go func() {
			close(started)
			n := int64(0)
			for {
				select {
				case <-c.readerDone:
					polls.Add(n)
					observed <- c.Alive()
					return
				default:
					n++ // the spin is the point; an empty default here trips SA5004
				}
			}
		}()
		<-started // the observer is running, not merely created

		c.markDead()

		select {
		case alive := <-observed:
			require.Falsef(t, alive,
				"iteration %d: a waiter woken by readerDone saw Alive() == true — markDead "+
					"closed the channel before publishing the flag, so a pool woken by "+
					"readerDone hands out a corpse", i)
		case <-time.After(10 * time.Second):
			require.FailNowf(t, "waiter never woke", "iteration %d", i)
		}
	}

	t.Logf("%d iterations, %d observer polls total", iterations, polls.Load())
	assert.Positivef(t, polls.Load(),
		"the observer never polled once across %d iterations: it found readerDone already "+
			"closed every time, so it observed the flag well after the window rather than "+
			"inside it", iterations)
}
