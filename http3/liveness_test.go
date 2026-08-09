package http3

import (
	"testing"
	"time"
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
	if !c.Alive() {
		t.Fatal("a fresh client reports dead")
	}
	c.markDead()

	select {
	case <-c.readerDone:
	default:
		t.Fatal("markDead did not close readerDone")
	}
	if c.Alive() {
		t.Error("readerDone is closed but Alive still reports true — the pool would " +
			"keep handing out this connection")
	}
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
			if alive {
				t.Fatalf("iteration %d: a waiter woken by readerDone saw Alive() == true", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: waiter never woke", i)
		}
	}
}
