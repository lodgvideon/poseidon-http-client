package http3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFakeConn_CloseLatchesCodeBeforeReleasingTheLock pins the property #924 turned
// out to be about, once that issue's reader-goroutine diagnosis was ruled out:
// fakeConn must latch its terminal error and its close code in ONE critical
// section, the way quic.Conn.closeWithErrorLocked does (quic/close.go:84-105).
// c.done closing is what tells the rest of the world a teardown happened, so by
// the time anyone else can observe it the code must already be latched.
//
// It matters because 34 assertions across 11 files in this package read
// fakeConn.closeCode to decide which error code the code under test produced. With
// the latch split across two c.mu sections a second teardown could run the whole
// latch in the gap, so closeCode reported whichever close acquired the lock second
// rather than which one terminated first. Measured before the fix: 385 failures in
// 19,200 race-instrumented runs of the two RFC 9204 bound tests, every one of them
// reporting 0x102 (H3_INTERNAL_ERROR, the reader's fatal) where the typed 0x202
// was expected.
//
// HOW FAR THIS TEST GOES, stated because a synchronisation fix cannot be validated
// by mutation alone. Reverting CloseWithError to the two-section form does NOT make
// this test fail: it passed 400/400 against the reverted code, because the closing
// goroutine re-takes c.mu on the instruction after terminate returns and wins that
// race essentially always. The discrimination was demonstrated by widening the
// window deliberately — the same reverted form with one runtime.Gosched() between
// terminate() and c.mu.Lock() turns this test red. So this is a guard against the
// gap being reopened WIDE, plus an executable statement of the invariant; the
// narrow-gap regression is caught only by the 24x800 parallel harness in #924, and
// that harness is probabilistic. There is no unit test that reliably catches the
// narrow form, and pretending otherwise would be worse than saying so.
//
// A fixture test, not a protocol test, hence no docs/RFC_COVERAGE.md row: the
// production path was never wrong. quic.Conn has always closed atomically.
func TestFakeConn_CloseLatchesCodeBeforeReleasingTheLock(t *testing.T) {
	c := &fakeConn{}
	c.mu.Lock()
	c.ensureInitLocked()
	c.mu.Unlock()

	observed := make(chan bool, 1)
	go func() {
		<-c.done // terminate has run, and it ran while c.mu was held
		c.mu.Lock()
		observed <- c.closed
		c.mu.Unlock()
	}()

	require.NoError(t, c.CloseWithError(false, H3QpackDecoderStreamError, ""),
		"CloseWithError on a fresh fakeConn")

	select {
	case closed := <-observed:
		assert.Truef(t, closed,
			"an observer that woke on c.done found closed=false, so terminate and the "+
				"code latch ran in two different critical sections; every closeCode "+
				"assertion in this package can then read a code from the wrong teardown")
	case <-time.After(5 * time.Second):
		t.Fatal("the observer never got c.mu; CloseWithError is holding it")
	}
}
