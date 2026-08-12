package conn

import (
	"bufio"
	"sync"
	"testing"
	"time"
)

// The control frames that must leave immediately — WINDOW_UPDATE, PING ACK,
// RST_STREAM, and the BDP tuner's PING — cannot go through commit, because
// commit is allowed to defer them into a convoy and the BDP sample in particular
// measures the write buffer instead of the link if it is held back.
//
// They used to call Conn.flushWrite directly, which pushes whatever a deferring
// writer left in the buffer without advancing the batcher's sequence or waking
// anyone. The frame's bytes reach the socket while the writer that owns them
// stays parked, waiting for some unrelated writer to commit. AutoTuneRecvWindow
// makes the BDP PING frequent, so with both options on this happens regularly
// (#455 item 4).
//
// Conn.flushNow / writeBatcher.flushNowLocked flush just as immediately and also
// do the bookkeeping, so the flush that carries a deferred frame's bytes is the
// one that releases it.

// gcDeferFixture parks one writer in commit and hands back the pieces needed to
// drive the flush under test. The returned func releases the parked writer and
// must be called.
//
// The channel is CLOSED rather than sent on, so both the assertion and the
// cleanup can wait on it. A one-shot send is consumed by whichever reads first,
// which made the cleanup block forever on an already-drained channel and fail
// the test for a reason that had nothing to do with the code under test.
func gcDeferFixture(t *testing.T) (wmu *sync.Mutex, b *writeBatcher, wb *bufio.Writer, parked <-chan struct{}, commitErr *error, release func()) {
	t.Helper()
	wmu = &sync.Mutex{}
	wb = bufio.NewWriterSize(&countingSink{}, writeBufferSize)
	b = newWriteBatcher(true, wmu, wb)

	// A queued writer is what makes commit defer rather than flush. Simulating
	// it with enter() keeps the test deterministic: a real second goroutine
	// would race to acquire wmu and might commit the convoy itself.
	b.enter()

	var cerr error
	done := make(chan struct{})
	go func() {
		wmu.Lock()
		_, _ = wb.WriteString("deferred frame")
		cerr = b.commit() // defers: waiters > 0 and the buffer is small
		wmu.Unlock()
		close(done) // the close is what publishes cerr to a reader of done
	}()

	// Wait until it is genuinely parked, not merely started. Without this the
	// flush under test could run first and the test would prove nothing.
	deadline := time.Now().Add(5 * time.Second)
	for {
		wmu.Lock()
		d := b.deferring
		wmu.Unlock()
		if d == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the writer never parked in commit; the fixture is not testing deferral")
		}
		time.Sleep(time.Millisecond)
	}

	return wmu, b, wb, done, &cerr, func() {
		wmu.Lock()
		_ = b.flushNowLocked()
		wmu.Unlock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("parked writer never released even by flushNowLocked")
		}
	}
}

// countingSink is an io.Writer that records how many times it was written to,
// which is the socket-write count a convoy exists to reduce.
type countingSink struct {
	writes int
}

func (s *countingSink) Write(p []byte) (int, error) {
	s.writes++
	return len(p), nil
}

// TestFlushNow_ReleasesDeferringWriter is the gate: the flush that carries a
// deferred frame's bytes must also release the writer that deferred it.
func TestFlushNow_ReleasesDeferringWriter(t *testing.T) {
	wmu, b, wb, parked, commitErr, release := gcDeferFixture(t)
	defer release()

	wmu.Lock()
	_, _ = wb.WriteString("PING") // the control frame that must leave now
	err := b.flushNowLocked()
	wmu.Unlock()
	if err != nil {
		t.Fatalf("flushNowLocked: %v", err)
	}

	select {
	case <-parked:
		if *commitErr != nil {
			t.Errorf("the released writer reported %v, want nil — its bytes were flushed", *commitErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the deferring writer was not released by the flush that carried its bytes; " +
			"it is still waiting for some unrelated writer to commit")
	}
}

// TestFlushNow_PlainFlushLeavesWriterParked is the control, and the reason the
// test above is not vacuous: the old behaviour — flushing the buffered writer
// directly, which is exactly what Conn.flushWrite does — pushes the same bytes
// to the socket and leaves the writer parked.
//
// Without this, a flushNowLocked that did nothing but flush would still pass the
// gate, because something else could have released the writer.
func TestFlushNow_PlainFlushLeavesWriterParked(t *testing.T) {
	wmu, _, wb, parked, _, release := gcDeferFixture(t)
	defer release()

	wmu.Lock()
	_, _ = wb.WriteString("PING")
	// The pre-fix path: bytes go out, no seq, no broadcast.
	if err := wb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	wmu.Unlock()

	select {
	case <-parked:
		t.Fatal("a plain flush released the deferring writer — then the gate above " +
			"cannot distinguish flushNowLocked from flushWrite and proves nothing")
	case <-time.After(250 * time.Millisecond):
		// Correct: still parked. release() frees it.
	}
}

// TestFlushNow_DisabledBatcherStillFlushes pins the no-op path: with group
// commit off there is no sequence or convoy to maintain, and flushNow must
// remain exactly a flush.
func TestFlushNow_DisabledBatcherStillFlushes(t *testing.T) {
	sink := &countingSink{}
	wb := bufio.NewWriterSize(sink, writeBufferSize)
	b := newWriteBatcher(false, &sync.Mutex{}, wb)

	if _, err := wb.WriteString("WINDOW_UPDATE"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.flushNowLocked(); err != nil {
		t.Fatalf("flushNowLocked: %v", err)
	}
	if sink.writes != 1 {
		t.Errorf("underlying writes = %d, want 1 — the frame did not reach the socket", sink.writes)
	}
	if b.seq != 0 {
		t.Errorf("seq = %d, want 0 — a disabled batcher has no convoy to sequence", b.seq)
	}
}
