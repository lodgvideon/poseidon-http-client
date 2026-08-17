package conn

import (
	"bufio"
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	wb = bufio.NewWriterSize(&countingSink{}, defaultWriteBufferSize)
	b = newWriteBatcher(true, wmu, wb, defaultWriteBufferSize/2)

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
		require.Falsef(t, time.Now().After(deadline),
			"the writer never parked in commit; the fixture is not testing deferral")
		time.Sleep(time.Millisecond)
	}

	return wmu, b, wb, done, &cerr, func() {
		wmu.Lock()
		_ = b.flushNowLocked()
		wmu.Unlock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			assert.Fail(t, "parked writer never released even by flushNowLocked")
		}
	}
}

// connDeferFixture is gcDeferFixture one layer up: a real Conn wired to a real
// batcher over a real buffered writer, with one writer parked in commit behind
// it. It is what makes the call sites testable — the batcher-level fixture
// cannot tell whether writeBDPPing calls flushNow or flushWrite.
func connDeferFixture(t *testing.T) (c *Conn, parked <-chan struct{}, release func()) {
	t.Helper()
	wb := bufio.NewWriterSize(&countingSink{}, defaultWriteBufferSize)
	c = &Conn{
		fr:      frame.NewFramer(wb, bytes.NewReader(nil)),
		streams: map[uint32]*Stream{},
	}
	c.wbatch = newWriteBatcher(true, &c.wmu, wb, defaultWriteBufferSize/2)
	c.wbatch.enter() // a queued writer, so the next commit defers

	done := make(chan struct{})
	go func() {
		c.wmu.Lock()
		_, _ = wb.WriteString("deferred frame")
		_ = c.wbatch.commit()
		c.wmu.Unlock()
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		c.wmu.Lock()
		d := c.wbatch.deferring
		c.wmu.Unlock()
		if d == 1 {
			break
		}
		require.Falsef(t, time.Now().After(deadline),
			"the writer never parked in commit; the fixture is not testing deferral")
		time.Sleep(time.Millisecond)
	}

	return c, done, func() {
		c.wmu.Lock()
		_ = c.wbatch.flushNowLocked()
		c.wmu.Unlock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			assert.Fail(t, "parked writer never released")
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

	require.NoErrorf(t, err, "flushNowLocked")
	select {
	case <-parked:
		assert.NoErrorf(t, *commitErr,
			"the released writer reported %v, want nil — its bytes were flushed", *commitErr)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the deferring writer was not released by the flush that carried its bytes; "+
			"it is still waiting for some unrelated writer to commit")
	}
	// Released by THIS flush, not by falling through and flushing itself. The
	// distinction is the whole convoy protocol: a waiter returns early only when
	// seq reached the target it sampled. If seq never advances, every deferring
	// writer falls through, and group commit silently stops batching while all
	// the tests above still pass.
	wmu.Lock()
	seq := b.seq
	wmu.Unlock()
	assert.EqualValuesf(t, 1, seq,
		"batcher seq = %d, want 1 — the flush did not record itself, so the "+
			"waiter was woken but had to flush again instead of returning", seq)
}

// TestFlushNow_ControlFrameWritersUseIt covers the call sites rather than the
// batcher, which the batcher-level tests above do not: reverting any one of
// these writers to flushWrite leaves them all passing.
//
// Each writer is driven on a Conn with a real batcher and a parked writer behind
// it, and must release that writer. writeBDPPing is the one #455 item 4 names —
// AutoTuneRecvWindow makes it frequent — but the others are the same shape and
// regress the same way.
func TestFlushNow_ControlFrameWritersUseIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(c *Conn) error
	}{
		{"writeBDPPing", func(c *Conn) error { return c.writeBDPPing() }},
		{"writeWindowUpdate", func(c *Conn) error { return c.writeWindowUpdate(0, 1024) }},
		{"writePingAck", func(c *Conn) error { return c.writePingAck([8]byte{1}) }},
		{"writeSettingsAck", func(c *Conn) error { return c.writeSettingsAck() }},
		{"writeRSTStreamID", func(c *Conn) error { return c.writeRSTStreamID(1, frame.ErrCodeCancel) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, parked, release := connDeferFixture(t)
			defer release()

			err := tc.write(c)

			require.NoErrorf(t, err, "%s", tc.name)
			select {
			case <-parked:
			case <-time.After(2 * time.Second):
				require.FailNowf(t, "the deferring writer was left parked",
					"%s did not release the writer deferring behind it — it flushed "+
						"that writer's bytes to the socket and left it parked, which is what "+
						"calling flushWrite instead of flushNow does", tc.name)
			}
		})
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
	err := wb.Flush()
	wmu.Unlock()

	require.NoErrorf(t, err, "Flush")
	select {
	case <-parked:
		require.FailNow(t, "a plain flush released the deferring writer — then the gate above "+
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
	wb := bufio.NewWriterSize(sink, defaultWriteBufferSize)
	b := newWriteBatcher(false, &sync.Mutex{}, wb, defaultWriteBufferSize/2)
	_, werr := wb.WriteString("WINDOW_UPDATE")
	require.NoErrorf(t, werr, "Write")

	err := b.flushNowLocked()

	require.NoErrorf(t, err, "flushNowLocked")
	assert.EqualValuesf(t, 1, sink.writes,
		"underlying writes = %d, want 1 — the frame did not reach the socket", sink.writes)
	assert.EqualValuesf(t, 0, b.seq,
		"seq = %d, want 0 — a disabled batcher has no convoy to sequence", b.seq)
}
