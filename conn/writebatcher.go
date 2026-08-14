package conn

import (
	"bufio"
	"sync"
	"sync/atomic"
)

// The convoy byte threshold used to be `const groupCommitFlushBytes =
// writeBufferSize / 2`, which was correct only while the write buffer was
// itself a constant. ConnOptions.WriteBufferSize made it a per-connection
// value, and a threshold that did not follow it would leave a 256 KiB-buffered
// connection convoying at 8 KiB — and a small-buffered one convoying past its
// own bufio auto-flush boundary, which is the exact hazard the threshold
// exists to avoid. It is a field on the batcher now; see flushBytes.

// writeBatcher implements the opt-in group-commit write optimization
// (ConnOptions.GroupCommit). When enabled, a HEADERS writer that finds another
// writer queued on the owning Conn's wmu (waiters>0, buffer below
// groupCommitFlushBytes) defers its flush so the next holder batches both frames
// into one tls.Conn.Write — fewer TLS record encrypts + socket writes under
// concurrent load. The deferring writer waits on cond and returns the convoy
// flush's (sticky, conn-fatal) error, so per-frame write-error semantics are
// preserved with no async contract.
//
// It is a strict no-op when disabled (commit is a plain flush) or when there is
// no wmu contention (waiters==0). seq, err, and deferring are all guarded by the
// Conn's wmu; the write path holds wmu continuously from the waiters check into
// cond.Wait (which atomically releases wmu), so the waker's broadcast cannot be
// missed. cond's locker IS that wmu.
//
// Concurrency contract: methods suffixed *Locked require the caller to hold wmu;
// enter/leave bracket the wmu.Lock and use an atomic so they can run outside it.
// Every method is nil-receiver-safe, so a hand-constructed Conn (no batcher,
// group-commit implicitly off) behaves exactly like the disabled path.
type writeBatcher struct {
	// enabled mirrors ConnOptions.GroupCommit; when false, commit is a plain
	// flush and the wake helpers do nothing.
	enabled bool
	// wb is the Conn's buffered writer (shared; wmu-serialized). Used for the
	// convoy byte-threshold check and the flush itself; nil for hand-constructed
	// test Conns, in which case the flush is a no-op.
	wb *bufio.Writer
	// flushBytes bounds a convoy: once this many bytes are buffered, the next
	// writer flushes instead of deferring, so the convoy size (and any deferring
	// writer's wait) stays bounded and flushes happen regularly under sustained
	// load. Half the write buffer, leaving headroom below the bufio auto-flush
	// boundary. Immutable after construction.
	flushBytes int
	// cond is broadcast after each flush; its locker is the Conn's wmu.
	cond *sync.Cond
	// waiters counts writers queued on wmu: incremented right before wmu.Lock,
	// decremented right after acquiring it (see enter/leave). Atomic because it
	// is touched outside wmu.
	waiters atomic.Int32
	// seq is the completed-flush counter; a deferring writer waits for it to
	// advance past the value it sampled. Guarded by wmu.
	seq uint64
	// err is the sticky first flush error (a transport write error is
	// connection-fatal). Guarded by wmu.
	err error
	// deferring counts writers currently blocked in cond.Wait; the flushing
	// writer skips the broadcast when it is zero. Guarded by wmu.
	deferring int
}

// newWriteBatcher builds a batcher whose cond is locked by wmu and which flushes
// wb. enabled comes from ConnOptions.GroupCommit; flushBytes is the convoy
// threshold, half the write buffer.
func newWriteBatcher(enabled bool, wmu *sync.Mutex, wb *bufio.Writer, flushBytes int) *writeBatcher {
	return &writeBatcher{enabled: enabled, wb: wb, flushBytes: flushBytes, cond: sync.NewCond(wmu)}
}

// enter increments the queued-writer count. Called right before wmu.Lock so a
// writer already holding wmu can observe that another writer is waiting and
// defer its flush into a convoy. No-op on a nil batcher.
func (b *writeBatcher) enter() {
	if b != nil {
		b.waiters.Add(1)
	}
}

// leave decrements the queued-writer count. Called right after acquiring wmu.
// No-op on a nil batcher.
func (b *writeBatcher) leave() {
	if b != nil {
		b.waiters.Add(-1)
	}
}

// commit flushes the buffered writer under wmu, applying group-commit deferral
// when enabled. With group-commit off it is exactly a flush. With it on, a
// writer that finds another already queued on wmu (and the buffer below the
// convoy threshold) defers: it waits for that queued writer to flush the convoy
// in one tls.Conn.Write, then returns that flush's (sticky, conn-fatal) error —
// so per-frame write-error semantics are preserved with no async contract. It
// waits at most one broadcast, then flushes itself, which (with the byte
// threshold) bounds both the wait and the convoy size and guarantees regular
// flushes under sustained load. Because the caller holds wmu continuously from
// the waiters check into cond.Wait (which atomically releases it), the waker's
// broadcast cannot be missed. MUST hold wmu.
func (b *writeBatcher) commit() error {
	if !b.enabled {
		return b.flush()
	}
	if b.waiters.Load() > 0 && b.wb != nil && b.wb.Buffered() < b.flushBytes {
		target := b.seq + 1
		b.deferring++
		b.cond.Wait() // atomically releases + reacquires wmu
		b.deferring--
		if b.seq >= target {
			return b.err // our convoy was flushed by the queued writer
		}
		// Spurious wakeup (or the queued writer errored before flushing):
		// fall through and flush ourselves so we always make progress.
	}
	return b.flushNowLocked()
}

// flushNowLocked flushes immediately — never deferring — and completes the
// bookkeeping a convoy depends on: it advances seq, records the sticky error,
// and wakes anyone blocked in commit.
//
// It exists for the frames that MUST leave now and therefore cannot go through
// commit: a WINDOW_UPDATE the peer is waiting on for send credit, a PING ACK, a
// best-effort RST, and above all the BDP tuner's PING, whose whole measurement
// is the DATA that arrives before its ACK — deferring it would time the write
// buffer instead of the link.
//
// Those sites used to call Conn.flushWrite directly, which pushes a deferring
// writer's bytes to the socket while advancing nothing. The convoy is then
// defeated twice over: the deferred frame has already gone out in this flush, so
// the writer that was supposed to carry it flushes a second time, and the
// deferring writer stays parked until that unrelated writer happens to commit
// rather than being released by the flush that actually carried its bytes.
// AutoTuneRecvWindow makes the BDP PING frequent, so with both options on the
// batching this is all for is undone regularly.
//
// MUST hold wmu.
func (b *writeBatcher) flushNowLocked() error {
	if !b.enabled {
		return b.flush()
	}
	err := b.flush()
	b.seq++
	if err != nil && b.err == nil {
		b.err = err // sticky: a transport write error is connection-fatal
	}
	if b.deferring > 0 {
		b.cond.Broadcast() // only when someone is actually deferring
	}
	return b.err
}

// flush flushes the buffered writer. A nil writer (hand-constructed test Conn)
// is a no-op, matching Conn.flushWrite.
func (b *writeBatcher) flush() error {
	if b.wb == nil {
		return nil
	}
	return b.wb.Flush()
}

// wakeDeferredLocked wakes a writer blocked in commit, but only when group-commit
// is on and someone is actually deferring (an uncontended write does nothing).
// Used on the error path where the writer buffered nothing to flush, so no
// convoy flush will otherwise occur. MUST hold wmu.
func (b *writeBatcher) wakeDeferredLocked() {
	if b != nil && b.enabled && b.deferring > 0 {
		b.cond.Broadcast()
	}
}

// wakeAllLocked unconditionally wakes any writer blocked in commit so it
// re-checks (e.g. against a now-closing transport) rather than hanging. Used by
// Conn.Close. MUST hold wmu.
func (b *writeBatcher) wakeAllLocked() {
	if b != nil && b.cond != nil {
		b.cond.Broadcast()
	}
}
