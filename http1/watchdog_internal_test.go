package http1

import (
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deadlineLog is a net.Conn stand-in that records, in order, every deadline
// installed on it. The embedded net.Conn is nil: only the methods below are
// ever called.
type deadlineLog struct {
	net.Conn
	mu     sync.Mutex
	writes []time.Time
	reads  []time.Time

	// slowPast delays the "release the blocked call now" deadline before
	// recording it. That is what makes the release-ordering test deterministic:
	// the watchdog's set is deliberately made to arrive late, so a release that
	// did NOT wait for the watchdog records the clear first and the past
	// deadline after it — i.e. latched on a connection about to be pooled.
	slowPast bool
}

func (d *deadlineLog) SetWriteDeadline(t time.Time) error {
	if d.slowPast && t.Equal(deadlineLongPast) {
		time.Sleep(2 * time.Millisecond)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes = append(d.writes, t)
	return nil
}

func (d *deadlineLog) SetReadDeadline(t time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads = append(d.reads, t)
	return nil
}

func (d *deadlineLog) Close() error { return nil }

// last returns the deadline most recently installed on the named side.
func (d *deadlineLog) last(k deadlineKind) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	log := d.reads
	if k == writeDeadline {
		log = d.writes
	}
	if len(log) == 0 {
		return time.Time{}, false
	}
	return log[len(log)-1], true
}

// waitFor polls until cond holds or the budget runs out.
func waitFor(cond func() bool, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// TestWatchdog_ReleaseWaitsForWatchdogBeforeClearing pins invariant 1, the one
// the per-call watchdog got from `<-exited`: the release must not clear the
// deadline until the watchdog can no longer install one, or a cancellation
// racing the return leaves a PAST deadline latched on a connection that is
// about to go back in the pool. The next request on it then fails with an
// immediate i/o timeout it had nothing to do with.
//
// The race is forced rather than waited for: ctx is cancelled while the arming
// is still live, so the watchdog's select has both branches ready, and the
// stand-in socket makes the watchdog's set the slow one. A release that did not
// wait would therefore record the clear first and the past deadline last, which
// is exactly what is asserted against — over enough iterations that the
// coin-flip in the watchdog's select cannot hide it.
func TestWatchdog_ReleaseWaitsForWatchdogBeforeClearing(t *testing.T) {
	const iterations = 50
	for i := 0; i < iterations; i++ {
		dl := &deadlineLog{slowPast: true}
		c := NewConn(dl)
		ctx, cancel := context.WithCancel(context.Background())
		armed := c.armDeadline(ctx, writeDeadline)
		require.Truef(t, armed, "iteration %d: a cancellable context did not arm the watchdog", i)
		// Cancel with the arming still live: the blocked-call window.
		cancel()

		c.releaseDeadline(writeDeadline, armed)

		got, ok := dl.last(writeDeadline)
		require.Truef(t, ok, "iteration %d: no write deadline was ever installed", i)
		require.Truef(t, got.IsZero(),
			"iteration %d: connection left carrying write deadline %v after release "+
				"(want the zero time) — a cancellation landed behind the clear, so this "+
				"connection would be pooled with a deadline already in the past",
			i, got)
		_ = c.Close()
	}
}

// TestWatchdog_ArmsWhenCtxHasDeadlineAndCancel pins invariant 2 at the arming
// level: a context carrying BOTH a deadline and a cancel — which is precisely
// what client.Do builds from Request.Timeout — must still get a watchdog. An
// earlier either/or version applied the deadline and armed nothing, so
// cancelling such a request released nothing until its deadline expired.
//
// The ctx deadline here is far enough out that the only way a past deadline can
// appear on the socket is the watchdog having been armed.
func TestWatchdog_ArmsWhenCtxHasDeadlineAndCancel(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	armed := c.armDeadline(ctx, writeDeadline)

	require.True(t, armed, "a context with both a deadline and a cancel did not arm the watchdog")
	got, _ := dl.last(writeDeadline)
	require.Truef(t, !got.IsZero() && got.After(time.Now()),
		"write deadline is %v, want the context's own (in the future)", got)
	cancel()
	fired := waitFor(func() bool {
		g, ok := dl.last(writeDeadline)
		return ok && g.Equal(deadlineLongPast)
	}, 5*time.Second)
	got, _ = dl.last(writeDeadline)
	require.Truef(t, fired,
		"write deadline after cancellation is %v, want %v — nothing released the "+
			"blocked call, so it would hang until the context's own deadline",
		got, deadlineLongPast)
	c.releaseDeadline(writeDeadline, armed)
}

// TestWatchdog_OneGoroutinePerConnAcrossManyArmings is the point of the change:
// arming is per call, the goroutine is per connection. A request/response arms
// several times over (head write, body writes, response read, every body
// chunk), and each arming used to cost a goroutine, two channels and two
// closures.
func TestWatchdog_OneGoroutinePerConnAcrossManyArmings(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// One arming first, so the lazily-started goroutine is already counted in
	// the baseline and the test measures growth rather than the constant.
	c.disarmWatch(c.armCancel(ctx, readDeadline))
	runtime.Gosched()
	before := runtime.NumGoroutine()

	for i := 0; i < 500; i++ {
		c.disarmWatch(c.armCancel(ctx, readDeadline))
	}

	// Settle: goroutines left over from earlier tests exit on their own
	// schedule, so a single sample can be off by a few in either direction.
	settled := waitFor(func() bool { return runtime.NumGoroutine() <= before }, 2*time.Second)
	assert.Truef(t, settled,
		"500 armings grew the goroutine count from %d to %d — the watchdog is "+
			"still per call, not per connection", before, runtime.NumGoroutine())
}

// TestWatchdog_ArmDisarmAllocatesNothing is the tripwire on the reported
// symptom: armCancel and armDeadline were 12 MB of the heap profile between
// them, all of it per-call scaffolding (two channels, two closures, and the
// method value each call site passed as the setter). Re-arming one long-lived
// watchdog has to allocate nothing at all, or the change bought nothing.
func TestWatchdog_ArmDisarmAllocatesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation measurement needs repeated runs")
	}
	dl := &deadlineLog{}
	c := NewConn(dl)
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// testify is deliberately ABSENT from these closures: AllocsPerRun measures
	// the whole process, and require/assert reflect and allocate.
	cancelAllocs := testing.AllocsPerRun(200, func() {
		c.disarmWatch(c.armCancel(ctx, readDeadline))
	})
	deadlineAllocs := testing.AllocsPerRun(200, func() {
		c.releaseDeadline(writeDeadline, c.armDeadline(ctx, writeDeadline))
	})

	assert.Zerof(t, cancelAllocs, "armCancel + disarmWatch allocates %v objects per call, want 0", cancelAllocs)
	assert.Zerof(t, deadlineAllocs,
		"armDeadline + releaseDeadline allocates %v objects per call, want 0", deadlineAllocs)
}

// TestWatchdog_ReArmsAfterFiring guards the state machine across a firing: the
// goroutine outlives the arming that tripped it, so a second arming on the same
// connection — a retry on a fresh context, or simply the next call in the same
// exchange — must still be watched. A per-call watchdog got this for free by
// construction; a long-lived one has to loop back correctly.
func TestWatchdog_ReArmsAfterFiring(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	defer func() { _ = c.Close() }()

	fire := func(round int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		armed := c.armCancel(ctx, readDeadline)
		require.Truef(t, armed, "round %d: arming did not take", round)

		cancel()

		fired := waitFor(func() bool {
			got, ok := dl.last(readDeadline)
			return ok && got.Equal(deadlineLongPast)
		}, 5*time.Second)
		got, _ := dl.last(readDeadline)
		require.Truef(t, fired,
			"round %d: read deadline after cancellation is %v, want %v", round, got, deadlineLongPast)
		c.disarmWatch(armed)
		// Clear the trace so the next round cannot pass on this round's set.
		dl.mu.Lock()
		dl.reads = nil
		dl.mu.Unlock()
	}
	fire(1)
	fire(2)
}

// TestWatchdog_CloseRetiresGoroutine pins the other half of the per-connection
// bargain: a goroutine that lives as long as the connection has to die with it.
// Conn.Close is the obligation every discard path already carries (the fd
// depends on it), so nothing new is asked of callers — but Close must actually
// retire the watchdog.
//
// Its exit is observed on wd.gone rather than through runtime.NumGoroutine,
// which cannot tell this goroutine from the ones other tests are winding down.
func TestWatchdog_CloseRetiresGoroutine(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.disarmWatch(c.armCancel(ctx, readDeadline))
	require.True(t, c.wd.started.Load(),
		"the watchdog goroutine never started, so this test proves nothing")
	select {
	case <-c.wd.gone:
		require.Fail(t, "the watchdog left before Close")
	default:
	}

	_ = c.Close()

	select {
	case <-c.wd.gone:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "the watchdog goroutine is still running after Close — it outlived its connection")
	}
}

// TestWatchdog_ArmAfterCloseDoesNotBlock covers the race Close opens: an
// in-flight exchange can arm after Close (a pool discarding a connection under
// a request that is still unwinding). Either outcome is sound — the goroutine
// takes this last arming and leaves after its disarm, or it has already gone
// and the arming reports that it did not take, the socket being closed anyway —
// but neither may wait forever on a channel nobody will receive from.
func TestWatchdog_ArmAfterCloseDoesNotBlock(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start the goroutine, then retire it.
	c.disarmWatch(c.armCancel(ctx, readDeadline))
	_ = c.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.disarmWatch(c.armCancel(ctx, readDeadline))
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "arm/disarm blocked after Close — a call still unwinding on a discarded "+
			"connection must not be held here")
	}
}

// TestWatchdog_ArmOnFreshClosedConnDoesNotBlock is the same race with the
// goroutine never having started: Close on a connection that was never armed
// still has to leave arming non-blocking.
func TestWatchdog_ArmOnFreshClosedConnDoesNotBlock(t *testing.T) {
	c := NewConn(&deadlineLog{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = c.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.disarmWatch(c.armCancel(ctx, readDeadline))
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "arming blocked on a connection closed before it was ever armed")
	}
}

// TestWatchdog_NonCancellableCtxStartsNothing keeps the lazy start honest: a
// connection only ever handed background contexts must not pay for a goroutine
// it can never need.
func TestWatchdog_NonCancellableCtxStartsNothing(t *testing.T) {
	dl := &deadlineLog{}
	c := NewConn(dl)
	defer func() { _ = c.Close() }()

	armed := c.armDeadline(context.Background(), writeDeadline)

	assert.False(t, armed, "a context with no Done channel armed the watchdog")
	assert.False(t, c.wd.started.Load(), "a context with no Done channel started the watchdog goroutine")
	// The deadline handling itself is unchanged: nothing is left latched.
	c.releaseDeadline(writeDeadline, false)
	got, ok := dl.last(writeDeadline)
	assert.Truef(t, ok && got.IsZero(), "write deadline after release is %v (set=%v), want the zero time", got, ok)
}
