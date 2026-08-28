package pool

import "time"

// Observer receives the connection-lifecycle events a pool produces. It is the
// half of a caller's hooks that a pool can raise: a pool dials, closes and
// re-resolves, and knows nothing about requests.
//
// An implementation is installed once and must stay callable for the life of
// the pool, so an implementation whose underlying callbacks can be swapped at
// runtime should read them per call rather than capture them.
//
// Every method may be called from the pool's actor goroutine. A slow
// implementation slows the pool.
type Observer interface {
	// OnDial fires after a dial attempt completes, successfully or not.
	OnDial(DialEvent)
	// OnConnClose fires for every connection the pool closes.
	OnConnClose(ConnCloseEvent)
	// OnResolverUpdate fires when a new address set is applied.
	OnResolverUpdate(ResolverUpdateEvent)
}

// Recorder counts and times the connection work a pool does. It is deliberately
// a set of nullary methods rather than a metrics struct: a pool increments four
// counters and observes two latencies, and naming those six is a smaller
// contract than sharing a type whose other ten fields are about requests.
//
// A nil Recorder is not valid; pools substitute NopRecorder for one.
type Recorder interface {
	DialAttempted()
	DialFailed()
	ConnClosed()
	GoAwayReceived()
	ObserveDial(time.Duration)
	ObserveAcquire(time.Duration)
}

// NopRecorder discards everything. Pools substitute it for a nil Recorder, so
// the recording call sites need no nil check on a path that runs per acquire.
type NopRecorder struct{}

// DialAttempted implements Recorder.
func (NopRecorder) DialAttempted() {}

// DialFailed implements Recorder.
func (NopRecorder) DialFailed() {}

// ConnClosed implements Recorder.
func (NopRecorder) ConnClosed() {}

// GoAwayReceived implements Recorder.
func (NopRecorder) GoAwayReceived() {}

// ObserveDial implements Recorder.
func (NopRecorder) ObserveDial(time.Duration) {}

// ObserveAcquire implements Recorder.
func (NopRecorder) ObserveAcquire(time.Duration) {}

// NopObserver discards every event. Pools substitute it for a nil Observer, so
// the reporting call sites carry no nil check.
type NopObserver struct{}

// OnDial implements Observer.
func (NopObserver) OnDial(DialEvent) {}

// OnConnClose implements Observer.
func (NopObserver) OnConnClose(ConnCloseEvent) {}

// OnResolverUpdate implements Observer.
func (NopObserver) OnResolverUpdate(ResolverUpdateEvent) {}
