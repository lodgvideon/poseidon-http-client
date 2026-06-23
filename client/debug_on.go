//go:build poseidondebug

package client

import (
	"log"
	"runtime"
	"sync/atomic"
)

// leakGuard catches a StreamResponse or Response.BodyReader that is
// garbage-collected without Close() — which silently leaks the pooled
// connection's stream slot (blocking idle eviction and graceful drain, and
// eventually starving the pool under sustained load). It is compiled in only
// under `-tags poseidondebug`; the production build uses the no-op stub in
// debug_off.go, so this dev-only safety net costs nothing in shipping binaries.
//
// Finalizers are deliberately confined to this build tag (they are unreliable
// as a production mechanism and add GC pressure) — here they turn the silent
// leak into a loud, attributable log line in tests and dev runs.
type leakGuard struct {
	what     string
	disarmed atomic.Bool
}

// leakReport receives the description of a leaked (GC'd-without-Close) object.
// Overridable in tests; defaults to a log line.
var leakReport = func(what string) {
	log.Printf("poseidon/client: LEAK — %s was garbage-collected without Close(); "+
		"its pooled stream slot was never released", what)
}

// armLeakGuard returns a guard whose finalizer reports a leak unless disarm is
// called first (i.e. unless Close ran).
func armLeakGuard(what string) *leakGuard {
	g := &leakGuard{what: what}
	runtime.SetFinalizer(g, func(g *leakGuard) {
		if !g.disarmed.Load() {
			leakReport(g.what)
		}
	})
	return g
}

// disarm marks the guard as properly closed and clears its finalizer.
func (g *leakGuard) disarm() {
	if g == nil {
		return
	}
	g.disarmed.Store(true)
	runtime.SetFinalizer(g, nil)
}
