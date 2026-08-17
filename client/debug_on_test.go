//go:build poseidondebug

package client

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests run only under `-tags poseidondebug` (the build that compiles in
// the leak detector). Normal `go test ./...` excludes this file entirely.

// TestLeakGuard_FiresOnGCWithoutClose verifies an armed guard that is
// garbage-collected before disarm() (i.e. Close was never called) reports.
func TestLeakGuard_FiresOnGCWithoutClose(t *testing.T) {
	got := make(chan string, 1)
	orig := leakReport
	leakReport = func(what string) {
		select {
		case got <- what:
		default:
		}
	}
	defer func() { leakReport = orig }()

	// Arm in a child frame and drop the reference without disarming.
	func() { _ = armLeakGuard("test-object") }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		select {
		case what := <-got:
			require.Equalf(t, "test-object", what, "leak report = %q, want test-object", what)
			return
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			require.Fail(t, "leak guard did not fire within deadline")
		}
	}
}

// TestLeakGuard_SilentWhenDisarmed verifies disarm() (called by Close)
// suppresses the report.
func TestLeakGuard_SilentWhenDisarmed(t *testing.T) {
	got := make(chan string, 1)
	orig := leakReport
	leakReport = func(what string) {
		select {
		case got <- what:
		default:
		}
	}
	defer func() { leakReport = orig }()

	func() {
		g := armLeakGuard("disarmed-object")
		g.disarm()
	}()

	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}

	var reported string
	fired := false
	select {
	case reported = <-got:
		fired = true
	default:
	}
	require.Falsef(t, fired, "disarmed guard still reported a leak: %q", reported)
}
