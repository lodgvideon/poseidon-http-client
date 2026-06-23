//go:build poseidondebug

package client

import (
	"runtime"
	"testing"
	"time"
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
			if what != "test-object" {
				t.Fatalf("leak report = %q, want test-object", what)
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("leak guard did not fire within deadline")
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
	select {
	case what := <-got:
		t.Fatalf("disarmed guard still reported a leak: %q", what)
	default:
	}
}
