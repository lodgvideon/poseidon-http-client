//go:build !poseidondebug

package client

// leakGuard is a no-op in normal builds. The unclosed-stream leak detector is
// compiled in only under the "poseidondebug" build tag (see debug_on.go); in
// production a stream carries one nil *leakGuard field and an inlined no-op
// disarm call — zero allocations, zero cost.
type leakGuard struct{}

// armLeakGuard returns nil in normal builds.
func armLeakGuard(string) *leakGuard { return nil }

// disarm is a no-op (safe on the nil receiver armLeakGuard returns).
func (*leakGuard) disarm() {}
