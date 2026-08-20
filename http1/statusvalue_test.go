package http1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStatusValue_IsSharedAndCapped pins the two properties that make the shared
// status table safe to hand to a caller. This test is internal to the package
// because statusValue is unexported and must stay that way — a test-only export
// would widen the public surface for nothing.
//
// The digits must be right for every code a server can send, and the returned
// slice must have no spare capacity: it aliases a package-level array shared by
// every response on every connection, so an append by any consumer has to
// reallocate rather than overwrite the next code's digits.
func TestStatusValue_IsSharedAndCapped(t *testing.T) {
	for _, code := range []int{100, 200, 204, 301, 404, 418, 500, 599, 999} {
		v := statusValue(code)

		want := string([]byte{byte('0' + code/100), byte('0' + (code/10)%10), byte('0' + code%10)})
		assert.Equalf(t, want, string(v), "statusValue(%d) = %q, want %q", code, v, want)
		assert.Equalf(t, 3, cap(v),
			"statusValue(%d) has cap %d, want 3: an append by a consumer would "+
				"scribble over the next status code's digits in the shared table", code, cap(v))
	}

	// Two calls for the same code must hand back the same backing array — that is
	// what makes it free — and adjacent codes must not collide.
	a, b := statusValue(404), statusValue(404)

	assert.Same(t, &a[0], &b[0], "statusValue(404) returned a fresh slice: the table is not being shared")
	assert.NotEqual(t, string(statusValue(200)), string(statusValue(201)),
		"adjacent status codes render identically")

	// Out of range falls back to the allocating path rather than indexing past the
	// table. readStatusLine cannot produce these, but the bound is what makes that
	// a fact about the caller rather than a load-bearing assumption here.
	outOfRange := map[int]string{-1: "-1", 0: "0", 99: "99", 1000: "1000", 1 << 20: "1048576"}
	for code, want := range outOfRange {
		got := string(statusValue(code))

		assert.Equalf(t, want, got, "statusValue(%d) = %q, want %q", code, got, want)
	}
}
