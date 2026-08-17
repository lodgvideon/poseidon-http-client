//go:build !race

package http1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// sinkConn and sinkAddr live in reqheader_emit_test.go, which carries no build
// tag: this file is !race-only, so defining them here would leave the untagged
// tests referencing an identifier that does not exist in a -race build.

// canonicalFields is the shape a real caller sends: header names spelled the way
// RFC 9110 spells them, with capitals. The all-lower-case spelling takes a
// different path through the emit loop, so a benchmark using it would miss the
// cost entirely.
func canonicalFields() []header.Field {
	return []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/api/v1/resource")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte("Accept"), Value: []byte("application/json")},
		{Name: []byte("User-Agent"), Value: []byte("poseidon/1.0")},
		{Name: []byte("Accept-Encoding"), Value: []byte("gzip")},
		{Name: []byte("Authorization"), Value: []byte("Bearer abcdefghijklmnop")},
		{Name: []byte("X-Request-Id"), Value: []byte("0123456789abcdef")},
	}
}

// BenchmarkWriteRequest_CanonicalHeaders measures the request head assembly that
// http1 performs per request. http1 is outside the bench-gate's package list, so
// nothing else in the repo reports this number.
func BenchmarkWriteRequest_CanonicalHeaders(b *testing.B) {
	c := http1.NewConn(sinkConn{})
	fields := canonicalFields()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := c.NewExchange()
		if err := ex.WriteRequest(ctx, fields, true); err != nil {
			b.Fatalf("WriteRequest: %v", err)
		}
	}
}

// TestWriteRequest_CaseFoldingCostsNoAllocations is the gate. http1 is outside
// the bench-gate's package list, so before this nothing in the repo would notice
// the request head growing an allocation per header.
//
// It pins the DIFFERENCE between canonical and lower-case spelling rather than
// an absolute count, which is the property that actually matters and does not go
// stale when something unrelated in WriteRequest changes: folding a name must
// cost nothing, however many allocations the rest of the path makes.
//
// Before the byte-wise fold this measured 16 against 11 — one allocation for each
// of the five capitalised names, from string(f.Name) plus strings.ToLower.
// The name ends in DoesNotAllocate deliberately: that is one of the alternatives
// in the CI allocation-gate step's -run pattern, and a !race test whose name
// misses it never runs there. See the comment on that step in ci.yml.
func TestWriteRequest_CaseFoldingDoesNotAllocate(t *testing.T) {
	ctx := context.Background()

	// testify is deliberately ABSENT from this closure: AllocsPerRun measures the
	// whole process, and require/assert reflect and allocate, so an assertion
	// inside would be charged to the request assembly it is meant to be judging.
	measure := func(fields []header.Field) float64 {
		c := http1.NewConn(sinkConn{})
		return testing.AllocsPerRun(200, func() {
			ex := c.NewExchange()
			if err := ex.WriteRequest(ctx, fields, true); err != nil {
				t.Fatalf("WriteRequest: %v", err)
			}
		})
	}

	canonical := canonicalFields()
	lower := canonicalFields()
	capitals := 0
	for i := range lower {
		for j, ch := range lower[i].Name {
			if ch >= 'A' && ch <= 'Z' {
				lower[i].Name[j] = ch + 'a' - 'A'
				capitals++
			}
		}
	}
	require.NotZero(t, capitals, "the canonical fixture has no upper-case letter, so this test "+
		"compares a request with itself and cannot fail")

	got, want := measure(canonical), measure(lower)

	assert.Equalf(t, want, got,
		"canonical header names cost %.1f allocations against %.1f for the "+
			"same request already lower-cased: folding a name is allocating again.\n"+
			"string(f.Name) and strings.ToLower are what this path avoids; see "+
			"appendASCIILower in conn.go.", got, want)
}

// BenchmarkWriteRequest_LowerCaseHeaders is the control: the same request with
// every name already lower-case. The gap between the two benchmarks is what
// case-folding costs, and it should be nothing.
func BenchmarkWriteRequest_LowerCaseHeaders(b *testing.B) {
	c := http1.NewConn(sinkConn{})
	fields := canonicalFields()
	for i := range fields {
		for j, ch := range fields[i].Name {
			if ch >= 'A' && ch <= 'Z' {
				fields[i].Name[j] = ch + 'a' - 'A'
			}
		}
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := c.NewExchange()
		if err := ex.WriteRequest(ctx, fields, true); err != nil {
			b.Fatalf("WriteRequest: %v", err)
		}
	}
}
