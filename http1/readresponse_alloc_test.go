//go:build !race

package http1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// replayConn, benchResponse and benchRequestFields live in
// readresponse_bench_test.go, which carries no build tag: this file is
// !race-only, so defining them here would leave the untagged tests referencing
// identifiers that do not exist in a -race build.

// exchangeAllocCeiling is what one complete keep-alive exchange costs — write the
// request head, read the response head — and both directions are errors. Above
// it, a per-exchange allocation came back; below it, the path improved and the
// win is not locked in until this drops.
//
// http1 is outside the bench-gate's package list, so nothing else in the repo
// reports this number and only this gate defends it.
//
// What the remaining twenty-five are, so nobody hunts for them twice. Measured
// with -memprofilerate=1 over 2000 iterations, which is the only way to get exact
// per-line counts: the default profile is SAMPLED at one object per 512 KB, and
// its per-line figures are extrapolations that disagree with ReportAllocs.
//
//	6.0  readLine — one string per line read, header lines and the status line
//	5.0  commitHeaderLine — one slab per field, holding name and value together
//	4.0  asciiLowerHeaderName — only for a header whose name arrives with
//	     uppercase; lowering happens in place otherwise
//	2.0  WriteRequest, 2.0 validateFields — the write half
//	1.1  strings.SplitN(line, " ", 3) parsing the status line into three parts
//	1.0  make([]header.Field, 0, 12) — the field slice itself, which escapes to
//	     the caller through StreamEvent and so cannot be shared or pooled without
//	     answering the ownership question #577 raises for conn
//	     — plus the framework around the benchmark
//
// Do not try to make that table sum to 25. The profile's own total runs about
// five below ReportAllocs in both directions (24.2 against 30 before this
// change, 21.2 against 25 after), because the two count different things —
// ReportAllocs counts mallocgc, the profile counts what it sampled and
// attributed. The table is for finding a site, not for arithmetic; the gate's
// number is ReportAllocs and that is the one to trust.
//
// Five went in the #630 slab. commitHeaderLine built the field with
// `Name: []byte(name), Value: []byte(value)`, measured at 5.0 and 4.0 allocs/op
// on those two lines; both are substrings of the same logical line, so they now
// share one backing array and cost 5.0 together.
//
// Three had gone before that: the synthesised :status field's name
// []byte(":status"), the strconv.Itoa string and the []byte conversion of it.
// They are statusName and statusValue in conn.go now, both allocation-free.
//
// The next one worth taking is readLine's 6.0. It is harder than it looks: the
// string has to outlive the read because the NEXT line may be an obs-fold
// continuation of it, so it cannot simply borrow the reader's buffer.
const exchangeAllocCeiling = 25

func TestReadResponse_AllocsPerExchange(t *testing.T) {
	c := http1.NewConn(&replayConn{script: []byte(benchResponse)})
	fields := benchRequestFields()
	ctx := context.Background()
	// One exchange outside the count, so nothing one-time is charged to the
	// steady state.
	ex := c.NewExchange()
	require.NoError(t, ex.WriteRequest(ctx, fields, true), "WriteRequest")
	_, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")

	// testify is deliberately ABSENT from this closure: AllocsPerRun measures the
	// whole process, and require/assert reflect and allocate, so an assertion here
	// would be counted as part of what it is judging. The hand-rolled t.Fatalf is
	// the only form that costs nothing when it does not fire.
	got := testing.AllocsPerRun(2000, func() {
		ex := c.NewExchange()
		if err := ex.WriteRequest(ctx, fields, true); err != nil {
			t.Fatalf("WriteRequest: %v", err)
		}
		if _, _, err := ex.ReadResponse(ctx); err != nil {
			t.Fatalf("ReadResponse: %v", err)
		}
	})

	assert.LessOrEqualf(t, int(got), exchangeAllocCeiling,
		"one exchange allocates %.0f, ceiling %d: a per-exchange allocation "+
			"came back on the HTTP/1.1 request/response hot path", got, exchangeAllocCeiling)
	assert.GreaterOrEqualf(t, int(got), exchangeAllocCeiling,
		"one exchange allocates %.0f, below the ceiling of %d: the path "+
			"improved — lower exchangeAllocCeiling to %.0f to lock the win in",
		got, exchangeAllocCeiling, got)
}
