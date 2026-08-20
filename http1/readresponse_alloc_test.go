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
// What the remaining thirty are, so nobody hunts for them twice. Measured with
// -memprofilerate=1 over 2000 iterations, which is the only way to get exact
// per-line counts: the default profile is SAMPLED at one object per 512 KB, and
// its per-line figures are extrapolations that disagree with ReportAllocs.
//
//	17   consumeHeaders — the bulk, spread across commitHeaderLine, readLine,
//	     asciiLowerHeaderName and validateFields, one group per header line
//	 1   readLine for the status line
//	 1   strings.SplitN(line, " ", 3) parsing that status line into three parts
//	 1   make([]header.Field, 0, 12) — the field slice itself, which escapes to
//	     the caller through StreamEvent and so cannot be shared or pooled without
//	     answering the ownership question #577 raises for conn
//	~10  the write half and the framework around it
//
// The three that are already gone were the synthesised :status field: the name
// []byte(":status"), the strconv.Itoa string, and the []byte conversion of it.
// They are now statusName and statusValue in conn.go, both allocation-free.
const exchangeAllocCeiling = 30

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
