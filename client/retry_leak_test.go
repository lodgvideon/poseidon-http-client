package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldBody is a stand-in for a streaming response body: Close records that
// it ran, the way responseBodyReader.Close returns the pooled connection.
type heldBody struct{ closed *int }

func (c heldBody) Read([]byte) (int, error) { return 0, io.EOF }
func (c heldBody) Close() error             { *c.closed++; return nil }

// TestRetryer_doLoop_ReleasesHeldResponseOnCancelDuringBackoff pins that a
// retryable streaming response is released when the loop bails on a cancelled
// backoff. canRetry excludes a request BodyReader but not a streaming RESPONSE
// (BodyMode=BodyStream), so the previous attempt's resp.BodyReader owns the
// pooled connection until Reset/Close closes it. Resetting only after the backoff
// sleep leaked that connection whenever the sleep returned early on a cancelled
// context — the loop's `return err` skipped the reset, and MaxConnsPerHost being
// the whole budget, later callers eventually starved.
//
// The leak is only expressible if the fixture actually hands the loop a response
// holding a resource AND actually cancels during the backoff, so both are
// counted rather than assumed: backoffCalls proves the cancel injection fired,
// and closed is the observable the fix is about. A run where the backoff was
// never reached would leave both at zero and pass exactly like a real fix.
func TestRetryer_doLoop_ReleasesHeldResponseOnCancelDuringBackoff(t *testing.T) {
	var closed, backoffCalls int
	// Attempt 0 succeeds with a streaming-style response holding a resource; the
	// predicate then asks for a retry (e.g. retry a 503).
	resp0 := &Response{Status: 503, BodyReader: heldBody{&closed}}
	f := &fakeDoer{t: t, results: []doResult{{resp0, nil}}}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Retryer{
		d: f,
		opts: RetryOptions{
			MaxAttempts: 2,
			IsRetryable: func(error, *Response) bool { return true },
			// Cancel the context as the backoff is computed, so sleepBackoff
			// returns ctx.Err() before the next attempt runs.
			Backoff: func(int) time.Duration {
				backoffCalls++
				cancel()
				return time.Hour
			},
		},
	}
	var resp Response

	err := r.doLoop(ctx, &Request{Method: "GET", Path: "/"}, &resp)

	t.Logf("injections: backoff computed %d time(s), BodyReader.Close called %d time(s)",
		backoffCalls, closed)
	require.Equalf(t, 1, backoffCalls,
		"the backoff was computed %d times, want 1 — the cancel is injected from inside it, "+
			"so a zero here means the loop never reached the backoff and this test proved "+
			"nothing", backoffCalls)
	require.Truef(t, errors.Is(err, context.Canceled),
		"doLoop err = %v, want context.Canceled", err)
	assert.Equalf(t, 1, closed,
		"BodyReader.Close called %d times, want 1 — a retryable streaming response must be "+
			"released before the loop bails on a cancelled backoff, or its pooled "+
			"connection leaks", closed)
}
