package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
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
func TestRetryer_doLoop_ReleasesHeldResponseOnCancelDuringBackoff(t *testing.T) {
	var closed int
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
				cancel()
				return time.Hour
			},
		},
	}

	var resp Response
	err := r.doLoop(ctx, &Request{Method: "GET", Path: "/"}, &resp)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("doLoop err = %v, want context.Canceled", err)
	}
	if closed != 1 {
		t.Fatalf("BodyReader.Close called %d times, want 1 — a retryable streaming "+
			"response must be released before the loop bails on a cancelled backoff, "+
			"or its pooled connection leaks", closed)
	}
}
