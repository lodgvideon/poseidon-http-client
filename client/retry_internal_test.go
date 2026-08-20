package client

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/http3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsIdempotent_Methods(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		req  *Request
		want bool
	}{
		{"GET", &Request{Method: "GET"}, true},
		{"HEAD", &Request{Method: "HEAD"}, true},
		{"OPTIONS", &Request{Method: "OPTIONS"}, true},
		{"PUT", &Request{Method: "PUT"}, true},
		{"DELETE", &Request{Method: "DELETE"}, true},
		{"TRACE", &Request{Method: "TRACE"}, true},
		{"POST", &Request{Method: "POST"}, false},
		{"PATCH", &Request{Method: "PATCH"}, false},
		{"empty", &Request{Method: ""}, false},
		{"force idempotent on POST", &Request{Method: "POST", Idempotency: ForceIdempotent}, true},
		{"force not-idempotent on GET", &Request{Method: "GET", Idempotency: ForceNotIdempotent}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := isIdempotent(c.req)

			assert.Equalf(t, c.want, got,
				"isIdempotent(%+v) = %v, want %v — this decides whether a transport failure "+
					"may be replayed at all", c.req, got, c.want)
		})
	}
}

func TestBuiltinShouldRetry(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"REFUSED_STREAM", &StreamResetError{Code: frame.ErrCodeRefusedStream}, true},
		{"RST CANCEL", &StreamResetError{Code: frame.ErrCodeCancel}, false},
		{"RST INTERNAL_ERROR", &StreamResetError{Code: frame.ErrCodeInternalError}, true},
		{"RST ENHANCE_YOUR_CALM", &StreamResetError{Code: frame.ErrCodeEnhanceYourCalm}, true},
		{"RST PROTOCOL_ERROR", &StreamResetError{Code: frame.ErrCodeProtocolError}, false},
		{"conn.ErrGoAway", conn.ErrGoAway, true},
		{"DialError", &DialError{Addr: "x", Err: errors.New("boom")}, true},
		{"ErrDialBackoff", ErrDialBackoff, true},
		{"ErrPoolClosed", ErrPoolClosed, false},
		{"ErrClosed", ErrClosed, false},
		{"ErrInvalidRequest", ErrInvalidRequest, false},
		{"random error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got := builtinShouldRetry(c.err)

			assert.Equalf(t, c.want, got,
				"builtinShouldRetry(%v) = %v, want %v — classifying a request the server may "+
					"have processed as retryable duplicates its effect", c.err, got, c.want)
		})
	}
}

func TestDefaultBackoff_Bounds(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))

	zero := defaultBackoff(0, rng)
	first := make([]time.Duration, 100)
	for i := range first {
		first[i] = defaultBackoff(1, rng)
	}
	overflowed := make([]time.Duration, 100)
	for i := range overflowed {
		overflowed[i] = defaultBackoff(20, rng)
	}

	assert.Zerof(t, zero,
		"defaultBackoff(0) = %v, want 0 — the first attempt must not be delayed", zero)
	for i, got := range first {
		require.GreaterOrEqualf(t, got, 75*time.Millisecond,
			"defaultBackoff(1) sample %d = %v, want in [75ms,125ms]", i, got)
		require.LessOrEqualf(t, got, 125*time.Millisecond,
			"defaultBackoff(1) sample %d = %v, want in [75ms,125ms]", i, got)
	}
	for i, got := range overflowed {
		require.GreaterOrEqualf(t, got, 3750*time.Millisecond,
			"defaultBackoff(20) sample %d = %v, want in [3.75s,6.25s] — the bit shift "+
				"overflows here and must clamp to the 5s cap, not wrap to zero", i, got)
		require.LessOrEqualf(t, got, 6250*time.Millisecond,
			"defaultBackoff(20) sample %d = %v, want in [3.75s,6.25s]", i, got)
	}
}

func TestNewRetryer_Defaults(t *testing.T) {
	t.Parallel()
	c := &Client{} // zero Client; we only inspect the Retryer fields here

	r := NewRetryer(c, RetryOptions{})

	assert.Equalf(t, 3, r.opts.MaxAttempts,
		"MaxAttempts default = %d, want 3 — a zero budget must mean the documented "+
			"default, not 'never retry'", r.opts.MaxAttempts)
	require.NotNil(t, r.opts.Backoff, "Backoff default = nil, want non-nil")
	// Smoke-test the default backoff returns a non-zero duration for attempt >= 1;
	// this exercises the rng path that NewRetryer wires up.
	assert.Positivef(t, r.opts.Backoff(1),
		"Backoff(1) must be > 0 — a zero backoff retries instantly and stampedes the peer")
}

func TestNewRetryer_PreservesNonZero(t *testing.T) {
	t.Parallel()
	c := &Client{}
	custom := func(int) time.Duration { return 0 }

	r := NewRetryer(c, RetryOptions{MaxAttempts: 7, Backoff: custom})

	assert.Equalf(t, 7, r.opts.MaxAttempts, "MaxAttempts = %d, want 7", r.opts.MaxAttempts)
	assert.NotNil(t, r.opts.Backoff,
		"Backoff was overwritten — a caller's explicit schedule must survive defaulting")
}

// fakeDoer is a scriptable retryDoer used by retry-loop tests.
// Bounds-checks every call; unexpected extra calls fail the test
// immediately rather than panicking with index-out-of-range.
type fakeDoer struct {
	t       testing.TB
	results []doResult
	stream  []streamResult
	calls   int
	streams int
	// attempts records the attempt number the loop passed on each call, in
	// order. It is what proves a replay is reported as a replay rather than as
	// a fresh request.
	attempts []int
}

type doResult struct {
	resp *Response
	err  error
}
type streamResult struct {
	resp *StreamResponse
	err  error
}

func (f *fakeDoer) doAttempt(_ context.Context, _ *Request, resp *Response, attempt int) error {
	require.Lessf(f.t, f.calls, len(f.results),
		"unexpected Do call #%d (only %d results scripted)", f.calls, len(f.results))
	r := f.results[f.calls]
	f.calls++
	f.attempts = append(f.attempts, attempt)
	if r.resp != nil {
		*resp = *r.resp
	}
	return r.err
}

func (f *fakeDoer) doStreamAttempt(_ context.Context, _ *Request, sr *StreamResponse, attempt int) error {
	require.Lessf(f.t, f.streams, len(f.stream),
		"unexpected DoStream call #%d (only %d results scripted)", f.streams, len(f.stream))
	r := f.stream[f.streams]
	f.streams++
	f.attempts = append(f.attempts, attempt)
	if r.resp != nil {
		// Copy only exported fields — StreamResponse contains sync.Once (noCopy).
		sr.Status = r.resp.Status
		sr.Headers = r.resp.Headers
	}
	return r.err
}

// newFakeRetryer wires a Retryer to f with an instant backoff by default, so
// loop tests measure the retry DECISION rather than the sleep schedule.
func newFakeRetryer(f *fakeDoer, opts RetryOptions) *Retryer {
	if opts.Backoff == nil {
		opts.Backoff = func(int) time.Duration { return 0 }
	}
	r := NewRetryer(&Client{}, opts)
	r.d = f
	return r
}

func TestRetryer_Do_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil}, // never reached
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"}, &res)

	require.Error(t, err, "expected error on POST + RST, got nil")
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — a non-idempotent request must not be replayed however "+
			"retryable the transport error looks", f.calls)
}

func TestRetryer_Do_BodyReader_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{
		Method:     "GET",
		Path:       "/",
		BodyReader: errReader{},
	}, &res)

	require.Error(t, err, "expected error, got nil")
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — a streaming body cannot be rewound, so BodyReader must "+
			"disable retry even for an idempotent method", f.calls)
}

func TestRetryer_Do_MaxAttemptsOne_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 1})
	var res Response

	_ = r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — a budget of exactly 1 is the boundary at which retry turns off",
		f.calls)
}

// errReader is a placeholder io.Reader for BodyReader tests; never
// actually read because retry skips before issuing the request.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("not read") }

func TestRetryer_Do_RefusedStream_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v, want nil after retry", err)
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	assert.Equalf(t, 3, f.calls,
		"calls = %d, want 3 — two REFUSED_STREAMs must each buy another attempt", f.calls)
}

func TestRetryer_Do_GoAway_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, conn.ErrGoAway},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v, want nil after GOAWAY retry", err)
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	assert.Equalf(t, 2, f.calls,
		"calls = %d, want 2 — GOAWAY says the request was not processed, so it is replayable",
		f.calls)
}

// TestRetryer_Do_H3RequestRejected_Retries drives the retry loop with an
// *http3.StreamResetError{H3_REQUEST_REJECTED} surfaced from the H3 Do path
// (as h3Exchange.Recv returns it verbatim). The Retryer must retry, exactly
// like the H2 REFUSED_STREAM / conn.ErrGoAway equivalents.
func TestRetryer_Do_H3RequestRejected_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &http3.StreamResetError{Code: http3.H3RequestRejected}},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v, want nil after H3 retry", err)
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	assert.Equalf(t, 2, f.calls,
		"calls = %d, want 2 — H3_REQUEST_REJECTED is the H3 twin of REFUSED_STREAM", f.calls)
}

// TestRetryer_Do_H3ResetNonRejected_Stops confirms a non-rejected H3 reset
// (which may have had application side effects) stops the loop after one call.
func TestRetryer_Do_H3ResetNonRejected_Stops(t *testing.T) {
	t.Parallel()
	rst := &http3.StreamResetError{Code: http3.H3RequestCancelled}
	f := &fakeDoer{t: t, results: []doResult{{nil, rst}}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.ErrorIsf(t, err, rst, "err = %v, want %v", err, rst)
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — H3_REQUEST_CANCELLED may have had application side effects, "+
			"so only H3_REQUEST_REJECTED earns a replay", f.calls)
}

// TestRetryer_Do_H3GoAway_Retries drives the loop with http3.ErrGoAway (the
// H3 analogue of conn.ErrGoAway): the server is going away and did not process
// the request, so it is retried on a fresh attempt.
func TestRetryer_Do_H3GoAway_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, http3.ErrGoAway},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v, want nil after H3 GOAWAY retry", err)
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	assert.Equalf(t, 2, f.calls, "calls = %d, want 2", f.calls)
}

// TestRetryer_Do_H3RequestRejected_NonIdempotent_NoRetry confirms the
// idempotency gate still applies to H3 errors: a POST is not retried even
// though H3_REQUEST_REJECTED is a retryable transport error.
func TestRetryer_Do_H3RequestRejected_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &http3.StreamResetError{Code: http3.H3RequestRejected}},
		{&Response{Status: 200}, nil}, // never reached
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"}, &res)

	require.Error(t, err, "expected error on POST + H3 reset, got nil")
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — the idempotency gate outranks the H3 classifier", f.calls)
}

// TestRetryer_Do_5xxStatus_NotRetried confirms a real 5xx HTTP status is a
// successful response (nil transport error), not a transport failure, so the
// default Retryer (no user IsRetryable) does not retry it.
func TestRetryer_Do_5xxStatus_NotRetried(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{&Response{Status: 503}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do err = %v, want nil (5xx is a response, not a transport error)", err)
	assert.Equalf(t, 503, resp.Status,
		"Status = %d, want 503 — the response must reach the caller unaltered", resp.Status)
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — retrying a 5xx by default would silently multiply load on a "+
			"struggling server", f.calls)
}

func TestRetryer_Do_DialError_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &DialError{Addr: "x:1", Err: errors.New("boom")}},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "Do err = %v, want nil after dial retry", err)
	assert.Equalf(t, 2, f.calls,
		"calls = %d, want 2 — a dial that never connected cannot have been processed", f.calls)
}

func TestRetryer_Do_ErrDialBackoff_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, ErrDialBackoff},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "Do err = %v, want nil after ErrDialBackoff retry", err)
	assert.Equalf(t, 2, f.calls, "calls = %d, want 2", f.calls)
}

func TestRetryer_Do_NonRetryableError_Stops(t *testing.T) {
	t.Parallel()
	other := errors.New("application error")
	f := &fakeDoer{t: t, results: []doResult{{nil, other}}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.ErrorIsf(t, err, other, "err = %v, want %v", err, other)
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — an unclassified error says nothing about whether the server "+
			"processed the request, so it must not be replayed", f.calls)
}

func TestRetryer_Do_IsRetryable_Custom5xx_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{&Response{Status: 503}, nil},
		{&Response{Status: 503}, nil},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		IsRetryable: func(_ error, resp *Response) bool {
			return resp != nil && resp.Status >= 500
		},
	})
	var resp Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "err = %v, want nil", err)
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	assert.Equalf(t, 3, f.calls,
		"calls = %d, want 3 — a user predicate must be consulted on SUCCESSFUL responses too, "+
			"not only on errors", f.calls)
}

func TestRetryer_Do_IsRetryable_NonBuiltinError_Retries(t *testing.T) {
	t.Parallel()
	custom := errors.New("custom transient")
	f := &fakeDoer{t: t, results: []doResult{
		{nil, custom},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		IsRetryable: func(err error, _ *Response) bool { return errors.Is(err, custom) },
	})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "err = %v, want nil after IsRetryable retry", err)
	assert.Equalf(t, 2, f.calls,
		"calls = %d, want 2 — the user predicate must extend the built-in classification", f.calls)
}

func TestRetryer_Do_CtxCanceled_StopsImmediately(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
	}}
	// Backoff long enough that ctx cancel must take the select.
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 5 * time.Second },
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	var res Response

	start := time.Now()
	err := r.Do(ctx, &Request{Method: "GET", Path: "/"}, &res)
	elapsed := time.Since(start)

	t.Logf("timings: backoff=5s, cancel at 50ms, Do returned after %v", elapsed)
	require.ErrorIsf(t, err, context.Canceled, "err = %v, want context.Canceled", err)
	assert.Lessf(t, elapsed, time.Second,
		"returned in %v, want <1s — a cancelled context must wake the backoff sleep instead "+
			"of holding the caller for the full 5s window", elapsed)
}

func TestRetryer_Do_HardStop_PoolClosed_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, ErrPoolClosed},
		{&Response{Status: 200}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		IsRetryable: func(error, *Response) bool { return true }, // even with this, stop
	})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.ErrorIsf(t, err, ErrPoolClosed, "err = %v, want ErrPoolClosed", err)
	assert.Equalf(t, 1, f.calls,
		"calls = %d, want 1 — a hard stop must outrank an over-eager user predicate, or a "+
			"closed pool spins the loop to exhaustion", f.calls)
}

func TestRetryer_Do_MaxAttempts_Exhausted(t *testing.T) {
	t.Parallel()
	last := &StreamResetError{Code: frame.ErrCodeRefusedStream}
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, last},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var res Response

	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &res)

	require.Samef(t, last, err,
		"err = %v, want the LAST attempt's error (%v) — surfacing an earlier one would "+
			"misreport why the request finally failed", err, last)
	assert.Equalf(t, 3, f.calls,
		"calls = %d, want 3 — the budget is a total attempt count, not a retry count", f.calls)
}

func TestRetryer_DoStream_RetriesBeforeHeaders(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var sr StreamResponse

	err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr)

	require.NoError(t, err, "DoStream err = %v, want nil", err)
	assert.Equalf(t, 2, f.streams,
		"streams = %d, want 2 — a reset before any HEADERS is still replayable", f.streams)
}

func TestRetryer_DoStream_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := newFakeRetryer(f, RetryOptions{MaxAttempts: 3})
	var sr StreamResponse

	err := r.DoStream(context.Background(), &Request{Method: "POST", Path: "/"}, &sr)

	require.Error(t, err, "expected err on POST + RST, got nil")
	assert.Equalf(t, 1, f.streams,
		"streams = %d, want 1 — the idempotency gate applies to the streaming path too",
		f.streams)
}

func TestRetryer_DoStream_Success_NoIsRetryableCall(t *testing.T) {
	t.Parallel()
	var called bool
	f := &fakeDoer{t: t, stream: []streamResult{{&StreamResponse{}, nil}}}
	r := newFakeRetryer(f, RetryOptions{
		MaxAttempts: 3,
		IsRetryable: func(error, *Response) bool {
			called = true
			return true
		},
	})
	var sr StreamResponse

	err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr)

	require.NoError(t, err, "DoStream err = %v", err)
	assert.False(t, called,
		"IsRetryable invoked for a successful DoStream — a successful return hands stream "+
			"ownership to the caller, so re-issuing would abandon a live stream")
}

// TestNewRetryer_DefaultBackoff_GoroutineSafe pins the goroutine-safety contract
// advertised by NewRetryer's doc-comment. Without the closure-internal mutex,
// the default backoff's rng.Int63n call would race under -race when called from
// multiple goroutines.
//
// The race detector is the oracle here, so mutation cannot validate this test:
// the unguarded version passes almost always WITHOUT -race, which is precisely
// the defect. The bounds check below is not the point — it only keeps the run
// from being wholly vacuous when the suite runs without the detector.
func TestNewRetryer_DefaultBackoff_GoroutineSafe(t *testing.T) {
	t.Parallel()
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	const goroutines, perGoroutine = 50, 4
	got := make([]time.Duration, goroutines*perGoroutine)
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= perGoroutine; j++ {
				got[i*perGoroutine+j-1] = r.opts.Backoff(j)
			}
		}(i)
	}
	wg.Wait()

	for i, d := range got {
		require.Positivef(t, d,
			"sample %d = %v; every attempt >= 1 must produce a positive backoff, and a "+
				"zero here means the shared rng was torn by a concurrent caller", i, d)
	}
}
