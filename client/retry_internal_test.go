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
			if got := isIdempotent(c.req); got != c.want {
				t.Errorf("isIdempotent(%+v) = %v, want %v", c.req, got, c.want)
			}
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
			if got := builtinShouldRetry(c.err); got != c.want {
				t.Errorf("builtinShouldRetry(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestDefaultBackoff_Bounds(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(42))
	if got := defaultBackoff(0, rng); got != 0 {
		t.Errorf("defaultBackoff(0) = %v, want 0", got)
	}
	for i := 0; i < 100; i++ {
		got := defaultBackoff(1, rng)
		if got < 75*time.Millisecond || got > 125*time.Millisecond {
			t.Fatalf("defaultBackoff(1) = %v, want in [75ms,125ms]", got)
		}
	}
	for i := 0; i < 100; i++ {
		got := defaultBackoff(20, rng)
		if got < 3750*time.Millisecond || got > 6250*time.Millisecond {
			t.Fatalf("defaultBackoff(20) = %v, want in [3.75s,6.25s]", got)
		}
	}
}

func TestNewRetryer_Defaults(t *testing.T) {
	t.Parallel()
	c := &Client{} // zero Client; we only inspect the Retryer fields here
	r := NewRetryer(c, RetryOptions{})
	if r.opts.MaxAttempts != 3 {
		t.Errorf("MaxAttempts default = %d, want 3", r.opts.MaxAttempts)
	}
	if r.opts.Backoff == nil {
		t.Error("Backoff default = nil, want non-nil")
	}
	// Smoke-test the default backoff returns a non-zero duration for
	// attempt >= 1; this exercises the rng path that NewRetryer wires up.
	if d := r.opts.Backoff(1); d <= 0 {
		t.Errorf("Backoff(1) = %v, want > 0", d)
	}
}

func TestNewRetryer_PreservesNonZero(t *testing.T) {
	t.Parallel()
	c := &Client{}
	custom := func(int) time.Duration { return 0 }
	r := NewRetryer(c, RetryOptions{MaxAttempts: 7, Backoff: custom})
	if r.opts.MaxAttempts != 7 {
		t.Errorf("MaxAttempts = %d, want 7", r.opts.MaxAttempts)
	}
	if r.opts.Backoff == nil {
		t.Error("Backoff was overwritten")
	}
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
	if f.calls >= len(f.results) {
		f.t.Fatalf("unexpected Do call #%d (only %d results scripted)", f.calls, len(f.results))
	}
	r := f.results[f.calls]
	f.calls++
	f.attempts = append(f.attempts, attempt)
	if r.resp != nil {
		*resp = *r.resp
	}
	return r.err
}

func (f *fakeDoer) doStreamAttempt(_ context.Context, _ *Request, sr *StreamResponse, attempt int) error {
	if f.streams >= len(f.stream) {
		f.t.Fatalf("unexpected DoStream call #%d (only %d results scripted)", f.streams, len(f.stream))
	}
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

func TestRetryer_Do_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil}, // never reached
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"}, &_res)
	if err == nil {
		t.Fatal("expected error on POST + RST, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-idempotent must not retry)", f.calls)
	}
}

func TestRetryer_Do_BodyReader_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{
		Method:     "GET",
		Path:       "/",
		BodyReader: errReader{},
	}, &_res)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (BodyReader must disable retry)", f.calls)
	}
}

func TestRetryer_Do_MaxAttemptsOne_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 1})
	r.d = f
	var _res Response
	_ = r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res)
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (MaxAttempts=1 disables retry)", f.calls)
	}
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
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var resp Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do err = %v, want nil after retry", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200", resp.Status)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}

func TestRetryer_Do_GoAway_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, conn.ErrGoAway},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var resp Response
	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)
	if err != nil || resp.Status != 200 {
		t.Fatalf("Do = %v, %v; want 200, nil", resp, err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
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
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var resp Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do err = %v, want nil after H3 retry", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200", resp.Status)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

// TestRetryer_Do_H3ResetNonRejected_Stops confirms a non-rejected H3 reset
// (which may have had application side effects) stops the loop after one call.
func TestRetryer_Do_H3ResetNonRejected_Stops(t *testing.T) {
	t.Parallel()
	rst := &http3.StreamResetError{Code: http3.H3RequestCancelled}
	f := &fakeDoer{t: t, results: []doResult{{nil, rst}}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res)
	if !errors.Is(err, rst) {
		t.Fatalf("err = %v, want %v", err, rst)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-rejected H3 reset must not retry)", f.calls)
	}
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
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var resp Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil || resp.Status != 200 {
		t.Fatalf("Do = %v, %v; want 200, nil", resp, err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
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
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"}, &_res)
	if err == nil {
		t.Fatal("expected error on POST + H3 reset, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-idempotent must not retry)", f.calls)
	}
}

// TestRetryer_Do_5xxStatus_NotRetried confirms a real 5xx HTTP status is a
// successful response (nil transport error), not a transport failure, so the
// default Retryer (no user IsRetryable) does not retry it.
func TestRetryer_Do_5xxStatus_NotRetried(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{&Response{Status: 503}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var resp Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do err = %v, want nil (5xx is a response, not a transport error)", err)
	}
	if resp.Status != 503 {
		t.Errorf("Status = %d, want 503", resp.Status)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (5xx status must not be retried)", f.calls)
	}
}

func TestRetryer_Do_DialError_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &DialError{Addr: "x:1", Err: errors.New("boom")}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var _res Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("Do err = %v, want nil after dial retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_ErrDialBackoff_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, ErrDialBackoff},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var _res Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("Do err = %v, want nil after ErrDialBackoff retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_NonRetryableError_Stops(t *testing.T) {
	t.Parallel()
	other := errors.New("application error")
	f := &fakeDoer{t: t, results: []doResult{{nil, other}}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res)
	if !errors.Is(err, other) {
		t.Fatalf("err = %v, want %v", err, other)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-retryable error must stop)", f.calls)
	}
}

func TestRetryer_Do_IsRetryable_Custom5xx_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{&Response{Status: 503}, nil},
		{&Response{Status: 503}, nil},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(_ error, resp *Response) bool {
			return resp != nil && resp.Status >= 500
		},
	})
	r.d = f
	var resp Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200", resp.Status)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}

func TestRetryer_Do_IsRetryable_NonBuiltinError_Retries(t *testing.T) {
	t.Parallel()
	custom := errors.New("custom transient")
	f := &fakeDoer{t: t, results: []doResult{
		{nil, custom},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(err error, _ *Response) bool {
			return errors.Is(err, custom)
		},
	})
	r.d = f
	var _res Response
	if err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res); err != nil {
		t.Fatalf("err = %v, want nil after IsRetryable retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_CtxCanceled_StopsImmediately(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
	}}
	// Backoff long enough that ctx cancel must take the select.
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 5 * time.Second },
	})
	r.d = f
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	var _res Response
	err := r.Do(ctx, &Request{Method: "GET", Path: "/"}, &_res)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 1*time.Second {
		t.Errorf("returned in %v, want <1s (ctx cancel must wake the backoff sleep)", elapsed)
	}
}

func TestRetryer_Do_HardStop_PoolClosed_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, results: []doResult{
		{nil, ErrPoolClosed},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(error, *Response) bool { return true }, // even with this, stop
	})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res)
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v, want ErrPoolClosed", err)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (hard stop must not retry)", f.calls)
	}
}

func TestRetryer_Do_MaxAttempts_Exhausted(t *testing.T) {
	t.Parallel()
	last := &StreamResetError{Code: frame.ErrCodeRefusedStream}
	f := &fakeDoer{t: t, results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, last},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var _res Response
	err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &_res)
	if err != last {
		t.Fatalf("err = %v, want last (%v)", err, last)
	}
	if f.calls != 3 {
		t.Errorf("calls = %d, want 3", f.calls)
	}
}

func TestRetryer_DoStream_RetriesBeforeHeaders(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	var sr StreamResponse
	if err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream err = %v, want nil", err)
	}
	if f.streams != 2 {
		t.Errorf("streams = %d, want 2", f.streams)
	}
}

func TestRetryer_DoStream_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{t: t, stream: []streamResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	var _sr StreamResponse
	err := r.DoStream(context.Background(), &Request{Method: "POST", Path: "/"}, &_sr)
	if err == nil {
		t.Fatal("expected err on POST + RST, got nil")
	}
	if f.streams != 1 {
		t.Errorf("streams = %d, want 1", f.streams)
	}
}

func TestRetryer_DoStream_Success_NoIsRetryableCall(t *testing.T) {
	t.Parallel()
	called := false
	f := &fakeDoer{t: t, stream: []streamResult{
		{&StreamResponse{}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(error, *Response) bool {
			called = true
			return true
		},
	})
	r.d = f
	var _sr StreamResponse
	if err := r.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &_sr); err != nil {
		t.Fatalf("DoStream err = %v", err)
	}
	if called {
		t.Error("IsRetryable invoked for successful DoStream — must not be (caller owns stream)")
	}
}

// TestNewRetryer_DefaultBackoff_GoroutineSafe pins the goroutine-
// safety contract advertised by NewRetryer's doc-comment. Without
// the closure-internal mutex, the default backoff's rng.Int63n call
// would race under -race when called from multiple goroutines.
func TestNewRetryer_DefaultBackoff_GoroutineSafe(t *testing.T) {
	t.Parallel()
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 1; j < 5; j++ {
				_ = r.opts.Backoff(j)
			}
		}()
	}
	wg.Wait()
}
