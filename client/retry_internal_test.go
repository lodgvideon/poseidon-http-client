package client

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestIsIdempotent_Methods(t *testing.T) {
	t.Parallel()
	tt := func(b bool) *bool { return &b }
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
		{"override true on POST", &Request{Method: "POST", Idempotent: tt(true)}, true},
		{"override false on GET", &Request{Method: "GET", Idempotent: tt(false)}, false},
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
		{"RST INTERNAL_ERROR", &StreamResetError{Code: frame.ErrCodeInternalError}, false},
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
	if r.rng == nil {
		t.Error("rng = nil after NewRetryer")
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
type fakeDoer struct {
	results []doResult
	stream  []streamResult
	calls   int
	streams int
}

type doResult struct {
	resp *Response
	err  error
}
type streamResult struct {
	resp *StreamResponse
	err  error
}

func (f *fakeDoer) Do(_ context.Context, _ *Request) (*Response, error) {
	r := f.results[f.calls]
	f.calls++
	return r.resp, r.err
}

func (f *fakeDoer) DoStream(_ context.Context, _ *Request) (*StreamResponse, error) {
	r := f.stream[f.streams]
	f.streams++
	return r.resp, r.err
}

func TestRetryer_Do_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil}, // never reached
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "POST", Path: "/"})
	if err == nil {
		t.Fatal("expected error on POST + RST, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-idempotent must not retry)", f.calls)
	}
}

func TestRetryer_Do_BodyReader_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 3})
	r.d = f
	_, err := r.Do(context.Background(), &Request{
		Method:     "GET",
		Path:       "/",
		BodyReader: errReader{},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (BodyReader must disable retry)", f.calls)
	}
}

func TestRetryer_Do_MaxAttemptsOne_NoRetry(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{MaxAttempts: 1})
	r.d = f
	_, _ = r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
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
	f := &fakeDoer{results: []doResult{
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{nil, &StreamResetError{Code: frame.ErrCodeRefusedStream}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	resp, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
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
	f := &fakeDoer{results: []doResult{
		{nil, conn.ErrGoAway},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	resp, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil || resp.Status != 200 {
		t.Fatalf("Do = %v, %v; want 200, nil", resp, err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_DialError_Retries(t *testing.T) {
	t.Parallel()
	f := &fakeDoer{results: []doResult{
		{nil, &DialError{Addr: "x:1", Err: errors.New("boom")}},
		{&Response{Status: 200}, nil},
	}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if err != nil {
		t.Fatalf("Do err = %v, want nil after dial retry", err)
	}
	if f.calls != 2 {
		t.Errorf("calls = %d, want 2", f.calls)
	}
}

func TestRetryer_Do_NonRetryableError_Stops(t *testing.T) {
	t.Parallel()
	other := errors.New("application error")
	f := &fakeDoer{results: []doResult{{nil, other}}}
	r := NewRetryer(&Client{}, RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})
	r.d = f
	_, err := r.Do(context.Background(), &Request{Method: "GET", Path: "/"})
	if !errors.Is(err, other) {
		t.Fatalf("err = %v, want %v", err, other)
	}
	if f.calls != 1 {
		t.Errorf("calls = %d, want 1 (non-retryable error must stop)", f.calls)
	}
}
