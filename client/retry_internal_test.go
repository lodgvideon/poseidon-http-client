package client

import (
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
