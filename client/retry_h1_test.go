package client

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// builtinShouldRetry had an H2 arm and an H3 arm and no H1 arm, so the canonical
// retryable HTTP/1.1 failure — a pooled keep-alive the server reaped between the
// checkout probe and the write — was never retried while REFUSED_STREAM, GOAWAY
// and H3_REQUEST_REJECTED always were. Nothing pinned either behaviour.

// TestBuiltinShouldRetry_H1ServerClosedIdle is the arm itself.
func TestBuiltinShouldRetry_H1ServerClosedIdle(t *testing.T) {
	err := fmt.Errorf("%w: %w", http1.ErrServerClosedIdle, io.EOF)
	if !builtinShouldRetry(err) {
		t.Error("ErrServerClosedIdle is not retryable — the request produced no response " +
			"at all, which is the same guarantee REFUSED_STREAM and GOAWAY carry")
	}
}

// TestBuiltinShouldRetry_H1OpaqueReadErrorIsNotRetried is the boundary. Only the
// typed signal is retryable; a read failure that is merely wrapped text says
// nothing about whether the server processed the request.
func TestBuiltinShouldRetry_H1OpaqueReadErrorIsNotRetried(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("http1: read status line: %w", io.EOF),
		fmt.Errorf("http1: read header line: %w", io.ErrUnexpectedEOF),
		errors.New("http1: read status line: connection reset by peer"),
	} {
		if builtinShouldRetry(err) {
			t.Errorf("%v was classified retryable; a response that stopped partway means "+
				"the server was answering and may have processed the request", err)
		}
	}
}

// TestCanRetry_StillRefusesNonIdempotent is the safety net that makes the arm
// above sound. The classifier judges the error only; whether the request may be
// replayed at all is canRetry's decision, and it must keep refusing a POST no
// matter how retryable the error looks.
func TestCanRetry_StillRefusesNonIdempotent(t *testing.T) {
	r := &Retryer{opts: RetryOptions{MaxAttempts: 3}}

	if r.canRetry(&Request{Method: "POST", Path: "/"}) {
		t.Error("canRetry allowed a POST — the H1 arm would then replay a request the " +
			"server may have processed")
	}
	if !r.canRetry(&Request{Method: "GET", Path: "/"}) {
		t.Error("canRetry refused a GET, so the new arm can never fire")
	}
	if r.canRetry(&Request{Method: "GET", Path: "/", Idempotency: ForceNotIdempotent}) {
		t.Error("canRetry ignored ForceNotIdempotent")
	}
}
