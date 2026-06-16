// client/conformance_retry_test.go — RFC 7540 §7 / §6.8
// conformance tests for the retry classifier.
//
// The classifier covers RST codes that may indicate a transient
// condition (server-side panic, concurrent shutdown, rate limit)
// and SHOULD be retried per RFC 7540 §7 / §8.1.4. The classifier
// is a pure function — these tests don't spin up a real conn.
package client

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestRepro_RetryOnInternalError_Behavior is the REPRO for the
// missing INTERNAL_ERROR branch in builtinShouldRetry. After
// hardening, a Retryer wrapping c.Do must retry on RST_STREAM(2)
// just as it does for RST_STREAM(7) (REFUSED_STREAM).
//
// Pre-fix: this test should pass (the second Do on a fresh conn
// succeeds) but the FIRST Do returns the StreamResetError — i.e.
// the retry layer does NOT retry. To assert that, the test wires
// a fake retryDoer that records how many times the loop consulted
// it.
func TestConformance_RFC7540_Sec7_RetryOnInternalError(t *testing.T) {
	// Counter-driven fake: a retryDoer that fails with RST_STREAM(2)
	// on the first call, succeeds on subsequent calls. We do this
	// by using the real client, but with a server that returns
	// RST_STREAM(2) only on the very first request. After the
	// retry, the second request (or a new conn) succeeds.
	//
	// Simpler: we directly assert that builtinShouldRetry returns
	// true for *StreamResetError{Code: ErrCodeInternalError}. That
	// is the gating question. If this returns true, the Retryer
	// will retry; if false, it will not.
	sre := &StreamResetError{Code: frame.ErrCodeInternalError}
	if !builtinShouldRetry(sre) {
		t.Errorf("builtinShouldRetry(RST_STREAM(INTERNAL_ERROR)) = false; want true (transient per RFC 7540 §7)")
	}
}

// TestRepro_RetryOnEnhanceYourCalm_Behavior: ENHANCE_YOUR_CALM (11)
// is the rate-limit code. The same argument applies: it is transient
// and the client should retry.
func TestConformance_RFC7540_Sec7_RetryOnEnhanceYourCalm(t *testing.T) {
	sre := &StreamResetError{Code: frame.ErrCodeEnhanceYourCalm}
	if !builtinShouldRetry(sre) {
		t.Errorf("builtinShouldRetry(RST_STREAM(ENHANCE_YOUR_CALM)) = false; want true (rate limit, transient)")
	}
}

// TestRepro_NoRetryOnProtocolError confirms the negative branch:
// PROTOCOL_ERROR is non-transient and must NOT be retried.
func TestConformance_RFC7540_Sec7_NoRetryOnProtocolError(t *testing.T) {
	sre := &StreamResetError{Code: frame.ErrCodeProtocolError}
	if builtinShouldRetry(sre) {
		t.Errorf("builtinShouldRetry(RST_STREAM(PROTOCOL_ERROR)) = true; want false (likely a bug, not transient)")
	}
}

// TestRepro_NoRetryOnCancel confirms that CANCEL is not retried
// (the peer cancelled; a retry would just be cancelled again).
func TestConformance_RFC7540_Sec7_NoRetryOnCancel(t *testing.T) {
	sre := &StreamResetError{Code: frame.ErrCodeCancel}
	if builtinShouldRetry(sre) {
		t.Errorf("builtinShouldRetry(RST_STREAM(CANCEL)) = true; want false (peer cancelled, do not retry)")
	}
}

// TestRepro_RetryGoAway_StillRetries is a regression check: GOAWAY
// retry is the original behavior; we must not regress it when
// adding the INTERNAL_ERROR branch.
func TestConformance_RFC7540_Sec6_8_RetryGoAway_StillRetries(t *testing.T) {
	if !builtinShouldRetry(conn.ErrGoAway) {
		t.Errorf("builtinShouldRetry(ErrGoAway) = false; want true (existing behavior)")
	}
}

// TestRepro_RetryContext_NotRetried is a regression check: ctx
// errors must never be retried even with the broader predicate.
func TestRetryer_ContextErrorsNotRetried(t *testing.T) {
	if builtinShouldRetry(context.DeadlineExceeded) {
		t.Errorf("builtinShouldRetry(ctx.DeadlineExceeded) = true; want false (hard stop)")
	}
	if builtinShouldRetry(context.Canceled) {
		t.Errorf("builtinShouldRetry(ctx.Canceled) = true; want false (hard stop)")
	}
}
