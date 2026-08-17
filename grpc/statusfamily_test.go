package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// A peer resetting the stream became a *Status; the connection dying under it
// leaked the transport error verbatim. Which one a caller got depended on
// whether conn delivered the failure as an event or as an error from Recv — an
// implementation detail of the transport. So retry classification needed
// errors.As(*Status) AND errors.Is(conn.ErrConnClosed) to be complete, and
// nothing said so.
//
// statusFromTransport folds the second family into the first at the one boundary
// every connection-level failure crosses, and keeps the original reachable so
// the mapping adds rather than replaces.

// TestStatusFromTransport_ConnectionDeathIsUnavailable is the main mapping.
func TestStatusFromTransport_ConnectionDeathIsUnavailable(t *testing.T) {
	bases := []error{
		conn.ErrConnClosed,
		conn.ErrStaleStream,
		conn.ErrStreamClosed,
		errors.New("some transport failure"),
	}

	for _, base := range bases {
		err := statusFromTransport(base)

		var st *Status
		require.Truef(t, errors.As(err, &st), "%v mapped to %T, want *Status", base, err)
		assert.Equalf(t, Unavailable, st.Code,
			"%v mapped to %v, want Unavailable — the RPC did not complete and "+
				"another connection might serve it", base, st.Code)
	}
}

// TestStatusFromTransport_ContextCodesAreNotUnavailable is the distinction that
// matters to a retry policy: a deadline the CALLER set is not the server being
// unavailable, and conflating them retries a request whose deadline has passed.
func TestStatusFromTransport_ContextCodesAreNotUnavailable(t *testing.T) {
	cases := []struct {
		err  error
		want Code
	}{
		{context.Canceled, Canceled},
		{context.DeadlineExceeded, DeadlineExceeded},
		{fmt.Errorf("conn: %w", context.DeadlineExceeded), DeadlineExceeded},
	}

	for _, tc := range cases {
		mapped := statusFromTransport(tc.err)

		var st *Status
		require.Truef(t, errors.As(mapped, &st), "%v did not map to a *Status", tc.err)
		assert.Equalf(t, tc.want, st.Code, "%v mapped to %v, want %v", tc.err, st.Code, tc.want)
	}
}

// TestStatusFromTransport_KeepsTheCause is what makes this additive. A caller
// that already wrote errors.Is against the transport sentinel must keep working.
func TestStatusFromTransport_KeepsTheCause(t *testing.T) {
	err := statusFromTransport(conn.ErrConnClosed)

	assert.ErrorIs(t, err, conn.ErrConnClosed,
		"the transport error is no longer reachable; the mapping replaced a family "+
			"instead of folding it in, and existing errors.Is checks stop firing")
	var st *Status
	require.True(t, errors.As(err, &st), "not a *Status")
	// Identity, not errors.Is: Unwrap must hand back the very error it was given.
	assert.Equalf(t, conn.ErrConnClosed, st.Unwrap(), "Unwrap = %v, want the original error", st.Unwrap())
}

// TestStatusFromTransport_NilStaysNil guards the boundary: pump calls this on
// every Recv error, and a nil must not become a spurious Unavailable.
func TestStatusFromTransport_NilStaysNil(t *testing.T) {
	err := statusFromTransport(nil)

	// Deliberately `== nil` rather than assert.Nil: this value is an interface,
	// and reflection-based nilness would also accept a typed nil *Status inside
	// it — which is exactly the regression a caller's `if err != nil` would trip
	// over.
	assert.Truef(t, err == nil, "statusFromTransport(nil) = %v, want nil", err)
}

// TestStatusFromWire_HasNoCause pins that a Status the PEER sent carries no
// cause: the peer sends a code and a message, not a Go error, and inventing one
// would make errors.Is answer questions about a transport failure that never
// happened.
func TestStatusFromWire_HasNoCause(t *testing.T) {
	st := &Status{Code: PermissionDenied, Message: "no"}

	cause := st.Unwrap()

	assert.Truef(t, cause == nil, "a wire Status unwraps to %v, want nil", cause)
	assert.False(t, errors.Is(st, conn.ErrConnClosed), "a wire Status matched a transport sentinel")
}

// TestRecv_TransportErrorIsAStatus drives the real path. The tests above call
// statusFromTransport directly, so removing its call from pump leaves them all
// green — the same "gate on the function, not the wiring" trap the H3 typed
// error hit.
//
// It cancels the caller's context, which is a failure conn reports by returning
// an error from Recv rather than by delivering an event. That distinction is the
// whole subject of the issue: a stream the PEER resets already arrived as a
// *Status, and only the error-returning failures leaked the transport type.
//
// (Closing the ClientConn is NOT such a case, which is worth writing down: conn
// resets its own streams with INTERNAL_ERROR on close, so that failure arrives
// as an event and was already a *Status — with code Internal, which is a
// separate question this change does not touch.)
func TestRecv_TransportErrorIsAStatus(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx, cancel := context.WithCancel(t.Context())
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	cancel()
	_, rerr := s.Recv(ctx)

	require.Error(t, rerr, "Recv returned no error after the context was cancelled")
	var st *Status
	require.Truef(t, errors.As(rerr, &st),
		"Recv returned %T (%v), want a *Status — a caller classifying failures "+
			"would need a second error family for exactly this case", rerr, rerr)
	assert.Equalf(t, Canceled, st.Code,
		"a cancelled context mapped to %v, want Canceled; Unavailable here would "+
			"have a retry policy replay a request the caller already gave up on", st.Code)
	assert.ErrorIs(t, rerr, context.Canceled,
		"the original context error is no longer reachable through the Status")
}
