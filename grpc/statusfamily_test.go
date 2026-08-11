package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
	for _, base := range []error{
		conn.ErrConnClosed,
		conn.ErrStaleStream,
		conn.ErrStreamClosed,
		errors.New("some transport failure"),
	} {
		err := statusFromTransport(base)
		var st *Status
		if !errors.As(err, &st) {
			t.Fatalf("%v mapped to %T, want *Status", base, err)
		}
		if st.Code != Unavailable {
			t.Errorf("%v mapped to %v, want Unavailable — the RPC did not complete and "+
				"another connection might serve it", base, st.Code)
		}
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
		var st *Status
		if !errors.As(statusFromTransport(tc.err), &st) {
			t.Fatalf("%v did not map to a *Status", tc.err)
		}
		if st.Code != tc.want {
			t.Errorf("%v mapped to %v, want %v", tc.err, st.Code, tc.want)
		}
	}
}

// TestStatusFromTransport_KeepsTheCause is what makes this additive. A caller
// that already wrote errors.Is against the transport sentinel must keep working.
func TestStatusFromTransport_KeepsTheCause(t *testing.T) {
	err := statusFromTransport(conn.ErrConnClosed)
	if !errors.Is(err, conn.ErrConnClosed) {
		t.Error("the transport error is no longer reachable; the mapping replaced a family " +
			"instead of folding it in, and existing errors.Is checks stop firing")
	}
	var st *Status
	if !errors.As(err, &st) {
		t.Fatal("not a *Status")
	}
	if st.Unwrap() != conn.ErrConnClosed { //nolint:errorlint // identity is the point
		t.Errorf("Unwrap = %v, want the original error", st.Unwrap())
	}
}

// TestStatusFromTransport_NilStaysNil guards the boundary: pump calls this on
// every Recv error, and a nil must not become a spurious Unavailable.
func TestStatusFromTransport_NilStaysNil(t *testing.T) {
	if err := statusFromTransport(nil); err != nil {
		t.Errorf("statusFromTransport(nil) = %v, want nil", err)
	}
}

// TestStatusFromWire_HasNoCause pins that a Status the PEER sent carries no
// cause: the peer sends a code and a message, not a Go error, and inventing one
// would make errors.Is answer questions about a transport failure that never
// happened.
func TestStatusFromWire_HasNoCause(t *testing.T) {
	st := &Status{Code: PermissionDenied, Message: "no"}
	if st.Unwrap() != nil {
		t.Errorf("a wire Status unwraps to %v, want nil", st.Unwrap())
	}
	if errors.Is(st, conn.ErrConnClosed) {
		t.Error("a wire Status matched a transport sentinel")
	}
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	cancel()

	_, rerr := s.Recv(ctx)
	if rerr == nil {
		t.Fatal("Recv returned no error after the context was cancelled")
	}
	var st *Status
	if !errors.As(rerr, &st) {
		t.Fatalf("Recv returned %T (%v), want a *Status — a caller classifying failures "+
			"would need a second error family for exactly this case", rerr, rerr)
	}
	if st.Code != Canceled {
		t.Errorf("a cancelled context mapped to %v, want Canceled; Unavailable here would "+
			"have a retry policy replay a request the caller already gave up on", st.Code)
	}
	if !errors.Is(rerr, context.Canceled) {
		t.Error("the original context error is no longer reachable through the Status")
	}
}
