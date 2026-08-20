package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// OnConnClose is documented as firing for every connection this client closes.
// It was honoured by the pools and by the HTTP/3 single conn; the HTTP/2 and
// HTTP/1.1 single conns closed silently. A cross-transport dashboard saw churn
// on some transports and zero on others doing identical work — the worst shape
// for an observability contract, because the silence looks like health.
//
// These pin the rule on all three single-connection transports, which is where
// it was broken.

// closeRecorder captures OnConnClose events and the ConnsClosed counter.
//
// events is guarded: the hook is documented to fire wherever the close happens,
// and on the pooled transports that is the pool actor's goroutine, not the
// caller's. TestH1SingleConn_NotReusableIsObservable already takes a mutex for
// exactly this; the two *_CloseIsObservable tests were relying on the hook
// always firing on the calling goroutine, which is an assumption about the code
// under test rather than a property of the fixture (#862).
type closeRecorder struct {
	mu      sync.Mutex
	events  []ConnCloseEvent
	metrics *Metrics
	ref     *atomic.Pointer[Hooks]
}

func newCloseRecorder() *closeRecorder {
	r := &closeRecorder{metrics: &Metrics{}}
	r.ref = &atomic.Pointer[Hooks]{}
	r.ref.Store(&Hooks{OnConnClose: func(e ConnCloseEvent) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, e)
	}})
	return r
}

// snapshot returns a copy of the events recorded so far.
func (r *closeRecorder) snapshot() []ConnCloseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ConnCloseEvent(nil), r.events...)
}

// TestSingleConn_CloseIsObservable is the HTTP/2 gate.
func TestSingleConn_CloseIsObservable(t *testing.T) {
	srv := startOneH2Server(t)
	defer srv.Close()
	r := newCloseRecorder()
	s := &singleConn{
		addr:        srv.Listener.Addr().String(),
		connOpts:    newConnOpts(),
		metrics:     r.metrics,
		hooksRef:    r.ref,
		dialTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, _, err := s.acquireConn(ctx)
	require.NoError(t, err, "acquireConn")

	_ = s.close()

	closed := r.metrics.Counters.ConnsClosed.Load()
	assert.Equalf(t, int64(1), closed,
		"ConnsClosed = %d after closing a live conn, want 1 — an HTTP/2 "+
			"single-conn teardown was invisible to the counter", closed)
	ev := r.snapshot()
	require.Lenf(t, ev, 1, "OnConnClose fired %d times, want 1", len(ev))
	assert.Equalf(t, CloseManual, ev[0].Reason, "Reason = %v, want CloseManual", ev[0].Reason)
}

// TestH1SingleConn_CloseIsObservable is the HTTP/1.1 gate for the explicit
// teardown.
func TestH1SingleConn_CloseIsObservable(t *testing.T) {
	r := newCloseRecorder()
	s := &h1singleConn{
		addr:        "h:80",
		dialer:      newH1FakeDialer(),
		metrics:     r.metrics,
		hooksRef:    r.ref,
		dialTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, release, _, err := s.openExchange(ctx)
	require.NoError(t, err, "openExchange")
	release.release()

	_ = s.close()

	// The H2 gate five lines above pins exact counts and the reason. This one
	// asserted only "> 0" and never looked at the reason, so a teardown that
	// fired the hook TWICE, or fired it with the wrong CloseReason, passed —
	// and double-counted connection churn is precisely what a close-
	// observability dashboard exists to surface (#862).
	closed := r.metrics.Counters.ConnsClosed.Load()
	assert.Equalf(t, int64(1), closed,
		"ConnsClosed = %d after an HTTP/1.1 single-conn teardown, want exactly 1; a "+
			"double count reads as churn that never happened", closed)
	ev := r.snapshot()
	require.Lenf(t, ev, 1,
		"OnConnClose fired %d times for one teardown, want exactly 1", len(ev))
	assert.Equalf(t, CloseManual, ev[0].Reason,
		"Reason = %v, want CloseManual — an explicit close attributed as CloseDead or "+
			"CloseNotReusable makes our own teardown look like peer-driven churn", ev[0].Reason)
}

// TestH1SingleConn_NotReusableIsObservable covers the case the issue calls out
// specifically: ordinary "Connection: close" churn, which on this transport is
// most of the connection lifecycle and was entirely silent.
//
// It drives a real response through the public client, because that is the only
// way to reach the path. The first version of this test called the transport's
// release and then close() — both of which report CloseManual — so it passed
// without ever touching the churn path it was named for. A mutation that
// deleted the churn call site went uncaught, which is how I found it.
func TestH1SingleConn_NotReusableIsObservable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Connection", "close")
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	var mu sync.Mutex
	var reasons []CloseReason
	c, err := NewClient(ClientOptions{
		Addr:      srv.Listener.Addr().String(),
		Transport: TransportH1SingleConn,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		Hooks: &Hooks{OnConnClose: func(e ConnCloseEvent) {
			mu.Lock()
			reasons = append(reasons, e.Reason)
			mu.Unlock()
		}},
	})
	require.NoError(t, err, "NewClient")
	defer func() { _ = c.Close() }()

	var resp Response
	err = c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp)

	require.NoError(t, err, "Do")
	mu.Lock()
	defer mu.Unlock()
	assert.Containsf(t, reasons, CloseNotReusable,
		"a Connection: close response produced reasons %v, none of them "+
			"CloseNotReusable — the ordinary HTTP/1.1 churn is still invisible", reasons)
}

// TestCloseReason_NotReusableHasALabel keeps the new value out of the "unknown"
// bucket, which is where a metric label silently lands if String is not updated.
func TestCloseReason_NotReusableHasALabel(t *testing.T) {
	got := CloseNotReusable.String()

	assert.Equalf(t, "not-reusable", got,
		"CloseNotReusable.String() = %q, want %q — an unlabelled reason "+
			"aggregates into \"unknown\" on every dashboard", got, "not-reusable")
}
