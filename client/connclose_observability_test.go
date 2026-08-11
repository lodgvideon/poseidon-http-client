package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
type closeRecorder struct {
	events  []ConnCloseEvent
	metrics *Metrics
	ref     *atomic.Pointer[Hooks]
}

func newCloseRecorder() *closeRecorder {
	r := &closeRecorder{metrics: &Metrics{}}
	r.ref = &atomic.Pointer[Hooks]{}
	r.ref.Store(&Hooks{OnConnClose: func(e ConnCloseEvent) { r.events = append(r.events, e) }})
	return r
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
	if _, _, err := s.acquireConn(ctx); err != nil {
		t.Fatalf("acquireConn: %v", err)
	}
	_ = s.close()

	if got := r.metrics.Counters.ConnsClosed.Load(); got != 1 {
		t.Errorf("ConnsClosed = %d after closing a live conn, want 1 — an HTTP/2 "+
			"single-conn teardown was invisible to the counter", got)
	}
	if len(r.events) != 1 {
		t.Fatalf("OnConnClose fired %d times, want 1", len(r.events))
	}
	if r.events[0].Reason != CloseManual {
		t.Errorf("Reason = %v, want CloseManual", r.events[0].Reason)
	}
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
	_, _, release, err := s.openExchange(ctx)
	if err != nil {
		t.Fatalf("openExchange: %v", err)
	}
	release()
	_ = s.close()

	if got := r.metrics.Counters.ConnsClosed.Load(); got == 0 {
		t.Error("ConnsClosed stayed 0 after an HTTP/1.1 single-conn teardown")
	}
	if len(r.events) == 0 {
		t.Fatal("OnConnClose never fired for an HTTP/1.1 single-conn teardown")
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	var resp Response
	if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, r := range reasons {
		if r == CloseNotReusable {
			return
		}
	}
	t.Errorf("a Connection: close response produced reasons %v, none of them "+
		"CloseNotReusable — the ordinary HTTP/1.1 churn is still invisible", reasons)
}

// TestCloseReason_NotReusableHasALabel keeps the new value out of the "unknown"
// bucket, which is where a metric label silently lands if String is not updated.
func TestCloseReason_NotReusableHasALabel(t *testing.T) {
	if got := CloseNotReusable.String(); got != "not-reusable" {
		t.Errorf("CloseNotReusable.String() = %q, want %q — an unlabelled reason "+
			"aggregates into \"unknown\" on every dashboard", got, "not-reusable")
	}
}
