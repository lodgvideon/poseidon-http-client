package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// Do(BodyStream) and DoStream used to run the same prologue and then apply the
// result by hand, each into its own struct. The copy drifted: both need to know
// that a response ended on its HEADERS frame — a 204, a 304, a HEAD, any
// status-only reply — and one of them recorded it while the other did not. A
// reader that does not know pumps one event too many and then blocks, or reports
// a benign RST_STREAM(NO_ERROR) as a reset.
//
// streamTarget makes the shared code CALL both, so the decision is passed as a
// parameter rather than reapplied twice. These pin the behaviour that guarantees.

// statusOnlyServer answers 204 with no body, so the response ends on HEADERS.
func statusOnlyServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// TestStreamTarget_BodyStream_EndsOnHeaders is the Do(BodyStream) arm.
func TestStreamTarget_BodyStream_EndsOnHeaders(t *testing.T) {
	addr := statusOnlyServer(t)
	c, err := NewClient(ClientOptions{Addr: addr, ConnOpts: newConnOpts()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
	// The read must report EOF immediately. Without the end-on-headers flag it
	// waits for a DATA or trailer event that is never coming.
	buf := make([]byte, 8)
	n, rerr := resp.BodyReader.Read(buf)
	if n != 0 || rerr == nil {
		t.Errorf("Read = (%d, %v), want (0, io.EOF) — the body reader is waiting for an "+
			"event a status-only response never sends", n, rerr)
	}
	_ = resp.BodyReader.Close()
}

// TestStreamTarget_DoStream_EndsOnHeaders is the DoStream arm of the same
// property, so neither path can lose it alone.
func TestStreamTarget_DoStream_EndsOnHeaders(t *testing.T) {
	addr := statusOnlyServer(t)
	c, err := NewClient(ClientOptions{Addr: addr, ConnOpts: newConnOpts()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer func() { _ = sr.Close() }()
	if sr.Status != 204 {
		t.Fatalf("status = %d, want 204", sr.Status)
	}
	if !sr.drained {
		t.Error("StreamResponse is not marked drained after a status-only response; Recv " +
			"would block on an event that never arrives")
	}
}
