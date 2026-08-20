package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, http2.ConfigureServer(srv.Config, &http2.Server{}), "ConfigureServer")
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// TestStreamTarget_BodyStream_EndsOnHeaders is the Do(BodyStream) arm.
func TestStreamTarget_BodyStream_EndsOnHeaders(t *testing.T) {
	addr := statusOnlyServer(t)
	c, err := NewClient(ClientOptions{Addr: addr, ConnOpts: newConnOpts()})
	require.NoError(t, err, "NewClient against the status-only server")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response
	require.NoError(t, c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp),
		"Do with BodyMode=BodyStream against a 204")
	require.Equalf(t, 204, resp.Status, "status = %d, want 204", resp.Status)
	buf := make([]byte, 8)

	// The read must report EOF immediately. Without the end-on-headers flag it
	// waits for a DATA or trailer event that is never coming.
	n, rerr := resp.BodyReader.Read(buf)

	assert.Zerof(t, n, "Read returned %d bytes from a status-only response", n)
	assert.ErrorIsf(t, rerr, io.EOF,
		"Read err = %v, want io.EOF — the body reader is waiting for an event a "+
			"status-only response never sends", rerr)
	_ = resp.BodyReader.Close()
}

// TestStreamTarget_DoStream_EndsOnHeaders is the DoStream arm of the same
// property, so neither path can lose it alone.
func TestStreamTarget_DoStream_EndsOnHeaders(t *testing.T) {
	addr := statusOnlyServer(t)
	c, err := NewClient(ClientOptions{Addr: addr, ConnOpts: newConnOpts()})
	require.NoError(t, err, "NewClient against the status-only server")
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var sr StreamResponse

	err = c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr)

	require.NoError(t, err, "DoStream against a 204")
	defer func() { _ = sr.Close() }()
	require.Equalf(t, 204, sr.Status, "status = %d, want 204", sr.Status)
	assert.True(t, sr.drained,
		"StreamResponse is not marked drained after a status-only response; Recv would "+
			"block on an event that never arrives")
}
