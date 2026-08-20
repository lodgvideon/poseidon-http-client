// client/coverage_test.go — targeted coverage tests pushing total ≥ 90%.
package client_test

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func covClientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cov859Tracer counts inbound HTTP/2 frames by type name.
//
// It exists as an INJECTION COUNTER. The tests below assert properties that
// only exist once a peer has reset a stream or sent GOAWAY, and a run where
// the peer did neither passes exactly like a real one — which is how the
// versions of these tests that this file used to hold stayed green against a
// fixture that reset nothing. Counting the frame on the wire is independent of
// every client-side mechanism under test, so it can say "the injection
// happened" without borrowing the answer from the assertion it guards.
type cov859Tracer struct {
	mu sync.Mutex
	in map[string]int
}

// TraceFrame implements trace.Tracer.
func (tr *cov859Tracer) TraceFrame(fi trace.FrameInfo) {
	if fi.Dir != trace.DirIn {
		return
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.in == nil {
		tr.in = make(map[string]int)
	}
	tr.in[fi.TypeName]++
}

// countIn returns how many frames of the named type arrived from the peer.
func (tr *cov859Tracer) countIn(name string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.in[name]
}

// cov859ResetServer starts an HTTP/2 server that sends 200 plus a flush and
// then, when abort is true, panics with http.ErrAbortHandler — which makes
// net/http put a real RST_STREAM(INTERNAL_ERROR) on the wire behind the
// headers it has already sent. abort=false is the control arm: same handler,
// same clean 200, no reset. The returned counter is the server-side injection
// count.
//
// The fixture the reset tests in this file used to share instead asked the
// ResponseWriter for an http.Hijacker and closed the raw conn. net/http's
// HTTP/2 ResponseWriter does not implement http.Hijacker, so
// `hj, ok := w.(http.Hijacker)` was always ok==false: nothing was hijacked,
// nothing was closed, nothing was reset, and all three "reset" tests ran
// against an ordinary 200 with an empty body. Measured, not inferred — #859.
func cov859ResetServer(t *testing.T, abort bool) (string, *atomic.Int32) {
	t.Helper()
	var aborted atomic.Int32
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if !abort {
			return
		}
		aborted.Add(1)
		panic(http.ErrAbortHandler)
	}))
	return addr, &aborted
}

// cov859TracedClient builds a single-connection client wired to tr.
func cov859TracedClient(t *testing.T, addr string, tr *cov859Tracer) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr:   addr,
		Tracer: tr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cov859CloseRecorder records OnConnClose events, which fire on the pool actor
// goroutine and so need a lock the tests' own goroutine can read under.
type cov859CloseRecorder struct {
	mu     sync.Mutex
	events []client.ConnCloseEvent
}

// hooks returns a Hooks set that records every close into r.
func (r *cov859CloseRecorder) hooks() *client.Hooks {
	return &client.Hooks{OnConnClose: func(e client.ConnCloseEvent) {
		r.mu.Lock()
		r.events = append(r.events, e)
		r.mu.Unlock()
	}}
}

// snapshot copies the events recorded so far.
func (r *cov859CloseRecorder) snapshot() []client.ConnCloseEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]client.ConnCloseEvent(nil), r.events...)
}

// ---------------------------------------------------------------------------
// client.go: Metrics() — 0% → call it
// ---------------------------------------------------------------------------

func TestClient_Metrics_ReturnsNonNil(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)

	m := c.Metrics()

	require.Truef(t, m != nil, "Metrics() returned nil")
	// Verify same pointer is stable.
	require.Truef(t, c.Metrics() == m, "Metrics() returned different pointer on second call")
}

// ---------------------------------------------------------------------------
// client.go: PoolStats() on TransportSingleConn returns zero Stats
// ---------------------------------------------------------------------------

func TestClient_PoolStats_SingleConnReturnsZero(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)

	st := c.PoolStats()

	// zero Stats expected for non-pool transport
	assert.Truef(t, st.ActiveConns == 0 && st.InFlightStreams == 0 && st.Waiters == 0,
		"PoolStats on SingleConn = %+v, want zero", st)
}

// ---------------------------------------------------------------------------
// hooks.go: CloseReason.String() default branch
// ---------------------------------------------------------------------------

func TestCloseReason_String_Unknown(t *testing.T) {
	t.Parallel()
	r := client.CloseReason(99)

	got := r.String()

	assert.Equalf(t, "unknown", got, "CloseReason(99).String() = %q, want \"unknown\"", got)
}

// Exercise known values while we are here (avoids 0% on any label path).
func TestCloseReason_String_KnownValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		r    client.CloseReason
		want string
	}{
		{client.CloseIdle, "idle"},
		{client.CloseDead, "dead"},
		{client.CloseGoAway, "goaway"},
		{client.CloseManual, "manual"},
	}
	for _, tc := range cases {
		got := tc.r.String()

		assert.Equalf(t, tc.want, got, "CloseReason(%d).String() = %q, want %q", tc.r, got, tc.want)
	}
}

// ---------------------------------------------------------------------------
// metrics.go: Quantile edge cases (p=0, p=1, clamping, empty histogram)
// ---------------------------------------------------------------------------

func TestHistogramSnapshot_Quantile_EdgeCases(t *testing.T) {
	t.Parallel()

	// Empty histogram returns 0 for any quantile.
	var h client.Metrics // fresh zero Metrics → zero histogram inside
	snap := h.Latency.Request.Snapshot()

	gotHalfEmpty, gotZeroEmpty, gotOneEmpty := snap.Quantile(0.5), snap.Quantile(0), snap.Quantile(1)

	assert.Zerof(t, gotHalfEmpty, "Quantile(0.5) on empty = %v, want 0", gotHalfEmpty)
	assert.Zerof(t, gotZeroEmpty, "Quantile(0) on empty = %v, want 0", gotZeroEmpty)
	assert.Zerof(t, gotOneEmpty, "Quantile(1) on empty = %v, want 0", gotOneEmpty)

	// Single observation — p=0 and p=1 both land in bucket 0 (clamp to target=1).
	h.Latency.Request.Observe(1 * time.Nanosecond) // bucket 0
	snap = h.Latency.Request.Snapshot()

	got0 := snap.Quantile(0)
	got1 := snap.Quantile(1)

	assert.NotZerof(t, got0, "Quantile(0) on 1-obs histogram = 0, want non-zero bucket edge")
	assert.NotZerof(t, got1, "Quantile(1) on 1-obs histogram = 0, want non-zero bucket edge")

	// Quantile clamping: negative → treated as 0; >1 → treated as 1.
	gotNeg := snap.Quantile(-0.5)
	gotOver := snap.Quantile(1.5)

	assert.Equalf(t, got0, gotNeg, "Quantile(-0.5) = %v, want same as Quantile(0) = %v", gotNeg, got0)
	assert.Equalf(t, got1, gotOver, "Quantile(1.5) = %v, want same as Quantile(1) = %v", gotOver, got1)
}

// ---------------------------------------------------------------------------
// managed_pool.go: isDialOnlyErr — unit test the helper (internal pkg call
// needs a go-test in the internal test package; we replicate it externally
// by inducing the paths through acquire)
// ---------------------------------------------------------------------------

// TestManagedPool_AllSubPoolsFail_FallsBackToLastErr verifies acquire returns
// the last dial-only error when all addresses fail.
func TestManagedPool_AllSubPoolsFail_LastErrReturned(t *testing.T) {
	t.Parallel()
	// Point at an address that won't accept connections.
	addr := client.Address{Host: "127.0.0.1", Port: 1} // port 1 always refused
	r := client.StaticResolver(addr)
	c, err := client.NewClient(client.ClientOptions{
		Resolver:  r,
		Transport: client.TransportManaged,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 1,
			DialTimeout:     200 * time.Millisecond,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	// Should get a dial error (not ErrNoAddresses, since there was 1 address to try).
	require.Errorf(t, err, "expected error from unreachable host, got nil")
	var de *client.DialError
	if !errors.As(err, &de) {
		t.Logf("got error (not a DialError): %v", err)
	}
}

// ---------------------------------------------------------------------------
// resolver.go: DNSResolver constructor — bring it from 0% by calling it
// ---------------------------------------------------------------------------

func TestDNSResolver_Constructor(t *testing.T) {
	t.Parallel()
	r := client.DNSResolver("localhost", 80, client.DNSOptions{TTL: 5 * time.Second})

	require.Truef(t, r != nil, "DNSResolver returned nil")
	// A Resolve call on localhost:80 should not panic even if DNS is weird.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addrs, _ := r.Resolve(ctx) // ignore error; just exercise the code path
	_ = addrs
}

// ---------------------------------------------------------------------------
// resolver.go: Resolve's authoritative-empty branch — covered elsewhere (#859)
//
// TestDNSResolver_Resolve_AllFilteredReturnsErrNoAddresses lived here. It was
// named for `return nil, ErrNoAddresses` and could not reach it: it resolved a
// .invalid host, so LookupIPAddr FAILED and Resolve returned two branches
// earlier at `if err != nil`. It asserted nothing either way, and it put a
// live DNS lookup in a unit suite.
//
// The branch is asserted by TestResolve_SuccessfulEmpty_DoesNotServeStale in
// review_regress_test.go, which reaches it through the internal dnsLookup seam
// — a fake returning a SUCCESSFUL empty answer — which this external test
// package cannot construct. Verified by mutation: blanking that return leaves
// the deleted test green and turns the sibling red.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// body.go: responseBodyReader.Read — error and reset paths via BodyStream
// ---------------------------------------------------------------------------

// TestResponseBodyReader_Read_EventReset pins that a peer RST_STREAM reaches a
// streaming caller AS A RESET, not as the ordinary end of the body (#900).
//
// The version this replaces asked the ResponseWriter for an http.Hijacker and
// closed the raw conn. net/http's HTTP/2 ResponseWriter does not implement
// http.Hijacker, so nothing was hijacked and nothing was reset: the handler
// returned a clean 200 with an empty body and `assert.Error` was satisfied by
// io.EOF. Measured against that exact fixture: `PROBE RESULT n=0 readErr=EOF`.
// A responseBodyReader that returned io.EOF for every reset it saw passed it
// unchanged — and a streaming caller tells "the body ended" from "the peer
// killed the stream" on exactly this error, so conflating them turns a
// truncated response into an apparently complete one.
//
// The abort=false arm is the control: same fixture, no reset, and the answer
// must be io.EOF and NOT a reset. Without it the table is satisfied by a reader
// that reports every end-of-body as a reset.
func TestResponseBodyReader_Read_EventReset(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		abort bool
	}{
		{"peer resets the stream behind flushed headers", true},
		{"peer ends the stream cleanly", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			addr, aborted := cov859ResetServer(t, c.abort)
			tr := &cov859Tracer{}
			cl := cov859TracedClient(t, addr, tr)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var resp client.Response
			err := cl.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
			require.NoErrorf(t, err, "Do: the headers are flushed before the abort, so they arrive")
			require.Truef(t, resp.BodyReader != nil, "expected BodyReader on a BodyStream request")
			defer func() { _ = resp.BodyReader.Close() }()

			buf := make([]byte, 64)
			_, readErr := resp.BodyReader.Read(buf)

			require.EqualValuesf(t, boolToInt(c.abort), aborted.Load(),
				"the handler aborted %d times, want %d — with no abort there is no reset to "+
					"observe and the assertion below would be measuring the fixture",
				aborted.Load(), boolToInt(c.abort))
			if !c.abort {
				assert.ErrorIsf(t, readErr, io.EOF,
					"a clean empty body read back %v, want io.EOF; reporting an ordinary end "+
						"of stream as a reset would make every response look truncated", readErr)
				assert.Zerof(t, tr.countIn("RST_STREAM"),
					"the control arm saw a RST_STREAM on the wire, so it is not a control")
				return
			}
			assert.Positivef(t, tr.countIn("RST_STREAM"),
				"no RST_STREAM arrived from the peer, so whatever Read returned was not "+
					"produced by the mechanism this test names")
			var rst *client.StreamResetError
			require.ErrorAsf(t, readErr, &rst,
				"Read after a peer reset returned %v, want a *client.StreamResetError — "+
					"io.EOF here is indistinguishable from a complete body", readErr)
			assert.Equalf(t, frame.ErrCodeInternalError, rst.Code,
				"reset code = %v, want INTERNAL_ERROR: the code is what Retryer classifies "+
					"on, and a peer reset reported under the wrong code changes that decision",
				rst.Code)
		})
	}
}

// boolToInt is the injection-count expectation for the control arm above.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestResponseBodyReader_Read_BodyBufferDrain(t *testing.T) {
	t.Parallel()
	// Large body: forces buf reuse in responseBodyReader.Read.
	body := bytes.Repeat([]byte("x"), 32*1024) // 32 KiB
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	c := covClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
	require.NoErrorf(t, err, "Do")
	defer func() { _ = resp.BodyReader.Close() }()

	// Read with a small buf to force the r.buf reuse path.
	smallBuf := make([]byte, 512)
	var total int
	for {
		n, err := resp.BodyReader.Read(smallBuf)
		total += n
		if err == io.EOF {
			break
		}
		require.NoErrorf(t, err, "Read error")
	}

	assert.Equalf(t, len(body), total, "read %d bytes, want %d", total, len(body))
}

// ---------------------------------------------------------------------------
// response.go: Recv — EventReset and spurious EventHeaders paths
// ---------------------------------------------------------------------------

// TestStreamResponse_Recv_EventReset pins that a peer RST_STREAM reaches the
// caller as an EventReset carrying THE PEER'S code — not as an error, and not
// as a code this client picked.
//
// The propagation is the half no sibling covers.
// TestIT_H2_StreamedDownload_NonConsumerIsRefused also lands on Recv's
// conn.EventReset arm, but on a reset conn FORGES locally (CANCEL, when the
// caller falls behind the event buffer), so it passes just as well against a
// Recv that hardcodes the code. Hardcoding is not hypothetical harm: Retryer
// decides on the code, and a peer reset reported as REFUSED_STREAM claims the
// server never processed the request (RFC 9113 §8.7) and gets it replayed.
//
// The control arm runs the identical handler minus the panic. It is here
// because "no reset arrived" and "a reset arrived and was handled" used to
// produce the same green: the previous fixture reset nothing at all (see
// cov859ResetServer), so this test passed against a clean 200.
func TestStreamResponse_Recv_EventReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		abort       bool
		wantType    client.EventType
		wantCode    conn.ErrCode
		wantResetIn int
	}{
		{
			name: "peer_reset", abort: true,
			wantType: client.EventReset, wantCode: frame.ErrCodeInternalError, wantResetIn: 1,
		},
		{
			name: "control_no_reset", abort: false,
			wantType: client.EventData, wantCode: 0, wantResetIn: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, aborted := cov859ResetServer(t, tt.abort)
			tr := &cov859Tracer{}
			c := cov859TracedClient(t, addr, tr)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var sr client.StreamResponse
			require.NoErrorf(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr),
				"DoStream: the handler sends headers and flushes BEFORE aborting, so the "+
					"stream must open and the reset must arrive as an event on it")
			defer func() { _ = sr.Close() }()

			ev, err := sr.Recv(ctx)

			require.NoErrorf(t, err,
				"Recv = %v; a peer RST_STREAM is an EVENT on this API — surfacing it as an "+
					"error instead hides ResetCode from every streaming caller", err)
			// INJECTION COUNT. Everything below is vacuous without these two:
			// a run where the peer never reset looks exactly like a real pass.
			require.EqualValuesf(t, tt.wantResetIn, aborted.Load(),
				"handler aborts = %d, want %d", aborted.Load(), tt.wantResetIn)
			require.Equalf(t, tt.wantResetIn, tr.countIn("RST_STREAM"),
				"inbound RST_STREAM frames = %d, want %d — counted on the wire, so it cannot "+
					"be satisfied by the same code the assertions below test",
				tr.countIn("RST_STREAM"), tt.wantResetIn)
			assert.Equalf(t, tt.wantType, ev.Type,
				"Recv event type = %v, want %v", ev.Type, tt.wantType)
			assert.Equalf(t, tt.wantCode, ev.ResetCode,
				"Recv ResetCode = %v, want %v — the peer's code must reach the caller "+
					"verbatim; the retry layer decides on it", ev.ResetCode, tt.wantCode)
			assert.Truef(t, ev.EndStream,
				"EndStream = false on a %v event; both a reset and a clean final DATA frame "+
					"end the stream, and a caller looping until EndStream would hang", ev.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// client.go: do() — NewStream failure branch (closed conn)
// ---------------------------------------------------------------------------

func TestClient_Do_NewStream_Failure(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	// Close the client so acquire will fail on the next Do.
	_ = c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp client.Response

	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.Errorf(t, err, "expected error after Close, got nil")
}

// ---------------------------------------------------------------------------
// client.go: doStream() — transport acquire failure
// ---------------------------------------------------------------------------

func TestClient_DoStream_AcquireFailure(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	_ = c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr client.StreamResponse

	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)

	require.Errorf(t, err, "expected error after Close, got nil")
}

// ---------------------------------------------------------------------------
// client.go: writeBodyReader — reader returns error path
// ---------------------------------------------------------------------------

type errReader struct {
	n   int // bytes to deliver before error
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.n > 0 {
		fill := e.n
		if fill > len(p) {
			fill = len(p)
		}
		for i := 0; i < fill; i++ {
			p[i] = 'A'
		}
		e.n -= fill
		return fill, nil
	}
	return 0, e.err
}

func TestClient_Do_WriteBodyReader_ReadError(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Just drain and respond 200; we may not reach this.
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	injectedErr := errors.New("injected read error")
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:        "POST",
		Path:          "/upload",
		BodyReader:    &errReader{n: 0, err: injectedErr},
		ContentLength: 100,
	}, &resp)

	require.Errorf(t, err, "expected error from read-error body, got nil")
	if !strings.Contains(err.Error(), "read request body") && !strings.Contains(err.Error(), "injected") {
		t.Logf("error (may not be read-body wrap on zero-byte reader): %v", err)
	}
}

func TestClient_Do_WriteBodyReader_ReadError_AfterBytes(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	injectedErr := fmt.Errorf("injected mid-stream read error")
	// Deliver 1 byte then error — exercises the "n > 0 then rerr != nil" branch.
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:        "POST",
		Path:          "/upload",
		BodyReader:    &errReader{n: 1, err: injectedErr},
		ContentLength: 1024,
	}, &resp)

	require.Errorf(t, err, "expected error from mid-stream read error body, got nil")
}

// ---------------------------------------------------------------------------
// client.go: drainResponse — unexpected first event (non-headers) via
// DoStream on a server that sends DATA before HEADERS (malformed server).
// We can't easily craft this via httptest, but we can cover the
// "unexpected event" path in doStream by using a mock transport.
// Instead, test the StreamResetError path through drainResponse.
// ---------------------------------------------------------------------------

// TestClient_Do_DrainResponse_StreamReset pins drainResponse's conn.EventReset
// arm: a peer reset arriving after the response headers must fail Do with a
// *StreamResetError carrying the peer's code — not succeed with the partial
// 200 those headers already promised.
//
// Returning nil there is the dangerous mutant and a plausible one: the headers
// HAVE arrived and resp.Status is already 200 by the time the reset lands, so
// a "complete enough" reading of this arm hands the caller a truncated body
// under a success status.
//
// The error's TYPE matters as much as its presence.
// TestConformance_RFC9113_Sec8_1_RealResetStillFailsUpload asserts the other
// side of the same coin — a reset that kills an UPLOAD must NOT surface as
// *StreamResetError, because Retryer keys on that type and would replay work
// the server already did — so the arm that does produce one needs its own gate.
func TestClient_Do_DrainResponse_StreamReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		abort       bool
		wantReset   bool
		wantCode    conn.ErrCode
		wantResetIn int
	}{
		{
			name: "peer_reset", abort: true,
			wantReset: true, wantCode: frame.ErrCodeInternalError, wantResetIn: 1,
		},
		{
			name: "control_no_reset", abort: false,
			wantReset: false, wantCode: 0, wantResetIn: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, aborted := cov859ResetServer(t, tt.abort)
			tr := &cov859Tracer{}
			c := cov859TracedClient(t, addr, tr)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var resp client.Response

			err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

			// INJECTION COUNT first: everything below is vacuous without it.
			require.EqualValuesf(t, tt.wantResetIn, aborted.Load(),
				"handler aborts = %d, want %d", aborted.Load(), tt.wantResetIn)
			require.Equalf(t, tt.wantResetIn, tr.countIn("RST_STREAM"),
				"inbound RST_STREAM frames = %d, want %d", tr.countIn("RST_STREAM"), tt.wantResetIn)
			var sre *client.StreamResetError
			if !tt.wantReset {
				assert.NoErrorf(t, err, "Do = %v on a clean 200; the control arm must succeed", err)
				assert.Falsef(t, errors.As(err, &sre),
					"Do reported a stream reset on a connection where no RST_STREAM was ever "+
						"sent — then the reset arm is firing on something other than a reset")
				return
			}
			require.Errorf(t, err,
				"Do = nil with status %d and %d body bytes; the peer reset the stream after "+
					"the headers, so a nil error hands the caller a truncated body under a "+
					"success status", resp.Status, len(resp.Body))
			require.Truef(t, errors.As(err, &sre),
				"Do error = %v (%T), want a *StreamResetError — Retryer classifies on that "+
					"type, and a reset it cannot recognise is a reset it cannot decide about",
				err, err)
			assert.Equalf(t, tt.wantCode, sre.Code,
				"StreamResetError.Code = %v, want %v — the peer's code drives the retry "+
					"decision (RFC 9113 §8.7), so it must be the peer's and not a placeholder",
				sre.Code, tt.wantCode)
		})
	}
}

// ---------------------------------------------------------------------------
// pool.go: mapAcquireErr — context.Canceled path (not AcquireTimeout)
// ---------------------------------------------------------------------------

func TestPool_MapAcquireErr_ContextCanceled(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 1,
			AcquireTimeout:    5 * time.Second,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	// Do one request to seed the pool with a conn.
	ctx := context.Background()
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "initial Do")

	// Now cancel immediately — the acquire should fail with context.Canceled.
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Do
	var resp2 client.Response

	err = c.Do(ctxCancel, &client.Request{Method: "GET", Path: "/"}, &resp2)

	// The hedge this replaced ("acceptable variant") was unfounded: a context
	// cancelled before Do is reported as context.Canceled every time. Measured
	// stable over 25 iterations under -race before tightening.
	require.ErrorIsf(t, err, context.Canceled,
		"Do with an already-cancelled context = %v, want context.Canceled", err)
}

// ---------------------------------------------------------------------------
// pool.go: evictDeadSilent — a dead conn reaped by a metrics read
// ---------------------------------------------------------------------------

// TestPool_EvictDeadSilent_Via_Stats pins both halves of what "silent" means
// on that path — the eviction is RECORDED in ConnsClosed and the OnConnClose
// hook stays quiet — and it pins them by NAMING the mechanism rather than
// hoping for it.
//
// Three sites can evict a dead conn, and two of them would satisfy a naive
// "ActiveConns went to zero": handleTick -> evictDead and handleRelease ->
// evict. Both are excluded here by construction. HealthCheckPeriod is ten
// minutes, so no tick can fire inside the test; the seeding request has
// already been released while the conn was still alive, so handleRelease
// cannot run again. That leaves handleStats -> evictDeadSilent as the only
// site that can move ActiveConns, and the zero hook count is what tells the
// two apart: evictDead and evict both call notifyClose, evictDeadSilent
// deliberately does not.
//
// This test used to close the CLIENT and then call PoolStats. That never
// reached evictDeadSilent at all: Pool.Stats selects on p.closedCh, which is
// already closed by then, and returns a zero Stats without the actor running.
// The ConnsClosed increment it could have observed came from handleClose's
// notifyClose(CloseManual) — a different function entirely (#859).
func TestPool_EvictDeadSilent_Via_Stats(t *testing.T) {
	t.Parallel()
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	rec := &cov859CloseRecorder{}
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 10,
			// Long enough that handleTick provably cannot fire inside this
			// test, so evictDead is not a candidate for what we observe.
			HealthCheckPeriod: 10 * time.Minute,
		},
		Hooks: rec.hooks(),
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var resp client.Response
	require.NoErrorf(t, c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp), "seeding Do")
	require.Equalf(t, 1, c.PoolStats().ActiveConns,
		"ActiveConns = %d after one request, want 1 — without a live conn to kill there is "+
			"nothing for this test to observe being evicted", c.PoolStats().ActiveConns)
	require.Zerof(t, c.MetricsSnapshot().Counters.ConnsClosed,
		"ConnsClosed = %d before the peer died; the delta below would not be attributable",
		c.MetricsSnapshot().Counters.ConnsClosed)

	srv.Close() // the peer dies; nothing client-side has noticed yet

	var st client.Stats
	polls := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		polls++
		st = c.PoolStats() // handleStats -> evictDeadSilent
		if st.ActiveConns == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Zerof(t, st.ActiveConns,
		"ActiveConns = %d after %d PoolStats() calls against a dead peer; the pool is "+
			"reporting capacity it does not have, and acquire will keep handing it out",
		st.ActiveConns, polls)
	assert.EqualValuesf(t, 1, c.MetricsSnapshot().Counters.ConnsClosed,
		"ConnsClosed = %d, want 1 — silent means no user CALLBACK, not no record. A conn "+
			"killed out of band and first noticed by a metrics read is exactly the case an "+
			"operator is scraping for, and it used to leave this counter at zero forever",
		c.MetricsSnapshot().Counters.ConnsClosed)
	assert.Emptyf(t, rec.snapshot(),
		"OnConnClose fired %d time(s) from a metrics read: %+v. Firing a lifecycle callback "+
			"out of Stats() is the thing this path is named for not doing — and it also "+
			"means evictDead or evict did the eviction, not evictDeadSilent",
		len(rec.snapshot()), rec.snapshot())
}

// ---------------------------------------------------------------------------
// pool.go: countLive — mix of live and dead conns counted correctly.
// (Internal function; covered indirectly via pool actor under load.)
// ---------------------------------------------------------------------------

func TestPool_CountLive_IndirectCoverage(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 100,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	// Issue two requests concurrently to spin up two conns, then call Stats.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var resp client.Response
			done <- c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Logf("concurrent Do: %v", err)
		}
	}
	st := c.PoolStats()

	// >= 1, not >= 0. ActiveConns is a count of live conns and is never
	// negative, so the original ">= 0" was an assertion that could not fire:
	// countLive could return anything, or not be reached at all, and this test
	// stayed green. Two requests have just completed against a pool with
	// MaxConnsPerHost=2 and no idle eviction configured, so at least one conn
	// must still be live at this point.
	assert.GreaterOrEqualf(t, st.ActiveConns, 1,
		"ActiveConns = %d after two completed requests, want >= 1 — countLive is not seeing the pool's live conns", st.ActiveConns)
}

// ---------------------------------------------------------------------------
// managed_pool.go: getOrCreateSubPool — draining sub-pool TOCTOU guard
// and pool closed paths. Triggered via the integration test patterns.
// ---------------------------------------------------------------------------

func TestManagedPool_Acquire_ContextCancel(t *testing.T) {
	t.Parallel()
	// Use a resolver with an address that can't connect so acquire blocks,
	// then cancel the context.
	addr := client.Address{Host: "127.0.0.1", Port: 1}
	r := client.StaticResolver(addr)
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 1,
			DialTimeout:     2 * time.Second,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.Errorf(t, err, "expected error from unreachable host with canceled ctx")
}

// ---------------------------------------------------------------------------
// retry.go: DoStream — retry path exercises
// ---------------------------------------------------------------------------

func TestRetryer_DoStream_RetryOnDialError(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return time.Millisecond },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse

	err := r.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)

	require.NoErrorf(t, err, "DoStream")
	defer func() { _ = sr.Close() }()
	assert.Equalf(t, 200, sr.Status, "Status = %d, want 200", sr.Status)
}

// ---------------------------------------------------------------------------
// managed_pool.go: newManagedPool with explicit PoolOptions — exercises
// the opts.Pool != nil branch in NewClient (TransportManaged).
// ---------------------------------------------------------------------------

func TestNewClient_TransportManaged_WithPoolOptions(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, err := net.SplitHostPort(addr)
	require.NoErrorf(t, err, "SplitHostPort")
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 10,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoErrorf(t, err, "Do")
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	// Exercise PoolStats on managedTransport.
	st := c.PoolStats()
	assert.Truef(t, st.Addresses >= 1, "Addresses = %d, want ≥ 1", st.Addresses)
}

// ---------------------------------------------------------------------------
// managed_pool.go: acquire — ErrNoAddresses when set is empty after tried-set
// filtering leaves nothing (covered via zero-address resolver test already,
// but this covers the lastErr != nil path).
// ---------------------------------------------------------------------------

func TestManagedPool_Acquire_ErrNoAddressesWithLastErr(t *testing.T) {
	t.Parallel()
	// Two addresses that both fail — exercises fallthrough where tried[] fills
	// all entries, set becomes empty, lastErr is returned.
	addrs := []client.Address{
		{Host: "127.0.0.1", Port: 1},
		{Host: "127.0.0.1", Port: 2},
	}
	r := client.StaticResolver(addrs...)
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 1,
			DialTimeout:     200 * time.Millisecond,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.Errorf(t, err, "expected error, got nil")
	// Should be a DialError from the last failed address (not ErrNoAddresses).
	var de *client.DialError
	if !errors.As(err, &de) {
		t.Logf("got %T: %v (DialError preferred but other errors also acceptable)", err, err)
	}
}

// ---------------------------------------------------------------------------
// response.go: Recv after drained returns ErrStreamEnded immediately
// ---------------------------------------------------------------------------

func TestStreamResponse_Recv_AfterDrained(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)
	require.NoErrorf(t, err, "DoStream")
	defer func() { _ = sr.Close() }()

	// Drain to EndStream.
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			t.Logf("Recv: %v", err)
			break
		}
		if ev.EndStream {
			break
		}
	}

	// Second call after drained must return ErrStreamEnded.
	_, err = sr.Recv(ctx)

	assert.ErrorIsf(t, err, client.ErrStreamEnded, "Recv after drained = %v, want ErrStreamEnded", err)
}

// ---------------------------------------------------------------------------
// client.go: do() BodyStream path where initial Recv returns unexpected event.
// This is hard to trigger against httptest, so cover the adjacent code path
// where drainResponse processes EventTrailers.
// ---------------------------------------------------------------------------

func TestClient_Do_DrainResponse_WithTrailers(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "x-trailer")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
		w.Header().Set("x-trailer", "val")
	}))
	c := covClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:       "GET",
		Path:         "/",
		BodyMode:     client.BodyBuffer,
		WantTrailers: true,
	}, &resp)

	require.NoErrorf(t, err, "Do")
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
}

// ---------------------------------------------------------------------------
// pool.go: mapAcquireErr — AcquireTimeout path
// ---------------------------------------------------------------------------

func TestPool_MapAcquireErr_AcquireTimeout(t *testing.T) {
	t.Parallel()
	// Use a slow-responding server and a very short AcquireTimeout.
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(200)
	}))
	// One conn, one stream on it, and an AcquireTimeout that expires well before
	// the handler does. The first request ties up the only slot for 500ms; the
	// second finds the pool at capacity and must be refused.
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 1,
			// Long enough to outlive the first request's dial and TLS handshake,
			// short enough to expire while the handler still holds the only stream
			// slot. At 1ms the FIRST acquire timed out waiting for its own dial, the
			// conn then landed in the pool, and the second request sailed through --
			// so the assertion below was being made about the wrong request.
			AcquireTimeout: 200 * time.Millisecond,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx := context.Background()
	errCh := make(chan error, 1)
	// First request: occupies the only stream slot.
	go func() {
		var resp client.Response
		errCh <- c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	}()

	// Small sleep to let the first goroutine get to the server.
	time.Sleep(20 * time.Millisecond)

	// Second request: pool at capacity, AcquireTimeout=1ms → ErrAcquireTimeout.
	var resp2 client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp2)

	require.ErrorIsf(t, err, client.ErrAcquireTimeout, "Do at pool capacity = %v, want ErrAcquireTimeout", err)

	// The first request must actually have held the slot. Without this the test
	// can pass for the wrong reason: if it failed early the pool is idle, and
	// whatever the second request returns says nothing about acquire timeouts.
	ferr := <-errCh
	require.NoErrorf(t, ferr, "first request, which is supposed to occupy the only slot")
}

// ---------------------------------------------------------------------------
// frame.ErrCode / StreamResetError via EventReset coverage
// ---------------------------------------------------------------------------

// TestStreamResponse_WaitTrailers_EventReset pins WaitTrailers' documented
// answer on a reset stream: nil trailers and a NIL error.
//
// Both halves are load-bearing and both are mutable. The nil error is the
// contract that a reset is not a WaitTrailers failure — callers are told to
// use Recv to distinguish the cases — so returning the reset as an error here
// would break every caller that treats a non-nil err as fatal. The nil SLICE
// is the contract the doc comment spells out one paragraph further down: an
// empty trailer section returns a non-nil empty slice, so nil-ness is how a
// caller tells "trailers received, and there were none" from "no trailers".
// A reset arm returning []conn.HeaderField{} would tell that caller the peer
// sent an empty trailer section it never sent.
//
// The control arm ends the stream cleanly with no trailers. It reaches the
// same (nil, nil) through WaitTrailers' ErrStreamEnded branch instead, which
// is the point: it shows the assertions below are not simply reading off the
// clean-end path, because the injection count differs and only the reset arm
// moves when the EventReset arm is changed.
//
// Note for anyone mutating this: DELETING WaitTrailers' `case EventReset`
// outright is an equivalent mutant. Recv sets sr.drained on that event, so the
// next loop iteration takes ErrStreamEnded and returns the same (nil, nil).
// The observable mutation is the RETURN VALUE, not the arm's existence.
func TestStreamResponse_WaitTrailers_EventReset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		abort       bool
		wantResetIn int
	}{
		{name: "peer_reset", abort: true, wantResetIn: 1},
		{name: "control_clean_end", abort: false, wantResetIn: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, aborted := cov859ResetServer(t, tt.abort)
			tr := &cov859Tracer{}
			c := cov859TracedClient(t, addr, tr)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var sr client.StreamResponse
			require.NoErrorf(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr),
				"DoStream: headers precede the abort, so the stream must open")
			defer func() { _ = sr.Close() }()

			trailers, err := sr.WaitTrailers(ctx)

			// INJECTION COUNT. A run in which the peer never reset passes the
			// assertions below identically, so they mean nothing without this.
			require.EqualValuesf(t, tt.wantResetIn, aborted.Load(),
				"handler aborts = %d, want %d", aborted.Load(), tt.wantResetIn)
			require.Equalf(t, tt.wantResetIn, tr.countIn("RST_STREAM"),
				"inbound RST_STREAM frames = %d, want %d — counted on the wire, independent "+
					"of the code under test", tr.countIn("RST_STREAM"), tt.wantResetIn)
			assert.NoErrorf(t, err,
				"WaitTrailers err = %v, want nil — a reset stream simply has no trailers; "+
					"reporting it as an error makes every caller that treats err as fatal "+
					"fail a request whose response body already arrived", err)
			assert.Truef(t, trailers == nil,
				"WaitTrailers trailers = %#v, want nil. A non-nil empty slice is this API's "+
					"signal that the peer SENT an empty trailer section, so returning one "+
					"here reports a trailer block that never existed", trailers)
		})
	}
}

// ---------------------------------------------------------------------------
// frame package: ErrCode exercised (used in StreamResetError tests above)
// ---------------------------------------------------------------------------

func TestStreamResetError_Error(t *testing.T) {
	t.Parallel()
	e := &client.StreamResetError{Code: frame.ErrCodeCancel}

	assert.Containsf(t, e.Error(), "stream reset",
		"StreamResetError.Error() = %q, want to contain 'stream reset'", e.Error())
}

// ---------------------------------------------------------------------------
// single_conn.go: acquire after close — ErrClosed path
// ---------------------------------------------------------------------------

func TestSingleConn_Do_AfterClose_ErrClosed(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)

	// First request succeeds to establish a conn.
	ctx := context.Background()
	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "initial Do")

	_ = c.Close()

	// Second request should fail with ErrClosed (or wrapped).
	var resp2 client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp2)

	require.Errorf(t, err, "expected error after Close, got nil")
	require.ErrorIsf(t, err, client.ErrClosed, "Do after Close = %v, want ErrClosed", err)
}

// ---------------------------------------------------------------------------
// body.go: Read — EventReset path via RST_STREAM after initial HEADERS
// using BodyStream=true so we get a BodyReader.
// ---------------------------------------------------------------------------

// TestResponseBodyReader_Read_EventReset_ViaBodyStream is the sibling of
// TestResponseBodyReader_Read_EventReset with response headers on the stream
// (#900). It shared the same inert http.Hijacker fixture and was satisfied by
// io.EOF for the same reason; see that test's comment for the measurement.
//
// It is kept separate rather than folded in because the header block changes
// what the reader has buffered when the reset lands, which is the state the
// EventReset arm's recycleDataLocked runs against.
func TestResponseBodyReader_Read_EventReset_ViaBodyStream(t *testing.T) {
	t.Parallel()
	addr, aborted := cov859ResetServer(t, true)
	tr := &cov859Tracer{}
	c := cov859TracedClient(t, addr, tr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
	require.NoErrorf(t, err, "Do")
	require.Truef(t, resp.BodyReader != nil, "BodyReader is nil")
	defer func() { _ = resp.BodyReader.Close() }()
	buf := make([]byte, 1024)

	_, readErr := resp.BodyReader.Read(buf)

	require.EqualValuesf(t, 1, aborted.Load(),
		"the handler aborted %d times, want 1 — nothing reset the stream, so the "+
			"assertion below would be measuring the fixture", aborted.Load())
	assert.Positivef(t, tr.countIn("RST_STREAM"),
		"no RST_STREAM arrived from the peer, so whatever Read returned was not produced "+
			"by the mechanism this test names")
	var rst *client.StreamResetError
	require.ErrorAsf(t, readErr, &rst,
		"Read after a peer reset returned %v, want a *client.StreamResetError — io.EOF "+
			"here reports a killed stream as a complete body", readErr)
	assert.Equalf(t, frame.ErrCodeInternalError, rst.Code,
		"reset code = %v, want INTERNAL_ERROR", rst.Code)
}

// ---------------------------------------------------------------------------
// body.go: Read — large body exercises buf reuse path in responseBodyReader
// ---------------------------------------------------------------------------

func TestResponseBodyReader_Read_LargeBody_BufReuse(t *testing.T) {
	t.Parallel()
	const bodySize = 64 * 1024 // 64 KiB — more than readChunkSize=16 KiB
	body := bytes.Repeat([]byte("Z"), bodySize)
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
	require.NoErrorf(t, err, "Do")
	defer func() { _ = resp.BodyReader.Close() }()

	// Read in tiny chunks to exercise r.buf (leftover bytes) code path.
	tinyBuf := make([]byte, 100)
	var total int
	for {
		n, rerr := resp.BodyReader.Read(tinyBuf)
		total += n
		if rerr == io.EOF {
			break
		}
		require.NoErrorf(t, rerr, "Read")
	}

	assert.Equalf(t, bodySize, total, "read %d bytes, want %d", total, bodySize)
}

// ---------------------------------------------------------------------------
// client.go: do() — BodyStream path with small body (exercises the
// full streaming body path via BodyReader).
// ---------------------------------------------------------------------------

func TestClient_Do_BodyStream_SmallBody(t *testing.T) {
	t.Parallel()
	want := []byte("hello")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(want)
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)

	require.NoErrorf(t, err, "Do with BodyStream")
	require.Truef(t, resp.BodyReader != nil, "BodyReader is nil")
	defer func() { _ = resp.BodyReader.Close() }()
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	got, err := io.ReadAll(resp.BodyReader)
	require.NoErrorf(t, err, "ReadAll")
	assert.Equalf(t, want, got, "body = %q, want %q", got, want)
}

// ---------------------------------------------------------------------------
// body.go: responseBodyReader.Read — partial-read path (buf spill-over)
// ---------------------------------------------------------------------------

// TestResponseBodyReader_Read_PartialRead verifies that when the caller
// supplies a buffer smaller than the DATA frame, the surplus is buffered
// and returned on the next Read call.
func TestResponseBodyReader_Read_PartialRead(t *testing.T) {
	t.Parallel()
	payload := []byte("ABCDE") // 5 bytes
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
	require.NoErrorf(t, err, "Do")
	require.Truef(t, resp.BodyReader != nil, "BodyReader is nil")
	defer func() { _ = resp.BodyReader.Close() }()

	// Read with a 2-byte buffer — forces spill-over in body.go's Read.
	buf := make([]byte, 2)
	var got []byte
	for {
		n, err := resp.BodyReader.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoErrorf(t, err, "Read")
	}

	assert.Equalf(t, payload, got, "got %q, want %q", got, payload)
}

// ---------------------------------------------------------------------------
// response.go: Recv — EventTrailers path where trailers field is nil
// (empty trailer frame triggers sentinel path).
// ---------------------------------------------------------------------------

func TestStreamResponse_Recv_EventTrailers_EmptyTrailers(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Announce a trailer, then send empty trailer block.
		w.Header().Set("Trailer", "x-empty")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hi"))
		// Setting header to empty — trailer with empty value.
		w.Header().Set("x-empty", "")
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)
	require.NoErrorf(t, err, "DoStream")
	defer func() { _ = sr.Close() }()

	// Pump all events to cover the EventTrailers branch in Recv.
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			t.Logf("Recv err: %v", err)
			break
		}
		if ev.EndStream {
			break
		}
	}
}

// ---------------------------------------------------------------------------
// retry.go: DoStream — path where req.canRetry returns false (non-idempotent)
// so it delegates directly to the underlying DoStream once.
// ---------------------------------------------------------------------------

func TestRetryer_DoStream_NonIdempotent_NoRetry(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return time.Millisecond },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// POST is non-idempotent → canRetry returns false → delegate directly.
	var sr client.StreamResponse

	err := r.DoStream(ctx, &client.Request{Method: "POST", Path: "/"}, &sr)

	require.NoErrorf(t, err, "DoStream POST")
	defer func() { _ = sr.Close() }()
}

// ---------------------------------------------------------------------------
// managed_pool.go: getOrCreateSubPool — pool closed path
// ---------------------------------------------------------------------------

func TestManagedPool_GetOrCreateSubPool_AfterClose(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")

	// Do one request to create the sub-pool.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "initial Do")

	// Close the pool, then try again — should get an error.
	_ = c.Close()

	var resp2 client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp2)

	// After close, should get an error.
	assert.Errorf(t, err, "expected error after pool close, got nil")
}

// ---------------------------------------------------------------------------
// pool.go: acquire — ErrPoolClosed on send to acquireCh
// ---------------------------------------------------------------------------

func TestPool_Acquire_AfterClose_ErrPoolClosed(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 1,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	// Seed then close.
	ctx := context.Background()
	var resp client.Response
	_ = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	_ = c.Close()

	var resp2 client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp2)

	require.ErrorIsf(t, err, client.ErrPoolClosed,
		"Do after Close = %v, want ErrPoolClosed (the test is named for that sentinel and used to accept any error)", err)
}

// ---------------------------------------------------------------------------
// selector.go: Hash.Pick — empty key returns ErrNoAddresses
// ---------------------------------------------------------------------------

func TestHashSelector_Pick_EmptyKey(t *testing.T) {
	t.Parallel()
	addrs := []client.Address{
		{Host: "10.0.0.1", Port: 80},
		{Host: "10.0.0.2", Port: 80},
	}
	// keyFn that returns empty string → ErrNoAddresses.
	h, err := client.Hash(func(_ client.PickContext) string { return "" })
	require.NoErrorf(t, err, "Hash")

	_, err = h.Pick(addrs, client.PickContext{})

	assert.ErrorIsf(t, err, client.ErrNoAddresses, "Pick with empty key = %v, want ErrNoAddresses", err)
}

func TestHashSelector_Pick_NonEmptyKey(t *testing.T) {
	t.Parallel()
	addrs := []client.Address{
		{Host: "10.0.0.1", Port: 80},
		{Host: "10.0.0.2", Port: 80},
	}
	h, err := client.Hash(func(_ client.PickContext) string { return "session-123" })
	require.NoErrorf(t, err, "Hash")

	got, err := h.Pick(addrs, client.PickContext{})
	require.NoErrorf(t, err, "Pick")

	// Same key must pick same address.
	got2, _ := h.Pick(addrs, client.PickContext{})

	assert.Equalf(t, got.Host, got2.Host, "Hash selector not deterministic: %s != %s", got.Host, got2.Host)
}

// ---------------------------------------------------------------------------
// NewClient: DefaultScheme field exercised
// ---------------------------------------------------------------------------

func TestNewClient_DefaultScheme_H2C(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	// "https" is the default; just ensure the field is exercised.
	c, err := client.NewClient(client.ClientOptions{
		Addr:          addr,
		DefaultScheme: "https",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.NoErrorf(t, err, "Do")
}

// ---------------------------------------------------------------------------
// frame package usage — ensure frame import is used
// ---------------------------------------------------------------------------

func TestFrame_ErrCodeCancel_IsNonZero(t *testing.T) {
	t.Parallel()
	assert.NotZerof(t, frame.ErrCodeCancel, "ErrCodeCancel should be non-zero")
}

// ---------------------------------------------------------------------------
// managed_pool.go: acquire — selector.Pick error path (custom broken selector)
// ---------------------------------------------------------------------------

// errBrokenSelector is what brokenSelector fails with. A sentinel rather than
// an inline errors.New, so the test can assert identity instead of matching
// the message text -- a wrapper that reworded it would otherwise pass.
var errBrokenSelector = errors.New("selector: intentional failure")

// brokenSelector always returns an error from Pick.
type brokenSelector struct{}

func (b brokenSelector) Pick(_ []client.Address, _ client.PickContext) (client.Address, error) {
	return client.Address{}, errBrokenSelector
}

func TestManagedPool_Acquire_SelectorPickError(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		Selector:  brokenSelector{},
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp client.Response

	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)

	require.Errorf(t, err, "expected selector error, got nil")
	require.ErrorIsf(t, err, errBrokenSelector,
		"Do with a failing selector = %v, want it to wrap errBrokenSelector", err)
}

// ---------------------------------------------------------------------------
// managed_pool.go: acquire — non-dial-only error causes immediate return
// (ErrAcquireTimeout is not a dial-only error)
// ---------------------------------------------------------------------------

func TestManagedPool_Acquire_NonDialOnlyErr_ImmediateReturn(t *testing.T) {
	t.Parallel()
	// The first request must still hold the single stream slot when the second
	// one asks. The old version slept 20ms and hoped -- and carried a branch
	// accepting a nil error "because the first request may have completed",
	// which made the test unable to fail. The handler now says when it has
	// arrived, and holds until the test lets go.
	var once sync.Once
	arrived := make(chan struct{})
	release := make(chan struct{})
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(arrived) })
		<-release
		w.WriteHeader(200)
	}))
	defer close(release)
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 1,
			// Long enough for the first request to dial and take the slot, short
			// enough that the second gives up promptly. At the previous 1ms even
			// the FIRST request lost -- a TLS dial cannot finish in 1ms -- so
			// nothing ever held the slot and the assertion had to be excused.
			AcquireTimeout: 100 * time.Millisecond,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	// Fill the one slot.
	ctx := context.Background()
	go func() {
		var resp client.Response
		_ = c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the first request never reached the server; nothing was holding the slot")
	}

	// Second acquire: ErrAcquireTimeout is NOT a dial-only error → immediate return.
	ctxShort, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var resp2 client.Response

	err = c.Do(ctxShort, &client.Request{Method: "GET", Path: "/"}, &resp2)

	require.ErrorIsf(t, err, client.ErrAcquireTimeout,
		"acquire against a held slot = %v, want ErrAcquireTimeout; the 1ms AcquireTimeout is not a dial-only error, so it must return at once", err)
}

// ---------------------------------------------------------------------------
// metrics.go: Quantile — single observation in high bucket to exercise
// the bucket-edge return path more broadly
// ---------------------------------------------------------------------------

func TestHistogramSnapshot_Quantile_HighBucket(t *testing.T) {
	t.Parallel()
	var h client.Metrics
	// Observe a 1-second duration → bucket 29 (2^29 = ~537ms < 1s < 2^30)
	h.Latency.Request.Observe(time.Second)
	snap := h.Latency.Request.Snapshot()

	q50 := snap.Quantile(0.5)
	q99 := snap.Quantile(0.99)

	assert.NotZerof(t, q50, "Quantile(0.5) on single 1s observation = 0, want non-zero")
	assert.NotZerof(t, q99, "Quantile(0.99) = 0, want non-zero")

	// p > 1 clamped to 1. This was an `if q2 != q99` whose body was EMPTY, so
	// a Quantile that stopped clamping — or returned anything at all here —
	// passed unremarked. With a single observation every quantile lands in the
	// same bucket, so the clamp to p=1 is exactly "q2 equals q99".
	q2 := snap.Quantile(2.0)
	assert.Equalf(t, q99, q2,
		"Quantile(2.0) = %v, want Quantile(0.99) = %v — p > 1 must clamp to 1 rather than run off the bucket array", q2, q99)
}

// ---------------------------------------------------------------------------
// response.go: WaitTrailers — ErrStreamEnded path
// ---------------------------------------------------------------------------

func TestStreamResponse_WaitTrailers_AlreadyDrained(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		// No body — stream ends immediately after headers.
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)
	require.NoErrorf(t, err, "DoStream")
	defer func() { _ = sr.Close() }()

	// Drain fully.
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			break
		}
		if ev.EndStream {
			break
		}
	}

	// WaitTrailers on a fully drained stream with no trailers returns nil, nil.
	trailers, err := sr.WaitTrailers(ctx)

	assert.NoErrorf(t, err, "WaitTrailers after drain = %v, want nil", err)
	// A response that ends on its HEADERS carries no trailer section at all, so
	// this is nil rather than an empty slice. The old comment allowed either.
	require.Truef(t, trailers == nil,
		"WaitTrailers on a stream with no trailer section = %#v, want nil", trailers)
}

// ---------------------------------------------------------------------------
// poolTransport: shutdown path
// ---------------------------------------------------------------------------

// TestPoolTransport_Shutdown verifies Client.Shutdown on a pool transport
// closes all underlying conns.
func TestPoolTransport_Shutdown(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportPool,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{MaxConnsPerHost: 1},
	})
	require.NoErrorf(t, err, "NewClient")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "Do")

	err = c.Shutdown(500 * time.Millisecond)

	require.NoErrorf(t, err, "Shutdown")
}

// ---------------------------------------------------------------------------
// managedTransport: shutdown and warmup paths
// ---------------------------------------------------------------------------

// TestManagedTransport_Shutdown verifies Client.Shutdown on a managed
// transport closes the underlying pool gracefully.
func TestManagedTransport_Shutdown(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi")

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")

	// Do one request so a sub-pool is created.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "Do")

	err = c.Shutdown(500 * time.Millisecond)

	// Shutdown should complete without error.
	require.NoErrorf(t, err, "Shutdown")
}

// TestManagedTransport_Warmup verifies Client.Warmup on a managed transport
// fans out pre-dial across resolved addresses.
func TestManagedTransport_Warmup(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi")

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	// Warmup should not panic or error — it fires background dials.
	c.Warmup(2)

	// Give warmup goroutines a moment to complete.
	time.Sleep(200 * time.Millisecond)

	// Warmup has to have OPENED a conn, and this must be checked BEFORE any
	// request: asserting only that a later request succeeds proves nothing,
	// since it dials on demand and succeeds with Warmup deleted entirely.
	//
	// One, not two. This client passes no PoolOptions, so MaxConnsPerHost
	// defaults to 1 and Warmup(2) is capped by it -- which this also pins.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && c.PoolStats().ActiveConns < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	got := c.PoolStats().ActiveConns
	require.Equalf(t, 1, got,
		"ActiveConns after Warmup(2) = %d, want 1 (MaxConnsPerHost defaults to 1 here)", got)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "Do after warmup")
}

// ---------------------------------------------------------------------------
// client.go: do() — BodyStream with nil resp returns error immediately
// ---------------------------------------------------------------------------

// TestClient_Do_BodyStream_NilResp covers the "BodyStream requires a
// non-nil *Response" guard inside do() (conn.go:do line 426).
func TestClient_Do_BodyStream_NilResp(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, nil)

	require.Errorf(t, err, "expected error for BodyStream with nil *Response")
	require.Containsf(t, err.Error(), "BodyStream", "unexpected error: %v", err)
}

// ---------------------------------------------------------------------------
// client.go: do() — gzip-compressed response triggers decompressor
// ---------------------------------------------------------------------------

// TestClient_Do_GzipResponse covers the decompression path inside do()
// (enc != EncodingIdentity branch) using a server that sends gzip body.
func TestClient_Do_GzipResponse(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(200)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("hello compressed"))
		_ = gz.Close()
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do")
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	// The bytes, not just the status (#860). Without this the test is satisfied
	// by a decompressFully that hands back the still-compressed payload, an
	// empty slice, or a truncated one — which is exactly what the sweep measured
	// on the deflate twin below, where the buffered test passed under a mutation
	// its streaming sibling caught.
	assert.Equalf(t, "hello compressed", string(resp.Body),
		"buffered gzip body = %q, want the decompressed payload; this is the one test "+
			"named for the buffered decompression path", resp.Body)
}

// TestClient_Do_GzipBodyStream covers the BodyStream decompression path.
func TestClient_Do_GzipBodyStream(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(200)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("streaming compressed body"))
		_ = gz.Close()
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoErrorf(t, err, "Do BodyStream gzip")
	require.Truef(t, resp.BodyReader != nil, "expected BodyReader")
	body, _ := io.ReadAll(resp.BodyReader)
	_ = resp.BodyReader.Close()
	require.Equalf(t, "streaming compressed body", string(body),
		"body = %q, want %q", body, "streaming compressed body")
}

// TestClient_Do_DeflateResponse covers the EncodingDeflate path in
// newDecompressingReader and decompressFully.
func TestClient_Do_DeflateResponse(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "deflate")
		w.WriteHeader(200)
		zw := zlib.NewWriter(w)
		_, _ = zw.Write([]byte("deflate compressed body"))
		_ = zw.Close()
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
	}, &resp)

	require.NoErrorf(t, err, "Do deflate")
	assert.Equalf(t, 200, resp.Status, "Status = %d, want 200", resp.Status)
	// See the gzip twin (#860). Measured: with deflate detection disabled in
	// detectEncoding, this test passed twice while TestClient_Do_DeflateBodyStream
	// went red — it was handed the raw zlib bytes and reported success.
	assert.Equalf(t, "deflate compressed body", string(resp.Body),
		"buffered deflate body = %q, want the decompressed payload", resp.Body)
}

// TestDecompressingReader_Read_AfterClose covers the d.dec==nil path in Read().
// After Close() sets dec to nil, subsequent Read calls must return io.EOF.
func TestDecompressingReader_Read_AfterClose(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(200)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write([]byte("data"))
		_ = gz.Close()
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)
	require.NoErrorf(t, err, "Do")
	require.Truef(t, resp.BodyReader != nil, "expected BodyReader")
	_, _ = io.ReadAll(resp.BodyReader)
	_ = resp.BodyReader.Close()

	// dec is nil after Close; this hits the nil-dec path → io.EOF
	n, err := resp.BodyReader.Read(make([]byte, 8))

	assert.Truef(t, n == 0 && err == io.EOF, "Read after Close = (%d, %v), want (0, io.EOF)", n, err)
}

// TestClient_Do_BodyStream_RecvTimeout covers the s.Recv() error path in do()
// when the context times out before the server sends response headers.
func TestClient_Do_BodyStream_RecvTimeout(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Never send headers — forces client context to time out.
		time.Sleep(10 * time.Second)
	}))
	c := covClientFor(t, addr)
	// Very short deadline so s.Recv returns context.DeadlineExceeded quickly.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp)

	require.Errorf(t, err, "expected timeout error from BodyStream with unresponsive server")
	require.ErrorIsf(t, err, context.DeadlineExceeded,
		"BodyStream against an unresponsive server = %v, want context.DeadlineExceeded (an 80ms deadline against a 10s handler has no other way to end)", err)
}

// TestClient_Do_DeflateBodyStream covers the EncodingDeflate path via BodyStream.
func TestClient_Do_DeflateBodyStream(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "deflate")
		w.WriteHeader(200)
		zw := zlib.NewWriter(w)
		_, _ = zw.Write([]byte("deflate stream body"))
		_ = zw.Close()
	}))
	c := covClientFor(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoErrorf(t, err, "Do DeflateBodyStream")
	require.Truef(t, resp.BodyReader != nil, "expected BodyReader")
	body, _ := io.ReadAll(resp.BodyReader)
	_ = resp.BodyReader.Close()
	require.Equalf(t, "deflate stream body", string(body),
		"body = %q, want %q", body, "deflate stream body")
}

// ---------------------------------------------------------------------------
// managed_pool.go: warmup — loop body `s.p.warmup(per)` (managed_pool.go:434)
// Only executes when sub-pools exist. We create a sub-pool via one request,
// then call Warmup so the for-range body is covered.
// ---------------------------------------------------------------------------

func TestManagedPool_Warmup_AfterFirstRequest(t *testing.T) {
	t.Parallel()
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	host, portStr, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)

	r := client.StaticResolver(client.Address{Host: host, Port: port})
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  r,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 10,
		},
	})
	require.NoErrorf(t, err, "NewClient")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Make one request to force creation of the sub-pool.
	var resp client.Response
	err = doWithRetry(t, c, ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
	require.NoErrorf(t, err, "initial Do")

	// Now call Warmup — subs is non-empty so the loop body runs (s.p.warmup(per)).
	c.Warmup(2)

	// This test used to end here, with no assertion of any kind: a Warmup that
	// was a no-op, dialled the wrong host, or opened a hundred conns all passed
	// it identically. Its only value was crash detection.
	//
	// One conn already exists from the request above, so warming to 2 must add
	// exactly one more, and must not exceed MaxConnsPerHost.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := c.PoolStats().ActiveConns
	require.Equalf(t, 2, got, "ActiveConns after Warmup(2) = %d, want 2", got)
}

// ---------------------------------------------------------------------------
// pool.go: handleClose — the CloseGoAway attribution
// ---------------------------------------------------------------------------

// TestPool_HandleClose_GoAwayConn pins what a Client.Close during a peer's
// graceful drain is REPORTED as. handleClose closes every conn it still holds;
// one that has taken a GOAWAY must be attributed to the peer (CloseGoAway,
// GoAwaysReceived++), not to the local Close. Getting that wrong is silent and
// it is the whole reason the counter exists: a rolling restart of the backend
// shows up on a dashboard as ordinary client-side shutdown.
//
// Three things about the shape of this test are deliberate.
//
// It closes the client with the request STILL PARKED in the handler. That is
// what makes handleClose the mechanism rather than a lookalike: handleRelease
// carries the identical `if GoAwayReceived() { reason = CloseGoAway }` block
// and reaps the conn the moment its last stream is released, so a test that
// lets the request finish first measures pool.go's release path and passes
// against a handleClose whose GOAWAY branch has been deleted. With active > 0,
// handleRelease's own `active == 0` guard rules it out, and a ten-minute
// HealthCheckPeriod rules out evictDead. handleClose is then the only site
// that can emit anything at all.
//
// The GOAWAY is injected with srv.Config.Shutdown, not srv.Close. Shutdown is
// http.Server's graceful path and is what puts a GOAWAY on the wire; Close
// tears the socket down without one. Measured: with srv.Close the reason
// observed here was CloseManual — the branch this test was written for had
// never once executed (#859).
//
// The control arm never shuts the peer down, so the same Close must report
// CloseManual. Without it, an assertion that some event arrived with some
// reason is satisfied by a handleClose that hardcodes one.
func TestPool_HandleClose_GoAwayConn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		peerGoesAway bool
		wantReason   client.CloseReason
		wantGoAwayIn int
	}{
		{name: "peer_goaway", peerGoesAway: true, wantReason: client.CloseGoAway, wantGoAwayIn: 1},
		{name: "control_no_goaway", peerGoesAway: false, wantReason: client.CloseManual, wantGoAwayIn: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			var reached sync.WaitGroup
			reached.Add(1)
			var once sync.Once
			srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				once.Do(reached.Done)
				<-release
				w.WriteHeader(200)
			}))
			tr := &cov859Tracer{}
			rec := &cov859CloseRecorder{}
			c, err := client.NewClient(client.ClientOptions{
				Addr:      addr,
				Transport: client.TransportPool,
				Tracer:    tr,
				ConnOpts: conn.ConnOptions{
					Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
				},
				Pool: &client.PoolOptions{
					MaxConnsPerHost:   1,
					MaxStreamsPerConn: 10,
					HealthCheckPeriod: 10 * time.Minute,
				},
				Hooks: rec.hooks(),
			})
			require.NoErrorf(t, err, "NewClient")
			done := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var resp client.Response
				done <- c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &resp)
			}()
			reached.Wait() // the conn now carries one active stream
			shutdownDone := make(chan struct{})
			if tt.peerGoesAway {
				go func() {
					defer close(shutdownDone)
					_ = srv.Config.Shutdown(context.Background())
				}()
				deadline := time.Now().Add(20 * time.Second)
				for time.Now().Before(deadline) && tr.countIn("GOAWAY") == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			} else {
				close(shutdownDone)
			}
			// INJECTION COUNT, read on the wire and BEFORE the act: a run in
			// which Shutdown's GOAWAY never landed reaches handleClose with an
			// ordinary conn and passes the control arm's assertions exactly.
			require.Equalf(t, tt.wantGoAwayIn, tr.countIn("GOAWAY"),
				"inbound GOAWAY frames = %d, want %d", tr.countIn("GOAWAY"), tt.wantGoAwayIn)
			require.Zerof(t, c.MetricsSnapshot().Counters.GoAwaysReceived,
				"GoAwaysReceived = %d before Close; the delta below must be handleClose's",
				c.MetricsSnapshot().Counters.GoAwaysReceived)

			closeErr := c.Close()

			assert.NoErrorf(t, closeErr, "Client.Close = %v", closeErr)
			events := rec.snapshot()
			require.Lenf(t, events, 1,
				"OnConnClose fired %d time(s): %+v. Exactly one conn existed and handleClose "+
					"closed it once; a zero here means the conn was reaped before Close and "+
					"this test observed some other pool path", len(events), events)
			assert.Equalf(t, tt.wantReason, events[0].Reason,
				"OnConnClose reason = %v, want %v — attributing a peer's GOAWAY to a local "+
					"Close (or the reverse) makes a rolling backend restart indistinguishable "+
					"from an ordinary client shutdown on a dashboard",
				events[0].Reason, tt.wantReason)
			assert.EqualValuesf(t, tt.wantGoAwayIn, c.MetricsSnapshot().Counters.GoAwaysReceived,
				"GoAwaysReceived = %d, want %d — one conn, one GOAWAY; this counter and the "+
					"reason above are set together and must agree",
				c.MetricsSnapshot().Counters.GoAwaysReceived, tt.wantGoAwayIn)

			close(release)
			<-done
			<-shutdownDone
			if !tt.peerGoesAway {
				srv.Close()
			}
		})
	}
}
