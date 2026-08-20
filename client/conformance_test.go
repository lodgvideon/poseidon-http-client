package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams verifies that the
// connection pool opens additional connections when the peer advertises a small
// MAX_CONCURRENT_STREAMS value, honoring RFC 7540 §5.1.2.
//
// The test forces N concurrent requests to all be in-flight simultaneously
// via a server-side barrier. While they are blocked in the handler, it
// snapshots PoolStats and asserts that ActiveConns >= ceil(N / peerCap),
// proving the pool actually opened additional conns to absorb the load
// rather than queueing. Snapshotting AFTER load (the previous design) was
// TOCTOU-fragile: if conns were evicted between request completion and the
// stats read, the assertion could pass on a buggy pool that never opened
// more than one conn.
func TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams(t *testing.T) {
	const (
		N            = 8
		peerCap      = 2
		expectedMin  = (N + peerCap - 1) / peerCap // ceil(N/peerCap) = 4
		serverMaxStr = peerCap
	)

	// Barrier coordination: each handler increments inflight and signals
	// `allInflight` when N have arrived. Test snapshots stats after that
	// signal, then closes `release` to let handlers respond.
	var inflight atomic.Int32
	allInflight := make(chan struct{})
	release := make(chan struct{})

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if inflight.Add(1) == int32(N) {
			close(allInflight)
		}
		<-release
		w.WriteHeader(200)
	}))
	err := http2.ConfigureServer(srv.Config, &http2.Server{MaxConcurrentStreams: serverMaxStr})
	require.NoError(t, err, "ConfigureServer")
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   expectedMin,
			MaxStreamsPerConn: 0, // unbounded local — peer cap governs
			HealthCheckPeriod: time.Second,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var _res client.Response
			if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &_res); err != nil {
				errs <- err
			}
		}()
	}

	// Wait until all N requests are simultaneously in-flight at the server.
	// If the pool gates correctly on peer MAX_CONCURRENT_STREAMS, this
	// requires opening >=expectedMin conns; otherwise the barrier never closes.
	select {
	case <-allInflight:
	case <-time.After(5 * time.Second):
		close(release)
		require.Failf(t, "not every request reached the server",
			"only %d/%d requests reached server within 5s — pool may be queueing instead of opening more conns", inflight.Load(), N)
	}

	// Snapshot stats while load is still pinned in handlers.
	s := c.PoolStats()
	if s.ActiveConns < expectedMin {
		close(release)
		require.Failf(t, "pool did not open extra conns to absorb the load",
			"ActiveConns = %d, want >= %d during %d-way load with peer MAX_CONCURRENT_STREAMS=%d", s.ActiveConns, expectedMin, N, peerCap)
	}

	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err, "Do")
	}
}

// TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease verifies that
// when the peer sends GOAWAY while a stream is in-flight, the pool evicts
// the dead conn via the release path (BUG-1 fix) — not via the background
// HealthCheckPeriod tick.
//
// RFC 7540 §6.8: the conn-layer guarantee that streams ≤ lastStreamID
// continue normally is separately verified by TestOnGoAway_StreamsAtOrBelowLastID_Survive
// in the conn package. Here we focus on the pool-level eviction contract.
func TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease(t *testing.T) {
	started := make(chan struct{}) // closed when handler goroutine starts
	proceed := make(chan struct{}) // closed when test allows handler to respond

	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started) // stream is in-flight
		<-proceed
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			// Long health-check period so eviction can ONLY happen via the
			// release path (BUG-1 fix), not the background tick.
			HealthCheckPeriod: 60 * time.Second,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	// Launch request in background; it will be in-flight while we trigger GOAWAY.
	requestDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var _res client.Response
		err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &_res)
		requestDone <- err
	}()

	// Wait for stream to reach the server handler (in-flight).
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(proceed)
		require.Fail(t, "request did not reach server handler")
	}

	// Trigger graceful shutdown: server sends GOAWAY and waits for handlers
	// to complete, then closes the connection. Whether GOAWAY arrives before
	// or after the response, the conn is dead by the time Shutdown returns.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		shCtx, shCancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer shCancel()
		_ = srv.Config.Shutdown(shCtx)
	}()

	// Allow handler to respond and Shutdown to complete.
	close(proceed)
	<-shutdownDone

	// The request may succeed (200) or fail with a connection error if the
	// connection was closed before the response frame arrived — both are
	// acceptable outcomes. What matters at the pool level is eviction.
	select {
	case err := <-requestDone:
		if err != nil {
			t.Logf("request result after GOAWAY: %v (expected, conn may close before response)", err)
		}
	case <-time.After(5 * time.Second):
		require.Fail(t, "request goroutine did not complete")
	}

	// KEY ASSERTION (RFC §6.8 pool contract): the dead conn must be evicted
	// via the release path — not via the background health-check tick (which
	// we set to 60s). Window of 5s is generous enough to absorb scheduler
	// latency between Do() returning and the readerLoop processing EOF, while
	// still proving the tick (60s) cannot be responsible.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Failf(t, "pool did not evict GOAWAY'd conn via release path",
		"ActiveConns = %d (HealthCheckPeriod = 60s, so tick cannot be the cause)", c.PoolStats().ActiveConns)
}

func TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway(t *testing.T) {
	srv, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   2,
			MaxStreamsPerConn: 4,
			HealthCheckPeriod: 50 * time.Millisecond,
			DialBackoff:       10 * time.Millisecond,
		},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()

	var _res client.Response
	err = c.Do(context.Background(), &client.Request{Method: "GET", Path: "/"}, &_res)
	require.NoError(t, err, "first Do")

	shCtx, shCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv.Config.Shutdown(shCtx)
	shCancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.PoolStats().ActiveConns == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Failf(t, "pool did not drain after peer shutdown",
		"ActiveConns = %d, want 0 after peer shutdown", c.PoolStats().ActiveConns)
}

// TestConformance_RFC7540_Sec8_1_BodyStream_EndStream checks RFC 7540 §8.1
// half-close at the observable client-layer boundary: a streaming response whose
// body reads to a clean io.EOF and whose Close returns nil ended cleanly — the
// peer half-closed (END_STREAM) rather than the stream being reset. This layer
// cannot see the DATA frame's END_STREAM flag directly; that wire-level assertion
// lives in the conn/ frame tests. What it pins here is that the streaming API
// surfaces a clean end as a clean end: the full payload, then EOF, then a
// no-error Close — a truncated body or a mid-stream RST would break one of those.
func TestConformance_RFC7540_Sec8_1_BodyStream_EndStream(t *testing.T) {
	payload := []byte("conformance body")
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res client.Response
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res)
	require.NoError(t, err, "Do")
	got, err := io.ReadAll(res.BodyReader)
	require.NoError(t, err, "ReadAll")
	require.NoError(t, res.BodyReader.Close(), "Close")

	require.Equalf(t, payload, got, "body = %q, want %q", got, payload)
	// Close returned without error — stream ended cleanly (END_STREAM
	// received, no RST_STREAM sent). Confirms §8.1 half-close.
}

// benignResetUploadBuf pins the peer's upload window instead of inheriting
// net/http2's 1 MiB default. Two reasons. It makes the mechanism the comments
// claim the actual mechanism: with a 64 KiB window a 128 KiB body cannot be
// written without credit the non-draining handler never refunds, so the write
// is genuinely parked rather than merely losing a timing race. And it removes
// a silent dependency on a Go default — measured on Go 1.26 a 64 KiB body
// reproduced the bug 20/20 against the 1 MiB default, i.e. well below the
// window, which is proof the default-sized version was timing-driven and would
// degrade without anyone noticing.
const benignResetUploadBuf = 64 << 10

// benignResetRequest is twice the pinned window above, so the upload blocks
// after the first window's worth of DATA.
const benignResetRequest = 2 * benignResetUploadBuf

// newNoDrainServer starts an h2 test server with a deliberately small upload
// window (see benignResetUploadBuf) running h.
func newNoDrainServer(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.Config.HTTP2 = &http.HTTP2Config{
		MaxReceiveBufferPerStream:     benignResetUploadBuf,
		MaxReceiveBufferPerConnection: benignResetUploadBuf,
	}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s, strings.TrimPrefix(s.URL, "https://")
}

// TestConformance_RFC9113_Sec8_1_BenignResetKeepsBufferedResponse pins RFC 9113
// §8.1 on the buffered Do path: "Clients MUST NOT discard responses as a result
// of receiving such a RST_STREAM". A server that answers without draining the
// request body resets the still-open upload with NO_ERROR; conn closes the
// stream on that reset per §5.1, so the upload fails — and sendRequest used to
// return that failure, throwing away a complete response already buffered on
// the stream. The conn layer has pinned the same clause since
// TestConformance_RFC9113_Sec8_1_CompleteResponseNotDiscardedByTrailingRSTNoError;
// this is its sibling one layer up. Same defect as grpc issue #337.
func TestConformance_RFC9113_Sec8_1_BenignResetKeepsBufferedResponse(t *testing.T) {
	_, addr := newNoDrainServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("answer"))
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/no-drain",
		Body:     make([]byte, benignResetRequest),
		BodyMode: client.BodyBuffer,
	}, &res)
	require.NoError(t, err, "Do; want the response the server already sent")
	require.Equal(t, 200, res.Status, "response status")
	require.Equalf(t, "answer", string(res.Body),
		"body = %q, want %q — the response must survive the benign reset intact", res.Body, "answer")
}

// TestConformance_RFC9113_Sec8_1_RealResetStillFailsUpload is the
// over-tolerance guard for the test above: tolerating a benign reset must not
// tolerate a real one. http.ErrAbortHandler makes net/http abort the response
// without logging, putting an RST_STREAM with a non-NO_ERROR code on the wire,
// and that must still fail the request rather than yield a 200 with an empty
// body.
//
// The error stays conn.ErrStreamClosed rather than becoming a
// *StreamResetError. That is deliberate and load-bearing, not an oversight:
// when the upload is cut short AND the response does not arrive, the reset the
// response path sees may be conn's own forged RST_STREAM(REFUSED_STREAM) from
// an event-buffer overflow — a code the retry layer reads as "the server did
// not process this request". Surfacing it would let Retryer replay a request
// the server had already answered; measured at 3 executions instead of 1.
// conn now sends CANCEL there, so that code no longer reaches the classifier
// at all — this assertion outlives the reason it was written for, and is kept
// because the error a caller sees for a failed upload should still be the
// upload's.
func TestConformance_RFC9113_Sec8_1_RealResetStillFailsUpload(t *testing.T) {
	_, addr := newNoDrainServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/abort",
		Body:     make([]byte, benignResetRequest),
		BodyMode: client.BodyBuffer,
	}, &res)
	require.Errorf(t, err, "Do = nil (status %d, body %q); want the peer's reset", res.Status, res.Body)
	require.ErrorIsf(t, err, conn.ErrStreamClosed,
		"Do error = %v (%T), want it to wrap conn.ErrStreamClosed", err, err)
	var sre *client.StreamResetError
	if errors.As(err, &sre) {
		require.Failf(t, "a failed upload surfaced *StreamResetError",
			"Do error = %v exposes *StreamResetError{%v}; the retry layer keys on that type and would replay the request", err, sre.Code)
	}
}

// TestConformance_RFC9113_Sec8_1_CutUploadFailureIsNotRetryable pins the
// boundary of the tolerance above: a request whose upload was cut short and
// whose response is then lost must not be replayed.
//
// It is now guarded twice and no longer differentiates preferSendCut on its
// own. conn used to shed an unbuffered response with a forged
// RST_STREAM(REFUSED_STREAM) — the code RFC 9113 §8.7 reserves for the
// server's promise that it never processed the request — which
// builtinShouldRetry believed, replaying work already done (measured: 3
// executions of one DELETE). preferSendCut kept that code away from the
// classifier. conn now sends CANCEL, so the lie is gone at its source and this
// test passes with preferSendCut reverted. The property is still worth
// pinning; the mechanism it happens to exercise is no longer the one it was
// written for. TestConformance_RFC9113_Sec8_7_LocalOverflowIsNotRetried pins
// the source directly, on a request whose upload succeeds.
func TestConformance_RFC9113_Sec8_1_CutUploadFailureIsNotRetryable(t *testing.T) {
	var executions atomic.Int32
	_, addr := newNoDrainServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		executions.Add(1)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		// Outrun the default 8-slot event buffer while the uploader is parked
		// on flow-control credit, so conn resets its own stream and the
		// response is genuinely lost.
		for i := 0; i < 64; i++ {
			_, _ = w.Write(make([]byte, 16<<10))
			w.(http.Flusher).Flush()
		}
	}))
	c := clientFor(t, addr)
	r := c.Retryer(client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := r.Do(ctx, &client.Request{
		Method:   "DELETE",
		Path:     "/resource",
		Body:     make([]byte, benignResetRequest),
		BodyMode: client.BodyBuffer,
	}, &res)
	if err == nil {
		t.Skipf("the response survived the reset (status %d) — this test needs the event buffer to overflow", res.Status)
	}

	n := executions.Load()
	require.Equalf(t, int32(1), n,
		"server executed the DELETE %d times, want 1: err = %v (%T) was classified as retryable", n, err, err)
}

// TestConformance_RFC9113_Sec8_1_BenignResetKeepsStreamedResponse is the
// BodyStream sibling of _BenignResetKeepsBufferedResponse. A status-only
// response (END_STREAM on the HEADERS frame) has no DATA or trailer event to
// end the body on, so a reader that does not carry EndStream over from the
// header event pumps one event too many and hands the caller the benign
// RST_STREAM(NO_ERROR) as a *StreamResetError — discarding the very response
// §8.1 says must be kept, on the one Do mode the buffered test does not cover.
func TestConformance_RFC9113_Sec8_1_BenignResetKeepsStreamedResponse(t *testing.T) {
	_, addr := newNoDrainServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/too-big",
		Body:     make([]byte, benignResetRequest),
		BodyMode: client.BodyStream,
	}, &res)
	require.NoError(t, err, "Do; want the 413 the server already sent")
	defer func() { _ = res.BodyReader.Close() }()

	require.Equal(t, http.StatusRequestEntityTooLarge, res.Status, "response status")
	body, rerr := io.ReadAll(res.BodyReader)
	require.NoError(t, rerr, "BodyReader read; want io.EOF at once: the response ended on its HEADERS frame")
	require.Emptyf(t, body, "body = %q, want empty", body)
}

// TestConformance_RFC9113_Sec8_1_BenignResetDuringTrailers covers the second
// half of the send sequence: the benign reset can just as well land between
// the body and the request trailers, and the trailer write then fails with the
// same ErrStreamClosed. Without the guard on that write the response is
// discarded exactly as it was for the body write.
// The body must SUCCEED for the trailer write to be reached at all, so the
// blocked-on-credit trick the other tests use does not apply here: it would
// fail the body write instead and skip the trailer branch entirely (which is
// how this test first passed while guarding nothing). Instead the body is a
// reader that hands over one chunk and then waits for the handler to return —
// the moment net/http2 emits the reset — before reporting EOF. Reaching EOF
// with endStream=false writes no further DATA, so the trailer HEADERS frame is
// the first thing that meets the closed stream.
func TestConformance_RFC9113_Sec8_1_BenignResetDuringTrailers(t *testing.T) {
	handlerReturned := make(chan struct{})
	_, addr := newNoDrainServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("answer"))
		w.(http.Flusher).Flush()
		close(handlerReturned)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:     "POST",
		Path:       "/no-drain-trailers",
		BodyReader: &resetRendezvousReader{done: handlerReturned},
		BodyMode:   client.BodyBuffer,
		Trailers:   []conn.HeaderField{{Name: []byte("x-checksum"), Value: []byte("deadbeef")}},
	}, &res)
	require.NoError(t, err, "Do; want the response the server already sent")
	require.Equalf(t, 200, res.Status, "status=%d body=%q, want 200 %q", res.Status, res.Body, "answer")
	require.Equalf(t, "answer", string(res.Body),
		"status=%d body=%q, want 200 %q", res.Status, res.Body, "answer")
}

// resetRendezvousReader yields one small chunk, then blocks until the server
// handler has returned (and, with a short grace, until our reader goroutine has
// processed the RST_STREAM it triggers) before reporting EOF.
type resetRendezvousReader struct {
	done chan struct{}
	sent bool
}

func (r *resetRendezvousReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		copy(p, "chunk")
		return 5, nil
	}
	<-r.done
	time.Sleep(100 * time.Millisecond)
	return 0, io.EOF
}

// TestConformance_RFC9113_Sec8_1_NonResetWriteFailureStillAborts is the scope
// guard on the tolerance: only a peer-closed stream is benign. A request body
// that fails to read is a local failure on a healthy stream — nothing is
// coming back — so it must abort at once rather than block in the response
// path until the context expires. Widening the guard to tolerate every write
// error passed the whole suite before this test existed.
func TestConformance_RFC9113_Sec8_1_NonResetWriteFailureStillAborts(t *testing.T) {
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	c := clientFor(t, addr)

	sentinel := errors.New("body source exploded")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var res client.Response
	start := time.Now()
	err := c.Do(ctx, &client.Request{
		Method:     "POST",
		Path:       "/",
		BodyReader: iotest.ErrReader(sentinel),
		BodyMode:   client.BodyBuffer,
	}, &res)
	d := time.Since(start)

	require.ErrorIsf(t, err, sentinel, "Do = %v (%T), want the body reader's own error", err, err)
	require.LessOrEqualf(t, d, 5*time.Second,
		"Do took %v — it waited for a response that was never coming", d)
}

// TestConformance_RFC9113_Sec8_7_LocalOverflowIsNotRetried pins the code conn
// puts on a stream it sheds itself.
//
// §8.7 names REFUSED_STREAM as one of exactly two mechanisms by which a peer
// guarantees a request went unprocessed — "The REFUSED_STREAM error code can be
// included in a RST_STREAM frame to indicate that the stream is being closed
// prior to any processing having occurred.  Any request that was sent on the
// reset stream can be safely retried." — and the retry classifier is built on
// that promise. conn used to make the promise on the server's behalf whenever
// its own per-stream event buffer overflowed, so a response the client merely
// failed to hold became a licence to run the request again. Measured before the
// fix: one DELETE executed three times.
//
// The upload here is tiny and completes normally; only the response overruns
// the buffer. That keeps the test on the plain overflow path rather than the
// cut-upload path of the §8.1 tests above, so it pins the code at its source
// and does not depend on preferSendCut.
func TestConformance_RFC9113_Sec8_7_LocalOverflowIsNotRetried(t *testing.T) {
	var executions atomic.Int32
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		// Outrun the default 8-slot event buffer before the caller can drain it.
		for i := 0; i < 128; i++ {
			_, _ = w.Write(make([]byte, 16<<10))
			w.(http.Flusher).Flush()
		}
	}))
	// One event slot: the reader refunds flow-control window at frame receipt,
	// not at consumption, so the peer is never throttled to the consumer's pace
	// and a single slot overflows on the second frame. That is the bound conn
	// sheds the stream on, reached here on purpose rather than by out-racing a
	// drain loop with volume.
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			StreamEventBuffer: 1,
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	r := c.Retryer(client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err = r.Do(ctx, &client.Request{
		Method:   "DELETE",
		Path:     "/resource",
		Body:     []byte("x"),
		BodyMode: client.BodyBuffer,
	}, &res)
	if err == nil {
		t.Skipf("the response fit after all (status %d, %d bytes) — this test needs the event buffer to overflow", res.Status, len(res.Body))
	}

	var sre *client.StreamResetError
	if errors.As(err, &sre) && sre.Code == frame.ErrCodeRefusedStream {
		require.Fail(t, "a locally-shed response reported REFUSED_STREAM, which promises the server never processed the request")
	}
	n := executions.Load()
	require.Equalf(t, int32(1), n,
		"server executed the DELETE %d times, want 1: err = %v (%T) was classified as retryable", n, err, err)
}

// TestConformance_ChunkedResponseSurvivesAtDefaultConfig is #344 end to end, at
// the configuration the issue was filed against: nothing set beyond the dialer.
//
// The consumer is parked before the server floods, on purpose. Draining inline
// lets a fast machine keep up and the channel never fills — the first version of
// this test passed with the sizing reverted, which is the same as not testing
// it. Here the reader takes the headers, signals, and only then does the handler
// write its chunks, so the events queue against a consumer that is not reading.
//
// 24 chunks + headers + the terminal marker is 26 events: past conn's floor of
// 8, inside the client's computed 64. Two changes are needed for it to pass —
// the sizing, and a terminal frame carrying no payload no longer shedding the
// stream.
func TestConformance_ChunkedResponseSurvivesAtDefaultConfig(t *testing.T) {
	const (
		chunks    = 24
		chunkSize = 512
	)
	consumerParked := make(chan struct{})
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-consumerParked
		for i := 0; i < chunks; i++ {
			_, _ = w.Write(make([]byte, chunkSize))
			w.(http.Flusher).Flush()
		}
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/chunked",
		BodyMode: client.BodyStream,
	}, &res)
	require.NoError(t, err, "Do")
	defer func() { _ = res.BodyReader.Close() }()

	// Headers are in hand and nothing is reading the body: release the flood
	// and give it time to queue against an idle consumer.
	close(consumerParked)
	time.Sleep(250 * time.Millisecond)

	body, err := io.ReadAll(res.BodyReader)
	require.NoErrorf(t, err, "read failed after %d bytes; the server wrote the response in full", len(body))
	require.Lenf(t, body, chunks*chunkSize, "body = %d bytes, want %d", len(body), chunks*chunkSize)
}
