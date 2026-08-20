package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Large-transfer retention. A QUIC receive stream once retained every byte it
// had received — an 8 MiB response still held 8 MiB after being fully consumed.
// Nothing tested a large streamed transfer for retention, so it survived. These
// tests ask the same question of H2 and H1.
//
// Every constant below is derived from something the test itself advertises to
// the peer. Note in particular that http2.Server's MaxUploadBufferPerStream /
// MaxUploadBufferPerConnection are NOT usable here: x/net@v0.56.0
// server.go:284+783 feeds MaxUploadBufferPerStream into the server's
// *advertised* SETTINGS_INITIAL_WINDOW_SIZE and server.go:798 uses
// MaxUploadBufferPerConnection for the server's own connection inflow. Both
// govern client->server uploads. A download is bounded by the windows the
// *client* advertises, which is what this file configures.
const (
	// ltBodyBytes is the streamed body size — 4x larger than the biggest body
	// in the existing suite, so O(body) retention is unmistakable.
	ltBodyBytes = 64 << 20

	// ltEventBuf is the per-stream event-channel depth this test configures via
	// ConnOptions.StreamEventBuffer.
	ltEventBuf = 8

	// ltMaxFrameSize is the SETTINGS_MAX_FRAME_SIZE this test advertises. It is
	// deliberately not the RFC 7540 §6.5.2 default of 16384, so a peer that
	// honours it can be told apart from one falling back to the default.
	ltMaxFrameSize = 32768

	// ltStreamWindow is the SETTINGS_INITIAL_WINDOW_SIZE this test advertises,
	// i.e. how many bytes the peer may have in flight to us on one stream
	// before it must wait for a WINDOW_UPDATE.
	//
	// 65535 is the RFC 7540 §6.5.2 default, chosen here because this test is
	// about retention rather than flow control and the default keeps it off the
	// interesting paths.
	//
	// A smaller window would work too. This comment used to say it must stay
	// >= conn's recvWindowRefundThreshold or the download deadlocks — true when
	// written, and fixed since: streamRefundThreshold (conn/conn.go) scales the
	// refund threshold down to half the window when the window is smaller, so a
	// refund is always reachable. conn's TestConn_SmallAdvertisedWindow_StillCompletes
	// pins exactly that, down to a 1024-byte window.
	ltStreamWindow = 65535

	// ltPipelineBytes is the most DATA the H2 receive pipeline can hold at once:
	// conn's reader goroutine parks at most StreamEventBuffer events on the
	// stream's channel before push() drops and resets (conn/stream.go), and each
	// event carries one DATA frame, which the peer may not make larger than the
	// MAX_FRAME_SIZE advertised above.
	ltPipelineBytes = ltEventBuf * ltMaxFrameSize // 256 KiB

	// ltRetentionBound is ltPipelineBytes plus 4x slack for the DATA that is
	// never on the channel: the frame the caller currently holds (sr.curData),
	// the framer's read buffer, and bufio/TLS record buffers — plus GC timing.
	// The slack is generous on purpose and does not weaken the test: the bound
	// is 1/64th of ltBodyBytes, so a reader that retained the body overshoots by
	// ~64x rather than by a slack factor.
	ltRetentionBound = 4 * ltPipelineBytes // 1 MiB
)

// ltLiveHeap returns bytes of reachable heap. Two GCs are required, not one:
// conn recycles DATA buffers through a sync.Pool, and a pooled object only
// becomes unreachable after it has passed through the victim cache, which takes
// two cycles. Without the second GC, pooled-but-dead buffers would be counted
// as retention and the measurement would be an overestimate.
func ltLiveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// ltH2Server starts an h2 server that streams ltBodyBytes from a single reused
// buffer, so anything the measurement sees growing is the client's, not the
// server's. Writes are 1 MiB, far above ltMaxFrameSize, to force the server to
// do the framing at the size we advertised rather than at our write boundaries.
//
// The handler flushes the response headers and then parks until release is
// closed, which is how a caller gets a quiet connection to measure against — up,
// headers parsed, not one DATA frame in flight. That is not tidiness. conn's
// reader fills the stream's event channel whether or not anybody is reading it,
// and push() drops the frame and sends RST_STREAM(CANCEL) once ltEventBuf events
// are queued (conn/stream.go), so every pause between DoStream returning and the
// drain loop starting is a race the reader can win. ltLiveHeap's two blocking
// GCs are exactly such a pause: measured, GOMAXPROCS=1 loses it 2 runs in 3, and
// Gremlins' coverage sweep — go test ./... with no -race, every package at once
// on a 4-vCPU runner — lost it twice out of twice, both times identically at
// "262141 of 67108864 bytes", i.e. with the drain loop not yet run once.
//
// Parking the body removes that race instead of widening the channel: ltEventBuf
// is what ltRetentionBound is derived from, so buying slack there would loosen
// the very bound this test exists to assert.
func ltH2Server(t *testing.T, release <-chan struct{}) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		// The flush is load-bearing: h2 holds HEADERS until the handler writes or
		// returns, and this one is about to do neither. Without it DoStream never
		// returns and nothing ever closes release.
		if !assert.NoError(t, http.NewResponseController(w).Flush(),
			"flush the response headers — without them DoStream never returns and "+
				"nothing closes release") {
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		buf := make([]byte, 1<<20)
		for sent := 0; sent < ltBodyBytes; sent += len(buf) {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	require.NoError(t, http2.ConfigureServer(srv.Config, &http2.Server{}), "ConfigureServer")
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "https://")
}

// ltH2Client dials addr with the windows and buffers the constants above name.
func ltH2Client(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportSingleConn,
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
			StreamEventBuffer: ltEventBuf,
			Settings: conn.AdvertisedSettings{
				InitialWindowSize: ltStreamWindow,
				MaxFrameSize:      ltMaxFrameSize,
			},
		},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestIT_H2_StreamedDownload_RetentionStaysBounded streams 64 MiB through
// DoStream, consuming each event as it arrives, and asserts that once the whole
// body has been consumed the stream is still holding no more than
// ltRetentionBound — i.e. retention is O(pipeline), not O(body).
//
// The measurement is taken after the body is fully consumed and before Close,
// which is precisely the shape of the QUIC bug this test exists for: an 8 MiB
// response that still held 8 MiB after being fully consumed. sr still
// references the *conn.Stream at that point, so anything the receive path kept
// per-stream is still reachable and still counted.
//
// NOTHING may allocate between `baseline` and `after` beyond the transfer under
// measurement — that is why the drain loop below records its outcome in local
// variables and every assertion runs after the second ltLiveHeap(). testify
// reflects and builds a []interface{} per call, so an assertion inside the loop
// would be charged to the client's retention.
//
// It is NOT sampled mid-transfer, and that is a real limitation rather than an
// oversight: runtime.GC()'s stop-the-world pause is long enough that the caller
// misses more than StreamEventBuffer frames, conn's push() drops them and
// resets the stream (CANCEL), and the transfer dies before it can be
// measured. So this test would not catch a receive path that ballooned during
// the transfer and freed at END_STREAM. See the reset branch below.
//
// Controls, in order of what they rule out:
//
//   - consumed == ltBodyBytes rules out a flat heap that is flat because the
//     server never really sent 64 MiB.
//   - the reset branch rules out a "pass" on a transfer that conn killed
//     rather than streamed.
//   - maxEvent > 16384 rules out the peer having ignored our advertised
//     MAX_FRAME_SIZE and fallen back to the RFC default; combined with
//     maxEvent <= ltMaxFrameSize it proves ltPipelineBytes is the real pipeline
//     size and not a number this test made up.
//   - the sibling NonConsumerIsRefused test rules out the pipeline bound being
//     absent altogether.
func TestIT_H2_StreamedDownload_RetentionStaysBounded(t *testing.T) {
	release := make(chan struct{})
	c := ltH2Client(t, ltH2Server(t, release))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var sr client.StreamResponse
	require.NoError(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer sr.Close()
	require.Equal(t, 200, sr.Status, "response status")

	// Baseline with the connection up and the headers parsed, so the delta
	// measures the body transfer and not connection setup — and with the server
	// still parked, so it measures a connection with nothing in flight. The second
	// half is not cosmetic: the body used to start flowing the moment DoStream
	// returned, so this line ran with an unknown 0..ltEventBuf DATA frames already
	// queued on the stream and the baseline quietly absorbed up to
	// ltPipelineBytes of the very thing being bounded. See ltH2Server for the
	// stream reset that same window kept causing.
	baseline := ltLiveHeap()
	close(release)
	var consumed int64
	var maxEvent int
	var recvErr error
	var resetCode conn.ErrCode
	sawReset := false
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			recvErr = err
			break
		}
		if ev.Type == client.EventReset {
			// Not a silent break: conn resets a stream whose caller falls
			// behind (CANCEL past StreamEventBuffer events). Treating that as
			// a normal end would let this test "pass" on a transfer that was
			// killed rather than streamed.
			sawReset, resetCode = true, ev.ResetCode
			break
		}
		if ev.Type != client.EventData {
			if ev.EndStream {
				break
			}
			continue
		}
		if len(ev.Data) > maxEvent {
			maxEvent = len(ev.Data)
		}
		consumed += int64(len(ev.Data))
		if ev.EndStream {
			break
		}
	}
	// Measured with the whole body consumed and sr — and through it the
	// *conn.Stream — still alive.
	after := ltLiveHeap()

	require.NoErrorf(t, recvErr, "Recv after %d bytes", consumed)
	require.Falsef(t, sawReset,
		"stream reset (%v) after %d of %d bytes — the caller fell behind the reader; "+
			"nothing was measured", resetCode, consumed, ltBodyBytes)
	require.EqualValuesf(t, ltBodyBytes, consumed,
		"consumed = %d bytes, want %d — the transfer under measurement did not happen",
		consumed, ltBodyBytes)
	require.Greaterf(t, maxEvent, 16384,
		"largest DATA event = %d, want > 16384 (the RFC 7540 default): the peer did not "+
			"honour the MAX_FRAME_SIZE=%d we advertised, so ltPipelineBytes is not derived "+
			"from anything real", maxEvent, ltMaxFrameSize)
	require.LessOrEqualf(t, maxEvent, ltMaxFrameSize,
		"largest DATA event = %d, want <= advertised MAX_FRAME_SIZE %d", maxEvent, ltMaxFrameSize)
	t.Logf("streamed %d MiB: live-heap delta after full consumption %+d KiB (bound %d KiB), largest DATA event %d B",
		consumed>>20, (int64(after)-int64(baseline))/1024, ltRetentionBound/1024, maxEvent)
	if after > baseline {
		assert.LessOrEqualf(t, after-baseline, uint64(ltRetentionBound),
			"after fully consuming %d MiB the stream still holds %d KiB, want <= %d KiB "+
				"(%d-event channel x %d B frames, x4 slack): retention scales with body, "+
				"not with the pipeline",
			ltBodyBytes>>20, (after-baseline)/1024, ltRetentionBound/1024, ltEventBuf, ltMaxFrameSize)
	}
}

// TestIT_H2_StreamedDownload_NonConsumerIsRefused is the control for
// RetentionStaysBounded: it proves the bound that keeps retention flat is
// actually live, by showing a caller that never consumes cannot make the peer
// push the body into us.
//
// It also pins WHICH bound that is, because it is not the obvious one. The peer
// is stopped by the event channel, not by flow control: conn's reader refunds
// receive-window credit in onDataReceived the moment a DATA frame is *parsed*
// (conn/conn.go:869-925), not when the application consumes it, so the window
// never applies application backpressure. Backpressure is instead
// Stream.push() dropping past StreamEventBuffer events and sending
// RST_STREAM(CANCEL). Hence the served > ltStreamWindow assertion
// below: a peer that was being held by flow control could not have exceeded it.
func TestIT_H2_StreamedDownload_NonConsumerIsRefused(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		buf := make([]byte, ltMaxFrameSize)
		for sent := 0; sent < ltBodyBytes; sent += len(buf) {
			n, err := w.Write(buf)
			served.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	require.NoError(t, http2.ConfigureServer(srv.Config, &http2.Server{}), "ConfigureServer")
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	c := ltH2Client(t, strings.TrimPrefix(srv.URL, "https://"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var sr client.StreamResponse
	require.NoError(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer sr.Close()

	// Consume nothing. Give the server a long window to push whatever it can.
	time.Sleep(2 * time.Second)

	got := served.Load()
	t.Logf("non-consumer absorbed %d bytes (%.0f KiB) of %d MiB; bound %d KiB, advertised stream window %d B",
		got, float64(got)/1024, ltBodyBytes>>20, ltRetentionBound/1024, ltStreamWindow)
	require.Lessf(t, got, int64(ltBodyBytes),
		"server pushed the whole %d-byte body into a client that consumed nothing: no "+
			"bound is enforced, and every retention number measured against this harness "+
			"is worthless", got)
	require.LessOrEqualf(t, got, int64(ltRetentionBound),
		"server pushed %d bytes into a client that consumed nothing, want <= %d "+
			"(%d-event channel x %d B frames, x4 slack)",
		got, ltRetentionBound, ltEventBuf, ltMaxFrameSize)
	require.Greaterf(t, got, int64(ltStreamWindow),
		"server pushed only %d bytes, want > the advertised per-stream window %d: this "+
			"test is meant to observe the event-channel bound, but something stopped the "+
			"peer earlier — re-derive the bound before trusting it", got, ltStreamWindow)

	// The caller learns about it as a reset, not as a stall. The code is CANCEL:
	// the client shed a response it could not buffer, so REFUSED_STREAM's
	// promise that the request went unprocessed (RFC 9113 §8.7) would be false
	// and would make the retry layer replay work the server already did.
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	for {
		ev, err := sr.Recv(rctx)
		require.NoError(t, err, "Recv: an EventReset must arrive rather than a stall")
		if ev.Type == client.EventReset {
			assert.Equal(t, frame.ErrCodeCancel, ev.ResetCode,
				"the shed response must be CANCEL: REFUSED_STREAM promises the request went "+
					"unprocessed (RFC 9113 §8.7), which would make the retry layer replay "+
					"work the server already did")
			break
		}
	}
}

// TestIT_H1_LargeDownload_StreamsAndDiscardsFlat covers the H1 half
// of the retention question.
//
// This used to assert the opposite of its first section: beginRespStream
// accepted only *conn.Stream and *h3Exchange, so every H1 transport was
// rejected with ErrStreamingUnsupported, and the comment here explained that
// asserting the rejection was the honest coverage "rather than inventing a
// streamed-H1 test for an API that does not exist". The API did exist —
// h1Exchange.Recv has always read one chunk per call and marked the last one
// EndStream. Only the dispatch rejected it. Both doors are open now and both
// are drained here.
//
// Do(BodyBuffer) retains the whole body by contract, so it has
// no retention claim to test. Do(BodyDiscard) does: it must count bytes without
// keeping them, at any size. That is asserted here at 64 MiB.
//
// There is deliberately no heap measurement here, unlike the H2 test. One was
// written and removed: h1Exchange is allocated fresh per request
// (h1_transport.go:181) and never pooled, so by the time Do returns it is
// unreachable and anything it retained is already collectable. A post-Do
// runtime.GC() measurement therefore cannot fail — verified by making
// h1Exchange retain every chunk it read, which the heap assertion did not
// notice. resp.Body and BytesReceived are what can actually be asserted here.
func TestIT_H1_LargeDownload_StreamsAndDiscardsFlat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(ltBodyBytes))
		w.WriteHeader(200)
		buf := make([]byte, 1<<20)
		for sent := 0; sent < ltBodyBytes; sent += len(buf) {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		// Raise the 32 MiB DefaultMaxResponseBodySize: it is charged against
		// BytesReceived even in BodyDiscard mode, where nothing is retained, so
		// the default would fail this request at 32 MiB before the retention
		// question could be asked.
		MaxResponseBodySize: 2 * ltBodyBytes,
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Both streaming doors are open on H1 now: h1Exchange.Recv reads a chunk at a
	// time and beginRespStream dispatches to it. Opening each and reading the
	// first chunk proves the dispatch reaches h1Exchange, which is all this test
	// needs from them — the retention question below is its subject.
	//
	// Deliberately NOT draining either body. This transport is a single
	// connection and the fixture is 64 MiB, so a full drain per door tripled the
	// bytes on one socket and blew the 60s budget under -race in CI while passing
	// in 0.16s locally. Closing early abandons the body, which forces a redial —
	// cheap, and the streamed-body drain is covered at a sane size by
	// TestH1_BodyStream_Incremental.
	var sr client.StreamResponse
	require.NoError(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr), "H1 DoStream")
	_ = sr.Close()
	var streamed client.Response
	require.NoError(t,
		c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &streamed),
		"H1 Do(BodyStream)")
	require.NotNil(t, streamed.BodyReader, "H1 Do(BodyStream) returned no BodyReader")
	first := make([]byte, 4096)
	_, err = io.ReadFull(streamed.BodyReader, first)
	require.NoError(t, err, "read the first chunk of the streamed H1 body")
	_ = streamed.BodyReader.Close()

	// The one large-transfer shape H1 does support must not retain the body.
	var resp client.Response
	err = c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyDiscard}, &resp)

	require.NoError(t, err, "Do(BodyDiscard)")
	require.EqualValuesf(t, ltBodyBytes, resp.BytesReceived,
		"BytesReceived = %d, want %d — the transfer under measurement did not happen",
		resp.BytesReceived, ltBodyBytes)
	assert.Emptyf(t, resp.Body,
		"BodyDiscard retained %d body bytes, want 0: the mode exists to count without keeping",
		len(resp.Body))
	t.Logf("H1 Do(BodyDiscard) counted %d MiB and retained %d body bytes",
		resp.BytesReceived>>20, len(resp.Body))
}

// ltBreakingH1Peer serves one response announcing `declared` body bytes, sends
// `send` of them, then breaks the connection — with a clean FIN, or with a TCP
// RST when rst is set (SO_LINGER 0 makes Close emit RST instead of FIN).
func ltBreakingH1Peer(t *testing.T, declared, send int, rst bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		_, _ = nc.Read(make([]byte, 4096)) // request; content ignored
		_, _ = fmt.Fprintf(nc, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", declared)
		_, _ = nc.Write(make([]byte, send))
		// Let the client drain `send` and block waiting for the rest, so the
		// break lands mid-body rather than racing the header parse.
		time.Sleep(100 * time.Millisecond)
		if rst {
			_ = nc.(*net.TCPConn).SetLinger(0)
		}
	}()
	return ln.Addr().String()
}

// TestIT_H1_MidBodyBreak_TruncationIsUnclassifiable pins what a caller sees
// today when a peer breaks mid-body, so that improving it is a deliberate
// change rather than an accident. Two halves, and the test's opinion is that
// both are arguably wrong:
//
//  1. Response is populated ALONGSIDE the error: Status is 200 and Body holds
//     the truncated prefix. A caller that checks Status instead of err — or
//     that logs err and uses resp anyway — silently accepts a short body as a
//     complete one. Arguably the Response should be zeroed on a truncation
//     error, or the truncation should be visible on the Response itself.
//     Nothing here is a promise; this half is pinned because it is dangerous,
//     not because it is right.
//
//  2. The two breaks produce two unclassifiable and unrelated errors for one
//     application event ("the peer went away mid-body"): a clean FIN yields an
//     untyped fmt.Errorf leaf from http1/conn.go:491, a RST yields a raw
//     *net.OpError whose text is platform-specific. Neither is matchable with
//     errors.Is/As against anything this package exports, so a caller cannot
//     ask "was my body truncated?" without string-matching. A typed error is
//     the obvious fix; this test is what makes adding one a conscious break.
//
// The RST half deliberately does not assert on the error text — it is
// "connection reset by peer" on Unix and "wsarecv: An existing connection was
// forcibly closed by the remote host" on Windows.
func TestIT_H1_MidBodyBreak_TruncationIsUnclassifiable(t *testing.T) {
	const declared, sent = 1000, 100

	run := func(t *testing.T, rst bool) (client.Response, error) {
		t.Helper()
		c, err := client.NewClient(client.ClientOptions{
			Transport: client.TransportH1SingleConn,
			Addr:      ltBreakingH1Peer(t, declared, sent, rst),
			ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
		})
		require.NoError(t, err, "NewClient")
		t.Cleanup(func() { _ = c.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var resp client.Response
		err = c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, &resp)
		return resp, err
	}

	// assertTruncatedButPopulated covers half 1, which both breaks share.
	assertTruncatedButPopulated := func(t *testing.T, resp client.Response, err error) {
		t.Helper()
		require.Error(t, err, "Do returned nil for a body truncated at 100 of 1000 bytes")
		assert.Equal(t, 200, resp.Status,
			"a caller that trusts Status over err sees a complete 200 for a truncated body")
		assert.Lenf(t, resp.Body, sent,
			"len(resp.Body) = %d, want %d — the truncated prefix is handed back alongside "+
				"the error", len(resp.Body), sent)
		assert.EqualValues(t, sent, resp.BytesReceived, "resp.BytesReceived")
	}

	t.Run("cleanFIN", func(t *testing.T) {
		resp, err := run(t, false)

		assertTruncatedButPopulated(t, resp, err)
		// Half 2: an untyped fmt.Errorf leaf. Unwrap()==nil is the load-bearing
		// assertion — it is what proves there is no wrapped sentinel a caller
		// could match on, and it is what a typed replacement would break.
		want := fmt.Sprintf("http1: premature EOF: got %d of %d bytes", sent, declared)
		assert.Equal(t, want, err.Error(), "the premature-EOF message is pinned verbatim")
		u := errors.Unwrap(err)
		assert.Truef(t, u == nil,
			"errors.Unwrap(err) = %v, want nil: the premature-EOF error is an untyped "+
				"fmt.Errorf leaf today. If this now wraps a sentinel, that is the improvement "+
				"this test exists to make deliberate — update it.", u)
		var opErr *net.OpError
		assert.Falsef(t, errors.As(err, &opErr),
			"err unexpectedly carries *net.OpError (%v); the clean-FIN and RST breaks are "+
				"pinned as producing different error shapes", opErr)
	})

	t.Run("tcpRST", func(t *testing.T) {
		resp, err := run(t, true)

		assertTruncatedButPopulated(t, resp, err)
		// Half 2, other shape: the raw transport error, unwrapped and
		// unannotated. The same application event as cleanFIN produces an error
		// with nothing in common with it — not even a mention of truncation.
		var opErr *net.OpError
		assert.Truef(t, errors.As(err, &opErr),
			"err = %#v, want a *net.OpError: a reset mid-body surfaces the raw transport "+
				"error today", err)
		assert.NotContainsf(t, err.Error(), "premature EOF",
			"err = %q now reports truncation; cleanFIN and tcpRST are pinned as NOT sharing "+
				"a classification. Unifying them is the improvement this test exists to make "+
				"deliberate — update it.", err.Error())
	})
}
