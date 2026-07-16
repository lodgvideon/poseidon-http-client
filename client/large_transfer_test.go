package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
func ltH2Server(t *testing.T) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		buf := make([]byte, 1<<20)
		for sent := 0; sent < ltBodyBytes; sent += len(buf) {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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
// It is NOT sampled mid-transfer, and that is a real limitation rather than an
// oversight: runtime.GC()'s stop-the-world pause is long enough that the caller
// misses more than StreamEventBuffer frames, conn's push() drops them and
// resets the stream (REFUSED_STREAM), and the transfer dies before it can be
// measured. So this test would not catch a receive path that ballooned during
// the transfer and freed at END_STREAM. See the EventReset branch below.
//
// Controls, in order of what they rule out:
//
//   - consumed == ltBodyBytes rules out a flat heap that is flat because the
//     server never really sent 64 MiB.
//   - the EventReset branch rules out a "pass" on a transfer that conn killed
//     rather than streamed.
//   - maxEvent > 16384 rules out the peer having ignored our advertised
//     MAX_FRAME_SIZE and fallen back to the RFC default; combined with
//     maxEvent <= ltMaxFrameSize it proves ltPipelineBytes is the real pipeline
//     size and not a number this test made up.
//   - the sibling NonConsumerIsRefused test rules out the pipeline bound being
//     absent altogether.
func TestIT_H2_StreamedDownload_RetentionStaysBounded(t *testing.T) {
	c := ltH2Client(t, ltH2Server(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	if sr.Status != 200 {
		t.Fatalf("status = %d, want 200", sr.Status)
	}

	// Baseline after the connection is up and headers are parsed, so the delta
	// measures the body transfer and not connection setup.
	baseline := ltLiveHeap()

	var consumed int64
	var maxEvent int
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv after %d bytes: %v", consumed, err)
		}
		if ev.Type == client.EventReset {
			// Not a silent break: conn resets a stream whose caller falls
			// behind (REFUSED_STREAM past StreamEventBuffer events). Treating
			// that as a normal end would let this test "pass" on a transfer
			// that was killed rather than streamed.
			t.Fatalf("stream reset (%v) after %d of %d bytes — the caller fell behind the reader; nothing was measured",
				ev.ResetCode, consumed, ltBodyBytes)
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

	if consumed != ltBodyBytes {
		t.Fatalf("consumed = %d bytes, want %d — the transfer under measurement did not happen", consumed, ltBodyBytes)
	}
	if maxEvent <= 16384 {
		t.Fatalf("largest DATA event = %d, want > 16384 (the RFC 7540 default): the peer did not honour the MAX_FRAME_SIZE=%d we advertised, so ltPipelineBytes is not derived from anything real",
			maxEvent, ltMaxFrameSize)
	}
	if maxEvent > ltMaxFrameSize {
		t.Fatalf("largest DATA event = %d, want <= advertised MAX_FRAME_SIZE %d", maxEvent, ltMaxFrameSize)
	}

	if after > baseline && after-baseline > ltRetentionBound {
		t.Fatalf("after fully consuming %d MiB the stream still holds %d KiB, want <= %d KiB (%d-event channel x %d B frames, x4 slack): retention scales with body, not with the pipeline",
			ltBodyBytes>>20, (after-baseline)/1024, ltRetentionBound/1024, ltEventBuf, ltMaxFrameSize)
	}
	t.Logf("streamed %d MiB: live-heap delta after full consumption %+d KiB (bound %d KiB), largest DATA event %d B",
		consumed>>20, (int64(after)-int64(baseline))/1024, ltRetentionBound/1024, maxEvent)
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
// RST_STREAM(REFUSED_STREAM). Hence the served > ltStreamWindow assertion
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
	if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
		t.Fatalf("ConfigureServer: %v", err)
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	c := ltH2Client(t, strings.TrimPrefix(srv.URL, "https://"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	// Consume nothing. Give the server a long window to push whatever it can.
	time.Sleep(2 * time.Second)

	got := served.Load()
	if got >= ltBodyBytes {
		t.Fatalf("server pushed the whole %d-byte body into a client that consumed nothing: no bound is enforced, and every retention number measured against this harness is worthless", got)
	}
	if got > ltRetentionBound {
		t.Fatalf("server pushed %d bytes into a client that consumed nothing, want <= %d (%d-event channel x %d B frames, x4 slack)",
			got, ltRetentionBound, ltEventBuf, ltMaxFrameSize)
	}
	if got <= ltStreamWindow {
		t.Fatalf("server pushed only %d bytes, want > the advertised per-stream window %d: this test is meant to observe the event-channel bound, but something stopped the peer earlier — re-derive the bound before trusting it",
			got, ltStreamWindow)
	}

	// The caller learns about it as RST_STREAM(REFUSED_STREAM), not as a stall.
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	for {
		ev, err := sr.Recv(rctx)
		if err != nil {
			t.Fatalf("Recv: %v, want an EventReset to arrive", err)
		}
		if ev.Type == client.EventReset {
			if ev.ResetCode != frame.ErrCodeRefusedStream {
				t.Fatalf("reset code = %v, want REFUSED_STREAM", ev.ResetCode)
			}
			break
		}
	}
	t.Logf("non-consumer stopped after %d bytes (%.0f KiB) of %d MiB, via RST_STREAM(REFUSED_STREAM); advertised stream window is %d B",
		got, float64(got)/1024, ltBodyBytes>>20, ltStreamWindow)
}

// TestIT_H1_LargeDownload_HasNoStreamingPathAndDiscardsFlat covers the H1 half
// of the retention question, and first establishes why that half is small.
//
// H1 has no incremental response API: client.go's beginRespStream only accepts
// *conn.Stream and *h3Exchange, so *h1Exchange — and therefore every H1
// transport — is rejected with ErrStreamingUnsupported. There is no H1
// equivalent of the H2 test above to write; asserting the rejection is the
// honest coverage, rather than inventing a streamed-H1 test for an API that
// does not exist.
//
// That leaves Do. Do(BodyBuffer) retains the whole body by contract, so it has
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
func TestIT_H1_LargeDownload_HasNoStreamingPathAndDiscardsFlat(t *testing.T) {
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// H1 has no streaming path at all — both doors are locked.
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); !errors.Is(err, client.ErrStreamingUnsupported) {
		t.Fatalf("H1 DoStream err = %v, want ErrStreamingUnsupported", err)
	}
	var streamed client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &streamed); !errors.Is(err, client.ErrStreamingUnsupported) {
		t.Fatalf("H1 Do(BodyStream) err = %v, want ErrStreamingUnsupported", err)
	}

	// The one large-transfer shape H1 does support must not retain the body.
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyDiscard}, &resp); err != nil {
		t.Fatalf("Do(BodyDiscard): %v", err)
	}

	if resp.BytesReceived != ltBodyBytes {
		t.Fatalf("BytesReceived = %d, want %d — the transfer under measurement did not happen", resp.BytesReceived, ltBodyBytes)
	}
	if len(resp.Body) != 0 {
		t.Fatalf("BodyDiscard retained %d body bytes, want 0", len(resp.Body))
	}
	t.Logf("H1 Do(BodyDiscard) counted %d MiB and retained %d body bytes",
		resp.BytesReceived>>20, len(resp.Body))
}

// ltBreakingH1Peer serves one response announcing `declared` body bytes, sends
// `send` of them, then breaks the connection — with a clean FIN, or with a TCP
// RST when rst is set (SO_LINGER 0 makes Close emit RST instead of FIN).
func ltBreakingH1Peer(t *testing.T, declared, send int, rst bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
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
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
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
		if err == nil {
			t.Fatal("Do returned nil error for a body truncated at 100 of 1000 bytes")
		}
		if resp.Status != 200 {
			t.Fatalf("resp.Status = %d, want 200 — a caller that trusts Status over err sees a complete 200", resp.Status)
		}
		if len(resp.Body) != sent {
			t.Fatalf("len(resp.Body) = %d, want %d — the truncated prefix is handed back alongside the error", len(resp.Body), sent)
		}
		if resp.BytesReceived != sent {
			t.Fatalf("resp.BytesReceived = %d, want %d", resp.BytesReceived, sent)
		}
	}

	t.Run("cleanFIN", func(t *testing.T) {
		resp, err := run(t, false)
		assertTruncatedButPopulated(t, resp, err)

		// Half 2: an untyped fmt.Errorf leaf. Unwrap()==nil is the load-bearing
		// assertion — it is what proves there is no wrapped sentinel a caller
		// could match on, and it is what a typed replacement would break.
		want := fmt.Sprintf("http1: premature EOF: got %d of %d bytes", sent, declared)
		if err.Error() != want {
			t.Fatalf("err = %q, want %q", err.Error(), want)
		}
		if u := errors.Unwrap(err); u != nil {
			t.Fatalf("errors.Unwrap(err) = %v, want nil: the premature-EOF error is an untyped fmt.Errorf leaf today. If this now wraps a sentinel, that is the improvement this test exists to make deliberate — update it.", u)
		}
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			t.Fatalf("err unexpectedly carries *net.OpError (%v); the clean-FIN and RST breaks are pinned as producing different error shapes", opErr)
		}
	})

	t.Run("tcpRST", func(t *testing.T) {
		resp, err := run(t, true)
		assertTruncatedButPopulated(t, resp, err)

		// Half 2, other shape: the raw transport error, unwrapped and
		// unannotated. The same application event as cleanFIN produces an error
		// with nothing in common with it — not even a mention of truncation.
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			t.Fatalf("err = %#v, want a *net.OpError: a reset mid-body surfaces the raw transport error today", err)
		}
		if strings.Contains(err.Error(), "premature EOF") {
			t.Fatalf("err = %q now reports truncation; cleanFIN and tcpRST are pinned as NOT sharing a classification. Unifying them is the improvement this test exists to make deliberate — update it.", err.Error())
		}
	})
}
