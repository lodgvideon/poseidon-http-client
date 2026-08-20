package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestValidateRequest_OK(t *testing.T) {
	req := &Request{Method: "GET", Path: "/"}

	err := validateRequest(req)

	require.NoError(t, err, "a bare GET / is the minimal valid request; rejecting it would refuse every caller")
}

func TestValidateRequest_NoMethod(t *testing.T) {
	req := &Request{Path: "/"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a request with no method")
}

func TestValidateRequest_NoPath(t *testing.T) {
	req := &Request{Method: "GET"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a request with no path")
}

func TestValidateRequest_WhitespacePaddedMethodRejected(t *testing.T) {
	req := &Request{Method: " GET ", Path: "/"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a padded method %q", req.Method)
}

func TestValidateRequest_WhitespacePaddedPathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: " / "}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a padded path %q", req.Path)
}

func TestValidateRequest_WhitespaceOnlyMethodRejected(t *testing.T) {
	req := &Request{Method: "   ", Path: "/"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a whitespace-only method")
}

func TestValidateRequest_WhitespaceOnlyPathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: "   "}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a whitespace-only path")
}

func TestValidateRequest_InternalWhitespaceMethodRejected(t *testing.T) {
	req := &Request{Method: "GET POST", Path: "/"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a method carrying an internal space")
}

func TestValidateRequest_InternalWhitespacePathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: "/foo bar"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a path carrying an internal space")
}

func TestValidateRequest_TabInMethodRejected(t *testing.T) {
	req := &Request{Method: "GET\t", Path: "/"}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a method carrying a HTAB")
}

func TestValidateRequest_PseudoHeaderInRegular(t *testing.T) {
	req := &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte(":authority"), Value: []byte("example.com")},
		},
	}

	err := validateRequest(req)

	require.ErrorIs(t, err, ErrInvalidRequest,
		"expected ErrInvalidRequest: a pseudo-header in the regular Headers slice would be emitted after "+
			"the real pseudo-headers, which RFC 7540 §8.1.2.1 makes malformed")
}

func TestValidateRequest_NilRequest(t *testing.T) {
	err := validateRequest(nil)

	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for a nil request, not a panic")
}

func TestParseStatus_Found(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	var rest []hpack.HeaderField

	st, err := parseStatus(in, &rest)

	require.NoError(t, err, "a well-formed :status must parse")
	assert.Equal(t, 200, st, "status = %d, want 200", st)
	require.Len(t, rest, 1, "regular headers wrong: %+v", rest)
	assert.Equal(t, "content-type", string(rest[0].Name),
		"the non-pseudo field must be handed back to the caller: %+v", rest)
}

func TestParseStatus_Missing(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	var dst []hpack.HeaderField

	_, err := parseStatus(in, &dst)

	require.ErrorIs(t, err, ErrEmptyResponse, "a response block with no :status is not a response")
}

func TestParseStatus_NotNumeric(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("OK")},
	}
	var dst []hpack.HeaderField

	_, err := parseStatus(in, &dst)

	require.ErrorIs(t, err, ErrInvalidStatus, "expected ErrInvalidStatus for a non-numeric :status")
}

func TestParseStatus_Negative(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("-1")},
	}
	var dst []hpack.HeaderField

	_, err := parseStatus(in, &dst)

	require.ErrorIs(t, err, ErrInvalidStatus, "expected ErrInvalidStatus for a negative :status")
}

// --- Test helpers (singleConn / Do / DoStream) ---

// nopHandler is a frame.Handler with no-op methods, used to skip frames
// during fake-server handshake while ReadFrame's contract is satisfied.
type nopHandler struct{}

func (nopHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (nopHandler) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (nopHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (nopHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (nopHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error {
	return nil
}
func (nopHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (nopHandler) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (nopHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (nopHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (nopHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }
func (nopHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error           { return nil }
func (nopHandler) OnOrigin(frame.FrameHeader, []string) error                      { return nil }

// readFull reads len(buf) bytes from r, retrying on short reads.
func readFull(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		x, err := r.Read(buf[n:])
		if x > 0 {
			n += x
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// runFakeH2Server does the HTTP/2 handshake on srv (server side of a
// net.Pipe), then invokes after with the server's *frame.Framer for
// per-test frame interactions. If after blocks, it must return when
// signaled by the test (typically by closing the pipe via c.Close()).
func runFakeH2Server(srv net.Conn, after func(srvFr *frame.Framer)) {
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readFull(srv, preface); err != nil {
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(frame.SettingsParams{}) }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	go func() { writeDone <- srvFr.WriteSettingsAck() }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	if after != nil {
		after(srvFr)
	}
}

// fakeDialer returns the client end of a net.Pipe. Each Dial spins up
// a fresh in-memory pipe pair and a goroutine running runFakeH2Server.
type fakeDialer struct {
	dialCount atomic.Int32
	srvAfter  func(srvFr *frame.Framer)
}

// Dial implements conn.Dialer.
func (d *fakeDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	d.dialCount.Add(1)
	cli, srv := net.Pipe()
	go runFakeH2Server(srv, d.srvAfter)
	return cli, nil
}

func TestSingleConn_Acquire_LazyDial(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: func(_ *frame.Framer) {
		<-stopSrv
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}, metrics: &Metrics{}}
	defer sc.close()
	require.EqualValues(t, 0, d.dialCount.Load(),
		"dial happened in the constructor; the transport must dial lazily on first acquire")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, release, err := sc.acquireConn(ctx)

	require.NoError(t, err, "acquire")
	defer release()
	assert.EqualValues(t, 1, d.dialCount.Load(), "dial count = %d, want 1", d.dialCount.Load())
	assert.True(t, c.IsAlive(), "acquired conn must be alive")
}

func TestSingleConn_Acquire_ReusesAliveConn(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: func(_ *frame.Framer) {
		<-stopSrv
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}, metrics: &Metrics{}}
	defer sc.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c1, rel1, err := sc.acquireConn(ctx)
	require.NoError(t, err, "first acquire")
	rel1()

	c2, rel2, err := sc.acquireConn(ctx)

	require.NoError(t, err, "second acquire")
	defer rel2()
	assert.Same(t, c1, c2, "expected reuse of the same conn")
	assert.EqualValues(t, 1, d.dialCount.Load(), "dial count = %d, want 1", d.dialCount.Load())
}

func TestSingleConn_Acquire_GoAwayTriggersRedial(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	var dialIdx atomic.Int32
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// First dialed peer sends GOAWAY immediately to drain the conn.
		if dialIdx.Add(1) == 1 {
			_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
		}
		<-stopSrv
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}, metrics: &Metrics{}}
	defer sc.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c1, rel1, err := sc.acquireConn(ctx)
	require.NoError(t, err, "first acquire")
	rel1()
	// Wait for reader to mark goAwayReceived on c1.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !c1.IsAlive() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.False(t, c1.IsAlive(),
		"peer GOAWAY was never observed on the first conn, so the redial below is not the one under test")

	c2, rel2, err := sc.acquireConn(ctx)

	require.NoError(t, err, "second acquire")
	defer rel2()
	assert.NotSame(t, c1, c2, "expected a fresh conn after GOAWAY")
	assert.EqualValues(t, 2, d.dialCount.Load(), "dial count = %d, want 2", d.dialCount.Load())
}

// failingDialer always errors.
type failingDialer struct {
	err       error
	dialCount atomic.Int32
}

func (d *failingDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	d.dialCount.Add(1)
	return nil, d.err
}

func TestSingleConn_Backoff_RefusesWithinWindow(t *testing.T) {
	d := &failingDialer{err: errors.New("boom")}
	sc := &singleConn{
		addr:     "fake:0",
		connOpts: conn.ConnOptions{Dialer: d},
		backoff:  500 * time.Millisecond,
		metrics:  &Metrics{},
	}
	defer sc.close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err1 := sc.acquireConn(ctx)
	_, _, err2 := sc.acquireConn(ctx)

	require.Error(t, err1, "first acquire must fail")
	require.Error(t, err2, "second acquire must fail")
	assert.EqualValues(t, 1, d.dialCount.Load(),
		"dial count = %d, want 1 (backoff suppressed second)", d.dialCount.Load())
}

func TestSingleConn_Acquire_ConcurrentDial_OnlyOneDials(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: func(_ *frame.Framer) {
		<-stopSrv
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}, metrics: &Metrics{}}
	defer sc.close()
	const N = 16
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results := make(chan *conn.Conn, N)

	for i := 0; i < N; i++ {
		go func() {
			c, _, err := sc.acquireConn(ctx)
			if err != nil {
				results <- nil
				return
			}
			results <- c
		}()
	}

	first := <-results
	require.NotNil(t, first, "first acquire returned nil conn")
	for i := 1; i < N; i++ {
		assert.Samef(t, first, <-results,
			"goroutine %d got different conn — the singleflight guard let a second dial through", i)
	}
	assert.EqualValues(t, 1, d.dialCount.Load(),
		"dial count = %d, want exactly 1 (singleflight)", d.dialCount.Load())
}

func TestSingleConn_Close_BlocksNewAcquires(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: func(_ *frame.Framer) {
		<-stopSrv
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}, metrics: &Metrics{}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := sc.acquireConn(ctx)
	require.NoError(t, err, "acquire")

	require.NoError(t, sc.close(), "close")

	assert.False(t, c.IsAlive(), "close must close underlying conn")
	_, _, err = sc.acquireConn(ctx)
	assert.ErrorIs(t, err, ErrClosed, "an acquire after close must be refused with ErrClosed")
}

func TestNewClient_RejectsEmptyAddr(t *testing.T) {
	_, err := NewClient(ClientOptions{ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}}})

	require.Error(t, err, "expected error on empty addr")
}

func TestNewClient_RejectsWhitespaceAddr(t *testing.T) {
	for _, addr := range []string{"  ", "fake :0", "\tfake:0"} {
		_, err := NewClient(ClientOptions{
			Addr:     addr,
			ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
		})

		assert.Errorf(t, err, "expected error for addr=%q", addr)
	}
}

func TestNewClient_RejectsNilDialer(t *testing.T) {
	_, err := NewClient(ClientOptions{Addr: "fake:0"})

	require.Error(t, err, "expected error on nil dialer")
}

func TestClient_Close_Idempotent(t *testing.T) {
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
	})
	require.NoError(t, err, "NewClient")

	err1 := c.Close()
	err2 := c.Close()

	assert.NoError(t, err1, "first Close")
	assert.NoError(t, err2, "second Close must be a no-op, not an error")
}

func TestClient_Do_AfterClose_ReturnsErrClosed(t *testing.T) {
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
	})
	require.NoError(t, err, "NewClient")
	_ = c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var res Response
	doErr := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res)
	var sr StreamResponse
	streamErr := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr)

	assert.ErrorIs(t, doErr, ErrClosed, "Do after Close must be ErrClosed, not a dial attempt")
	assert.ErrorIs(t, streamErr, ErrClosed, "DoStream after Close must be ErrClosed, not a dial attempt")
}

func TestStreamResetError_Error_Format(t *testing.T) {
	e := &StreamResetError{Code: frame.ErrCodeRefusedStream}

	msg := e.Error()

	assert.Containsf(t, msg, "stream reset",
		"error message = %q; a caller reading logs cannot tell this from a transport failure without it", msg)
}

func TestDialError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("boom")
	e := &DialError{Addr: "fake:0", Err: inner}

	msg := e.Error()

	assert.Containsf(t, msg, "fake:0", "error missing addr: %q", msg)
	assert.True(t, e.Unwrap() == inner, "Unwrap must return the wrapped error itself, not a copy")
	assert.True(t, errors.Is(e, inner), "errors.Is must walk through DialError.Unwrap")
}

func TestStreamResponse_RecvAfterDrain_ReturnsErrStreamEnded(t *testing.T) {
	d := &fakeDialer{srvAfter: minimalGETServer()}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr StreamResponse
	require.NoError(t, c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer sr.Close()

	_, err = sr.Recv(ctx)

	// Server returned status=200 + END_STREAM in initial HEADERS, so
	// drained==true. Recv must return ErrStreamEnded.
	assert.ErrorIs(t, err, ErrStreamEnded,
		"a Recv on an already-drained stream must say so rather than block for events that cannot come")
}

func TestEventType_String(t *testing.T) {
	cases := map[EventType]string{
		EventNone:     "none",
		EventData:     "data",
		EventTrailers: "trailers",
		EventReset:    "reset",
		EventType(99): "unknown",
	}

	for et, want := range cases {
		got := et.String()

		assert.Equalf(t, want, got, "EventType(%d).String() = %q, want %q", et, got, want)
	}
}

func TestDeriveAuthority(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		{"example.com:80", "example.com"},
		{"example.com:443", "example.com"},
		{"example.com:8080", "example.com:8080"},
		{"example.com", "example.com"},
		{"127.0.0.1:9090", "127.0.0.1:9090"},
		{"[::1]:443", "[::1]"},
		{"[::1]:80", "[::1]"},
		{"[::1]:8080", "[::1]:8080"},
		{"[2001:db8::1]:443", "[2001:db8::1]"},
		// Edge cases that must fall through to the raw addr unchanged.
		{":443", ":443"},
		{":8080", ":8080"},
	}

	for _, tc := range cases {
		got := deriveAuthority(tc.addr)

		assert.Equalf(t, tc.want, got, "deriveAuthority(%q) = %q, want %q", tc.addr, got, tc.want)
	}
}

// captureHandler is a frame.Handler that records HEADERS, DATA, and
// RST_STREAM observations under a mutex so test scenarios can poll
// for arrivals between ReadFrame calls.
type captureHandler struct {
	mu      sync.Mutex
	headers []capturedHeaders
	data    []capturedData
	rsts    []capturedRST
}

type capturedHeaders struct {
	streamID  uint32
	block     []byte
	endStream bool
}

type capturedData struct {
	streamID  uint32
	payload   []byte
	endStream bool
}

type capturedRST struct {
	streamID uint32
	code     frame.ErrCode
}

func newCaptureHandler() *captureHandler { return &captureHandler{} }

func (h *captureHandler) firstHeadersStreamID() (uint32, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.headers) == 0 {
		return 0, false
	}
	return h.headers[0].streamID, true
}

func (h *captureHandler) bodyEnded(streamID uint32) ([]byte, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var buf []byte
	ended := false
	for _, d := range h.data {
		if d.streamID != streamID {
			continue
		}
		buf = append(buf, d.payload...)
		if d.endStream {
			ended = true
		}
	}
	if !ended {
		return nil, false
	}
	return buf, true
}

func (h *captureHandler) firstRST(streamID uint32) (frame.ErrCode, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.rsts {
		if r.streamID == streamID {
			return r.code, true
		}
	}
	return 0, false
}

func (h *captureHandler) headerBlock(streamID uint32) ([]byte, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, hd := range h.headers {
		if hd.streamID == streamID {
			return hd.block, true
		}
	}
	return nil, false
}

func (h *captureHandler) OnData(fh frame.FrameHeader, payload []byte, _ uint8) error {
	h.mu.Lock()
	cp := append([]byte(nil), payload...)
	h.data = append(h.data, capturedData{
		streamID:  fh.StreamID,
		payload:   cp,
		endStream: fh.Flags&frame.FlagDataEndStream != 0,
	})
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	h.mu.Lock()
	cp := append([]byte(nil), hb...)
	h.headers = append(h.headers, capturedHeaders{
		streamID:  fh.StreamID,
		block:     cp,
		endStream: fh.Flags&frame.FlagHeadersEndStream != 0,
	})
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }

func (h *captureHandler) OnRSTStream(fh frame.FrameHeader, code frame.ErrCode) error {
	h.mu.Lock()
	h.rsts = append(h.rsts, capturedRST{streamID: fh.StreamID, code: code})
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error {
	return nil
}

func (h *captureHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}

func (h *captureHandler) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (h *captureHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (h *captureHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (h *captureHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }
func (h *captureHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error           { return nil }
func (h *captureHandler) OnOrigin(frame.FrameHeader, []string) error                      { return nil }

// minimalGETServer replies to the first incoming HEADERS frame with
// :status=200 and END_STREAM. Any subsequent frames are ignored.
func minimalGETServer() func(srvFr *frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: block,
				EndHeaders:    true,
				EndStream:     true,
			})
			return
		}
	}
}

func TestClient_Do_GET_NoBody_ReturnsStatus200(t *testing.T) {
	d := &fakeDialer{srvAfter: minimalGETServer()}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "Status = %d, want 200", res.Status)
}

// echoPOSTServer reads HEADERS + DATA frames until END_STREAM, then
// writes back HEADERS(:status=200) + DATA(echo) with END_STREAM.
// captured (if non-nil) is filled with the request body the server saw.
func echoPOSTServer(captured *[]byte) func(srvFr *frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		var streamID uint32
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			if streamID == 0 {
				if sid, ok := capH.firstHeadersStreamID(); ok {
					streamID = sid
				}
			}
			if streamID == 0 {
				continue
			}
			body, ended := capH.bodyEnded(streamID)
			if !ended {
				continue
			}
			if captured != nil {
				*captured = append((*captured)[:0], body...)
			}
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      streamID,
				BlockFragment: block,
				EndHeaders:    true,
			})
			_ = srvFr.WriteData(streamID, true, body)
			return
		}
	}
}

func TestClient_Do_POST_BodyBytes(t *testing.T) {
	var captured []byte
	d := &fakeDialer{srvAfter: echoPOSTServer(&captured)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body := []byte("hello world")

	var res Response
	err = c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		Body:     body,
		BodyMode: BodyBuffer,
	}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "status = %d", res.Status)
	assert.Equal(t, string(body), string(res.Body), "echoed body = %q, want %q", res.Body, body)
	assert.Equal(t, string(body), string(captured), "server saw %q, want %q", captured, body)
}

// TestClient_Do_POST_BodyReader uses a small body to stay within a
// single frame; net.Pipe is unbuffered + synchronous and chokes on
// multi-frame uploads. The integration suite covers chunked uploads
// against a real net/http2.Server (Task 20).
func TestClient_Do_POST_BodyReader(t *testing.T) {
	want := bytes.Repeat([]byte("ab"), 100) // 200 B, single frame
	var captured []byte
	d := &fakeDialer{srvAfter: echoPOSTServer(&captured)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		BodyMode:   BodyBuffer,
	}, &res)

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, res.Status, "status = %d", res.Status)
	assert.Truef(t, bytes.Equal(captured, want),
		"server captured %d bytes, want %d", len(captured), len(want))
	assert.Truef(t, bytes.Equal(res.Body, want),
		"echoed body length %d, want %d", len(res.Body), len(want))
}

func TestClient_Do_BodyDiscard_DiscardsButCounts(t *testing.T) {
	want := []byte("0123456789abcdef")
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: block,
				EndHeaders:    true,
			})
			_ = srvFr.WriteData(sid, true, want)
			return
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res)

	require.NoError(t, err, "Do")
	assert.Nil(t, res.Body, "Body should be nil with BodyDiscard, got %v", res.Body)
	assert.EqualValues(t, len(want), res.BytesReceived,
		"BytesReceived = %d, want %d — a discarded body is still counted", res.BytesReceived, len(want))
}

func TestClient_Do_WantTrailers_CapturesTrailers(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: respBlock,
				EndHeaders:    true,
			})
			_ = srvFr.WriteData(sid, false, []byte("body"))
			tEnc := hpack.NewEncoder()
			trailerBlock := tEnc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte("grpc-status"), Value: []byte("0")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: trailerBlock,
				EndHeaders:    true,
				EndStream:     true,
			})
			return
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		WantTrailers: true,
	}, &res)

	require.NoError(t, err, "Do")
	require.Lenf(t, res.Trailers, 1, "trailers = %+v", res.Trailers)
	assert.Equalf(t, "grpc-status", string(res.Trailers[0].Name), "trailers = %+v", res.Trailers)
}

func TestClient_Do_StreamReset_ReturnsTypedError(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			_ = srvFr.WriteRSTStream(sid, frame.ErrCodeRefusedStream)
			return
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res)

	var rs *StreamResetError
	require.Truef(t, errors.As(err, &rs),
		"expected *StreamResetError, got %v — a caller cannot classify a reset it cannot type-assert", err)
	assert.Equal(t, frame.ErrCodeRefusedStream, rs.Code, "code = %v, want REFUSED_STREAM", rs.Code)
}

func TestClient_DoStream_RecvDataChunks(t *testing.T) {
	chunks := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: block,
				EndHeaders:    true,
			})
			for i, ck := range chunks {
				_ = srvFr.WriteData(sid, i == len(chunks)-1, ck)
			}
			return
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var sr StreamResponse
	require.NoError(t, c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer sr.Close()
	var got [][]byte
	for {
		ev, rerr := sr.Recv(ctx)
		require.NoError(t, rerr, "Recv")
		if ev.Type == EventData {
			cp := make([]byte, len(ev.Data))
			copy(cp, ev.Data)
			got = append(got, cp)
		}
		if ev.EndStream {
			break
		}
	}

	assert.Equal(t, 200, sr.Status, "status = %d", sr.Status)
	require.Len(t, got, 3, "got %d chunks, want 3 — DATA frames were coalesced or dropped", len(got))
	for i, want := range chunks {
		assert.Truef(t, bytes.Equal(got[i], want), "chunk %d = %q, want %q", i, got[i], want)
	}
}

func TestClient_DoStream_CloseBeforeEnd_SendsRSTCancel(t *testing.T) {
	gotRST := make(chan frame.ErrCode, 1)
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		var sid uint32
		var sentResponse bool
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			if sid == 0 {
				if v, ok := capH.firstHeadersStreamID(); ok {
					sid = v
				}
			}
			if sid == 0 {
				continue
			}
			if !sentResponse {
				enc := hpack.NewEncoder()
				block := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
					StreamID:      sid,
					BlockFragment: block,
					EndHeaders:    true,
				})
				_ = srvFr.WriteData(sid, false, []byte("partial"))
				sentResponse = true
			}
			if code, ok := capH.firstRST(sid); ok {
				gotRST <- code
				return
			}
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr StreamResponse
	require.NoError(t, c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	_, err = sr.Recv(ctx)
	require.NoError(t, err, "first Recv")

	require.NoError(t, sr.Close(), "Close")

	select {
	case code := <-gotRST:
		assert.Equal(t, frame.ErrCodeCancel, code,
			"RST code = %v, want CANCEL — an undrained stream must be cancelled, not abandoned", code)
	case <-time.After(2 * time.Second):
		assert.Fail(t, "server did not see RST_STREAM(CANCEL)")
	}
}

// TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst asserts that
// the client emits all pseudo-headers (names starting with ':') before
// any regular header in the on-wire HEADERS block (RFC 7540 §8.1.2.1).
func TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst(t *testing.T) {
	captured := make(chan []hpack.HeaderField, 1)
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			block, ok := capH.headerBlock(sid)
			if !ok {
				continue
			}
			dec := hpack.NewDecoder()
			var fields []hpack.HeaderField
			_ = dec.DecodeBlock(block, func(f hpack.HeaderField) error {
				nm := append([]byte(nil), f.Name...)
				vl := append([]byte(nil), f.Value...)
				fields = append(fields, hpack.HeaderField{Name: nm, Value: vl})
				return nil
			})
			captured <- fields
			enc := hpack.NewEncoder()
			respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: respBlock,
				EndHeaders:    true,
				EndStream:     true,
			})
			return
		}
	}}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var res Response
	err = c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("x-trace-id"), Value: []byte("abc")},
		},
	}, &res)

	require.NoError(t, err, "Do")
	fields := <-captured
	require.NotEmpty(t, fields, "the peer decoded no fields from the HEADERS block, so ordering is untested")
	seenRegular := false
	for _, f := range fields {
		isPseudo := len(f.Name) > 0 && f.Name[0] == ':'
		assert.Falsef(t, isPseudo && seenRegular,
			"pseudo-header %q after regular: %+v — RFC 7540 §8.1.2.1 makes such a block malformed", f.Name, fields)
		if !isPseudo {
			seenRegular = true
		}
	}
}

func TestClient_NewClient_Pool_RequiresPoolOptions(t *testing.T) {
	t.Parallel()

	_, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportPool,
	})

	require.ErrorIs(t, err, ErrInvalidPoolOptions, "err = %v, want ErrInvalidPoolOptions", err)
}

func TestClient_NewClient_SingleConn_RejectsPoolOptions(t *testing.T) {
	t.Parallel()

	_, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportSingleConn,
		Pool:      &PoolOptions{MaxConnsPerHost: 4},
	})

	require.ErrorIs(t, err, ErrInvalidPoolOptions, "err = %v, want ErrInvalidPoolOptions", err)
}

func TestClient_NewClient_InvalidTransportKind(t *testing.T) {
	t.Parallel()

	_, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportKind(42),
	})

	require.ErrorIs(t, err, ErrInvalidTransportKind, "err = %v, want ErrInvalidTransportKind", err)
}

func TestClient_NewClient_Pool_Constructs(t *testing.T) {
	t.Parallel()

	c, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2},
	})

	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	_, ok := c.tr.(*poolTransport)
	assert.Truef(t, ok, "tr type = %T, want *poolTransport", c.tr)
}
