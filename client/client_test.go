package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestValidateRequest_OK(t *testing.T) {
	req := &Request{Method: "GET", Path: "/"}
	if err := validateRequest(req); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRequest_NoMethod(t *testing.T) {
	req := &Request{Path: "/"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_NoPath(t *testing.T) {
	req := &Request{Method: "GET"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_WhitespacePaddedMethodRejected(t *testing.T) {
	req := &Request{Method: " GET ", Path: "/"}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_WhitespacePaddedPathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: " / "}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_WhitespaceOnlyMethodRejected(t *testing.T) {
	req := &Request{Method: "   ", Path: "/"}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_WhitespaceOnlyPathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: "   "}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_InternalWhitespaceMethodRejected(t *testing.T) {
	req := &Request{Method: "GET POST", Path: "/"}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_InternalWhitespacePathRejected(t *testing.T) {
	req := &Request{Method: "GET", Path: "/foo bar"}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_TabInMethodRejected(t *testing.T) {
	req := &Request{Method: "GET\t", Path: "/"}
	if err := validateRequest(req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_PseudoHeaderInRegular(t *testing.T) {
	req := &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte(":authority"), Value: []byte("example.com")},
		},
	}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_NilRequest(t *testing.T) {
	err := validateRequest(nil)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestParseStatus_Found(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	var rest []hpack.HeaderField
	st, err := parseStatus(in, &rest)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if st != 200 {
		t.Fatalf("status = %d, want 200", st)
	}
	if len(rest) != 1 || string(rest[0].Name) != "content-type" {
		t.Fatalf("regular headers wrong: %+v", rest)
	}
}

func TestParseStatus_Missing(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	var dst []hpack.HeaderField
	if _, err := parseStatus(in, &dst); !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}

func TestParseStatus_NotNumeric(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("OK")},
	}
	var dst []hpack.HeaderField
	if _, err := parseStatus(in, &dst); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("expected ErrInvalidStatus, got %v", err)
	}
}

func TestParseStatus_Negative(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("-1")},
	}
	var dst []hpack.HeaderField
	if _, err := parseStatus(in, &dst); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("expected ErrInvalidStatus, got %v", err)
	}
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
func (nopHandler) OnPing(frame.FrameHeader, [8]byte) error                      { return nil }
func (nopHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (nopHandler) OnWindowUpdate(frame.FrameHeader, uint32) error               { return nil }
func (nopHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error    { return nil }
func (nopHandler) OnOrigin(frame.FrameHeader, []string) error                    { return nil }

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

	if d.dialCount.Load() != 0 {
		t.Fatalf("dial happened in constructor; count=%d", d.dialCount.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, release, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer release()

	if d.dialCount.Load() != 1 {
		t.Fatalf("dial count = %d, want 1", d.dialCount.Load())
	}
	if !c.IsAlive() {
		t.Fatal("acquired conn must be alive")
	}
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
	c1, rel1, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	rel1()
	c2, rel2, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer rel2()

	if c1 != c2 {
		t.Fatal("expected reuse of the same conn")
	}
	if d.dialCount.Load() != 1 {
		t.Fatalf("dial count = %d, want 1", d.dialCount.Load())
	}
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
	c1, rel1, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	rel1()
	// Wait for reader to mark goAwayReceived on c1.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !c1.IsAlive() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	c2, rel2, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer rel2()
	if c1 == c2 {
		t.Fatal("expected a fresh conn after GOAWAY")
	}
	if d.dialCount.Load() != 2 {
		t.Fatalf("dial count = %d, want 2", d.dialCount.Load())
	}
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
	if _, _, err := sc.acquire(ctx); err == nil {
		t.Fatal("first acquire must fail")
	}
	if _, _, err := sc.acquire(ctx); err == nil {
		t.Fatal("second acquire must fail")
	}
	if got := d.dialCount.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1 (backoff suppressed second)", got)
	}
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
			c, _, err := sc.acquire(ctx)
			if err != nil {
				results <- nil
				return
			}
			results <- c
		}()
	}
	first := <-results
	if first == nil {
		t.Fatal("first acquire returned nil conn")
	}
	for i := 1; i < N; i++ {
		got := <-results
		if got != first {
			t.Fatalf("goroutine %d got different conn", i)
		}
	}
	if got := d.dialCount.Load(); got != 1 {
		t.Fatalf("dial count = %d, want exactly 1 (singleflight)", got)
	}
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
	c, _, err := sc.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	_ = sc.close()
	if c.IsAlive() {
		t.Fatal("close must close underlying conn")
	}
	if _, _, err := sc.acquire(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestNewClient_RejectsEmptyAddr(t *testing.T) {
	_, err := NewClient(ClientOptions{ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}}})
	if err == nil {
		t.Fatal("expected error on empty addr")
	}
}

func TestNewClient_RejectsWhitespaceAddr(t *testing.T) {
	for _, addr := range []string{"  ", "fake :0", "\tfake:0"} {
		_, err := NewClient(ClientOptions{
			Addr:     addr,
			ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
		})
		if err == nil {
			t.Fatalf("expected error for addr=%q", addr)
		}
	}
}

func TestNewClient_RejectsNilDialer(t *testing.T) {
	_, err := NewClient(ClientOptions{Addr: "fake:0"})
	if err == nil {
		t.Fatal("expected error on nil dialer")
	}
}

func TestClient_Close_Idempotent(t *testing.T) {
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestClient_Do_AfterClose_ReturnsErrClosed(t *testing.T) {
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_ = c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var _res Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &_res); !errors.Is(err, ErrClosed) {
		t.Fatalf("Do after Close = %v, want ErrClosed", err)
	}
	var _sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &_sr); !errors.Is(err, ErrClosed) {
		t.Fatalf("DoStream after Close = %v, want ErrClosed", err)
	}
}

func TestStreamResetError_Error_Format(t *testing.T) {
	e := &StreamResetError{Code: frame.ErrCodeRefusedStream}
	if !strings.Contains(e.Error(), "stream reset") {
		t.Fatalf("error message = %q", e.Error())
	}
}

func TestDialError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("boom")
	e := &DialError{Addr: "fake:0", Err: inner}
	if !strings.Contains(e.Error(), "fake:0") {
		t.Fatalf("error missing addr: %q", e.Error())
	}
	if e.Unwrap() != inner {
		t.Fatalf("Unwrap = %v, want %v", e.Unwrap(), inner)
	}
	if !errors.Is(e, inner) {
		t.Fatal("errors.Is must walk through DialError.Unwrap")
	}
}

func TestStreamResponse_RecvAfterDrain_ReturnsErrStreamEnded(t *testing.T) {
	d := &fakeDialer{srvAfter: minimalGETServer()}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	// Server returned status=200 + END_STREAM in initial HEADERS, so
	// drained==true. Recv must return ErrStreamEnded.
	if _, err := sr.Recv(ctx); !errors.Is(err, ErrStreamEnded) {
		t.Fatalf("Recv after drain = %v, want ErrStreamEnded", err)
	}
}

func TestEventType_String(t *testing.T) {
	cases := map[EventType]string{
		EventData:     "data",
		EventTrailers: "trailers",
		EventReset:    "reset",
		EventType(0):  "unknown",
	}
	for et, want := range cases {
		if got := et.String(); got != want {
			t.Errorf("EventType(%d).String() = %q, want %q", et, got, want)
		}
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
		if got != tc.want {
			t.Errorf("deriveAuthority(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// captureHandler is a frame.Handler that records HEADERS, DATA, and
// RST_STREAM observations under a mutex so test scenarios can poll
// for arrivals between ReadFrame calls.
type captureHandler struct {
	mu       sync.Mutex
	headers  []capturedHeaders
	data     []capturedData
	rsts     []capturedRST
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

func (h *captureHandler) OnPing(frame.FrameHeader, [8]byte) error                      { return nil }
func (h *captureHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (h *captureHandler) OnWindowUpdate(frame.FrameHeader, uint32) error               { return nil }
func (h *captureHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error    { return nil }
func (h *captureHandler) OnOrigin(frame.FrameHeader, []string) error                    { return nil }

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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var res Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("Status = %d, want 200", res.Status)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body := []byte("hello world")
	var res Response
	if err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		Body:     body,
		WantBody: true,
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if string(res.Body) != string(body) {
		t.Fatalf("echoed body = %q, want %q", res.Body, body)
	}
	if string(captured) != string(body) {
		t.Fatalf("server saw %q, want %q", captured, body)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res Response
	if err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		WantBody:   true,
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if !bytes.Equal(captured, want) {
		t.Fatalf("server captured %d bytes, want %d", len(captured), len(want))
	}
	if !bytes.Equal(res.Body, want) {
		t.Fatalf("echoed body length %d, want %d", len(res.Body), len(want))
	}
}

func TestClient_Do_WantBody_False_DiscardsButCounts(t *testing.T) {
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var res Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/"}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Body != nil {
		t.Fatalf("Body should be nil with WantBody=false, got %v", res.Body)
	}
	if res.BytesReceived != int64(len(want)) {
		t.Fatalf("BytesReceived = %d, want %d", res.BytesReceived, len(want))
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var res Response
	if err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		WantTrailers: true,
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if len(res.Trailers) != 1 || string(res.Trailers[0].Name) != "grpc-status" {
		t.Fatalf("trailers = %+v", res.Trailers)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var _res Response
	err = c.Do(ctx, &Request{Method: "GET", Path: "/"}, &_res)
	var rs *StreamResetError
	if !errors.As(err, &rs) {
		t.Fatalf("expected *StreamResetError, got %v", err)
	}
	if rs.Code != frame.ErrCodeRefusedStream {
		t.Fatalf("code = %v, want REFUSED_STREAM", rs.Code)
	}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()
	if sr.Status != 200 {
		t.Fatalf("status = %d", sr.Status)
	}
	var got [][]byte
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventData {
			cp := make([]byte, len(ev.Data))
			copy(cp, ev.Data)
			got = append(got, cp)
		}
		if ev.EndStream {
			break
		}
	}
	if len(got) != 3 {
		t.Fatalf("got %d chunks, want 3", len(got))
	}
	for i, want := range chunks {
		if !bytes.Equal(got[i], want) {
			t.Fatalf("chunk %d = %q, want %q", i, got[i], want)
		}
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	if _, err := sr.Recv(ctx); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if err := sr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case code := <-gotRST:
		if code != frame.ErrCodeCancel {
			t.Fatalf("RST code = %v, want CANCEL", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not see RST_STREAM(CANCEL)")
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
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var _res2 Response
	if err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("x-trace-id"), Value: []byte("abc")},
		},
	}, &_res2); err != nil {
		t.Fatalf("Do: %v", err)
	}
	fields := <-captured
	seenRegular := false
	for _, f := range fields {
		isPseudo := len(f.Name) > 0 && f.Name[0] == ':'
		if isPseudo && seenRegular {
			t.Fatalf("pseudo-header %q after regular: %+v", f.Name, fields)
		}
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
	if !errors.Is(err, ErrInvalidPoolOptions) {
		t.Fatalf("err = %v, want ErrInvalidPoolOptions", err)
	}
}

func TestClient_NewClient_SingleConn_RejectsPoolOptions(t *testing.T) {
	t.Parallel()
	_, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportSingleConn,
		Pool:      &PoolOptions{MaxConnsPerHost: 4},
	})
	if !errors.Is(err, ErrInvalidPoolOptions) {
		t.Fatalf("err = %v, want ErrInvalidPoolOptions", err)
	}
}

func TestClient_NewClient_InvalidTransportKind(t *testing.T) {
	t.Parallel()
	_, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportKind(42),
	})
	if !errors.Is(err, ErrInvalidTransportKind) {
		t.Fatalf("err = %v, want ErrInvalidTransportKind", err)
	}
}

func TestClient_NewClient_Pool_Constructs(t *testing.T) {
	t.Parallel()
	c, err := NewClient(ClientOptions{
		Addr:      "example.com:443",
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}},
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2},
	})
	if err != nil {
		t.Fatalf("NewClient = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if _, ok := c.tr.(*poolTransport); !ok {
		t.Fatalf("tr type = %T, want *poolTransport", c.tr)
	}
}
