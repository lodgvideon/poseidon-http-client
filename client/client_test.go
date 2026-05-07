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
	st, rest, err := parseStatus(in)
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
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}

func TestParseStatus_NotNumeric(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("OK")},
	}
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
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
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// Hold the connection alive until the test cleans up.
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
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
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
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
	var dialIdx atomic.Int32
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		// First dialed peer sends GOAWAY immediately to drain the conn.
		if dialIdx.Add(1) == 1 {
			_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
		}
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
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
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}
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
	if got := d.dialCount.Load(); got > 2 {
		t.Fatalf("dial count = %d, want 1 or 2 (race-loser permitted)", got)
	}
}

func TestSingleConn_Close_BlocksNewAcquires(t *testing.T) {
	d := &fakeDialer{srvAfter: func(srvFr *frame.Framer) {
		time.Sleep(2 * time.Second)
	}}
	sc := &singleConn{addr: "fake:0", connOpts: conn.ConnOptions{Dialer: d}}

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
	res, err := c.Do(ctx, &Request{Method: "GET", Path: "/"})
	if err != nil {
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
	res, err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		Body:     body,
		WantBody: true,
	})
	if err != nil {
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
	res, err := c.Do(ctx, &Request{
		Method: "POST", Path: "/echo",
		BodyReader: bytes.NewReader(want),
		WantBody:   true,
	})
	if err != nil {
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
