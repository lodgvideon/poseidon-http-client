package client

import (
	"context"
	"errors"
	"io"
	"net"
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
