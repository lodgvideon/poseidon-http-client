package conn

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// traceRecorder is a frame.Tracer that keeps a copy of every event. It COPIES,
// because the Framer reuses one FrameInfo per direction — the retention rule
// frame.Tracer documents.
//
// The mutex is not decoration: a *Conn hands one tracer to two goroutines at
// once, its reader loop and whichever writer holds wmu. Under -race this test
// is also the proof that the two scratch structs on the Framer do not collide.
type traceRecorder struct {
	mu     sync.Mutex
	events []frame.FrameInfo
}

func (r *traceRecorder) TraceFrame(fi *frame.FrameInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, *fi)
}

func (r *traceRecorder) snapshot() []frame.FrameInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]frame.FrameInfo(nil), r.events...)
}

// count returns how many events match dir and typ.
func (r *traceRecorder) count(dir frame.Direction, typ frame.FrameType) int {
	n := 0
	for _, e := range r.snapshot() {
		if e.Dir == dir && e.Header.Type == typ {
			n++
		}
	}
	return n
}

// TestTrace_RealPeer_RoundTrip is the end-to-end claim of #610: with a tracer
// installed, a request against a real net/http2 server produces a readable
// account of the wire — the handshake included.
//
// It runs against httptest rather than net.Pipe on purpose. The reader
// goroutine writes frames back from inside its own dispatch (the SETTINGS ACK,
// the WINDOW_UPDATE refunds), so a single Framer emits in both directions from
// two goroutines under real timing; net.Pipe's synchronous unbuffered semantics
// would not reach that.
func TestTrace_RealPeer_RoundTrip(t *testing.T) {
	body := make([]byte, 64<<10)
	for i := range body {
		body[i] = byte(i)
	}
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/octet-stream")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	rec := &traceRecorder{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer:            &TLSDialer{Config: cfg},
		Tracer:            rec,
		StreamEventBuffer: 64,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if got := drainBody(ctx, t, s); len(got) != len(body) {
		t.Fatalf("body = %d bytes, want %d", len(got), len(body))
	}

	// The handshake is traced, which is the point of installing the tracer
	// before handshakeSettings rather than after it: a peer that is not the peer
	// you think it is goes wrong here, before any request exists.
	if n := rec.count(frame.DirSend, frame.FrameSettings); n == 0 {
		t.Error("no outbound SETTINGS traced; the handshake is invisible")
	}
	if n := rec.count(frame.DirRecv, frame.FrameSettings); n == 0 {
		t.Error("no inbound SETTINGS traced")
	}
	if n := rec.count(frame.DirSend, frame.FrameHeaders); n != 1 {
		t.Errorf("outbound HEADERS traced %d times, want 1", n)
	}
	if n := rec.count(frame.DirRecv, frame.FrameHeaders); n == 0 {
		t.Error("no inbound HEADERS traced")
	}
	if n := rec.count(frame.DirRecv, frame.FrameData); n == 0 {
		t.Error("no inbound DATA traced")
	}
	// A 64 KiB body outgrows the RFC-mandated 65535-byte connection window, so
	// the refunds have to appear — this is the exact signal the flow-control
	// deadlock behind #471/#472 would have shown as absent.
	if n := rec.count(frame.DirSend, frame.FrameWindowUpdate); n == 0 {
		t.Error("no outbound WINDOW_UPDATE traced on a body larger than the initial window")
	}

	// Every event must name a frame type this codec knows, and every send must
	// be self-consistent: a stale-detail bug shows up here as an ErrCode on a
	// frame type that has none.
	for i, e := range rec.snapshot() {
		switch e.Header.Type {
		case frame.FrameData, frame.FrameHeaders, frame.FrameSettings,
			frame.FrameWindowUpdate, frame.FramePing, frame.FrameRSTStream,
			frame.FrameGoAway, frame.FramePriority, frame.FrameContinuation,
			frame.FramePushPromise, frame.FrameAltSvc, frame.FrameOrigin:
		default:
			t.Errorf("event %d: unexpected frame type %v from a net/http2 peer", i, e.Header.Type)
		}
		if e.Header.Type != frame.FrameRSTStream && e.Header.Type != frame.FrameGoAway && e.ErrCode != 0 {
			t.Errorf("event %d (%v) carried a stale error code %v", i, e.Header.Type, e.ErrCode)
		}
		if e.Header.Type != frame.FrameWindowUpdate && e.WindowIncrement != 0 {
			t.Errorf("event %d (%v) carried a stale window increment %d", i, e.Header.Type, e.WindowIncrement)
		}
	}
}

// TestTrace_NilTracerIsTheDefault: nothing about a ConnOptions without a Tracer
// changes, and the zero value stays the working default.
func TestTrace_NilTracerIsTheDefault(t *testing.T) {
	if (ConnOptions{}).defaulted().Tracer != nil {
		t.Fatal("defaulted() invented a Tracer")
	}
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if got := string(drainBody(ctx, t, s)); got != "ok" {
		t.Fatalf("body = %q, want %q", got, "ok")
	}
}

// TestTrace_GoAwayCarriesItsCode pins the case #570 lost: a GOAWAY's error code
// has to survive into the trace event, not just its last-stream-id.
func TestTrace_GoAwayCarriesItsCode(t *testing.T) {
	rec := &traceRecorder{}
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, func(srvFr *frame.Framer) {
			if err := srvFr.WriteGoAway(7, frame.ErrCodeEnhanceYourCalm, nil); err != nil {
				t.Logf("server GOAWAY: %v", err)
			}
			// Hold the pipe open long enough for the client's reader loop to
			// consume the frame; pipeServer closes srv on return.
			time.Sleep(200 * time.Millisecond)
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{Tracer: rec})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer func() { _ = c.Close() }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range rec.snapshot() {
			if e.Dir == frame.DirRecv && e.Header.Type == frame.FrameGoAway {
				if e.ErrCode != frame.ErrCodeEnhanceYourCalm || e.LastStreamID != 7 {
					t.Fatalf("GOAWAY event = code %v last %d, want ENHANCE_YOUR_CALM / 7",
						e.ErrCode, e.LastStreamID)
				}
				<-done
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("inbound GOAWAY never appeared in the trace")
}
