package conn

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// syncRecorder collects frame events from a live connection. It has a mutex
// because the reader goroutine and the request goroutine both emit, which is
// the contract trace.Tracer states and the thing -race is here to check.
type syncRecorder struct {
	mu  sync.Mutex
	got []trace.FrameInfo
}

func (r *syncRecorder) TraceFrame(info trace.FrameInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if info.Params != nil {
		cp := *info.Params
		info.Params = &cp
	}
	r.got = append(r.got, info)
}

func (r *syncRecorder) snapshot() []trace.FrameInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]trace.FrameInfo(nil), r.got...)
}

func (r *syncRecorder) find(dir trace.Direction, name string) (trace.FrameInfo, bool) {
	for _, info := range r.snapshot() {
		if info.Dir == dir && info.TypeName == name {
			return info, true
		}
	}
	return trace.FrameInfo{}, false
}

// TestIntegration_Tracer_ObservesRealExchange drives one request against a real
// net/http2 server with a tracer installed, and requires the seam to have seen
// both directions of the connection — including the SETTINGS exchange, which is
// written before the caller gets a Conn back at all.
func TestIntegration_Tracer_ObservesRealExchange(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain")
		_, _ = w.Write([]byte("hello from the peer"))
	}))
	defer srv.Close()

	rec := &syncRecorder{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
		Tracer: rec,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	st, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := st.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if body := drainBody(ctx, t, st); !bytes.Contains(body, []byte("hello from the peer")) {
		t.Fatalf("body = %q", body)
	}

	// The handshake SETTINGS we sent, with the parameters decoded.
	settings, ok := rec.find(trace.DirOut, "SETTINGS")
	if !ok {
		t.Fatal("no outbound SETTINGS traced; the tracer was installed after the handshake")
	}
	if !settings.Detail.Has(trace.DetailParams) || settings.Params == nil {
		t.Fatalf("outbound SETTINGS traced without parameters: %+v", settings)
	}
	var sawWindow bool
	for _, p := range settings.Params.All() {
		if p.Name == "INITIAL_WINDOW_SIZE" {
			sawWindow = true
		}
	}
	if !sawWindow {
		t.Errorf("outbound SETTINGS params = %+v, want INITIAL_WINDOW_SIZE among them", settings.Params.All())
	}

	// Our request, and the peer's answer to it.
	req, ok := rec.find(trace.DirOut, "HEADERS")
	if !ok {
		t.Fatal("no outbound HEADERS traced")
	}
	if req.StreamID == 0 || !strings.Contains(req.FlagNames, "END_STREAM") {
		t.Errorf("request HEADERS = %+v, want a non-zero stream with END_STREAM", req)
	}
	if _, ok := rec.find(trace.DirIn, "HEADERS"); !ok {
		t.Error("no inbound HEADERS traced")
	}
	if _, ok := rec.find(trace.DirIn, "DATA"); !ok {
		t.Error("no inbound DATA traced")
	}
	if _, ok := rec.find(trace.DirIn, "SETTINGS"); !ok {
		t.Error("no inbound SETTINGS traced")
	}

	for _, info := range rec.snapshot() {
		if info.Proto != trace.ProtoH2 {
			t.Fatalf("frame traced as %v, want h2: %+v", info.Proto, info)
		}
	}
}

// TestIntegration_Tracer_TextOutput is the same exchange rendered by the
// built-in tracer, which is what a user actually reads. It asserts on shape,
// not on an exact transcript: the peer decides how many WINDOW_UPDATEs it sends.
func TestIntegration_Tracer_TextOutput(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	var out bytes.Buffer
	tracer := trace.NewTextTracer(&out)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer: &TLSDialer{Config: cfg},
		Tracer: tracer,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	st, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := st.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	drainBody(ctx, t, st)
	c.Close()
	if err := tracer.Close(); err != nil {
		t.Fatalf("tracer Close: %v", err)
	}

	log := out.String()
	for _, want := range []string{
		"h2 -> SETTINGS",
		"h2 -> HEADERS stream=1",
		"h2 <- SETTINGS",
		"h2 <- HEADERS stream=1",
		"h2 <- DATA stream=1",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("frame log does not contain %q\n---\n%s", want, log)
		}
	}
	// The one thing the log must never contain: a header value. The framing
	// layer only ever sees the compressed block, and this is what keeps it that
	// way as the event struct grows.
	if strings.Contains(log, "example.com") {
		t.Errorf("frame log leaked a header value:\n%s", log)
	}
}
