package conn

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "Dial")
	defer c.Close()
	st, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	require.NoError(t, st.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")
	body := drainBody(ctx, t, st)

	require.Truef(t, bytes.Contains(body, []byte("hello from the peer")), "body = %q", body)

	// The handshake SETTINGS we sent, with the parameters decoded.
	settings, ok := rec.find(trace.DirOut, "SETTINGS")
	require.True(t, ok, "no outbound SETTINGS traced; the tracer was installed after the handshake")
	require.Truef(t, settings.Detail.Has(trace.DetailParams),
		"outbound SETTINGS traced without the params detail bit: %+v", settings)
	require.NotNilf(t, settings.Params, "outbound SETTINGS traced without parameters: %+v", settings)
	var sawWindow bool
	for _, p := range settings.Params.All() {
		if p.Name == "INITIAL_WINDOW_SIZE" {
			sawWindow = true
		}
	}
	assert.Truef(t, sawWindow,
		"outbound SETTINGS params = %+v, want INITIAL_WINDOW_SIZE among them", settings.Params.All())

	// Our request, and the peer's answer to it.
	req, ok := rec.find(trace.DirOut, "HEADERS")
	require.True(t, ok, "no outbound HEADERS traced")
	assert.NotZerof(t, req.StreamID, "request HEADERS = %+v, want a non-zero stream", req)
	assert.Containsf(t, req.FlagNames, "END_STREAM", "request HEADERS = %+v, want END_STREAM", req)
	_, inHeaders := rec.find(trace.DirIn, "HEADERS")
	assert.True(t, inHeaders, "no inbound HEADERS traced")
	_, inData := rec.find(trace.DirIn, "DATA")
	assert.True(t, inData, "no inbound DATA traced")
	_, inSettings := rec.find(trace.DirIn, "SETTINGS")
	assert.True(t, inSettings, "no inbound SETTINGS traced")
	for _, info := range rec.snapshot() {
		assert.Equalf(t, trace.ProtoH2, info.Proto, "frame traced as %v, want h2: %+v", info.Proto, info)
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
	require.NoError(t, err, "Dial")
	st, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	require.NoError(t, st.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")
	drainBody(ctx, t, st)
	c.Close()
	require.NoError(t, tracer.Close(), "tracer Close")

	log := out.String()
	for _, want := range []string{
		"h2 -> SETTINGS",
		"h2 -> HEADERS stream=1",
		"h2 <- SETTINGS",
		"h2 <- HEADERS stream=1",
		"h2 <- DATA stream=1",
	} {
		assert.Containsf(t, log, want, "frame log does not contain %q", want)
	}
	// The one thing the log must never contain: a header value. The framing
	// layer only ever sees the compressed block, and this is what keeps it that
	// way as the event struct grows.
	assert.NotContains(t, log, "example.com",
		"frame log leaked a header value: the framing layer only ever sees the compressed block")
}
