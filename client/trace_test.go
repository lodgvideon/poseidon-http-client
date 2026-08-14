package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// countingTracer records frame types per direction. One instance serves every
// connection a client dials, from two goroutines each, which is the fan-out
// ClientOptions.Tracer documents — so the lock is load-bearing and this file
// is meaningful under -race.
type countingTracer struct {
	mu   sync.Mutex
	seen map[frame.Direction]map[frame.FrameType]int
}

func newCountingTracer() *countingTracer {
	return &countingTracer{seen: map[frame.Direction]map[frame.FrameType]int{
		frame.DirRecv: {}, frame.DirSend: {},
	}}
}

func (c *countingTracer) TraceFrame(fi *frame.FrameInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[fi.Dir][fi.Header.Type]++
}

func (c *countingTracer) count(d frame.Direction, t frame.FrameType) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[d][t]
}

func insecureDialer() *conn.TLSDialer {
	return &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}
}

// TestTracer_ReachesTheWireThroughClientOptions is the top of the plumbing:
// one field on ClientOptions and every HTTP/2 frame the client moves is
// observable, with no reference to conn.ConnOptions in the caller's code.
func TestTracer_ReachesTheWireThroughClientOptions(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	tr := newCountingTracer()

	c, err := client.NewClient(client.ClientOptions{
		Addr:     addr,
		ConnOpts: conn.ConnOptions{Dialer: insecureDialer()},
		Tracer:   tr,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/x"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if n := tr.count(frame.DirSend, frame.FrameHeaders); n == 0 {
		t.Error("no outbound HEADERS traced")
	}
	if n := tr.count(frame.DirRecv, frame.FrameHeaders); n == 0 {
		t.Error("no inbound HEADERS traced")
	}
	if n := tr.count(frame.DirSend, frame.FrameSettings); n == 0 {
		t.Error("no outbound SETTINGS traced; the handshake is invisible")
	}
}

// TestTracer_ClientOptionsOverridesConnOpts pins the documented precedence:
// the outer field wins, so one knob cannot disagree with the other.
func TestTracer_ClientOptionsOverridesConnOpts(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	outer, inner := newCountingTracer(), newCountingTracer()

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: insecureDialer(),
			Tracer: inner,
		},
		Tracer: outer,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/x"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	if outer.count(frame.DirSend, frame.FrameHeaders) == 0 {
		t.Error("ClientOptions.Tracer saw nothing")
	}
	if n := inner.count(frame.DirSend, frame.FrameHeaders); n != 0 {
		t.Errorf("ConnOpts.Tracer also fired (%d HEADERS); the outer field must win outright", n)
	}
}

// TestTracer_WithTracerOption covers the functional-options constructors, which
// are how most callers build a client and which cannot reach ConnOpts directly.
func TestTracer_WithTracerOption(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)
	tr := newCountingTracer()

	c, err := client.NewSingleConnClient(addr, insecureDialer(), client.WithTracer(tr))
	if err != nil {
		t.Fatalf("NewSingleConnClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/x"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if tr.count(frame.DirSend, frame.FrameHeaders) == 0 {
		t.Error("WithTracer did not reach the wire")
	}
}

// TestTracer_TextTracerEndToEnd is the whole feature in one test: install the
// built-in tracer, make a request, and read the wire.
func TestTracer_TextTracerEndToEnd(t *testing.T) {
	t.Parallel()
	_, addr := newH2TestServer(t)

	var out lockedBuffer
	// WithFlushInterval(0) so output is deterministic — Close flushes.
	tracer := trace.New(&out, trace.WithoutTimestamps(), trace.WithFlushInterval(0))

	c, err := client.NewSingleConnClient(addr, insecureDialer(), client.WithTracer(tracer))
	if err != nil {
		t.Fatalf("NewSingleConnClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/x"}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	// Close the client first: its reader goroutine is a writer to the tracer,
	// and closing the tracer under it would race the flush.
	_ = c.Close()
	if err := tracer.Close(); err != nil {
		t.Fatalf("tracer.Close: %v", err)
	}

	got := out.String()
	for _, want := range []string{
		"-> SETTINGS stream=0",
		"<- SETTINGS stream=0",
		"-> HEADERS stream=1",
		"<- HEADERS stream=1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("trace output missing %q\n---\n%s", want, got)
		}
	}
	// Header field names and values must not be in there. The block is HPACK
	// bytes and this tracer never renders a payload unless asked.
	for _, forbidden := range []string{"payload=", ":path", "/x"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("trace output leaked %q by default\n---\n%s", forbidden, got)
		}
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
