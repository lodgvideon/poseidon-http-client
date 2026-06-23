package client

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"
)

func TestH_LowercasesName(t *testing.T) {
	h := H("Content-Type", "application/json")
	if string(h.Name) != "content-type" {
		t.Errorf("H name = %q, want lowercase content-type", h.Name)
	}
	if string(h.Value) != "application/json" {
		t.Errorf("H value = %q", h.Value)
	}
}

func TestGETPOST_DefaultsCaptureBody(t *testing.T) {
	g := GET("/x")
	if g.Method != "GET" || g.Path != "/x" || !g.WantBody {
		t.Errorf("GET = %+v, want GET /x WantBody=true", g)
	}
	body := []byte("hi")
	p := POST("/y", body)
	if p.Method != "POST" || p.Path != "/y" || !p.WantBody || !bytes.Equal(p.Body, body) {
		t.Errorf("POST = %+v, want POST /y WantBody=true body=hi", p)
	}
}

func TestWithHeaders_SetsAndChains(t *testing.T) {
	r := GET("/").WithHeaders(H("accept", "text/plain"), H("x-k", "v"))
	if len(r.Headers) != 2 || string(r.Headers[0].Name) != "accept" {
		t.Fatalf("WithHeaders = %+v", r.Headers)
	}
}

func TestResponseHeader_CaseInsensitive(t *testing.T) {
	r := &Response{Headers: []HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}}
	if v, ok := r.Header("Content-Type"); !ok || string(v) != "application/json" {
		t.Errorf("Header(Content-Type) = %q,%v", v, ok)
	}
	if s, ok := r.HeaderString("CONTENT-TYPE"); !ok || s != "application/json" {
		t.Errorf("HeaderString = %q,%v", s, ok)
	}
	if _, ok := r.Header("missing"); ok {
		t.Error("Header(missing) ok=true, want false")
	}
}

func TestResponseHeader_ZeroAlloc(t *testing.T) {
	r := &Response{Headers: []HeaderField{
		{Name: []byte("accept"), Value: []byte("*/*")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}}
	if n := testing.AllocsPerRun(100, func() {
		if _, ok := r.Header("content-type"); !ok {
			t.Fatal("not found")
		}
	}); n != 0 {
		t.Errorf("Header allocates %v/op, want 0", n)
	}
}

func TestCopyBodyAndClone_DetachFromReset(t *testing.T) {
	r := &Response{
		Status:        200,
		Headers:       []HeaderField{{Name: []byte("etag"), Value: []byte("abc")}},
		Body:          append([]byte(nil), "payload"...),
		BytesReceived: 7,
	}
	cb := r.CopyBody()
	cl := r.Clone()
	r.Reset() // recycle/zero the source

	if string(cb) != "payload" {
		t.Errorf("CopyBody survived Reset as %q", cb)
	}
	if cl.Status != 200 || string(cl.Body) != "payload" || cl.BytesReceived != 7 {
		t.Errorf("Clone = %+v", cl)
	}
	if len(cl.Headers) != 1 || string(cl.Headers[0].Value) != "abc" {
		t.Errorf("Clone headers = %+v", cl.Headers)
	}
	// Clone must own its memory: mutating the clone must not be visible
	// through any shared backing array (there is none).
	cl.Headers[0].Value[0] = 'X'
}

func TestDataCopy(t *testing.T) {
	ev := StreamEvent{Type: EventData, Data: []byte("chunk")}
	cp := ev.DataCopy()
	if string(cp) != "chunk" {
		t.Errorf("DataCopy = %q", cp)
	}
	if ev.DataCopy() == nil && len(ev.Data) > 0 {
		t.Error("DataCopy returned nil for non-empty data")
	}
	if (StreamEvent{Type: EventReset}).DataCopy() != nil {
		t.Error("DataCopy of non-data event should be nil")
	}
}

// TestStream_AutoCloses drives the auto-closing Stream helper against a real
// h2 server that flushes several DATA frames, reassembling via DataCopy.
func TestStream_AutoCloses(t *testing.T) {
	const chunk = 4096
	const total = 4 * chunk
	pattern := make([]byte, total)
	for i := range pattern {
		pattern[i] = byte(i % 251)
	}
	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		for off := 0; off < total; off += chunk {
			_, _ = w.Write(pattern[off : off+chunk])
			if fl != nil {
				fl.Flush()
			}
		}
	})
	c := poolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	got := make([]byte, 0, total)
	err := c.Stream(ctx, GET("/"), func(ev StreamEvent) error {
		if ev.Type == EventData {
			got = append(got, ev.DataCopy()...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if !bytes.Equal(got, pattern) {
		t.Fatalf("Stream body mismatch: got %d bytes, want %d", len(got), total)
	}
	// The whole point of Stream is that it auto-Closes the StreamResponse,
	// releasing the pooled stream slot. Verifying only the bytes is too weak —
	// they arrive even if Close is forgotten. Assert the slot is actually
	// released: with Close, the pool actor drops InFlightStreams back to 0;
	// without it, the slot leaks and stays at 1. Release is processed
	// asynchronously by the pool actor, so poll with a bounded deadline.
	deadline := time.Now().Add(3 * time.Second)
	for c.PoolStats().InFlightStreams != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := c.PoolStats().InFlightStreams; n != 0 {
		t.Fatalf("Stream leaked a pooled stream slot: InFlightStreams=%d, want 0 (auto-Close missing?)", n)
	}
}
