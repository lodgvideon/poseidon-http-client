package client

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestH_LowercasesName(t *testing.T) {
	h := H("Content-Type", "application/json")

	assert.Equalf(t, "content-type", string(h.Name),
		"H name = %q, want lowercase content-type — HTTP/2 forbids uppercase field names "+
			"(RFC 7540 §8.1.2) and a peer must treat one as malformed", h.Name)
	assert.Equalf(t, "application/json", string(h.Value),
		"H value = %q, want it carried through verbatim; only the NAME is case-folded", h.Value)
}

func TestGETPOST_DefaultsCaptureBody(t *testing.T) {
	body := []byte("hi")

	g := GET("/x")
	p := POST("/y", body)

	assert.Equal(t, "GET", g.Method, "GET() method = %q", g.Method)
	assert.Equal(t, "/x", g.Path, "GET() path = %q", g.Path)
	assert.Equalf(t, BodyBuffer, g.BodyMode,
		"GET() BodyMode = %v, want BodyBuffer — the sugar helpers exist so a caller gets a "+
			"buffered response without naming the mode", g.BodyMode)
	assert.Equal(t, "POST", p.Method, "POST() method = %q", p.Method)
	assert.Equal(t, "/y", p.Path, "POST() path = %q", p.Path)
	assert.Equal(t, BodyBuffer, p.BodyMode, "POST() BodyMode = %v, want BodyBuffer", p.BodyMode)
	assert.Truef(t, bytes.Equal(p.Body, body),
		"POST() body = %q, want %q", p.Body, body)
}

func TestWithHeaders_SetsAndChains(t *testing.T) {
	r := GET("/").WithHeaders(H("accept", "text/plain"), H("x-k", "v"))

	require.Lenf(t, r.Headers, 2,
		"WithHeaders produced %d headers, want 2 — a variadic that keeps only the last "+
			"would silently drop the caller's other fields", len(r.Headers))
	assert.Equal(t, "accept", string(r.Headers[0].Name), "Headers[0] = %+v", r.Headers[0])
	assert.Equal(t, "x-k", string(r.Headers[1].Name), "Headers[1] = %+v", r.Headers[1])
}

func TestResponseHeader_CaseInsensitive(t *testing.T) {
	r := &Response{Headers: []HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}}

	v, okExact := r.Header("Content-Type")
	s, okUpper := r.HeaderString("CONTENT-TYPE")
	_, okMissing := r.Header("missing")

	assert.True(t, okExact, "Header(Content-Type) not found; lookups must fold ASCII case")
	assert.Equalf(t, "application/json", string(v), "Header(Content-Type) = %q", v)
	assert.True(t, okUpper, "HeaderString(CONTENT-TYPE) not found")
	assert.Equalf(t, "application/json", s, "HeaderString = %q", s)
	assert.Falsef(t, okMissing,
		"Header(missing) reported found — a lookup that never misses would hand the caller "+
			"another field's value")
}

func TestResponseHeader_ZeroAlloc(t *testing.T) {
	r := &Response{Headers: []HeaderField{
		{Name: []byte("accept"), Value: []byte("*/*")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}}
	// No testify inside the measured closure: it reflects and allocates, and
	// AllocsPerRun counts the whole process. The closure records what it saw and
	// the assertions run afterwards.
	var found bool

	n := testing.AllocsPerRun(100, func() {
		_, found = r.Header("content-type")
	})

	require.True(t, found,
		"the measured lookup did not find the header, so the allocation count below is "+
			"measuring the miss path rather than the hit path")
	assert.Equalf(t, float64(0), n,
		"Header allocates %v/op, want 0 — it returns a slice aliasing Response-owned memory "+
			"precisely so a hot request loop pays nothing to read a field", n)
}

func TestCopyBodyAndClone_DetachFromReset(t *testing.T) {
	r := &Response{
		Status:        200,
		Headers:       []HeaderField{{Name: []byte("etag"), Value: []byte("abc")}},
		Body:          append([]byte(nil), "payload"...),
		BytesReceived: 7,
	}
	// Capture the source's backing bytes before Clone/Reset so we can prove the
	// clone does not alias them (Reset truncates but does not zero, so these
	// arrays keep their original bytes).
	srcVal := r.Headers[0].Value
	srcBody := r.Body

	cb := r.CopyBody()
	cl := r.Clone()
	r.Reset() // recycle/zero the source

	assert.Equalf(t, "payload", string(cb),
		"CopyBody survived Reset as %q — it exists so a caller can retain the body past "+
			"the next request on this Response", cb)
	assert.Equal(t, 200, cl.Status, "Clone status = %d", cl.Status)
	assert.Equal(t, "payload", string(cl.Body), "Clone body = %q", cl.Body)
	assert.Equal(t, int64(7), cl.BytesReceived, "Clone BytesReceived = %d", cl.BytesReceived)
	require.Lenf(t, cl.Headers, 1, "Clone headers = %+v, want one field", cl.Headers)
	assert.Equalf(t, "abc", string(cl.Headers[0].Value), "Clone header value = %q", cl.Headers[0].Value)
	// Deep-copy proof: mutating the clone must NOT change the source's backing
	// bytes. A shallow clone aliasing r's arrays would leak the mutation through
	// srcVal/srcBody (still readable after Reset).
	cl.Headers[0].Value[0] = 'X'
	cl.Body[0] = 'Z'
	assert.Equalf(t, byte('a'), srcVal[0],
		"Clone is shallow: a header mutation leaked back into the source (val=%c)", srcVal[0])
	assert.Equalf(t, byte('p'), srcBody[0],
		"Clone is shallow: a body mutation leaked back into the source (body=%c)", srcBody[0])
}

func TestDataCopy(t *testing.T) {
	ev := StreamEvent{Type: EventData, Data: []byte("chunk")}

	cp := ev.DataCopy()
	nonData := (StreamEvent{Type: EventReset}).DataCopy()

	assert.Equalf(t, "chunk", string(cp),
		"DataCopy = %q — it exists so a caller can retain a payload past the Recv that "+
			"recycles the pooled buffer", cp)
	assert.NotNilf(t, cp, "DataCopy returned nil for a non-empty %d-byte payload", len(ev.Data))
	assert.Nilf(t, nonData,
		"DataCopy of a non-data event = %q, want nil — copying a stale Data slice would "+
			"hand the caller another stream's bytes", nonData)
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

	require.NoError(t, err, "Stream over a chunked 16 KiB response")
	require.Truef(t, bytes.Equal(got, pattern),
		"Stream body mismatch: got %d bytes, want %d", len(got), total)
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
	assert.Zerof(t, c.PoolStats().InFlightStreams,
		"Stream leaked a pooled stream slot: InFlightStreams=%d, want 0 (auto-Close missing?)",
		c.PoolStats().InFlightStreams)
}
