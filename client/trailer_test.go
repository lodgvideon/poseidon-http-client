package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func newTrailerH2Server(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = true
	s.StartTLS()
	t.Cleanup(s.Close)
	addr := strings.TrimPrefix(s.URL, "https://")
	return s, addr
}

func trailerClientFor(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// --- Request trailer sending ---

func TestDo_RequestTrailers_Static(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // must drain body to receive trailers
		got := r.Trailer.Get("X-Checksum")
		if got != "abc123" {
			t.Errorf("server: X-Checksum = %q, want %q", got, "abc123")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-checksum"), Value: []byte("abc123")}},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDo_RequestTrailers_Func(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Dynamic")
		if got != "dynamic-value" {
			t.Errorf("server: X-Dynamic = %q, want %q", got, "dynamic-value")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-static"), Value: []byte("ignored")}},
		TrailerFunc: func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x-dynamic"), Value: []byte("dynamic-value")}}
		},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDo_RequestTrailers_FuncNilFallback(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Fallback")
		if got != "fallback-value" {
			t.Errorf("server: X-Fallback = %q, want %q", got, "fallback-value")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-fallback"), Value: []byte("fallback-value")}},
		TrailerFunc: func() []hpack.HeaderField { return nil }, // nil → fallback to Trailers
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDo_RequestTrailers_NoBody(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Nobod")
		if got != "yes" {
			t.Errorf("server: X-Nobod = %q, want %q", got, "yes")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	// No Body, no BodyReader — wire: HEADERS(endStream=false) → HEADERS(trailers,END_STREAM)
	if err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Trailers: []hpack.HeaderField{{Name: []byte("x-nobod"), Value: []byte("yes")}},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDo_RequestTrailers_PseudoHeader(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}},
	}, &res)
	if !errors.Is(err, client.ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestDo_RequestTrailers_FuncPseudoHeader(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method: "POST",
		Path:   "/",
		Body:   []byte("hello"),
		TrailerFunc: func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte(":pseudo"), Value: []byte("bad")}}
		},
	}, &res)
	if !errors.Is(err, client.ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestDoStream_RequestTrailers(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Stream-Req")
		if got != "stream-ok" {
			t.Errorf("server: X-Stream-Req = %q, want %q", got, "stream-ok")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-stream-req"), Value: []byte("stream-ok")}},
	}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	// Drain the response stream
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.EndStream {
			break
		}
	}
	if sr.Status != 200 {
		t.Fatalf("status = %d, want 200", sr.Status)
	}
}

// --- Response trailer receiving ---

func TestDo_ResponseTrailers(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "X-Checksum")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
		w.Header().Set("X-Checksum", "abc123")
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method: "GET", Path: "/",
		WantBody: true, WantTrailers: true,
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	var found bool
	for _, f := range res.Trailers {
		if strings.EqualFold(string(f.Name), "x-checksum") && string(f.Value) == "abc123" {
			found = true
		}
	}
	if !found {
		t.Fatalf("x-checksum trailer not found in %v", res.Trailers)
	}
}

// --- StreamResponse.WaitTrailers ---

func TestDoStream_WaitTrailers_AfterDrain(t *testing.T) {
	// Server sends body + trailers. Client drains EventData via Recv,
	// then calls WaitTrailers which pumps Recv internally for EventTrailers.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "X-Tag")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data"))
		w.Header().Set("X-Tag", "after-drain")
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	// Drain one EventData frame (DATA frame has endStream=false because trailers follow).
	ev, err := sr.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	// Fast path: trailer arrived together with (or instead of) data on some runs.
	if ev.Type == client.EventTrailers {
		for _, f := range ev.Trailers {
			if strings.EqualFold(string(f.Name), "x-tag") && string(f.Value) == "after-drain" {
				return
			}
		}
		t.Fatalf("x-tag trailer not found in first Recv result %v", ev.Trailers)
	}
	if ev.Type != client.EventData {
		t.Fatalf("expected EventData or EventTrailers, got %v", ev.Type)
	}

	// WaitTrailers pumps Recv internally to get EventTrailers.
	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		t.Fatalf("WaitTrailers: %v", err)
	}
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-tag") && string(f.Value) == "after-drain" {
			found = true
		}
	}
	if !found {
		t.Fatalf("x-tag trailer not found in %v", trailers)
	}
}

func TestDoStream_WaitTrailers_CachedFromRecv(t *testing.T) {
	// Server sends body + trailers. Client calls Recv twice (EventData,
	// EventTrailers), which caches sr.trailers. WaitTrailers returns from cache.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "X-Cached")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data"))
		w.Header().Set("X-Cached", "yes")
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	// Recv EventData
	ev, err := sr.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(data): %v", err)
	}
	if ev.Type != client.EventData {
		t.Fatalf("expected EventData, got %v", ev.Type)
	}

	// Recv EventTrailers — this sets sr.trailers cache
	ev, err = sr.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(trailers): %v", err)
	}
	if ev.Type != client.EventTrailers {
		t.Fatalf("expected EventTrailers, got %v", ev.Type)
	}

	// WaitTrailers returns cached result without additional Recv calls.
	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		t.Fatalf("WaitTrailers: %v", err)
	}
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-cached") && string(f.Value) == "yes" {
			found = true
		}
	}
	if !found {
		t.Fatalf("x-cached trailer not found in %v", trailers)
	}
}

func TestDoStream_WaitTrailers_None(t *testing.T) {
	// Server sends no trailers; WaitTrailers returns nil, nil.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204) // no body, no trailers
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		t.Fatalf("WaitTrailers: %v", err)
	}
	if trailers != nil {
		t.Fatalf("expected nil trailers, got %v", trailers)
	}
}

func TestDoStream_WaitTrailers_Discard(t *testing.T) {
	// WaitTrailers called before body is drained. EventData is discarded
	// internally; trailers are still returned.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "X-Discard")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-to-discard"))
		w.Header().Set("X-Discard", "discarded")
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	// Call WaitTrailers immediately — it discards EventData internally.
	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		t.Fatalf("WaitTrailers: %v", err)
	}
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-discard") && string(f.Value) == "discarded" {
			found = true
		}
	}
	if !found {
		t.Fatalf("x-discard trailer not found in %v", trailers)
	}
}

func TestDo_RequestTrailers_FuncOnly(t *testing.T) {
	// TrailerFunc set, Trailers nil — TrailerFunc result is the sole source.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Func-Only")
		if got != "func-only-value" {
			t.Errorf("server: X-Func-Only = %q, want %q", got, "func-only-value")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method: "POST",
		Path:   "/",
		Body:   []byte("hello"),
		TrailerFunc: func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x-func-only"), Value: []byte("func-only-value")}}
		},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDo_RequestTrailers_WithStreamBody(t *testing.T) {
	// StreamBody=true + Trailers: request trailers still sent after streaming body.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Stream-Body")
		if got != "stream-trailer" {
			t.Errorf("server: X-Stream-Body = %q, want %q", got, "stream-trailer")
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:     "POST",
		Path:       "/",
		Body:       []byte("body"),
		StreamBody: true,
		Trailers:   []hpack.HeaderField{{Name: []byte("x-stream-body"), Value: []byte("stream-trailer")}},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	// With StreamBody=true, resp.BodyReader is set; drain and close it.
	if res.BodyReader != nil {
		_, _ = io.ReadAll(res.BodyReader)
		_ = res.BodyReader.Close()
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200", res.Status)
	}
}

func TestDoStream_WaitTrailers_Reuse(t *testing.T) {
	// StreamResponse reuse across two consecutive DoStream calls.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "X-Reuse")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data"))
		w.Header().Set("X-Reuse", "yes")
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var sr client.StreamResponse
	for i := 0; i < 2; i++ {
		if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr); err != nil {
			t.Fatalf("iter %d: DoStream: %v", i, err)
		}
		trailers, err := sr.WaitTrailers(ctx)
		if err != nil {
			t.Fatalf("iter %d: WaitTrailers: %v", i, err)
		}
		var found bool
		for _, f := range trailers {
			if strings.EqualFold(string(f.Name), "x-reuse") && string(f.Value) == "yes" {
				found = true
			}
		}
		if !found {
			t.Fatalf("iter %d: x-reuse trailer not found in %v", i, trailers)
		}
		_ = sr.Close() // must close before next DoStream reuse
	}
}

// TestConformance_RFC7540_Sec8_1_3_RequestTrailers verifies that the client
// sends request trailers as a HEADERS+END_STREAM frame after all DATA frames
// (RFC 7540 §8.1.3). Conformance is verified by the server successfully
// receiving the trailer field — which requires the correct wire sequence.
func TestConformance_RFC7540_Sec8_1_3_RequestTrailers(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Conformance")
		if got == "" {
			http.Error(w, "trailer missing — HEADERS+END_STREAM not received", 500)
			return
		}
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("conformance-body"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-conformance"), Value: []byte("rfc8.1.3")}},
	}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Status != 200 {
		t.Fatalf("status = %d, want 200; trailer HEADERS+END_STREAM not received by server", res.Status)
	}
}

func BenchmarkDo_WithTrailers(b *testing.B) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	b.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "https://")

	c, err := client.NewClient(client.ClientOptions{
		Addr: addr,
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })

	req := &client.Request{
		Method: "POST",
		Path:   "/",
		Body:   []byte("bench-body"),
		Trailers: []hpack.HeaderField{
			{Name: []byte("x-bench"), Value: []byte("value")},
		},
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var res client.Response
		if err := c.Do(ctx, req, &res); err != nil {
			b.Fatalf("Do: %v", err)
		}
		res.Reset()
	}
}
