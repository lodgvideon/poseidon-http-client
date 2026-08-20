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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// --- Request trailer sending ---

func TestDo_RequestTrailers_Static(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // must drain body to receive trailers
		got := r.Trailer.Get("X-Checksum")
		assert.Equalf(t, "abc123", got, "server: X-Checksum = %q, want %q", got, "abc123")
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
		Trailers: []hpack.HeaderField{{Name: []byte("x-checksum"), Value: []byte("abc123")}},
	}, &res)
	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
}

func TestDo_RequestTrailers_Func(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Dynamic")
		assert.Equalf(t, "dynamic-value", got, "server: X-Dynamic = %q, want %q", got, "dynamic-value")
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
		Trailers: []hpack.HeaderField{{Name: []byte("x-static"), Value: []byte("ignored")}},
		TrailerFunc: func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x-dynamic"), Value: []byte("dynamic-value")}}
		},
	}, &res)
	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
}

func TestDo_RequestTrailers_FuncNilFallback(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Fallback")
		assert.Equalf(t, "fallback-value", got, "server: X-Fallback = %q, want %q", got, "fallback-value")
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:      "POST",
		Path:        "/",
		Body:        []byte("hello"),
		Trailers:    []hpack.HeaderField{{Name: []byte("x-fallback"), Value: []byte("fallback-value")}},
		TrailerFunc: func() []hpack.HeaderField { return nil }, // nil → fallback to Trailers
	}, &res)
	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
}

func TestDo_RequestTrailers_NoBody(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Nobod")
		assert.Equalf(t, "yes", got, "server: X-Nobod = %q, want %q", got, "yes")
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	// No Body, no BodyReader — wire: HEADERS(endStream=false) → HEADERS(trailers,END_STREAM)
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Trailers: []hpack.HeaderField{{Name: []byte("x-nobod"), Value: []byte("yes")}},
	}, &res)
	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
}

// TestDo_RequestTrailers_PseudoHeader pins that a pseudo-header in the static
// Trailers is refused.
//
// errors.Is(err, ErrInvalidRequest) alone does NOT pin it. resolveTrailers
// guards the trailer name twice — once for a leading ':' and once with
// isTokenName — and ':' is not a token character, so the token guard rejects a
// pseudo-header on its own and returns the same sentinel. Measured: deleting the
// pseudo-header guard by itself left this test green, deleting isTokenName by
// itself failed it, and deleting both failed it. The message check below is what
// names the mechanism that must do the rejecting.
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

	require.ErrorIsf(t, err, client.ErrInvalidRequest, "expected ErrInvalidRequest, got %v", err)
	require.ErrorContainsf(t, err, "pseudo-header",
		"the request was refused as %v, but not BY the pseudo-header guard — the token "+
			"check rejects a leading ':' too, so this assertion would survive that guard's "+
			"removal without naming which mechanism rejected it", err)
}

// TestDo_RequestTrailers_FuncPseudoHeader is the dynamic twin. Same two-guard
// problem as the static case above, so it names the mechanism the same way.
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

	require.ErrorIsf(t, err, client.ErrInvalidRequest, "expected ErrInvalidRequest, got %v", err)
	require.ErrorContainsf(t, err, "pseudo-header",
		"the request was refused as %v, but not BY the pseudo-header guard — the token "+
			"check rejects a leading ':' too", err)
}

// TestConformance_RFC7540_Sec8_1_2_6_FuncTrailerInjectionRejected pins that a
// dynamic trailer (TrailerFunc) carrying a non-token name, or a name or value
// with CR, LF or NUL, is refused before any HEADERS frame is written. Static
// Trailers are validated by validateRequest, but TrailerFunc output is dynamic
// and bypasses it; without a send-time check an injected name would ride the
// Trailer announcement on the initial HEADERS frame and the trailer HEADERS
// frame verbatim (RFC 7540 §8.1.2.6 makes such a message malformed; §10.3 is the
// downgrade-splitting vector). The static equivalents are already rejected.
func TestConformance_RFC7540_Sec8_1_2_6_FuncTrailerInjectionRejected(t *testing.T) {
	cases := []struct {
		name string
		tf   func() []hpack.HeaderField
	}{
		{"name CRLF", func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x\r\nEvil"), Value: []byte("v")}}
		}},
		{"name not a token (space)", func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x sum"), Value: []byte("v")}}
		}},
		{"name not a token (colon)", func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x:sum"), Value: []byte("v")}}
		}},
		{"value CRLF", func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x-sum"), Value: []byte("ok\r\nX-Injected: 1")}}
		}},
		{"value NUL", func() []hpack.HeaderField {
			return []hpack.HeaderField{{Name: []byte("x-sum"), Value: []byte("a\x00b")}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(200)
			}))
			c := trailerClientFor(t, addr)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var res client.Response
			err := c.Do(ctx, &client.Request{
				Method: "POST", Path: "/", Body: []byte("hello"), TrailerFunc: tc.tf,
			}, &res)
			require.ErrorIsf(t, err, client.ErrInvalidRequest, "expected ErrInvalidRequest, got %v", err)
		})
	}
}

func TestDoStream_RequestTrailers(t *testing.T) {
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Stream-Req")
		assert.Equalf(t, "stream-ok", got, "server: X-Stream-Req = %q, want %q", got, "stream-ok")
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	err := c.DoStream(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("hello"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-stream-req"), Value: []byte("stream-ok")}},
	}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	// Drain the response stream
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		require.NoError(t, err, "Recv")
		if ev.EndStream {
			break
		}
	}
	require.Equalf(t, 200, sr.Status, "status = %d, want 200", sr.Status)
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
	err := c.Do(ctx, &client.Request{
		Method: "GET", Path: "/",
		BodyMode: client.BodyBuffer, WantTrailers: true,
	}, &res)
	require.NoError(t, err, "Do")
	var found bool
	for _, f := range res.Trailers {
		if strings.EqualFold(string(f.Name), "x-checksum") && string(f.Value) == "abc123" {
			found = true
		}
	}
	require.Truef(t, found, "x-checksum trailer not found in %v", res.Trailers)
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
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	// Drain one EventData frame (DATA frame has endStream=false because trailers follow).
	ev, err := sr.Recv(ctx)
	require.NoError(t, err, "Recv")
	// Fast path: trailer arrived together with (or instead of) data on some runs.
	if ev.Type == client.EventTrailers {
		var found bool
		for _, f := range ev.Trailers {
			if strings.EqualFold(string(f.Name), "x-tag") && string(f.Value) == "after-drain" {
				found = true
			}
		}
		require.Truef(t, found,
			"x-tag trailer not found in first Recv result %v", ev.Trailers)
		return
	}
	require.Equalf(t, client.EventData, ev.Type, "expected EventData or EventTrailers, got %v", ev.Type)

	// WaitTrailers pumps Recv internally to get EventTrailers.
	trailers, err := sr.WaitTrailers(ctx)
	require.NoError(t, err, "WaitTrailers")
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-tag") && string(f.Value) == "after-drain" {
			found = true
		}
	}
	require.Truef(t, found, "x-tag trailer not found in %v", trailers)
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
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	// Recv EventData
	ev, err := sr.Recv(ctx)
	require.NoError(t, err, "Recv(data)")
	require.Equalf(t, client.EventData, ev.Type, "expected EventData, got %v", ev.Type)

	// Recv EventTrailers — this sets sr.trailers cache
	ev, err = sr.Recv(ctx)
	require.NoError(t, err, "Recv(trailers)")
	require.Equalf(t, client.EventTrailers, ev.Type, "expected EventTrailers, got %v", ev.Type)

	// WaitTrailers returns cached result without additional Recv calls.
	trailers, err := sr.WaitTrailers(ctx)
	require.NoError(t, err, "WaitTrailers")
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-cached") && string(f.Value) == "yes" {
			found = true
		}
	}
	require.Truef(t, found, "x-cached trailer not found in %v", trailers)
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
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	trailers, err := sr.WaitTrailers(ctx)
	require.NoError(t, err, "WaitTrailers")
	require.Nilf(t, trailers, "expected nil trailers, got %v", trailers)
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
	err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	// Call WaitTrailers immediately — it discards EventData internally.
	trailers, err := sr.WaitTrailers(ctx)
	require.NoError(t, err, "WaitTrailers")
	var found bool
	for _, f := range trailers {
		if strings.EqualFold(string(f.Name), "x-discard") && string(f.Value) == "discarded" {
			found = true
		}
	}
	require.Truef(t, found, "x-discard trailer not found in %v", trailers)
}

func TestDo_RequestTrailers_FuncOnly(t *testing.T) {
	// TrailerFunc set, Trailers nil — TrailerFunc result is the sole source.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Func-Only")
		assert.Equalf(t, "func-only-value", got, "server: X-Func-Only = %q, want %q", got, "func-only-value")
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
			return []hpack.HeaderField{{Name: []byte("x-func-only"), Value: []byte("func-only-value")}}
		},
	}, &res)
	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
}

func TestDo_RequestTrailers_WithBodyStream(t *testing.T) {
	// BodyStream=true + Trailers: request trailers still sent after streaming body.
	_, addr := newTrailerH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		got := r.Trailer.Get("X-Stream-Body")
		assert.Equalf(t, "stream-trailer", got, "server: X-Stream-Body = %q, want %q", got, "stream-trailer")
		w.WriteHeader(200)
	}))
	c := trailerClientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("body"),
		BodyMode: client.BodyStream,
		Trailers: []hpack.HeaderField{{Name: []byte("x-stream-body"), Value: []byte("stream-trailer")}},
	}, &res)
	require.NoError(t, err, "Do")
	// With BodyStream=true, resp.BodyReader is set; drain and close it.
	if res.BodyReader != nil {
		_, _ = io.ReadAll(res.BodyReader)
		_ = res.BodyReader.Close()
	}
	require.Equalf(t, 200, res.Status, "status = %d, want 200", res.Status)
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
		err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/", WantTrailers: true}, &sr)
		require.NoErrorf(t, err, "iter %d: DoStream", i)
		trailers, err := sr.WaitTrailers(ctx)
		require.NoErrorf(t, err, "iter %d: WaitTrailers", i)
		var found bool
		for _, f := range trailers {
			if strings.EqualFold(string(f.Name), "x-reuse") && string(f.Value) == "yes" {
				found = true
			}
		}
		require.Truef(t, found, "iter %d: x-reuse trailer not found in %v", i, trailers)
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
	err := c.Do(ctx, &client.Request{
		Method:   "POST",
		Path:     "/",
		Body:     []byte("conformance-body"),
		Trailers: []hpack.HeaderField{{Name: []byte("x-conformance"), Value: []byte("rfc8.1.3")}},
	}, &res)

	require.NoError(t, err, "Do")
	require.Equalf(t, 200, res.Status, "status = %d, want 200; trailer HEADERS+END_STREAM not received by server", res.Status)
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
