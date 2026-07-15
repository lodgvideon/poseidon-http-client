package client_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// earlyHintsServer starts an h2 test server that emits `interim` 103 Early
// Hints responses (as Cloudflare/Fastly/Shopify do) before the final 200.
func earlyHintsServer(t *testing.T, interim int, body string) *client.Client {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < interim; i++ {
			w.Header().Add("Link", "</style.css>; rel=preload; as=style")
			w.WriteHeader(http.StatusEarlyHints) // 103
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c, err := client.NewClient(client.ClientOptions{
		Addr: srv.Listener.Addr().String(),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   1,
			MaxStreamsPerConn: 0,
			HealthCheckPeriod: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_FinalStatusWins pins that a
// 103 Early Hints response preceding the final 200 does not become the
// caller-visible status, and that the final response headers are not
// misdelivered as a trailer section. RFC 7540 §8.1: trailers are HEADERS
// after a *final* (non-informational) status.
func TestConformance_RFC7540_Sec8_1_InterimResponse_FinalStatusWins(t *testing.T) {
	c := earlyHintsServer(t, 1, "the actual body")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET", Path: "/", BodyMode: client.BodyBuffer, WantTrailers: true,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200 (interim 1xx latched as final)", resp.Status)
	}
	if got := string(resp.Body); got != "the actual body" {
		t.Errorf("Body = %q, want %q", got, "the actual body")
	}
	// The real 200 header block must land in Headers, never in Trailers.
	for _, f := range resp.Trailers {
		if n := string(f.Name); n == ":status" || n == "content-type" {
			t.Errorf("final response header %q delivered as a trailer", n)
		}
	}
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_DoStream pins the same §8.1
// rule on the DoStream path (StreamResponse.Recv).
func TestConformance_RFC7540_Sec8_1_InterimResponse_DoStream(t *testing.T) {
	c := earlyHintsServer(t, 1, "streamed body")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var sr client.StreamResponse
	if err := c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer func() { _ = sr.Close() }()

	if sr.Status != 200 {
		t.Errorf("Status = %d, want 200 (interim 1xx latched as final)", sr.Status)
	}
	var body []byte
	for {
		ev, err := sr.Recv(ctx)
		if err != nil {
			break
		}
		if ev.Type == client.EventData {
			body = append(body, ev.Data...)
		}
		if ev.EndStream {
			break
		}
	}
	if string(body) != "streamed body" {
		t.Errorf("Body = %q, want %q", body, "streamed body")
	}
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_BodyReader pins the same §8.1
// rule on the Do+BodyStream path (Response.BodyReader, body.go).
func TestConformance_RFC7540_Sec8_1_InterimResponse_BodyReader(t *testing.T) {
	c := earlyHintsServer(t, 1, "reader body")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{
		Method: "GET", Path: "/", BodyMode: client.BodyStream,
	}, &resp)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.BodyReader.Close() }()

	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200 (interim 1xx latched as final)", resp.Status)
	}
	body, err := io.ReadAll(resp.BodyReader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(body) != "reader body" {
		t.Errorf("Body = %q, want %q", body, "reader body")
	}
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_Flood pins that an unbounded
// stream of 1xx informational responses is rejected rather than pumped
// forever. Bound matches http1/http3 maxInterimResponses (100).
func TestConformance_RFC7540_Sec8_1_InterimResponse_Flood(t *testing.T) {
	c := earlyHintsServer(t, 150, "never reached")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyBuffer}, &resp)
	if err == nil {
		t.Fatalf("Do succeeded with Status=%d; want an error: a 1xx flood must be bounded", resp.Status)
	}
}
