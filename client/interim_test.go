package client_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// earlyHintsServer starts an h2 test server that emits `interim` 103 Early
// Hints responses (as Cloudflare/Fastly/Shopify do) before the final 200.
func earlyHintsServer(t *testing.T, interim int, body string) *client.Client {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Every header is set before the first WriteHeader, and none is touched
		// after. net/http's HTTP/2 server hands the header map to its write
		// goroutine when it emits a 1xx and encodes it there, so a handler that
		// mutates the map after a 1xx races with that encode. The race is in the
		// harness, not in the client under test — but it is real, and it is
		// flaky enough to pass -race by luck.
		w.Header().Add("Link", "</style.css>; rel=preload; as=style")
		w.Header().Set("Content-Type", "text/plain")
		for i := 0; i < interim; i++ {
			w.WriteHeader(http.StatusEarlyHints) // 103
		}
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
	require.NoError(t, err, "NewClient")
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

	require.NoError(t, err, "Do")
	assert.Equal(t, 200, resp.Status,
		"an interim 1xx was latched as the final status; RFC 7540 §8.1 makes 1xx "+
			"informational, so the caller must never see it as the answer")
	assert.Equal(t, "the actual body", string(resp.Body), "response body")
	// The real 200 header block must land in Headers, never in Trailers.
	for _, f := range resp.Trailers {
		n := string(f.Name)
		assert.Falsef(t, n == ":status" || n == "content-type",
			"final response header %q delivered as a trailer: trailers are HEADERS after "+
				"a FINAL status, so the 200 block is not one", n)
	}
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_DoStream pins the same §8.1
// rule on the DoStream path (StreamResponse.Recv).
func TestConformance_RFC7540_Sec8_1_InterimResponse_DoStream(t *testing.T) {
	c := earlyHintsServer(t, 1, "streamed body")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sr client.StreamResponse
	require.NoError(t, c.DoStream(ctx, &client.Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer func() { _ = sr.Close() }()

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

	assert.Equal(t, 200, sr.Status,
		"an interim 1xx was latched as the final status on the DoStream path")
	assert.Equal(t, "streamed body", string(body), "streamed response body")
}

// TestConformance_RFC7540_Sec8_1_InterimResponse_BodyReader pins the same §8.1
// rule on the Do+BodyStream path (Response.BodyReader, body.go).
func TestConformance_RFC7540_Sec8_1_InterimResponse_BodyReader(t *testing.T) {
	c := earlyHintsServer(t, 1, "reader body")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var resp client.Response
	resp.Reset()
	require.NoError(t,
		c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp),
		"Do")
	defer func() { _ = resp.BodyReader.Close() }()

	body, err := io.ReadAll(resp.BodyReader)

	assert.Equal(t, 200, resp.Status,
		"an interim 1xx was latched as the final status on the BodyReader path")
	require.NoError(t, err, "ReadAll")
	assert.Equal(t, "reader body", string(body), "body read through Response.BodyReader")
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

	assert.Errorf(t, err,
		"Do succeeded with Status=%d; an unbounded 1xx flood must be refused rather "+
			"than pumped forever — the peer controls how many it sends", resp.Status)
}
