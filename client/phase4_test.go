package client

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventNone_ZeroValue(t *testing.T) {
	var ev StreamEvent

	got := ev.Type

	assert.EqualValuesf(t, 0, EventNone,
		"EventNone = %d, want 0 — the zero value of StreamEvent.Type must mean 'no event'", EventNone)
	assert.Equalf(t, "none", EventNone.String(),
		"EventNone.String() = %q, want none", EventNone.String())
	assert.Equalf(t, EventNone, got,
		"zero StreamEvent.Type = %v, want EventNone — a freshly allocated event must not "+
			"read as a real one", got)
}

func TestClientRetryer_E2E(t *testing.T) {
	c, err := NewSingleConnClient(status204Server(t), insecureDialer())
	require.NoError(t, err, "NewSingleConnClient")
	defer c.Close()
	r := c.Retryer(RetryOptions{MaxAttempts: 3})
	require.NotNil(t, r, "Client.Retryer returned nil")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response
	err = r.Do(ctx, GET("/"), &resp)

	require.NoError(t, err, "Retryer.Do")
	assert.Equalf(t, 204, resp.Status, "status = %d, want 204", resp.Status)
}

func TestMetrics_StatusClassSplit(t *testing.T) {
	addr := h2TestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	})
	c, err := NewSingleConnClient(addr, insecureDialer())
	require.NoError(t, err, "NewSingleConnClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp Response
	resp.Reset()
	require.NoError(t, c.Do(ctx, GET("/"), &resp), "Do 2xx")
	resp.Reset()
	// non-2xx, still err==nil (idiomatic)
	require.NoError(t, c.Do(ctx, GET("/bad"), &resp), "Do non-2xx")

	snap := c.MetricsSnapshot()
	assert.EqualValuesf(t, 1, snap.Counters.Responses2xx,
		"Responses2xx = %d, want 1 — a load generator measuring real success rate needs the "+
			"status split, not just 'got a response'", snap.Counters.Responses2xx)
	assert.EqualValuesf(t, 1, snap.Counters.ResponsesNon2xx,
		"ResponsesNon2xx = %d, want 1 — a 503 must not be counted as a success",
		snap.Counters.ResponsesNon2xx)
	// The split must sum to RequestsSucceeded (both requests completed).
	assert.EqualValuesf(t, 2, snap.Counters.RequestsSucceeded,
		"RequestsSucceeded = %d, want 2 — Responses2xx + ResponsesNon2xx must equal it",
		snap.Counters.RequestsSucceeded)
}
