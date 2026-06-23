package client

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestEventNone_ZeroValue(t *testing.T) {
	if EventNone != 0 {
		t.Errorf("EventNone = %d, want 0", EventNone)
	}
	if EventNone.String() != "none" {
		t.Errorf("EventNone.String() = %q, want none", EventNone.String())
	}
	var ev StreamEvent
	if ev.Type != EventNone {
		t.Errorf("zero StreamEvent.Type = %v, want EventNone", ev.Type)
	}
}

func TestClientRetryer_E2E(t *testing.T) {
	c, err := NewSingleConnClient(status204Server(t), insecureDialer())
	if err != nil {
		t.Fatalf("NewSingleConnClient: %v", err)
	}
	defer c.Close()

	r := c.Retryer(RetryOptions{MaxAttempts: 3})
	if r == nil {
		t.Fatal("Client.Retryer returned nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response
	if err := r.Do(ctx, GET("/"), &resp); err != nil {
		t.Fatalf("Retryer.Do: %v", err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
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
	if err != nil {
		t.Fatalf("NewSingleConnClient: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp Response
	resp.Reset()
	if err := c.Do(ctx, GET("/"), &resp); err != nil { // 2xx
		t.Fatalf("Do 2xx: %v", err)
	}
	resp.Reset()
	if err := c.Do(ctx, GET("/bad"), &resp); err != nil { // non-2xx, still err==nil (idiomatic)
		t.Fatalf("Do non-2xx: %v", err)
	}

	snap := c.MetricsSnapshot()
	if snap.Counters.Responses2xx != 1 {
		t.Errorf("Responses2xx = %d, want 1", snap.Counters.Responses2xx)
	}
	if snap.Counters.ResponsesNon2xx != 1 {
		t.Errorf("ResponsesNon2xx = %d, want 1", snap.Counters.ResponsesNon2xx)
	}
	// The split must sum to RequestsSucceeded (both requests completed).
	if snap.Counters.RequestsSucceeded != 2 {
		t.Errorf("RequestsSucceeded = %d, want 2", snap.Counters.RequestsSucceeded)
	}
}
