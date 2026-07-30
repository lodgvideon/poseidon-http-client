package prometheus

import (
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
)

func TestHookMetrics_HooksSetsEveryCallback(t *testing.T) {
	h := NewHookMetrics().Hooks()

	if h.OnRequestStart == nil || h.OnRequestComplete == nil || h.OnRetry == nil ||
		h.OnDial == nil || h.OnConnClose == nil || h.OnResolverUpdate == nil {
		t.Fatalf("Hooks() left a callback nil: %+v", h)
	}
}

func TestHookMetrics_RequestLifecycle(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Path: "/a", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{
		Method: "GET", Path: "/a", Authority: "api:443",
		Status: 200, Latency: 50 * time.Millisecond,
		BytesSent: 10, BytesRecv: 2048,
	})

	f := gather(t, hm)
	host := map[string]string{"host": "api:443", "method": "GET"}

	if got := labelledValue(t, f, "poseidon_http_requests_total",
		map[string]string{"host": "api:443", "method": "GET", "status": "200"}); got != 1 {
		t.Errorf("requests_total{status=200} = %v, want 1", got)
	}
	if got := labelledValue(t, f, "poseidon_http_requests_in_flight", host); got != 0 {
		t.Errorf("in_flight = %v, want 0 after completion", got)
	}
	if got := labelledValue(t, f, "poseidon_http_request_body_bytes_total", host); got != 10 {
		t.Errorf("request body bytes = %v, want 10", got)
	}
	if got := labelledValue(t, f, "poseidon_http_response_body_bytes_total", host); got != 2048 {
		t.Errorf("response body bytes = %v, want 2048", got)
	}

	fam := f["poseidon_http_request_duration_seconds"]
	if fam == nil {
		t.Fatalf("duration histogram not exposed")
	}
	if got := fam.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("duration observations = %d, want 1", got)
	}
}

func TestHookMetrics_InFlightRisesBetweenStartAndComplete(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()
	host := map[string]string{"host": "api:443", "method": "POST"}

	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})
	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})

	if got := labelledValue(t, gather(t, hm), "poseidon_http_requests_in_flight", host); got != 2 {
		t.Fatalf("in_flight = %v, want 2 while both are open", got)
	}

	h.OnRequestComplete(client.RequestCompleteEvent{Method: "POST", Authority: "api:443", Status: 204})

	if got := labelledValue(t, gather(t, hm), "poseidon_http_requests_in_flight", host); got != 1 {
		t.Errorf("in_flight = %v, want 1 after one completion", got)
	}
}

// A transport failure reports status 0, which would read as a real status
// code. It must be labelled "error" instead.
func TestHookMetrics_FailedRequestIsLabelledError(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{
		Method: "GET", Authority: "api:443",
		Status: 0, Err: errors.New("connection reset"),
	})

	f := gather(t, hm)
	if got := labelledValue(t, f, "poseidon_http_requests_total",
		map[string]string{"host": "api:443", "method": "GET", "status": StatusError}); got != 1 {
		t.Errorf(`requests_total{status="error"} = %v, want 1`, got)
	}
	if got := labelledValue(t, f, "poseidon_http_requests_in_flight",
		map[string]string{"host": "api:443", "method": "GET"}); got != 0 {
		t.Errorf("in_flight = %v, want 0; a failed request must still decrement", got)
	}
}

func TestHookMetrics_NonZeroStatusWithNoBodyBytes(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	// DoStream always reports BytesRecv == 0; the byte counters must simply
	// stay unset rather than creating a zero series.
	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{Method: "GET", Authority: "api:443", Status: 200})

	f := gather(t, hm)
	if _, ok := f["poseidon_http_response_body_bytes_total"]; ok {
		t.Errorf("response byte counter should not be created for a zero-byte report")
	}
}

func TestHookMetrics_Retry(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnRetry(client.RetryEvent{Method: "GET", Path: "/a", Attempt: 1, Backoff: time.Second})
	h.OnRetry(client.RetryEvent{Method: "GET", Path: "/b", Attempt: 2, Backoff: 2 * time.Second})

	// Path must never become a label — it is unbounded.
	f := gather(t, hm)
	if got := labelledValue(t, f, "poseidon_http_retries_total", map[string]string{"method": "GET"}); got != 2 {
		t.Errorf("retries_total = %v, want 2 (both paths in one series)", got)
	}
	for _, lp := range f["poseidon_http_retries_total"].GetMetric()[0].GetLabel() {
		if lp.GetName() == "path" {
			t.Errorf("path must not be a label")
		}
	}
}

func TestHookMetrics_DialOutcomes(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnDial(client.DialEvent{Addr: "10.0.0.1:443", Duration: 3 * time.Millisecond})
	h.OnDial(client.DialEvent{Addr: "10.0.0.1:443", Err: errors.New("refused"), Duration: time.Millisecond})

	f := gather(t, hm)
	if got := labelledValue(t, f, "poseidon_http_dials_total",
		map[string]string{"addr": "10.0.0.1:443", "outcome": "ok"}); got != 1 {
		t.Errorf("dials ok = %v, want 1", got)
	}
	if got := labelledValue(t, f, "poseidon_http_dials_total",
		map[string]string{"addr": "10.0.0.1:443", "outcome": "error"}); got != 1 {
		t.Errorf("dials error = %v, want 1", got)
	}
	if got := f["poseidon_http_dial_duration_seconds"].GetMetric()[0].GetHistogram().GetSampleCount(); got != 2 {
		t.Errorf("dial duration observations = %d, want 2 (both outcomes timed)", got)
	}
}

func TestHookMetrics_ConnCloseReasonLabel(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseIdle})
	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseGoAway})
	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseGoAway})

	f := gather(t, hm)
	if got := labelledValue(t, f, "poseidon_http_conns_closed_total",
		map[string]string{"addr": "a:443", "reason": "idle"}); got != 1 {
		t.Errorf("closed idle = %v, want 1", got)
	}
	if got := labelledValue(t, f, "poseidon_http_conns_closed_total",
		map[string]string{"addr": "a:443", "reason": "goaway"}); got != 2 {
		t.Errorf("closed goaway = %v, want 2", got)
	}
}

func TestHookMetrics_ResolverUpdate(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnResolverUpdate(client.ResolverUpdateEvent{
		Added:   []client.Address{{Host: "a", Port: 443}, {Host: "b", Port: 443}},
		Removed: []client.Address{{Host: "c", Port: 443}},
		Total:   4,
	})

	f := gather(t, hm)
	if got := singleValue(t, f, "poseidon_http_resolver_addresses"); got != 4 {
		t.Errorf("resolver addresses = %v, want 4", got)
	}
	if got := labelledValue(t, f, "poseidon_http_resolver_changes_total", map[string]string{"op": "added"}); got != 2 {
		t.Errorf("added = %v, want 2", got)
	}
	if got := labelledValue(t, f, "poseidon_http_resolver_changes_total", map[string]string{"op": "removed"}); got != 1 {
		t.Errorf("removed = %v, want 1", got)
	}
}

// Collector and HookMetrics must be registerable side by side: their names
// live in different subsystems precisely so they cannot collide.
func TestHookMetrics_CoexistsWithCollector(t *testing.T) {
	reg := prom.NewPedanticRegistry()

	if err := reg.Register(NewCollector(&fakeSource{})); err != nil {
		t.Fatalf("register collector: %v", err)
	}
	if err := reg.Register(NewHookMetrics()); err != nil {
		t.Fatalf("register hook metrics alongside collector: %v", err)
	}
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather both: %v", err)
	}
}

func TestHookMetrics_CustomDurationBuckets(t *testing.T) {
	hm := NewHookMetrics(WithDurationBuckets([]float64{0.001, 0.01}))
	h := hm.Hooks()

	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{
		Method: "GET", Authority: "api:443", Status: 200, Latency: 5 * time.Millisecond,
	})

	f := gather(t, hm)
	buckets := f["poseidon_http_request_duration_seconds"].GetMetric()[0].GetHistogram().GetBucket()
	if len(buckets) != 2 {
		t.Fatalf("bucket count = %d, want 2", len(buckets))
	}
	if got := buckets[0].GetCumulativeCount(); got != 0 {
		t.Errorf("le=0.001 count = %d, want 0 (5ms is slower)", got)
	}
	if got := buckets[1].GetCumulativeCount(); got != 1 {
		t.Errorf("le=0.01 count = %d, want 1", got)
	}
}

func TestHookMetrics_NamespaceApplies(t *testing.T) {
	hm := NewHookMetrics(WithNamespace("lg"))
	h := hm.Hooks()
	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseManual})

	f := gather(t, hm)
	if _, ok := f["lg_http_conns_closed_total"]; !ok {
		t.Errorf("namespace not applied; got %v", familyNames(f))
	}
}
