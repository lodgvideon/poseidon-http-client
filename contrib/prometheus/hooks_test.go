package prometheus

import (
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHookMetrics_HooksSetsEveryCallback(t *testing.T) {
	hm := NewHookMetrics()

	h := hm.Hooks()

	// assert, not require: nothing below dereferences these, so reporting
	// every unset callback in one run beats aborting on the first.
	assert.NotNil(t, h.OnRequestStart, "Hooks() must set OnRequestStart; an unset callback silently drops that series")
	assert.NotNil(t, h.OnRequestComplete, "Hooks() must set OnRequestComplete; an unset callback silently drops that series")
	assert.NotNil(t, h.OnRetry, "Hooks() must set OnRetry; an unset callback silently drops that series")
	assert.NotNil(t, h.OnDial, "Hooks() must set OnDial; an unset callback silently drops that series")
	assert.NotNil(t, h.OnConnClose, "Hooks() must set OnConnClose; an unset callback silently drops that series")
	assert.NotNil(t, h.OnResolverUpdate, "Hooks() must set OnResolverUpdate; an unset callback silently drops that series")
}

func TestHookMetrics_RequestLifecycle(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()
	host := map[string]string{"host": "api:443", "method": "GET"}

	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Path: "/a", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{
		Method: "GET", Path: "/a", Authority: "api:443",
		Status: 200, Latency: 50 * time.Millisecond,
		BytesSent: 10, BytesRecv: 2048,
	})

	f := gather(t, hm)
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_requests_total",
		map[string]string{"host": "api:443", "method": "GET", "status": "200"}),
		`requests_total{status="200"}: a completed request must be counted under its real status code`)
	assert.Equal(t, float64(0), labelledValue(t, f, "poseidon_http_requests_in_flight", host),
		"in_flight must return to 0 after completion; start and complete are paired one-to-one so the gauge cannot drift")
	assert.Equal(t, float64(10), labelledValue(t, f, "poseidon_http_request_body_bytes_total", host),
		"request body bytes must report BytesSent, excluding framing overhead")
	assert.Equal(t, float64(2048), labelledValue(t, f, "poseidon_http_response_body_bytes_total", host),
		"response body bytes must report BytesRecv, excluding framing overhead")

	fam := f["poseidon_http_request_duration_seconds"]
	require.NotNil(t, fam, "duration histogram not exposed")
	assert.Equal(t, uint64(1), fam.GetMetric()[0].GetHistogram().GetSampleCount(),
		"exactly one latency observation per completed request")
}

func TestHookMetrics_InFlightCountsOpenRequests(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()
	host := map[string]string{"host": "api:443", "method": "POST"}

	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})
	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})

	assert.Equal(t, float64(2), labelledValue(t, gather(t, hm), "poseidon_http_requests_in_flight", host),
		"in_flight must count every started-but-unfinished request, not just the latest")
}

func TestHookMetrics_InFlightFallsOnCompletion(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()
	host := map[string]string{"host": "api:443", "method": "POST"}
	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})
	h.OnRequestStart(client.RequestStartEvent{Method: "POST", Authority: "api:443"})

	h.OnRequestComplete(client.RequestCompleteEvent{Method: "POST", Authority: "api:443", Status: 204})

	assert.Equal(t, float64(1), labelledValue(t, gather(t, hm), "poseidon_http_requests_in_flight", host),
		"one completion must decrement in_flight by exactly one, leaving the still-open request counted")
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
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_requests_total",
		map[string]string{"host": "api:443", "method": "GET", "status": StatusError}),
		`requests_total{status="error"}: a request that never got a response must not be labelled status="0", which reads as a real code`)
	assert.Equal(t, float64(0), labelledValue(t, f, "poseidon_http_requests_in_flight",
		map[string]string{"host": "api:443", "method": "GET"}),
		"in_flight must be 0; a failed request must still decrement or the gauge leaks upward forever")
}

func TestHookMetrics_NonZeroStatusWithNoBodyBytes(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	// DoStream always reports BytesRecv == 0; the byte counters must simply
	// stay unset rather than creating a zero series.
	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{Method: "GET", Authority: "api:443", Status: 200})

	f := gather(t, hm)
	assert.NotContains(t, f, "poseidon_http_response_body_bytes_total",
		"a zero-byte report must not create a series; DoStream reports BytesRecv == 0 for every request and would otherwise fill the counter with meaningless zeroes")
}

func TestHookMetrics_Retry(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnRetry(client.RetryEvent{Method: "GET", Path: "/a", Attempt: 1, Backoff: time.Second})
	h.OnRetry(client.RetryEvent{Method: "GET", Path: "/b", Attempt: 2, Backoff: 2 * time.Second})

	f := gather(t, hm)
	assert.Equal(t, float64(2), labelledValue(t, f, "poseidon_http_retries_total", map[string]string{"method": "GET"}),
		"both paths must land in one series: retries are labelled by method only")
	for _, lp := range f["poseidon_http_retries_total"].GetMetric()[0].GetLabel() {
		assert.NotEqual(t, "path", lp.GetName(),
			"path must never become a label — a load generator walks an unbounded path space and would explode cardinality")
	}
}

func TestHookMetrics_DialOutcomes(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnDial(client.DialEvent{Addr: "10.0.0.1:443", Duration: 3 * time.Millisecond})
	h.OnDial(client.DialEvent{Addr: "10.0.0.1:443", Err: errors.New("refused"), Duration: time.Millisecond})

	f := gather(t, hm)
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_dials_total",
		map[string]string{"addr": "10.0.0.1:443", "outcome": "ok"}),
		`dials_total{outcome="ok"}: a dial with no error must be counted as ok`)
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_dials_total",
		map[string]string{"addr": "10.0.0.1:443", "outcome": "error"}),
		`dials_total{outcome="error"}: a failed dial must be split out, not folded into ok`)
	assert.Equal(t, uint64(2),
		f["poseidon_http_dial_duration_seconds"].GetMetric()[0].GetHistogram().GetSampleCount(),
		"both outcomes must be timed; a failed dial's latency is the interesting one")
}

func TestHookMetrics_ConnCloseReasonLabel(t *testing.T) {
	hm := NewHookMetrics()
	h := hm.Hooks()

	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseIdle})
	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseGoAway})
	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseGoAway})

	f := gather(t, hm)
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_conns_closed_total",
		map[string]string{"addr": "a:443", "reason": "idle"}),
		`conns_closed_total{reason="idle"}: close reasons must stay in separate series or the cause of churn is unreadable`)
	assert.Equal(t, float64(2), labelledValue(t, f, "poseidon_http_conns_closed_total",
		map[string]string{"addr": "a:443", "reason": "goaway"}),
		`conns_closed_total{reason="goaway"}: repeated closes for one reason must accumulate in that reason's series`)
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
	assert.Equal(t, float64(4), singleValue(t, f, "poseidon_http_resolver_addresses"),
		"resolver_addresses is a gauge of the latest resolved set size, not a delta")
	assert.Equal(t, float64(2), labelledValue(t, f, "poseidon_http_resolver_changes_total", map[string]string{"op": "added"}),
		`resolver_changes_total{op="added"} must count the Added entries, not the Removed ones`)
	assert.Equal(t, float64(1), labelledValue(t, f, "poseidon_http_resolver_changes_total", map[string]string{"op": "removed"}),
		`resolver_changes_total{op="removed"} must count the Removed entries, not the Added ones`)
}

// Collector and HookMetrics must be registerable side by side: their names
// live in different subsystems precisely so they cannot collide.
func TestHookMetrics_CoexistsWithCollector(t *testing.T) {
	reg := prom.NewPedanticRegistry()

	errCollector := reg.Register(NewCollector(&fakeSource{}))
	errHooks := reg.Register(NewHookMetrics())
	_, errGather := reg.Gather()

	require.NoError(t, errCollector, "register collector")
	require.NoError(t, errHooks,
		"HookMetrics must register alongside Collector; the http subsystem exists so the two name spaces cannot collide")
	require.NoError(t, errGather, "gather both collectors from one registry")
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
	require.Len(t, buckets, 2, "WithDurationBuckets must replace the default boundaries, not extend them")
	assert.Equal(t, uint64(0), buckets[0].GetCumulativeCount(),
		"le=0.001 must not count the 5ms observation")
	assert.Equal(t, uint64(1), buckets[1].GetCumulativeCount(),
		"le=0.01 must count the 5ms observation")
}

func TestHookMetrics_NamespaceApplies(t *testing.T) {
	hm := NewHookMetrics(WithNamespace("lg"))
	h := hm.Hooks()

	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseManual})

	f := gather(t, hm)
	assert.Containsf(t, f, "lg_http_conns_closed_total",
		"namespace not applied to the hook series; got %v", familyNames(f))
}
