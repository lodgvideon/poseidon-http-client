package prometheus

import (
	"errors"
	"sync"
	"sync/atomic"
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

// TestStatusLabel_ErrTakesPrecedenceOverStatus fills in the off-diagonal of a
// two-condition decision table that only had its diagonal: (no error, real
// status) and (error, no status) were covered, (error, real status) and (no
// error, zero status) were not.
//
// The first of those is the one that matters. StatusError exists so a request
// that never produced a response is not filed under a status code, and nothing
// enforced the precedence — a client that sets Status before discovering the
// failure, or a caller building the event by hand, would have every failure
// counted as a successful 500.
func TestStatusLabel_ErrTakesPrecedenceOverStatus(t *testing.T) {
	boom := errors.New("connection reset")
	cases := []struct {
		name string
		e    client.RequestCompleteEvent
		want string
	}{
		{"no error, real status", client.RequestCompleteEvent{Status: 200}, "200"},
		{"error, no status", client.RequestCompleteEvent{Err: boom}, StatusError},
		{"error and a status", client.RequestCompleteEvent{Status: 500, Err: boom}, StatusError},
		{"no error, zero status", client.RequestCompleteEvent{}, "0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.e

			got := statusLabel(e)

			assert.Equalf(t, tc.want, got,
				"statusLabel(Status=%d, Err=%v) = %q, want %q: Err must win over any status that happens to be set, or a request that never got a response is counted as a real response and the error rate reads as zero",
				e.Status, e.Err, got, tc.want)
		})
	}
}

// TestHookMetrics_ConstLabelsReachEverySeries covers WithConstLabels on
// HookMetrics, which threads the labels through nine *Vec constructors plus one
// bare Gauge with nothing asserting any of it — TestCollector_NamespaceAndConstLabels
// covers the other collector, and TestHookMetrics_NamespaceApplies checks only
// the namespace, on one family.
//
// Every hook is driven first so that every family actually exists: a *Vec with
// no observation exposes no family at all, and a loop over "whatever was
// exposed" would pass vacuously. The length assertion is what says the loop saw
// all eleven.
func TestHookMetrics_ConstLabelsReachEverySeries(t *testing.T) {
	const label, value = "target", "api"
	hm := NewHookMetrics(WithConstLabels(prom.Labels{label: value}))
	h := hm.Hooks()

	h.OnRequestStart(client.RequestStartEvent{Method: "GET", Authority: "api:443"})
	h.OnRequestComplete(client.RequestCompleteEvent{
		Method: "GET", Authority: "api:443", Status: 200,
		Latency: time.Millisecond, BytesSent: 10, BytesRecv: 20,
	})
	h.OnRetry(client.RetryEvent{Method: "GET", Attempt: 1})
	h.OnDial(client.DialEvent{Addr: "10.0.0.1:443", Duration: time.Millisecond})
	h.OnConnClose(client.ConnCloseEvent{Addr: "10.0.0.1:443", Reason: client.CloseIdle})
	h.OnResolverUpdate(client.ResolverUpdateEvent{Added: []client.Address{{Host: "a", Port: 443}}, Total: 1})

	f := gather(t, hm)
	require.Lenf(t, f, 11,
		"exposed %d families, want all 11 (ten *Vec plus the bare resolver gauge); a family missing here means the loop below never looked at it and the test would pass without checking anything: %v",
		len(f), familyNames(f))
	for name, fam := range f {
		for _, m := range fam.GetMetric() {
			found := ""
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label {
					found = lp.GetValue()
				}
			}
			assert.Equalf(t, value, found,
				"%s carries %s=%q, want %q: const labels are how several clients in one process are told apart, and one collector missing them silently merges two clients' series — or collides at registration",
				name, label, found, value)
		}
	}
}

func TestHookMetrics_RegistrationCollidesUnlessConstLabelsDiffer(t *testing.T) {
	t.Run("two HookMetrics with the same names collide", func(t *testing.T) {
		reg := prom.NewPedanticRegistry()
		require.NoError(t, reg.Register(NewHookMetrics()), "the first HookMetrics must register")

		err := reg.Register(NewHookMetrics())

		var already prom.AlreadyRegisteredError
		assert.ErrorAsf(t, err, &already,
			"registering a second unlabelled HookMetrics returned %v, want AlreadyRegisteredError: HookMetrics has the same shape as Collector and must fail the same way", err)
	})

	t.Run("distinct const labels let both register", func(t *testing.T) {
		reg := prom.NewPedanticRegistry()
		require.NoError(t,
			reg.Register(NewHookMetrics(WithConstLabels(prom.Labels{"target": "api"}))),
			"the first labelled HookMetrics must register")

		err := reg.Register(NewHookMetrics(WithConstLabels(prom.Labels{"target": "auth"})))

		require.NoErrorf(t, err,
			"a second HookMetrics distinguished by a const label was rejected (%v); every one of its eleven collectors has to carry the labels for that to work", err)
	})
}

// TestHookMetrics_ConcurrentObservationsAreNotLost is the -race half of "hooks
// fire synchronously on the request path", which in a load generator is many
// goroutines at once, while a Prometheus scrape calls Collect from the HTTP
// handler's goroutine. Every other test in this file drives the hooks from the
// single test goroutine, so -race — already enabled in the contrib-prometheus
// CI job — had nothing to observe.
//
// Two properties at once: no data race, and no lost increments under
// contention. It is expected to pass as written, because the underlying
// prom.*Vec types are safe for concurrent use; it earns its place the moment
// someone adds bookkeeping of their own inside a hook body — a plain map cache
// keyed by authority, say, which is the obvious way to try to avoid the
// per-call WithLabelValues lookup.
//
// The one-goroutine arm is the control: it runs the identical assertions with
// the contention removed, so a failure that is really about the fixture rather
// than about concurrency shows up in both. Both arms report how many
// observations they actually performed and how many concurrent scrapes ran,
// because a run where the concurrency never happened passes exactly like a real
// one.
func TestHookMetrics_ConcurrentObservationsAreNotLost(t *testing.T) {
	const perGoroutine = 250
	const host, method, addr = "api:443", "GET", "10.0.0.1:443"

	for _, tc := range []struct {
		name       string
		goroutines int
	}{
		{"control: one goroutine", 1},
		{"contended: eight goroutines", 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hm := NewHookMetrics()
			h := hm.Hooks()
			reg := prom.NewPedanticRegistry()
			require.NoError(t, reg.Register(hm), "register the collector under test")
			var observed atomic.Int64
			type scrapeResult struct {
				n   int
				err error
			}
			done := make(chan struct{})
			scraping := make(chan struct{})
			scrapes := make(chan scrapeResult, 1)

			go func() {
				var r scrapeResult
				for {
					select {
					case <-done:
						scrapes <- r
						return
					default:
						if _, err := reg.Gather(); err != nil && r.err == nil {
							r.err = err
						}
						r.n++
						if r.n == 1 {
							close(scraping)
						}
					}
				}
			}()
			// Wait for the scrape loop to be provably running before starting
			// the hooks. Spawning the goroutine is not enough: without -race the
			// control arm finished all 250 rounds before the scheduler ran the
			// scraper once, and the overlap this test exists to create never
			// happened. Measured, and the reason the scrape count below is both
			// asserted and logged.
			<-scraping
			var wg sync.WaitGroup
			for range tc.goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range perGoroutine {
						h.OnRequestStart(client.RequestStartEvent{Method: method, Authority: host})
						h.OnRequestComplete(client.RequestCompleteEvent{
							Method: method, Authority: host, Status: 200, Latency: time.Millisecond,
						})
						h.OnDial(client.DialEvent{Addr: addr, Duration: time.Millisecond})
						h.OnConnClose(client.ConnCloseEvent{Addr: addr, Reason: client.CloseIdle})
						observed.Add(1)
					}
				}()
			}
			wg.Wait()
			close(done)
			scraped := <-scrapes

			want := float64(tc.goroutines * perGoroutine)
			t.Logf("%d goroutines x %d rounds = %d observations performed, alongside %d concurrent scrapes",
				tc.goroutines, perGoroutine, observed.Load(), scraped.n)
			require.Equalf(t, int64(tc.goroutines*perGoroutine), observed.Load(),
				"only %d of %d observation rounds ran; the assertions below would be measuring a scenario that did not happen",
				observed.Load(), tc.goroutines*perGoroutine)
			require.NotZerof(t, scraped.n,
				"the scrape goroutine gathered %d times; without at least one Gather overlapping the hooks, the Collect-during-request half of this test proves nothing", scraped.n)
			require.NoError(t, scraped.err,
				"a concurrent Gather failed; a scrape must never see a half-updated collector")
			f := gather(t, hm)
			assert.Equalf(t, want, labelledValue(t, f, "poseidon_http_requests_total",
				map[string]string{"host": host, "method": method, "status": "200"}),
				"requests_total must reach %v under %d concurrent hooks; a counter that drops writes under load is worse than no counter, because the graph still looks plausible", want, tc.goroutines)
			assert.Equalf(t, float64(0), labelledValue(t, f, "poseidon_http_requests_in_flight",
				map[string]string{"host": host, "method": method}),
				"in_flight did not return to 0 after %d paired start/complete rounds; a gauge that drifts under concurrency is the failure this pairing exists to prevent", tc.goroutines*perGoroutine)
			assert.Equalf(t, want, labelledValue(t, f, "poseidon_http_dials_total",
				map[string]string{"addr": addr, "outcome": "ok"}),
				"dials_total must reach %v under %d concurrent hooks; a lost dial makes the dial rate under-report exactly when it matters", want, tc.goroutines)
			assert.Equalf(t, want, labelledValue(t, f, "poseidon_http_conns_closed_total",
				map[string]string{"addr": addr, "reason": "idle"}),
				"conns_closed_total must reach %v under %d concurrent hooks; a lost close hides connection churn", want, tc.goroutines)
		})
	}
}

func TestHookMetrics_NamespaceApplies(t *testing.T) {
	hm := NewHookMetrics(WithNamespace("lg"))
	h := hm.Hooks()

	h.OnConnClose(client.ConnCloseEvent{Addr: "a:443", Reason: client.CloseManual})

	f := gather(t, hm)
	assert.Containsf(t, f, "lg_http_conns_closed_total",
		"namespace not applied to the hook series; got %v", familyNames(f))
}
