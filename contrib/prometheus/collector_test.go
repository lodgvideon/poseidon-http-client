package prometheus

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A real *client.Client must satisfy StatsSource — that is the whole
// contract of this package. Compile-time, so it needs no connection.
var _ StatsSource = (*client.Client)(nil)

// fakeSource is a StatsSource returning canned values.
//
// Both calls are counted. Counting only PoolStats proved the no-caching
// property for one of the two things a scrape reads, and a collector that
// cached the metrics snapshot would have reported stale counters forever with
// the suite green.
type fakeSource struct {
	stats        client.Stats
	metrics      client.MetricsSnapshot
	calls        int
	metricsCalls int
}

func (f *fakeSource) PoolStats() client.Stats { f.calls++; return f.stats }

func (f *fakeSource) MetricsSnapshot() client.MetricsSnapshot {
	f.metricsCalls++
	return f.metrics
}

// gather registers c with a pedantic registry and returns the exposed
// families by name. The pedantic registry validates that every metric
// matches its Desc, so a label/Desc mismatch fails the test rather than
// silently producing a broken scrape.
func gather(t *testing.T, c prom.Collector) map[string]*dto.MetricFamily {
	t.Helper()

	reg := prom.NewPedanticRegistry()
	require.NoError(t, reg.Register(c), "register the collector under test")

	families, err := reg.Gather()
	require.NoError(t, err,
		"gather: a pedantic registry rejects any metric that does not match its Desc, so this is a broken scrape, not a flake")

	out := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		out[f.GetName()] = f
	}
	return out
}

// singleValue returns the gauge or counter value of a family's only series.
func singleValue(t *testing.T, families map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()

	f, ok := families[name]
	require.Truef(t, ok, "metric %q not exposed", name)
	require.Lenf(t, f.GetMetric(), 1, "metric %q must expose exactly one series", name)

	m := f.GetMetric()[0]
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	}
	require.FailNowf(t, "unexpected metric type",
		"metric %q is neither gauge nor counter", name)
	return 0
}

// labelledValue returns the counter value of the series carrying the given
// label values, matched as a subset.
func labelledValue(t *testing.T, families map[string]*dto.MetricFamily, name string, want map[string]string) float64 {
	t.Helper()

	f, ok := families[name]
	require.Truef(t, ok, "metric %q not exposed", name)

	for _, m := range f.GetMetric() {
		got := make(map[string]string, len(m.GetLabel()))
		for _, lp := range m.GetLabel() {
			got[lp.GetName()] = lp.GetValue()
		}
		matched := true
		for k, v := range want {
			if got[k] != v {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		switch {
		case m.GetCounter() != nil:
			return m.GetCounter().GetValue()
		case m.GetGauge() != nil:
			return m.GetGauge().GetValue()
		}
	}
	require.FailNowf(t, "no matching series",
		"metric %q has no series with labels %v", name, want)
	return 0
}

func TestCollector_PoolGauges(t *testing.T) {
	src := &fakeSource{stats: client.Stats{
		ActiveConns:      4,
		InFlightStreams:  3,
		Waiters:          2,
		InFlightDials:    1,
		Addresses:        5,
		DrainingSubpools: 6,
	}}
	want := []struct {
		name  string
		value float64
	}{
		{"poseidon_pool_active_conns", 4},
		{"poseidon_pool_inflight_streams", 3},
		{"poseidon_pool_waiters", 2},
		{"poseidon_pool_inflight_dials", 1},
		{"poseidon_pool_addresses", 5},
		{"poseidon_pool_draining_subpools", 6},
	}

	f := gather(t, NewCollector(src))

	for _, w := range want {
		assert.Equalf(t, w.value, singleValue(t, f, w.name),
			"%s must republish its own client.Stats field; a crossed wire here misreports pool health", w.name)
	}
}

// The HTTP/1.1 pool is the case the user-facing question turns on: its
// InFlightStreams counts checked-out connections, since one exchange
// occupies one connection exclusively.
func TestCollector_H1PoolActiveConnsExposed(t *testing.T) {
	src := &fakeSource{stats: client.Stats{ActiveConns: 8, InFlightStreams: 8}}

	f := gather(t, NewCollector(src))

	assert.Equal(t, float64(8), singleValue(t, f, "poseidon_pool_active_conns"),
		"active conns: an HTTP/1.1 pool must report its live connection count")
	assert.Equal(t, float64(8), singleValue(t, f, "poseidon_pool_inflight_streams"),
		"inflight streams: for HTTP/1.1 this equals the checked-out connection count, one exchange per connection")
}

// zeroScalarFamilies is every gauge and every unlabelled counter Collector
// publishes. responses_total is excluded because it carries two series under
// one family name and singleValue insists on exactly one.
var zeroScalarFamilies = []string{
	"poseidon_pool_active_conns",
	"poseidon_pool_inflight_streams",
	"poseidon_pool_waiters",
	"poseidon_pool_inflight_dials",
	"poseidon_pool_addresses",
	"poseidon_pool_draining_subpools",
	"poseidon_requests_started_total",
	"poseidon_requests_succeeded_total",
	"poseidon_requests_errored_total",
	"poseidon_retries_total",
	"poseidon_dials_total",
	"poseidon_dials_failed_total",
	"poseidon_conns_closed_total",
	"poseidon_goaways_received_total",
}

var zeroHistogramFamilies = []string{
	"poseidon_request_duration_seconds",
	"poseidon_dial_duration_seconds",
	"poseidon_acquire_duration_seconds",
}

// A single-conn transport, a closed pool, or a client scraped before its first
// request yields the zero Stats and the zero MetricsSnapshot. Every series must
// still be exposed reading 0.
//
// The stated property — "a disappearing series looks like a broken exporter" —
// applies to all eighteen families, not to the one gauge this used to check.
// Suppressing a zero counter or a zero-count histogram is the natural
// micro-optimisation to reach for and was invisible: the previous form asserted
// only poseidon_pool_active_conns, and Describe-vs-Collect drift is checked by
// containment, which an omitted metric satisfies.
func TestCollector_ZeroStatsStillExposesEverySeries(t *testing.T) {
	f := gather(t, NewCollector(&fakeSource{}))

	require.Lenf(t, f, len(zeroScalarFamilies)+len(zeroHistogramFamilies)+1,
		"a zero-state scrape exposed %d families, want every one of the eighteen; got %v", len(f), familyNames(f))
	for _, name := range zeroScalarFamilies {
		assert.Equalf(t, float64(0), singleValue(t, f, name),
			"%s must be exposed reading 0 before anything has happened; a series that only appears once it is non-zero reads to a dashboard as a broken exporter and to an alert as missing data", name)
	}
	for _, class := range []string{"2xx", "non2xx"} {
		assert.Equalf(t, float64(0),
			labelledValue(t, f, "poseidon_responses_total", map[string]string{"class": class}),
			`poseidon_responses_total{class=%q} must be exposed reading 0; both classes exist from the first scrape or a rate() over them starts from nothing`, class)
	}
	for _, name := range zeroHistogramFamilies {
		fam, ok := f[name]
		require.Truef(t, ok, "histogram %q not exposed at zero state", name)
		h := fam.GetMetric()[0].GetHistogram()
		require.NotNilf(t, h, "%q is not a histogram", name)
		assert.Equalf(t, uint64(0), h.GetSampleCount(),
			"%s must be exposed with a zero sample count rather than omitted", name)
		assert.Lenf(t, h.GetBucket(), MaxBucketExp-MinBucketExp+1,
			"%s must publish its full boundary window even with nothing observed", name)
	}
}

func TestCollector_Counters(t *testing.T) {
	var m client.MetricsSnapshot
	m.Counters = client.CountersSnapshot{
		RequestsStarted:   100,
		RequestsSucceeded: 90,
		RequestsErrored:   10,
		Responses2xx:      80,
		ResponsesNon2xx:   10,
		Retries:           7,
		DialsAttempted:    12,
		DialsFailed:       2,
		ConnsClosed:       3,
		GoAwaysReceived:   1,
	}
	src := &fakeSource{metrics: m}
	want := []struct {
		name  string
		value float64
	}{
		{"poseidon_requests_started_total", 100},
		{"poseidon_requests_succeeded_total", 90},
		{"poseidon_requests_errored_total", 10},
		{"poseidon_retries_total", 7},
		{"poseidon_dials_total", 12},
		{"poseidon_dials_failed_total", 2},
		{"poseidon_conns_closed_total", 3},
		{"poseidon_goaways_received_total", 1},
	}

	f := gather(t, NewCollector(src))

	for _, w := range want {
		assert.Equalf(t, w.value, singleValue(t, f, w.name),
			"%s must republish its own CountersSnapshot field", w.name)
	}
	assert.Equal(t, float64(80),
		labelledValue(t, f, "poseidon_responses_total", map[string]string{"class": "2xx"}),
		`responses_total{class="2xx"} must carry Responses2xx, not the other class`)
	assert.Equal(t, float64(10),
		labelledValue(t, f, "poseidon_responses_total", map[string]string{"class": "non2xx"}),
		`responses_total{class="non2xx"} must carry ResponsesNon2xx, not the other class`)
}

func TestCollector_LatencyHistograms(t *testing.T) {
	var m client.MetricsSnapshot
	m.Latency.Request = snapshotWith(map[int]int64{20: 4})
	m.Latency.Request.Sum = 4_000_000
	m.Latency.Dial = snapshotWith(map[int]int64{25: 2})
	m.Latency.Acquire = snapshotWith(map[int]int64{15: 9})
	want := []struct {
		name  string
		count uint64
	}{
		{"poseidon_request_duration_seconds", 4},
		{"poseidon_dial_duration_seconds", 2},
		{"poseidon_acquire_duration_seconds", 9},
	}

	f := gather(t, NewCollector(&fakeSource{metrics: m}))

	for _, w := range want {
		fam, ok := f[w.name]
		require.Truef(t, ok, "metric %q not exposed", w.name)
		h := fam.GetMetric()[0].GetHistogram()
		require.NotNilf(t, h, "metric %q is not a histogram", w.name)
		assert.Equalf(t, w.count, h.GetSampleCount(),
			"%s count must equal the client snapshot's observation count exactly", w.name)
		assert.Lenf(t, h.GetBucket(), MaxBucketExp-MinBucketExp+1,
			"%s must publish exactly the [MinBucketExp, MaxBucketExp] window", w.name)
	}
}

func TestCollector_DescribeCoversEveryCollectedMetric(t *testing.T) {
	c := NewCollector(&fakeSource{})
	described := make(chan *prom.Desc, 64)
	c.Describe(described)
	close(described)
	seen := make(map[string]bool)
	for d := range described {
		seen[d.String()] = true
	}

	collected := make(chan prom.Metric, 64)
	c.Collect(collected)
	close(collected)

	for m := range collected {
		assert.Truef(t, seen[m.Desc().String()],
			"Collect emitted an undescribed metric (%s); Describe and Collect have drifted apart and a registry will reject the scrape", m.Desc())
	}
}

func TestCollector_ScrapeReadsTheClientEachTime(t *testing.T) {
	src := &fakeSource{}
	c := NewCollector(src)
	gather(t, c)
	firstPool, firstMetrics := src.calls, src.metricsCalls

	gather(t, c)

	assert.Greaterf(t, src.calls, firstPool,
		"PoolStats called %d times across two scrapes; values must not be cached or every scrape after the first reports stale pool state", src.calls)
	assert.Greaterf(t, src.metricsCalls, firstMetrics,
		"MetricsSnapshot called %d times across two scrapes; a scrape reads two things from the client and caching either one republishes numbers that stopped moving", src.metricsCalls)
}

func TestCollector_NamespaceAndConstLabels(t *testing.T) {
	c := NewCollector(&fakeSource{},
		WithNamespace("lg"),
		WithConstLabels(prom.Labels{"target": "api"}))

	f := gather(t, c)

	_, ok := f["lg_pool_active_conns"]
	require.Truef(t, ok, "namespace not applied; got families %v", familyNames(f))
	assert.Equal(t, float64(0),
		labelledValue(t, f, "lg_pool_active_conns", map[string]string{"target": "api"}),
		"const label missing or attached to the wrong series; const labels are how several clients in one process are told apart")
}

func TestCollector_EmptyNamespaceLeavesNamesUnprefixed(t *testing.T) {
	f := gather(t, NewCollector(&fakeSource{}, WithNamespace("")))

	_, ok := f["pool_active_conns"]
	assert.Truef(t, ok,
		"an empty namespace must leave metric names unprefixed; got families %v", familyNames(f))
}

// emitted takes the one metric a send is expected to have queued, without
// blocking. Collect owes the registry exactly one metric per Desc; a send that
// queues none stalls the scrape, and asserting that with a bare receive would
// report it as a test timeout rather than as this property failing.
func emitted(t *testing.T, ch <-chan prom.Metric) prom.Metric {
	t.Helper()

	select {
	case m := <-ch:
		require.NotNil(t, m, "a nil metric reaches the registry as a nil-pointer dereference in the scrape goroutine")
		return m
	default:
		require.FailNow(t, "nothing was emitted",
			"every Desc must yield exactly one metric even when building it fails; a Collect that silently skips one leaves the registry waiting for a metric that never arrives")
		return nil
	}
}

// TestSend_LabelMismatchDegradesToInvalidMetric executes the branch the
// collector's own Descs can never reach: they are internally consistent, so
// every existing test takes the happy path and the deliberate degrade-rather-
// than-panic decision was documented and unexecuted.
//
// It is not cosmetic. NewConstMetric's error is returned, not panicked, only
// because send checks it; a panic here happens inside the scrape goroutine of
// whatever HTTP handler is serving /metrics and takes the process down. A
// registry turns the invalid metric into a gather error instead.
func TestSend_LabelMismatchDegradesToInvalidMetric(t *testing.T) {
	ch := make(chan prom.Metric, 1)
	d := prom.NewDesc("degrade_send", "a Desc declaring one label", []string{"needed"}, nil)

	send(ch, d, prom.GaugeValue, 1) // no value supplied for "needed"

	// Non-blocking: send is synchronous and the channel is buffered, so
	// whatever it emitted is already there. A bare receive would turn "emitted
	// nothing" into a ten-minute hang and a nameless timeout instead of a
	// failure that says which property broke.
	m := emitted(t, ch)
	assert.Error(t, m.Write(nil),
		"a label/Desc mismatch must surface as an invalid metric the registry rejects, never as a panic in the scrape goroutine; a metrics endpoint is never worth the process")
}

func TestSendHistogram_LabelMismatchDegradesToInvalidMetric(t *testing.T) {
	ch := make(chan prom.Metric, 1)
	d := prom.NewDesc("degrade_histogram", "a Desc declaring one label", []string{"needed"}, nil)

	sendHistogram(ch, d, client.HistogramSnapshot{}) // no value supplied for "needed"

	m := emitted(t, ch)
	assert.Error(t, m.Write(nil),
		"NewConstHistogram has its own error path and it must degrade the same way send does; the two are reached by the same broken Desc and must not disagree about whether that is fatal")
}

// TestCollector_RegistrationCollidesUnlessConstLabelsDiffer pins both
// directions of the decision a second client in one process runs into. The
// positive half is the contract ExampleWithConstLabels documents — "several
// clients in one process are told apart by a const label rather than by
// separate registries" — and had no test at all; the negative half is the error
// a user hits first, at startup, with a message that does not name the fix.
//
// A one-sided test here is satisfied by a registry that always says yes.
func TestCollector_RegistrationCollidesUnlessConstLabelsDiffer(t *testing.T) {
	t.Run("two collectors with the same names collide", func(t *testing.T) {
		reg := prom.NewPedanticRegistry()
		require.NoError(t, reg.Register(NewCollector(&fakeSource{})), "the first collector must register")

		err := reg.Register(NewCollector(&fakeSource{}))

		var already prom.AlreadyRegisteredError
		assert.ErrorAsf(t, err, &already,
			"registering a second unlabelled collector returned %v, want AlreadyRegisteredError: a caller has to be able to classify this without matching the message text", err)
	})

	t.Run("distinct const labels let both register", func(t *testing.T) {
		reg := prom.NewPedanticRegistry()
		require.NoError(t,
			reg.Register(NewCollector(&fakeSource{}, WithConstLabels(prom.Labels{"target": "api"}))),
			"the first labelled collector must register")

		err := reg.Register(NewCollector(&fakeSource{}, WithConstLabels(prom.Labels{"target": "auth"})))

		require.NoErrorf(t, err,
			"a second collector distinguished by a const label was rejected (%v); that is the whole reason WithConstLabels exists, and it only works if EVERY Desc carries the labels — one that does not collides on its own", err)
		_, gatherErr := reg.Gather()
		assert.NoError(t, gatherErr,
			"two labelled collectors registered but did not gather; a scrape that fails after a clean startup is worse than a startup that refuses")
	})

	t.Run("the same collector instance twice collides", func(t *testing.T) {
		reg := prom.NewPedanticRegistry()
		c := NewCollector(&fakeSource{})
		require.NoError(t, reg.Register(c), "the first registration must succeed")

		err := reg.Register(c)

		var already prom.AlreadyRegisteredError
		assert.ErrorAsf(t, err, &already,
			"registering the same instance twice returned %v, want AlreadyRegisteredError: double registration in an init path must fail loudly rather than double-count", err)
	})
}

func familyNames(f map[string]*dto.MetricFamily) []string {
	out := make([]string, 0, len(f))
	for n := range f {
		out = append(out, n)
	}
	return out
}
