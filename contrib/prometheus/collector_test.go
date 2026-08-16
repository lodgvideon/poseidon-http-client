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
type fakeSource struct {
	stats   client.Stats
	metrics client.MetricsSnapshot
	calls   int
}

func (f *fakeSource) PoolStats() client.Stats { f.calls++; return f.stats }

func (f *fakeSource) MetricsSnapshot() client.MetricsSnapshot { return f.metrics }

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

// A single-conn transport, or a closed pool, yields the zero Stats. The
// gauges must still be exposed, reading 0 — a disappearing series looks
// like a broken exporter.
func TestCollector_ZeroStatsStillExposesGauges(t *testing.T) {
	f := gather(t, NewCollector(&fakeSource{}))

	assert.Equal(t, float64(0), singleValue(t, f, "poseidon_pool_active_conns"),
		"active conns must still be exposed reading 0; a disappearing series looks like a broken exporter")
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
	first := src.calls

	gather(t, c)

	assert.Greaterf(t, src.calls, first,
		"PoolStats called %d times across two scrapes; values must not be cached or every scrape after the first reports stale pool state", src.calls)
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

func familyNames(f map[string]*dto.MetricFamily) []string {
	out := make([]string, 0, len(f))
	for n := range f {
		out = append(out, n)
	}
	return out
}
