package prometheus

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
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
	if !ok {
		t.Fatalf("metric %q not exposed", name)
	}
	if n := len(f.GetMetric()); n != 1 {
		t.Fatalf("metric %q has %d series, want 1", name, n)
	}
	m := f.GetMetric()[0]
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	}
	t.Fatalf("metric %q is neither gauge nor counter", name)
	return 0
}

// labelledValue returns the counter value of the series carrying the given
// label values, matched as a subset.
func labelledValue(t *testing.T, families map[string]*dto.MetricFamily, name string, want map[string]string) float64 {
	t.Helper()
	f, ok := families[name]
	if !ok {
		t.Fatalf("metric %q not exposed", name)
	}
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
	t.Fatalf("metric %q has no series with labels %v", name, want)
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

	f := gather(t, NewCollector(src))

	for name, want := range map[string]float64{
		"poseidon_pool_active_conns":      4,
		"poseidon_pool_inflight_streams":  3,
		"poseidon_pool_waiters":           2,
		"poseidon_pool_inflight_dials":    1,
		"poseidon_pool_addresses":         5,
		"poseidon_pool_draining_subpools": 6,
	} {
		if got := singleValue(t, f, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// The HTTP/1.1 pool is the case the user-facing question turns on: its
// InFlightStreams counts checked-out connections, since one exchange
// occupies one connection exclusively.
func TestCollector_H1PoolActiveConnsExposed(t *testing.T) {
	src := &fakeSource{stats: client.Stats{ActiveConns: 8, InFlightStreams: 8}}

	f := gather(t, NewCollector(src))

	if got := singleValue(t, f, "poseidon_pool_active_conns"); got != 8 {
		t.Errorf("active conns = %v, want 8", got)
	}
	if got := singleValue(t, f, "poseidon_pool_inflight_streams"); got != 8 {
		t.Errorf("inflight streams = %v, want 8", got)
	}
}

// A single-conn transport, or a closed pool, yields the zero Stats. The
// gauges must still be exposed, reading 0 — a disappearing series looks
// like a broken exporter.
func TestCollector_ZeroStatsStillExposesGauges(t *testing.T) {
	f := gather(t, NewCollector(&fakeSource{}))

	if got := singleValue(t, f, "poseidon_pool_active_conns"); got != 0 {
		t.Errorf("active conns = %v, want 0", got)
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

	f := gather(t, NewCollector(src))

	for name, want := range map[string]float64{
		"poseidon_requests_started_total":   100,
		"poseidon_requests_succeeded_total": 90,
		"poseidon_requests_errored_total":   10,
		"poseidon_retries_total":            7,
		"poseidon_dials_total":              12,
		"poseidon_dials_failed_total":       2,
		"poseidon_conns_closed_total":       3,
		"poseidon_goaways_received_total":   1,
	} {
		if got := singleValue(t, f, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}

	if got := labelledValue(t, f, "poseidon_responses_total", map[string]string{"class": "2xx"}); got != 80 {
		t.Errorf("responses 2xx = %v, want 80", got)
	}
	if got := labelledValue(t, f, "poseidon_responses_total", map[string]string{"class": "non2xx"}); got != 10 {
		t.Errorf("responses non2xx = %v, want 10", got)
	}
}

func TestCollector_LatencyHistograms(t *testing.T) {
	var m client.MetricsSnapshot
	m.Latency.Request = snapshotWith(map[int]int64{20: 4})
	m.Latency.Request.Sum = 4_000_000
	m.Latency.Dial = snapshotWith(map[int]int64{25: 2})
	m.Latency.Acquire = snapshotWith(map[int]int64{15: 9})

	f := gather(t, NewCollector(&fakeSource{metrics: m}))

	for name, want := range map[string]uint64{
		"poseidon_request_duration_seconds": 4,
		"poseidon_dial_duration_seconds":    2,
		"poseidon_acquire_duration_seconds": 9,
	} {
		fam, ok := f[name]
		if !ok {
			t.Fatalf("metric %q not exposed", name)
		}
		h := fam.GetMetric()[0].GetHistogram()
		if h == nil {
			t.Fatalf("metric %q is not a histogram", name)
		}
		if got := h.GetSampleCount(); got != want {
			t.Errorf("%s count = %d, want %d", name, got, want)
		}
		if got := len(h.GetBucket()); got != MaxBucketExp-MinBucketExp+1 {
			t.Errorf("%s bucket count = %d, want %d", name, got, MaxBucketExp-MinBucketExp+1)
		}
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
		if !seen[m.Desc().String()] {
			t.Errorf("Collect emitted an undescribed metric: %s", m.Desc())
		}
	}
}

func TestCollector_ScrapeReadsTheClientEachTime(t *testing.T) {
	src := &fakeSource{}
	c := NewCollector(src)

	gather(t, c)
	first := src.calls
	gather(t, c)

	if src.calls <= first {
		t.Errorf("PoolStats called %d times across two scrapes; values must not be cached", src.calls)
	}
}

func TestCollector_NamespaceAndConstLabels(t *testing.T) {
	c := NewCollector(&fakeSource{},
		WithNamespace("lg"),
		WithConstLabels(prom.Labels{"target": "api"}))

	f := gather(t, c)

	if _, ok := f["lg_pool_active_conns"]; !ok {
		t.Fatalf("namespace not applied; got families %v", familyNames(f))
	}
	if got := labelledValue(t, f, "lg_pool_active_conns", map[string]string{"target": "api"}); got != 0 {
		t.Errorf("const label missing or value wrong: %v", got)
	}
}

func TestCollector_EmptyNamespaceLeavesNamesUnprefixed(t *testing.T) {
	f := gather(t, NewCollector(&fakeSource{}, WithNamespace("")))

	if _, ok := f["pool_active_conns"]; !ok {
		t.Errorf("expected unprefixed name; got families %v", familyNames(f))
	}
}

func familyNames(f map[string]*dto.MetricFamily) []string {
	out := make([]string, 0, len(f))
	for n := range f {
		out = append(out, n)
	}
	return out
}
