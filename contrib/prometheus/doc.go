// Package prometheus exports poseidon-http-client instrumentation to
// Prometheus.
//
// It is a separate Go module — importing it does not add a Prometheus
// dependency to the poseidon core:
//
//	go get github.com/lodgvideon/poseidon-http-client/contrib/prometheus
//
// # Two collectors
//
// The package offers two independent collectors. Use either or both.
//
// [Collector] is a pull-only mirror of what the client already tracks:
// pool gauges from [client.Client.PoolStats] and counters plus latency
// histograms from [client.Client.MetricsSnapshot]. It needs no hooks, adds
// no per-request cost, and carries no dynamic labels.
//
// [HookMetrics] is opt-in and installs [client.Hooks] to record per-request
// series labelled by host, method and status — detail the aggregate counters
// cannot express. Hooks run synchronously on the request path, so this trades
// a small per-request cost for that detail.
//
//	c, _ := client.NewH1PoolClient(...)
//
//	reg := prom.NewRegistry()
//	reg.MustRegister(poseidonprom.NewCollector(c))
//
//	hm := poseidonprom.NewHookMetrics()
//	reg.MustRegister(hm)
//	c.SetHooks(hm.Hooks())
//
// # Cardinality
//
// No metric is ever labelled by request path: a load generator walks an
// unbounded path space and would blow up the series count. Host, method,
// status and close-reason are all bounded, and are the labels used here.
//
// # Latency-histogram precision
//
// The client stores latency in log2 buckets: bucket i holds observations
// whose nanosecond duration satisfies floor(log2(ns)) == i. [Collector]
// republishes them as a Prometheus histogram whose bucket boundaries land
// exactly on the log2 edges, so every reported cumulative count is exact —
// but the boundaries are a factor of 2 apart, which is too coarse for a
// latency SLO. For real quantiles use [HookMetrics], whose
// request_duration_seconds histogram observes each request individually
// with configurable buckets ([WithDurationBuckets]).
package prometheus
