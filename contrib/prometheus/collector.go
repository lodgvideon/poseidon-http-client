package prometheus

import (
	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
)

// StatsSource is the slice of *client.Client that [Collector] reads. Taking
// an interface rather than the concrete type keeps the collector testable
// without a live connection, and lets callers wrap a client of their own.
//
// *client.Client satisfies it as-is.
type StatsSource interface {
	PoolStats() client.Stats
	MetricsSnapshot() client.MetricsSnapshot
}

// Collector publishes a client's pool state and aggregate counters. It
// implements [prom.Collector] and reads the client only when scraped, so
// it costs nothing on the request path.
//
// Pool gauges come from [client.Client.PoolStats]. That call is a
// round-trip to the pool's actor goroutine: cheap at scrape frequency,
// but do not put it in a hot loop. A single-connection transport, or a
// closed pool, reports the zero [client.Stats] — every pool gauge then
// reads 0.
type Collector struct {
	src StatsSource

	// pool gauges
	activeConns      *prom.Desc
	inFlightStreams  *prom.Desc
	waiters          *prom.Desc
	inFlightDials    *prom.Desc
	addresses        *prom.Desc
	drainingSubpools *prom.Desc

	// counters
	requestsStarted   *prom.Desc
	requestsSucceeded *prom.Desc
	requestsErrored   *prom.Desc
	responses         *prom.Desc
	retries           *prom.Desc
	dials             *prom.Desc
	dialsFailed       *prom.Desc
	connsClosed       *prom.Desc
	goAways           *prom.Desc

	// latency histograms
	requestDuration *prom.Desc
	dialDuration    *prom.Desc
	acquireDuration *prom.Desc
}

// NewCollector builds a [Collector] over c. Register it with a
// [prom.Registerer]:
//
//	reg.MustRegister(poseidonprom.NewCollector(c))
func NewCollector(c StatsSource, opts ...Option) *Collector {
	o := resolve(opts)
	name := func(n string) string { return prom.BuildFQName(o.namespace, "", n) }
	desc := func(n, help string, labels []string) *prom.Desc {
		return prom.NewDesc(name(n), help, labels, o.constLabels)
	}

	return &Collector{
		src: c,

		activeConns:      desc("pool_active_conns", "Live connections held by the pool.", nil),
		inFlightStreams:  desc("pool_inflight_streams", "Active streams summed across pooled connections. For HTTP/1.1 this is the number of checked-out connections, since one exchange occupies one connection.", nil),
		waiters:          desc("pool_waiters", "Acquires queued waiting for pool capacity.", nil),
		inFlightDials:    desc("pool_inflight_dials", "Dials currently in progress.", nil),
		addresses:        desc("pool_addresses", "Addresses in the resolved set. Managed transports only; 0 otherwise.", nil),
		drainingSubpools: desc("pool_draining_subpools", "Sub-pools draining after leaving the resolved set. Managed transports only; 0 otherwise.", nil),

		requestsStarted:   desc("requests_started_total", "Requests started.", nil),
		requestsSucceeded: desc("requests_succeeded_total", "Requests that received a response of any status.", nil),
		requestsErrored:   desc("requests_errored_total", "Requests that failed at the transport or protocol layer, with no response.", nil),
		responses:         desc("responses_total", "Responses received, split by status class.", []string{"class"}),
		retries:           desc("retries_total", "Retry attempts made.", nil),
		dials:             desc("dials_total", "Dials attempted.", nil),
		dialsFailed:       desc("dials_failed_total", "Dials that failed.", nil),
		connsClosed:       desc("conns_closed_total", "Connections closed or evicted, summed over all close reasons.", nil),
		goAways:           desc("goaways_received_total", "GOAWAY frames received from peers.", nil),

		requestDuration: desc("request_duration_seconds", "End-to-end request latency, republished from the client's log2 histogram. Bucket counts are exact; boundaries are a factor of 2 apart.", nil),
		dialDuration:    desc("dial_duration_seconds", "Dial latency, republished from the client's log2 histogram. Bucket counts are exact; boundaries are a factor of 2 apart.", nil),
		acquireDuration: desc("acquire_duration_seconds", "Time spent acquiring a connection or stream from the pool, republished from the client's log2 histogram. Bucket counts are exact; boundaries are a factor of 2 apart.", nil),
	}
}

// Describe implements [prom.Collector].
func (c *Collector) Describe(ch chan<- *prom.Desc) {
	for _, d := range c.descs() {
		ch <- d
	}
}

// descs lists every Desc the collector can emit, in a single place so
// Describe cannot drift from Collect.
func (c *Collector) descs() []*prom.Desc {
	return []*prom.Desc{
		c.activeConns, c.inFlightStreams, c.waiters, c.inFlightDials,
		c.addresses, c.drainingSubpools,
		c.requestsStarted, c.requestsSucceeded, c.requestsErrored,
		c.responses, c.retries, c.dials, c.dialsFailed, c.connsClosed, c.goAways,
		c.requestDuration, c.dialDuration, c.acquireDuration,
	}
}

// Collect implements [prom.Collector].
func (c *Collector) Collect(ch chan<- prom.Metric) {
	st := c.src.PoolStats()
	m := c.src.MetricsSnapshot()

	gauge := func(d *prom.Desc, v int) {
		send(ch, d, prom.GaugeValue, float64(v))
	}
	gauge(c.activeConns, st.ActiveConns)
	gauge(c.inFlightStreams, st.InFlightStreams)
	gauge(c.waiters, st.Waiters)
	gauge(c.inFlightDials, st.InFlightDials)
	gauge(c.addresses, st.Addresses)
	gauge(c.drainingSubpools, st.DrainingSubpools)

	counter := func(d *prom.Desc, v int64, labels ...string) {
		send(ch, d, prom.CounterValue, float64(v), labels...)
	}
	counter(c.requestsStarted, m.Counters.RequestsStarted)
	counter(c.requestsSucceeded, m.Counters.RequestsSucceeded)
	counter(c.requestsErrored, m.Counters.RequestsErrored)
	counter(c.responses, m.Counters.Responses2xx, "2xx")
	counter(c.responses, m.Counters.ResponsesNon2xx, "non2xx")
	counter(c.retries, m.Counters.Retries)
	counter(c.dials, m.Counters.DialsAttempted)
	counter(c.dialsFailed, m.Counters.DialsFailed)
	counter(c.connsClosed, m.Counters.ConnsClosed)
	counter(c.goAways, m.Counters.GoAwaysReceived)

	sendHistogram(ch, c.requestDuration, m.Latency.Request)
	sendHistogram(ch, c.dialDuration, m.Latency.Dial)
	sendHistogram(ch, c.acquireDuration, m.Latency.Acquire)
}

// send emits one const metric, degrading to an invalid metric rather than
// panicking. A panic here would happen inside the scrape goroutine and take
// the process down, which is never worth it for a metrics endpoint.
func send(ch chan<- prom.Metric, d *prom.Desc, t prom.ValueType, v float64, labels ...string) {
	m, err := prom.NewConstMetric(d, t, v, labels...)
	if err != nil {
		ch <- prom.NewInvalidMetric(d, err)
		return
	}
	ch <- m
}

// sendHistogram emits one const histogram converted from the client's
// log2 buckets.
func sendHistogram(ch chan<- prom.Metric, d *prom.Desc, s client.HistogramSnapshot) {
	count, sum, buckets := log2Histogram(s)
	m, err := prom.NewConstHistogram(d, count, sum, buckets)
	if err != nil {
		ch <- prom.NewInvalidMetric(d, err)
		return
	}
	ch <- m
}
