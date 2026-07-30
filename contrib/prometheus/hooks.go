package prometheus

import (
	"strconv"

	"github.com/lodgvideon/poseidon-http-client/client"
	prom "github.com/prometheus/client_golang/prometheus"
)

// StatusError is the value of the "status" label when a request failed at
// the transport or protocol layer and never produced a response. The client
// reports status 0 in that case, which would otherwise look like a real
// status code.
const StatusError = "error"

// HookMetrics records per-request series driven by [client.Hooks]. It
// implements [prom.Collector], so register it like any other collector and
// install its hooks on the client:
//
//	hm := poseidonprom.NewHookMetrics()
//	reg.MustRegister(hm)
//	c.SetHooks(hm.Hooks())
//
// Hooks fire synchronously on the request path. Every callback here is a
// label lookup plus an atomic add — cheap, but not free; if you do not need
// per-host or per-status detail, [Collector] alone costs nothing per request.
//
// # Labels
//
// Series are labelled by host, method, status, close reason and dial
// outcome — all bounded sets. Request path is deliberately never a label:
// a load generator walks an unbounded path space.
type HookMetrics struct {
	requestsTotal     *prom.CounterVec
	requestDuration   *prom.HistogramVec
	requestsInFlight  *prom.GaugeVec
	requestBodyBytes  *prom.CounterVec
	responseBodyBytes *prom.CounterVec
	retriesTotal      *prom.CounterVec
	dialsTotal        *prom.CounterVec
	dialDuration      *prom.HistogramVec
	connsClosedTotal  *prom.CounterVec
	resolverAddresses prom.Gauge
	resolverChanges   *prom.CounterVec
}

// NewHookMetrics builds the per-request collectors. Nothing is recorded
// until the hooks returned by [HookMetrics.Hooks] are installed on a client.
func NewHookMetrics(opts ...Option) *HookMetrics {
	o := resolve(opts)
	counterOpts := func(name, help string) prom.CounterOpts {
		return prom.CounterOpts{
			Namespace: o.namespace, Subsystem: HookSubsystem,
			Name: name, Help: help, ConstLabels: o.constLabels,
		}
	}
	histOpts := func(name, help string) prom.HistogramOpts {
		return prom.HistogramOpts{
			Namespace: o.namespace, Subsystem: HookSubsystem,
			Name: name, Help: help, ConstLabels: o.constLabels,
			Buckets: o.durationBuckets,
		}
	}

	return &HookMetrics{
		requestsTotal: prom.NewCounterVec(
			counterOpts("requests_total", `Requests completed, by response status. Status is "error" when the request failed with no response.`),
			[]string{"host", "method", "status"}),
		requestDuration: prom.NewHistogramVec(
			histOpts("request_duration_seconds", "End-to-end request latency, observed per request."),
			[]string{"host", "method"}),
		requestsInFlight: prom.NewGaugeVec(
			prom.GaugeOpts{
				Namespace: o.namespace, Subsystem: HookSubsystem,
				Name: "requests_in_flight", Help: "Requests started but not yet completed.",
				ConstLabels: o.constLabels,
			},
			[]string{"host", "method"}),
		requestBodyBytes: prom.NewCounterVec(
			counterOpts("request_body_bytes_total", "Request body bytes sent, excluding framing overhead."),
			[]string{"host", "method"}),
		responseBodyBytes: prom.NewCounterVec(
			counterOpts("response_body_bytes_total", "Response body bytes received, excluding framing overhead. Always 0 for DoStream, which does not report a received-byte count."),
			[]string{"host", "method"}),
		retriesTotal: prom.NewCounterVec(
			counterOpts("retries_total", "Retry attempts. Not labelled by host: the retry event does not carry an authority."),
			[]string{"method"}),
		dialsTotal: prom.NewCounterVec(
			counterOpts("dials_total", "Dials completed, by outcome."),
			[]string{"addr", "outcome"}),
		dialDuration: prom.NewHistogramVec(
			histOpts("dial_duration_seconds", "Dial latency, observed per dial."),
			[]string{"addr"}),
		connsClosedTotal: prom.NewCounterVec(
			counterOpts("conns_closed_total", "Connections closed or evicted, by reason."),
			[]string{"addr", "reason"}),
		resolverAddresses: prom.NewGauge(prom.GaugeOpts{
			Namespace: o.namespace, Subsystem: HookSubsystem,
			Name: "resolver_addresses", Help: "Addresses in the latest resolved set.",
			ConstLabels: o.constLabels,
		}),
		resolverChanges: prom.NewCounterVec(
			counterOpts("resolver_changes_total", "Addresses added to or removed from the resolved set."),
			[]string{"op"}),
	}
}

// collectors lists the embedded collectors in one place so Describe and
// Collect cannot drift apart.
func (h *HookMetrics) collectors() []prom.Collector {
	return []prom.Collector{
		h.requestsTotal, h.requestDuration, h.requestsInFlight,
		h.requestBodyBytes, h.responseBodyBytes, h.retriesTotal,
		h.dialsTotal, h.dialDuration, h.connsClosedTotal,
		h.resolverAddresses, h.resolverChanges,
	}
}

// Describe implements [prom.Collector].
func (h *HookMetrics) Describe(ch chan<- *prom.Desc) {
	for _, c := range h.collectors() {
		c.Describe(ch)
	}
}

// Collect implements [prom.Collector].
func (h *HookMetrics) Collect(ch chan<- prom.Metric) {
	for _, c := range h.collectors() {
		c.Collect(ch)
	}
}

// Hooks returns a fresh [client.Hooks] wired to this HookMetrics, ready for
// [client.Client.SetHooks].
//
// It sets every hook field. To keep hooks of your own, take the result and
// wrap the fields you need rather than replacing them:
//
//	hooks := hm.Hooks()
//	inner := hooks.OnRequestComplete
//	hooks.OnRequestComplete = func(e client.RequestCompleteEvent) {
//		inner(e)
//		myOwnLogging(e)
//	}
//	c.SetHooks(hooks)
func (h *HookMetrics) Hooks() *client.Hooks {
	return &client.Hooks{
		OnRequestStart:    h.onRequestStart,
		OnRequestComplete: h.onRequestComplete,
		OnRetry:           h.onRetry,
		OnDial:            h.onDial,
		OnConnClose:       h.onConnClose,
		OnResolverUpdate:  h.onResolverUpdate,
	}
}

func (h *HookMetrics) onRequestStart(e client.RequestStartEvent) {
	h.requestsInFlight.WithLabelValues(e.Authority, e.Method).Inc()
}

func (h *HookMetrics) onRequestComplete(e client.RequestCompleteEvent) {
	// The client pairs OnRequestStart with OnRequestComplete one-to-one on
	// both Do and DoStream, with no early return between them, so the
	// in-flight gauge cannot drift.
	h.requestsInFlight.WithLabelValues(e.Authority, e.Method).Dec()

	h.requestsTotal.WithLabelValues(e.Authority, e.Method, statusLabel(e)).Inc()
	h.requestDuration.WithLabelValues(e.Authority, e.Method).Observe(e.Latency.Seconds())
	if e.BytesSent > 0 {
		h.requestBodyBytes.WithLabelValues(e.Authority, e.Method).Add(float64(e.BytesSent))
	}
	if e.BytesRecv > 0 {
		h.responseBodyBytes.WithLabelValues(e.Authority, e.Method).Add(float64(e.BytesRecv))
	}
}

func (h *HookMetrics) onRetry(e client.RetryEvent) {
	h.retriesTotal.WithLabelValues(e.Method).Inc()
}

func (h *HookMetrics) onDial(e client.DialEvent) {
	outcome := "ok"
	if e.Err != nil {
		outcome = "error"
	}
	h.dialsTotal.WithLabelValues(e.Addr, outcome).Inc()
	h.dialDuration.WithLabelValues(e.Addr).Observe(e.Duration.Seconds())
}

func (h *HookMetrics) onConnClose(e client.ConnCloseEvent) {
	h.connsClosedTotal.WithLabelValues(e.Addr, e.Reason.String()).Inc()
}

func (h *HookMetrics) onResolverUpdate(e client.ResolverUpdateEvent) {
	h.resolverAddresses.Set(float64(e.Total))
	if n := len(e.Added); n > 0 {
		h.resolverChanges.WithLabelValues("added").Add(float64(n))
	}
	if n := len(e.Removed); n > 0 {
		h.resolverChanges.WithLabelValues("removed").Add(float64(n))
	}
}

// statusLabel maps a completion event to its "status" label value. The
// client sets Status only when Err is nil, so a failed request would
// otherwise be labelled status="0" and read as a real code.
func statusLabel(e client.RequestCompleteEvent) string {
	if e.Err != nil {
		return StatusError
	}
	return strconv.Itoa(e.Status)
}
