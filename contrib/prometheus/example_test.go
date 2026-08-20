package prometheus_test

import (
	"log"
	"net/http"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	poseidonprom "github.com/lodgvideon/poseidon-http-client/contrib/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Pool state alone — no per-request cost, no hooks. For an HTTP/1.1 pool
// this is what reports the live connection count, since HTTP/1.1 checks a
// connection out exclusively for each exchange.
func ExampleNewCollector() {
	c, err := client.NewH1PoolClient("127.0.0.1:8080", &conn.PlaintextDialer{},
		client.PoolOptions{MaxConnsPerHost: 32})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	reg := prom.NewRegistry()
	reg.MustRegister(poseidonprom.NewCollector(c))

	// A mux of this example's own. http.DefaultServeMux is process-global and
	// panics on a second registration of the same pattern, so two examples that
	// both reached for it would take each other down the moment either grew an
	// "// Output:" comment and started actually running.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
}

// Pool state plus per-request series labelled by host, method and status.
func ExampleNewHookMetrics() {
	c, err := client.NewH1PoolClient("127.0.0.1:8080", &conn.PlaintextDialer{},
		client.PoolOptions{MaxConnsPerHost: 32})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	reg := prom.NewRegistry()
	reg.MustRegister(poseidonprom.NewCollector(c))

	hm := poseidonprom.NewHookMetrics()
	reg.MustRegister(hm)
	c.SetHooks(hm.Hooks())

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
}

// Several clients in one process are told apart by a const label rather
// than by separate registries.
func ExampleWithConstLabels() {
	reg := prom.NewRegistry()

	for _, target := range []string{"api:8080", "auth:8080"} {
		c, err := client.NewH1PoolClient(target, &conn.PlaintextDialer{},
			client.PoolOptions{MaxConnsPerHost: 8})
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = c.Close() }()

		reg.MustRegister(poseidonprom.NewCollector(c,
			poseidonprom.WithConstLabels(prom.Labels{"target": target})))
	}
}

// Keeping hooks of your own alongside the metric hooks.
func ExampleHookMetrics_Hooks() {
	hm := poseidonprom.NewHookMetrics()

	hooks := hm.Hooks()
	record := hooks.OnRequestComplete
	hooks.OnRequestComplete = func(e client.RequestCompleteEvent) {
		record(e)
		if e.Err != nil {
			log.Printf("%s %s: %v", e.Method, e.Path, e.Err)
		}
	}

	c, err := client.NewH1PoolClient("127.0.0.1:8080", &conn.PlaintextDialer{},
		client.PoolOptions{MaxConnsPerHost: 4})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	c.SetHooks(hooks)
}
