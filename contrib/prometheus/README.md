# poseidon-http-client — Prometheus exporter

Prometheus instrumentation for [`poseidon-http-client`](https://github.com/lodgvideon/poseidon-http-client).

This is a **separate Go module**. Importing it does not add a Prometheus
dependency to the poseidon core, and the core does not need to change for it
to work — it is built entirely on the public `Client.PoolStats()`,
`Client.MetricsSnapshot()` and `Client.SetHooks()` surface.

```bash
go get github.com/lodgvideon/poseidon-http-client/contrib/prometheus
```

## Two collectors

| | `Collector` | `HookMetrics` |
|---|---|---|
| Source | `PoolStats()` + `MetricsSnapshot()` | `client.Hooks` callbacks |
| Cost per request | none | one label lookup + atomic add |
| Labels | none (const labels only) | host, method, status, reason, … |
| Needs hooks installed | no | yes |

Use `Collector` alone if aggregate numbers are enough. Add `HookMetrics`
when you need per-host or per-status breakdown.

```go
c, err := client.NewH1PoolClient("api:8080", &conn.PlaintextDialer{},
    client.PoolOptions{MaxConnsPerHost: 32})

reg := prom.NewRegistry()
reg.MustRegister(poseidonprom.NewCollector(c))

// optional — per-request detail
hm := poseidonprom.NewHookMetrics()
reg.MustRegister(hm)
c.SetHooks(hm.Hooks())

http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

## Metrics

### `Collector` — pull only, read at scrape time

Pool gauges come from `Client.PoolStats()`. A single-connection transport,
or a closed pool, reports zero for all of them.

| Metric | Type | Notes |
|---|---|---|
| `poseidon_pool_active_conns` | gauge | live connections in the pool |
| `poseidon_pool_inflight_streams` | gauge | HTTP/1.1: checked-out connections, since one exchange occupies one connection |
| `poseidon_pool_waiters` | gauge | acquires queued at the cap |
| `poseidon_pool_inflight_dials` | gauge | dials in progress |
| `poseidon_pool_addresses` | gauge | managed transports only |
| `poseidon_pool_draining_subpools` | gauge | managed transports only |
| `poseidon_requests_started_total` | counter | |
| `poseidon_requests_succeeded_total` | counter | a response of any status arrived |
| `poseidon_requests_errored_total` | counter | transport/protocol failure, no response |
| `poseidon_responses_total{class}` | counter | `class` is `2xx` or `non2xx` |
| `poseidon_retries_total` | counter | |
| `poseidon_dials_total` | counter | |
| `poseidon_dials_failed_total` | counter | |
| `poseidon_conns_closed_total` | counter | all close reasons summed |
| `poseidon_goaways_received_total` | counter | |
| `poseidon_request_duration_seconds` | histogram | republished log2 — see precision below |
| `poseidon_dial_duration_seconds` | histogram | idem |
| `poseidon_acquire_duration_seconds` | histogram | idem; **no hook equivalent exists** |

### `HookMetrics` — recorded on the request path

| Metric | Type | Labels |
|---|---|---|
| `poseidon_http_requests_total` | counter | `host`, `method`, `status` |
| `poseidon_http_request_duration_seconds` | histogram | `host`, `method` |
| `poseidon_http_requests_in_flight` | gauge | `host`, `method` |
| `poseidon_http_request_body_bytes_total` | counter | `host`, `method` |
| `poseidon_http_response_body_bytes_total` | counter | `host`, `method` |
| `poseidon_http_retries_total` | counter | `method` |
| `poseidon_http_dials_total` | counter | `addr`, `outcome` |
| `poseidon_http_dial_duration_seconds` | histogram | `addr` |
| `poseidon_http_conns_closed_total` | counter | `addr`, `reason` |
| `poseidon_http_resolver_addresses` | gauge | — |
| `poseidon_http_resolver_changes_total` | counter | `op` |

## Things worth knowing

- **`status="error"`.** The client reports status `0` when a request fails
  with no response. That would read as a real status code, so it is
  labelled `error` instead.
- **No `path` label, anywhere.** A load generator walks an unbounded path
  space; a path label would blow up the series count. `host`, `method`,
  `status`, `reason` and `outcome` are all bounded.
- **`retries_total` has no `host` label** — `client.RetryEvent` does not
  carry an authority.
- **`response_body_bytes_total` stays at 0 for `DoStream`.** The client
  reports `BytesRecv: 0` on the streaming path; only `Do` counts received
  bytes.
- **`PoolStats()` is a round-trip to the pool's actor goroutine.** Cheap at
  scrape frequency, but do not put a scrape in a hot loop.
- **Hooks run synchronously on the request path.** The callbacks here are
  atomic adds; if you wrap them, keep your own code just as cheap and do no
  I/O.
- **`Collector` histogram precision.** The client stores latency in log2
  buckets, so `Collector` republishes boundaries a factor of 2 apart
  (≈1.02 µs to ≈34.36 s). The bucket *counts* are exact — the boundaries
  land on the log2 edges — but the spacing is too coarse for a latency SLO.
  For real quantiles use `HookMetrics`, which observes each request with
  configurable buckets (`WithDurationBuckets`).
- **Both collectors can be registered together.** They live in different
  name spaces (`poseidon_*` vs `poseidon_http_*`) precisely so they cannot
  collide.

## Versioning

The module is versioned independently of the parent, with the Go submodule
tag convention:

```bash
git tag contrib/prometheus/v0.1.0
git push origin contrib/prometheus/v0.1.0
```

`go get github.com/lodgvideon/poseidon-http-client/contrib/prometheus@v0.1.0`
resolves through that tag. Until such a tag exists, `go get` will only
resolve a pseudo-version from a commit hash.

The module requires `github.com/lodgvideon/poseidon-http-client v0.10.0` or
later — the release that added the HTTP/1.1 pool transports and their
`PoolStats()` support. A consumer that also imports the parent at a newer
version gets that newer version by minimal version selection.
