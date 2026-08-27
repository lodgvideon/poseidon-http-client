# Connection pooling lives outside `client`

The connection pool, the resolver/selector vocabulary and the connection-lifecycle
events were built inside `client` and are reachable only from there, but nothing in
them is HTTP-specific: the pool trades in `conn.Conn` liveness and
`SETTINGS_MAX_CONCURRENT_STREAMS`, and it touches 3 of `Hooks`' 6 fields, 4 of
`Counters`' 10 and 2 of `Latency`' 3 — all of them about connections, none about
requests. When `grpc` needed the same pooling (#940) and the same per-call
observability (#941), the only way to reuse it was to import `client`. We moved the
vocabulary to a public `pool/` package and the machinery to `internal/poolcore/`
instead, aliasing every moved name back into `client` so no caller breaks.

## Considered options

- **`grpc` imports `client`.** Cheapest, and rejected on a measured cost: it links
  the whole HTTP client — retry, compression, HTTP/1.1, HTTP/3 and QUIC — into a
  gRPC-only binary. #915 measures 577,536 bytes for the HTTP/3 arms of that graph
  alone, and #492 established that this repo treats an unnecessary import edge as a
  defect with a CI gate, not a matter of taste. It also falsifies the dependency
  order `grpc/doc.go` states.
- **A `grpcpool` package composing both.** Keeps `grpc` and `client` untouched, but
  pays the same link cost and still forces a gRPC-only caller to import `client` for
  `PoolOptions` and `Resolver`.
- **`client` imports `grpc`.** Does not work: `client.DoStream` writes the whole
  request body before returning, so client-streaming and bidi calls are not
  expressible through it — the reason `grpc` was built on `conn` in the first place.
- **Moving `Hooks` and `Metrics` wholesale** rather than only the connection events.
  Rejected because the neutral package would then own `Method`, `Path` and
  `StatusCode`, and #941 needs a second, gRPC-shaped `Hooks` regardless — an alias
  cannot merge the two.

## Consequences

`Pool` becomes generic over its element (`Pool[E]` with one `func(E) *conn.Conn`
accessor) so a gRPC `Channel` can pool `*grpc.ClientConn` — which precomputes its
`:authority`, `:scheme` and content-type bytes once per connection instead of once
per RPC. This does **not** reopen #480: the three existing pools stay separate
siblings because `h1` uses exclusive checkout and `h3` runs on QUIC, whereas gRPC
introduces no transport difference at all.
