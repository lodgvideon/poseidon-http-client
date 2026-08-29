# poolcore.Pool stays non-generic; the pooled connection type crosses as `any`, confined to one cast per consumer

Issue #940 needs `internal/poolcore.Pool` to hand out `*grpc.ClientConn` as well as
`*conn.Conn`, and the design as first recorded called this "`Pool[E]` generic with a
`func(E) *conn.Conn` accessor." Implementing that literally means `ManagedConn`'s `C`
field becomes the type parameter, which breaks all 37 existing `ManagedConn{...}`
struct literals across this week's poolcore tests and touches the ~19 internal call
sites (`mc.C.IsAlive()`, `.GoAwayReceived()`, `.Close()`, `.PeerMaxConcurrentStreams()`)
for no behavioural gain — none of that machinery differs between a `*conn.Conn` and a
`*grpc.ClientConn`, since the latter is a thin, stateless wrapper over the former
(`grpc.ClientConn`'s only fields are precomputed once from `Options` and never change).

We instead keep `Pool` and `ManagedConn` non-generic. `ManagedConn` gains one field,
`Typed any`, populated once at dial time (never per-`Acquire`, since re-wrapping would
re-allocate and re-validate on every acquire) via a new optional `wrap func(*conn.Conn)
(any, error)` parameter on `New` — `nil` for `client`'s existing HTTP/2 usage, so its
four existing call sites are untouched. The one type assertion recovering
`*grpc.ClientConn` from `Typed` lives in a single small helper inside the `grpc`
package; `poolcore` never imports or knows about `grpc`.

## Considered Options

- **`Pool[E]` with `ManagedConn.C` typed as `E`** (as first recorded): real compile-time
  safety inside `poolcore`, but changes an existing field's type — exactly the cost
  `pool_shared.go` and #480 already measured and rejected for the H2/H3 case ("moving a
  FIELD into a shared type churns the regression gate. Methods are free; fields are
  not."). Rejected: no consumer of `poolcore` needed that safety badly enough to pay for
  it, and #480's own recorded shape for this file's *next* generic step already uses an
  accessor pattern, not a field-type change — so this option is not even forward-looking.

## Consequences

- Whichever code calls `pool.Acquire` for a typed connection must go through its
  package's own confining helper, never read `mc.Typed` directly — same discipline as
  wrapping a third-party `Map` (Ch. 8, *Clean Code*): the raw/`any` value stays inside
  exactly one adapter.
- `Pool`'s internal eviction path must keep closing via the raw `*conn.Conn`
  (`mc.C.Close()`), never via a wrapped value's own `.Close()` —
  `grpc.ClientConn.Close()` is deliberately a no-op when it doesn't own the connection
  (`owned == false`, which is what wrapping via `grpc.NewClientConn` produces), so
  routing eviction through it would silently leak the connection.
- If `wrap` fails (e.g. `grpc.NewClientConn`'s `Authority` check), `dialOne` must treat
  it as a failed dial: close the freshly-dialed raw conn and report the error, not leave
  it half-adopted into the pool.
