# gRPC guide

`grpc` speaks the gRPC-over-HTTP/2 wire protocol on top of the `conn` layer.
It is a transport, not a framework: there is no protobuf dependency, no code
generation, and no service registry. Messages cross the API as `[]byte`, which
is what a load generator wants — a pre-marshaled fixture replayed millions of
times costs nothing to re-encode.

```
grpc → conn → frame, hpack     (plus frame directly, for RST_STREAM codes)
```

HTTP/2 only. `http3.Request` carries a `[]byte` body with no incremental send
path, so the streaming call shapes cannot be expressed on that stack yet.

It does **not** go through `client`. That is deliberate: `client.DoStream`
writes the entire request body before it returns, so client-streaming and
bidirectional calls are not expressible through it. `conn.Stream` is genuinely
full-duplex, so `grpc` sits directly on top of it.

## Quick start

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

cc, err := grpc.Dial(ctx, "api.example.com:443", grpc.Options{
    Conn: conn.ConnOptions{
        Dialer: &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
    },
})
if err != nil {
    return err
}
defer cc.Close()

resp, err := cc.Invoke(ctx, "/helloworld.Greeter/SayHello", reqBytes, nil)
```

A `ClientConn` is **one** HTTP/2 connection multiplexing as many concurrent
calls as the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` allows. There is no
pooling yet: open a second `ClientConn` when you want a second connection, and
build the pool, the health checking and the reconnect-on-GOAWAY yourself.

If you arrive from grpc-go, note the name is narrower here than there. grpc-go's
`ClientConn` is a virtual channel owning a resolver, a balancer and a set of
subconnections; this one is the single-connection object underneath that. It
does not resolve names, load-balance, or reconnect.

The context deadline becomes the `grpc-timeout` header, so the server cancels
its own work when the caller gives up. A context without a deadline sends no
`grpc-timeout`.

## The four call shapes

`Invoke` is unary sugar. Everything else uses `NewStream`.

| Shape | Send side | Receive side |
|---|---|---|
| unary | `SendLast` once | one `Recv`, then `io.EOF` |
| server-streaming | `SendLast` once | `Recv` until `io.EOF` |
| client-streaming | `Send` N-1 times, `SendLast` | one `Recv`, then `io.EOF` |
| bidirectional | `Send` from goroutine A | `Recv` from goroutine B |

`SendLast` writes the final message and half-closes in the same DATA frame.
`Send` followed by `CloseSend` does the same thing in two, and the second
carries no payload — but it still costs its own flush, which over TLS is a
separate record with its own header and AEAD tag, and (Go enables
`TCP_NODELAY`) usually a separate segment. For a small message that is
comparable to the message itself, so prefer `SendLast` wherever the last
message is known in advance. `Send` + `CloseSend` stays correct, and is the
right pair for a bidirectional call that only learns it is done after the last
message has gone.

```go
s, err := cc.NewStream(ctx, "/chat.Chat/Session", nil)
if err != nil {
    return err
}
defer s.Close()

go func() {
    for _, m := range outbound {
        if err := s.Send(ctx, m); err != nil {
            return
        }
    }
    _ = s.CloseSend(ctx)
}()

for {
    msg, err := s.Recv(ctx)
    if errors.Is(err, io.EOF) {
        break // the call completed with grpc-status OK
    }
    if err != nil {
        return err // a *Status, or a transport failure
    }
    handle(msg)
}
```

The send and receive halves are independent. That works because a `conn.Stream`
writer blocked on HTTP/2 flow-control credit does **not** hold the connection's
write lock — `writeData` calls `acquireSendCredits` with `wmu` released — so a
stalled `Send` never stops the reader goroutine from draining events.

Each half on its own is single-goroutine: `Send`/`CloseSend` are serialised
internally, but `Recv`/`Header` must be driven by one goroutine only.

## Status handling

`Recv` returns `io.EOF` when the call succeeded and a `*Status` when it failed,
so one `err != nil` check covers both the RPC-level and the transport-level
failure:

```go
var st *grpc.Status
if errors.As(err, &st) {
    log.Printf("rpc failed: %v %s", st.Code, st.Message)
}
```

Four different wire shapes all land in `Status`:

1. **Trailers with `grpc-status`** — the normal completion.
2. **Trailers-Only** — a single HEADERS frame with END_STREAM carrying both
   `:status: 200` and `grpc-status`. This is how gRPC servers report most
   errors, and it arrives from `conn` as `EventHeaders`, *not* `EventTrailers`,
   because no response header block preceded it. A client that only looks in
   the trailer block reports these RPCs as silent successes.
3. **A non-200 HTTP status** — typically from a proxy that never saw gRPC at
   all. Mapped per the specification's table: 404 → `UNIMPLEMENTED`,
   401 → `UNAUTHENTICATED`, 429/502/503/504 → `UNAVAILABLE`, and so on.
4. **RST_STREAM** — mapped from the HTTP/2 error code: `REFUSED_STREAM` →
   `UNAVAILABLE`, `CANCEL` → `CANCELED`, `ENHANCE_YOUR_CALM` →
   `RESOURCE_EXHAUSTED`, everything else → `INTERNAL`.

A 200 response that ends with no `grpc-status` anywhere is `UNKNOWN`, not a
success — that is the 200 row of the mapping table, and it reads that way
precisely because a genuinely successful response would have carried a status. A
stream that ends mid-message is `INTERNAL`, unless the peer also sent a status,
in which case the peer's own diagnosis wins: the truncation is a consequence of
whatever it reported, and replacing `UNAVAILABLE` with `INTERNAL` would turn a
retriable failure into a permanent one.

## Metadata

Build request metadata with `AppendMetadata`, which lowercases the key and
base64-encodes the value when the key ends in `-bin`:

```go
md, _ := grpc.AppendMetadata(nil, "x-request-id", []byte("req-7"))
md, _ = grpc.AppendMetadata(md, "trace-bin", traceID)
s, err := cc.NewStream(ctx, "/pkg.Svc/Method", md)
```

Keys the transport owns — `content-type`, `te`, `user-agent`, the connection-
specific fields HTTP/2 forbids outright, and the whole `grpc-` namespace the
protocol reserves — are rejected with `ErrReservedMetadata`.

The `grpc-` rule has one deliberate escape hatch. The specification reserves that
prefix for *future* protocol use, so refusing it is right for application
metadata — but `grpc-trace-bin` and `grpc-tags-bin` are written by grpc-go's own
instrumentation rather than by applications, and a client that cannot emit them
cannot join a census-instrumented deployment's traces:

```go
cc, err := grpc.Dial(ctx, addr, grpc.Options{
    AllowReservedMetadata: []string{"grpc-trace-bin"},
})
md, _ := cc.AppendMetadata(nil, "grpc-trace-bin", spanContext)
```

It exempts the listed names from **that check only**. Name syntax, value
validation, and the transport's own headers are unaffected: listing
`content-type` or `grpc-timeout` changes nothing. Use `cc.AppendMetadata` rather
than the package-level `grpc.AppendMetadata` for an exempted key — the package
function cannot see a connection's options and stays strict. Names that are not
lowercase tokens and values carrying CR, LF, NUL or edge whitespace are rejected
with `ErrInvalidMetadata`; that check is the last gate before the wire, since
neither `conn` nor `hpack` validates outbound fields, and it is what stops a
caller-forwarded value from becoming an injected request at an HTTP/1.1
downgrading hop.

Read response metadata with `Header(ctx)` (blocks until the server's header
block arrives) and `Trailer()` (valid once `Recv` has reported the end of the
stream, and callable only from the goroutine driving `Recv`).

```go
v, ok, err := grpc.MetadataValue(hdr, "x-trace-bin")
```

`MetadataValue` reverses the `-bin` encoding, accepting both padded and unpadded
base64. `ok` and `err` are separate on purpose: a value the peer sent but
corrupted must not read the same as a value the peer never sent, or an
application checking for a signature takes its nothing-to-verify branch on
exactly the input an attacker controls.

Credentials never enter the HPACK dynamic table. `authorization`,
`proxy-authorization` and `cookie` are marked sensitive automatically — that is
a floor, and any other field can be marked by setting `Sensitive` on it
directly. Without it a bearer token would be indexed once and then emitted as a
one-byte reference on every later call over the same connection.

## Message framing

Every gRPC message is a 1-byte compressed flag plus a 4-byte big-endian length
plus the payload. HTTP/2 DATA boundaries have nothing to do with message
boundaries: one DATA frame may carry several messages, and one message may span
many frames. Reassembly is internal; `Recv` hands back a fresh copy the caller
owns. `AppendMessage` is exported for the other side of the wire — a test
server, a recorded fixture.

`MaxRecvMessageSize` (default 4 MiB, matching gRPC) is checked **against the
declared length prefix**, so a peer announcing 4 GiB is refused on the prefix
alone rather than after its payload arrives. One DATA frame of the payload may
be buffered before the check runs — HTTP/2 flow control bounds that — and the
reassembly buffer settles at roughly one message.

The same number is also the per-stream memory budget, not only a limit: `Dial`
sizes conn's event channel from it, because conn refunds flow-control window as
frames arrive rather than as the application consumes them, so nothing else
throttles a fast server to a slow consumer. Use the `MaxRecvMessageSize`
`CallOption` to raise it for the one method that needs it instead of paying for
it on every call.

## Compression

The client advertises `grpc-accept-encoding: identity` and never sets the
compressed flag. A compliant server therefore never compresses. One that does
anyway is reported as `ErrCompressed` rather than being silently mis-decoded.

Do **not** reach for `client.Request.CompressBody` here — that is HTTP
`content-encoding` over the whole body, which is a different thing from gRPC's
per-message `grpc-encoding`.

## Keepalive

Keepalive is **off by default, and that is the safe setting.** gRPC servers
enforce a minimum ping interval — grpc-go's default is 5 minutes — and refuse
pings on a connection with no active stream. Two violations and the server sends
`GOAWAY(ENHANCE_YOUR_CALM)` with debug data `too_many_pings` and drops the
connection.

`Options.Conn.KeepaliveInterval` enables PINGs and `KeepaliveTimeout` bounds how
long a missing ACK is tolerated. The loop pings unconditionally — it does not
back off after a GOAWAY and does not skip idle connections — so set an interval
the server is configured to permit, or leave it at zero. `ClientConn.Conn()`
exposes the underlying `*conn.Conn` for a one-off `Ping` or for `Stats`.

## Throughput over a real network

`Options.Conn.AutoTuneRecvWindow` is off by default and is the first thing to
turn on for anything that streams bulk data.

RFC 9113 §6.5.2 starts both HTTP/2 receive windows at 65535 bytes and leaves
them there until an endpoint says otherwise. Until it is raised, the *whole
connection* — every stream on it together — can have only 64 KiB in flight per
round trip: roughly 6.5 MB/s at 10 ms RTT, whatever the link and the CPU can do.
Loopback hides this completely, so a local benchmark will not show it and a
staging run across a region will.

```go
cc, err := grpc.Dial(ctx, addr, grpc.Options{
    Authority: "api.example.com",
    Conn: conn.ConnOptions{
        AutoTuneRecvWindow: true,
    },
})
```

The connection then measures what the peer can actually deliver in one round
trip and grows both windows to twice that, up to a ceiling derived from
`StreamEventBuffer × Settings.MaxFrameSize` — which `Dial` already sizes from
`MaxRecvMessageSize`, so a client configured for large messages gets a
correspondingly large window without a second knob. Set `MaxRecvWindow` to
override the ceiling; raise `StreamEventBuffer` alongside it if you do, since
that budget is what makes the window safe to grow into.

## Not covered

Name resolution, load balancing, retry policy, and per-call authentication live
above this package. So does protobuf: `Invoke` and `Send` take the bytes you
give them.
