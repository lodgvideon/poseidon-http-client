# gRPC guide

`grpc` speaks the gRPC-over-HTTP/2 wire protocol on top of the `conn` layer.
It is a transport, not a framework: there is no protobuf dependency, no code
generation, and no service registry. Messages cross the API as `[]byte`, which
is what a load generator wants — a pre-marshaled fixture replayed millions of
times costs nothing to re-encode.

```
grpc → conn → frame, hpack
```

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
        Dialer:            &conn.TLSDialer{Config: &tls.Config{NextProtos: []string{"h2"}}},
        KeepaliveInterval: 30 * time.Second,
    },
})
if err != nil {
    return err
}
defer cc.Close()

resp, err := cc.Invoke(ctx, "/helloworld.Greeter/SayHello", reqBytes, nil)
```

A `ClientConn` is **one** HTTP/2 connection multiplexing as many concurrent
calls as the peer's `SETTINGS_MAX_CONCURRENT_STREAMS` allows. That is gRPC's own
model, so there is no pool to configure — open a second `ClientConn` when you
want a second connection.

The context deadline becomes the `grpc-timeout` header, so the server cancels
its own work when the caller gives up. A context without a deadline sends no
`grpc-timeout`.

## The four call shapes

`Invoke` is unary sugar. Everything else uses `NewStream`.

| Shape | Send side | Receive side |
|---|---|---|
| unary | `Send` once, `CloseSend` | one `Recv`, then `io.EOF` |
| server-streaming | `Send` once, `CloseSend` | `Recv` until `io.EOF` |
| client-streaming | `Send` N times, `CloseSend` | one `Recv`, then `io.EOF` |
| bidirectional | `Send` from goroutine A | `Recv` from goroutine B |

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

A 200 response that ends with no `grpc-status` anywhere is `INTERNAL`, not a
success. So is a stream that ends in the middle of a message.

## Metadata

Build request metadata with `AppendMetadata`, which lowercases the key and
base64-encodes the value when the key ends in `-bin`:

```go
md, _ := grpc.AppendMetadata(nil, "x-request-id", []byte("req-7"))
md, _ = grpc.AppendMetadata(md, "trace-bin", traceID)
s, err := cc.NewStream(ctx, "/pkg.Svc/Method", md)
```

Keys the transport owns — `content-type`, `te`, `user-agent`, `grpc-timeout`,
`grpc-encoding`, `grpc-accept-encoding`, and any pseudo-header — are rejected
with `ErrReservedMetadata` rather than silently overriding the request.

Read response metadata with `Header(ctx)` (blocks until the server's header
block arrives) and `Trailer()` (valid once `Recv` has reported the end of the
stream). `MetadataValue` reverses the `-bin` encoding, accepting both padded
and unpadded base64.

## Message framing

Every gRPC message is a 1-byte compressed flag plus a 4-byte big-endian length
plus the payload. HTTP/2 DATA boundaries have nothing to do with message
boundaries: one DATA frame may carry several messages, and one message may span
many frames. `Decoder` handles the reassembly; `Recv` hands back a fresh copy
the caller owns.

`MaxRecvMessageSize` (default 4 MiB, matching gRPC) is checked **against the
declared length prefix**, before any of the payload is buffered, so a hostile
peer cannot make the client allocate on its say-so.

## Compression

The client advertises `grpc-accept-encoding: identity` and never sets the
compressed flag. A compliant server therefore never compresses. One that does
anyway is reported as `ErrCompressed` rather than being silently mis-decoded.

Do **not** reach for `client.Request.CompressBody` here — that is HTTP
`content-encoding` over the whole body, which is a different thing from gRPC's
per-message `grpc-encoding`.

## Keepalive

Set `Options.Conn.KeepaliveInterval` to enable HTTP/2 PINGs, and
`KeepaliveTimeout` to bound how long a missing ACK is tolerated before the
connection is closed. `ClientConn.Conn()` exposes the underlying `*conn.Conn`
for a one-off `Ping` or for `Stats`.

The client does not implement gRPC's server-side keepalive-enforcement dance: a
server that answers aggressive PINGs with `GOAWAY(ENHANCE_YOUR_CALM)` will close
the connection, and in-flight calls fail with that as their status. Pick an
interval the server permits.

## Not covered

Name resolution, load balancing, retry policy, and per-call authentication live
above this package. So does protobuf: `Invoke` and `Send` take the bytes you
give them.
