# poseidon-http-client

A from-scratch HTTP/1.1 + HTTP/2 + HTTP/3 client for load generation. This
glossary fixes the words the codebase uses for the things it manages — the
ones where a reader arriving from `net/http`, `grpc-go` or an RFC would
otherwise assume the wrong meaning.

## Language

### Connection management

**Channel**:
A gRPC entry point that owns many connections, a Resolver and a Selector, and
routes each call to one of them. What `grpc-go` names `ClientConn`.
_Avoid_: Client, Pool, VirtualConn

**ClientConn**:
Exactly one HTTP/2 connection carrying gRPC calls. The thing a Channel owns
several of; `grpc-go` would call it a SubConn.
_Avoid_: Channel, SubConn, Transport

**Backend**:
One resolved address serving the target. A Resolver produces the set of them;
a Selector picks one per call.
_Avoid_: Host, Server, Node, Upstream, Instance

**Transparent retry**:
Re-sending a call on another connection after the peer proved it never
processed the first attempt — a GOAWAY above `lastStreamID`, or
`RST_STREAM(REFUSED_STREAM)`. Safe for non-idempotent methods, which is what
separates it from a retry policy.
_Avoid_: Retry, Replay, Failover
