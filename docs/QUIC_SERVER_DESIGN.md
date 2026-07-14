# Server-role QUIC (S-series) — design

Status: **draft for review.** S1–S2d (handshake layer) are implemented and green;
this doc designs S3 (server connection) before touching the core engine.

## Context

`poseidon-http-client`'s `quic` package implements the **client** role of QUIC v1
(RFC 9000/9001/9002). The S-series adds the **server** role so a server (in
`poseidon-http-server`) can accept HTTP/3 connections.

Landed so far — all **additive** (`quic/server.go`, one function in
`quic/handshake.go`), zero changes to the client core:

| Brick | API | Role |
|---|---|---|
| S1 | `NewServerHandshake` | server TLS handshake (`tls.QUICServer`) |
| S2a | `AcceptInitial` | decrypt client Initial → ClientHello + CIDs |
| S2b | `SealPacket` | seal Initial/Handshake/1-RTT packets |
| S2c | `StartServerHandshake` → `ServerFlight` | drive TLS server, seal first flight |
| S2d | `ServerFlight.HandleClientHandshake` | complete handshake, install 1-RTT keys |

A real client `Conn` completes a full handshake against these
(`TestStartServerHandshake_FullHandshake`). **The server can handshake; it cannot
yet carry application streams.**

## The gap

`Conn` (conn.go, 399 L) + `conn_recv.go` (1037 L) are **client-coupled**:

- **Stream IDs.** `nextBidiStreamID` starts at 0 (client-initiated bidi). A server
  initiates 1,5,9… and must *accept* the client's 0,4,8… (requests).
- **Accept path.** Only `acceptPeerUniStream` / `AcceptUniStream` exist — the
  client accepts server-initiated **uni** streams. A server must accept
  client-initiated **bidi** (requests) and **uni** (control/QPACK) streams.
- **CIDs.** `gotServerCID`/`serverSCID` adopt the *server's* SCID. A server issues
  its own SCID and uses the client's SCID as its DCID; this logic must be
  role-gated.
- **Establishment.** `NewConn` generates a random DCID and drives the client
  Initial. A server starts from `StartServerHandshake`'s output (already past its
  first flight).

Stream ID role map (RFC 9000 §2.1) — bit 0 = initiator, bit 1 = directionality:

| ID mod 4 | Streams | Client | Server |
|---|---|---|---|
| 0 | client bidi (requests) | opens | **accepts** |
| 1 | server bidi (push) | accepts | opens (rare) |
| 2 | client uni (control/QPACK) | opens | **accepts** |
| 3 | server uni (control/QPACK) | accepts | opens |

## Approach: refactor the shared `Conn` to be role-agnostic

**Chosen: (A) one engine, role flag** — not (B) a separate `ServerConn`. The
recv/send/ACK/loss/CC machinery in `conn_recv.go` is role-neutral; duplicating or
extracting it for a parallel type costs more than gating the handful of
role-specific decisions. Add `isServer bool` to `Conn` and branch only where the
role actually differs (establishment, stream-ID assignment, accept, CID adoption,
HANDSHAKE_DONE).

### Establishment

New constructor, additive, alongside `NewConn`:

```go
// NewServerConn builds a connected server-role Conn from a completed
// StartServerHandshake, seeded with the 1-RTT keys and connection IDs. It does
// not run the client Initial path; the caller has already sent the server flight.
func NewServerConn(pc PacketConn, f *ServerFlight, clientDCID, clientSCID []byte) (*Conn, error)
```

It sets `isServer=true`, installs `f.AppSealer`/`f.AppOpener` as the 1-RTT keys and
`f.HandshakeSealer`/`f.HandshakeOpener` as handshake keys, sets `dcid=clientSCID`,
`scid=f.SCID`, seeds `sendPN` spaces, and marks `handshakeComplete`. (The listener
in S4 owns the `PacketConn` demux; here `pc` is a per-connection view.)

### Recv (the delicate part — conn_recv.go)

On a STREAM frame for an unknown ID, `OnStream` currently assumes a client. Gate:
if `isServer` and the ID is client-initiated (bit 0 == 0) and within our advertised
`initial_max_streams_bidi/uni`, **auto-create and accept** the stream (seeding
`recvMax` from `initial_max_stream_data_bidi_local`, `sendMax` from the peer's
`_bidi_remote`); else keep today's behavior. Reuse the existing reassembly, flow
control, RESET/STOP_SENDING, and MAX_STREAM_DATA paths unchanged.

### Accept API

Generalize `acceptedUni`/`AcceptUniStream` to an accept queue that carries both uni
and bidi, and add `AcceptBidiStream()` (the H3 server's request source). Keep
`AcceptUniStream` working (the client's control/QPACK streams).

### HANDSHAKE_DONE

The **server** sends HANDSHAKE_DONE (RFC 9001 §4.1.2) once the handshake is
confirmed; the client already handles receiving it (`OnHandshakeDone`). Add
emission on the server side.

## Phasing (each independently green + reviewable)

- **S3a** — `isServer` flag + `NewServerConn`; unit-test the seeded connected state.
- **S3b** — server-side `OnStream` accept of client bidi/uni in `conn_recv.go`;
  test by feeding a 1-RTT packet with a STREAM frame and asserting accept + readable
  data.
- **S3c** — `AcceptBidiStream` + server-initiated stream open (server IDs).
- **S3d** — HANDSHAKE_DONE emission + **full 1-RTT request/response round-trip**
  between a real client `Conn` and a server `Conn` over the in-memory `chanPC`
  harness. This is the milestone that proves the server carries application data.

## Test strategy

Reuse the `chanPC` in-memory datagram harness. The capstone test: a real client
`Conn` opens a bidi stream, writes request bytes; the server `Conn` accepts it via
`AcceptBidiStream`, reads them, writes a response; the client reads the response.
No mocks — both peers are the real engine, so every frame is validated by the other
side (the same discipline that made S1–S2d self-checking).

## Non-goals (defer, mirroring HTTP3_DESIGN.md G.9)

0-RTT; connection migration & path validation; stateless Retry / address-validation
tokens (S4 concern); server push; NEW_TOKEN issuance; QPACK dynamic table. A first
server accepts a single client per connection with static-QPACK H3.

## After S3

- **S4** — UDP listener/endpoint: non-connected `net.UDPConn`, demux inbound
  datagrams by DCID across connections, hand accepted `Conn`s to the caller.
- **H3 mapping** (in `poseidon-http-server`): `AcceptBidiStream` → read HEADERS via
  `http3.FrameReader` + `qpack.Decoder` → `*http.Request` → `http.Handler` →
  `qpack`-encode → `http3.AppendHeaders`/`AppendData` → trailers; server control
  stream + SETTINGS + GOAWAY drain.

## Review questions

1. Role flag on `Conn` (A) vs. a separate `ServerConn` (B) — confirm A.
2. Is modifying `conn_recv.go`'s `OnStream` acceptable, or should server accept live
   behind a new method the recv loop dispatches to when `isServer`?
3. Should S3 land on this branch (`feat/quic-server-role`) or a fresh one, given it
   moves from additive to core changes?
