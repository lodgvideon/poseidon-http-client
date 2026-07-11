# HTTP/3 Client — Interop Test Plan

Risk-based plan for testing the from-scratch HTTP/3 client (`http3.Dial` →
`Client.Do`) against **real, diverse server implementations** under **real
network conditions**. Tracks progress against the `//go:build interop` suite in
[`http3/interop_test.go`](../http3/interop_test.go), run by the docker harness in
[`test/integration/http3`](../test/integration/http3).

## 1. SUT constraints that shape the plan

- **Sequential API.** `Client` is single-goroutine and "not safe for concurrent
  use"; `Do` is blocking, one request/response at a time. QUIC multiplexing
  exists at the transport, but the public API exposes no concurrent in-flight
  requests, so suites D1/D2 (concurrent streams) are **not exercisable via the
  public API** — a documented limitation, not a writable test.
- **Static-only QPACK decoder.** The client advertises
  `SETTINGS_QPACK_MAX_TABLE_CAPACITY=0`; a server that uses the dynamic table
  anyway must fail as `QPACK_DECOMPRESSION_FAILED`, not hang.
- **ChaCha20-Poly1305 header protection deferred.** A ChaCha20-only server is a
  known incompatibility → graceful failure, documented.
- **Receive-path resource bounds** (PRs #162–166) — must not false-trip on real
  servers, and must hold under adversarial input.

## 2. Risk model — test-effort priority

| Rank | Failure class | Why it bites this client |
|---|---|---|
| R1 | QPACK header mismatch | static-only decoder; server dynamic-table use breaks it |
| R2 | Flow-control stalls | large/long transfers exercise MAX_STREAM_DATA/MAX_DATA loops |
| R3 | Loss recovery / CC | only 10% loss tested; PTO/retransmit/cwnd untested at scale |
| R4 | Stream lifecycle | reset / STOP_SENDING / retirement leak (#166) on long conns |
| R5 | Key update & long conns | 1 MiB spans one; sustained spans many; CID rotation |
| R6 | Frame/message-order validation | malformed → correct H3 error, not hang |
| R7 | TLS/crypto | ChaCha20 HP deferred; cert-verify path untested |
| R8 | Resource bounds (#162–166) | no false-trip on real servers; hold under attack |

## 3. Test dimensions (matrix)

- **Server impl:** Caddy `quic-go`/Go ✓ · nginx C ✓ · + quiche (Rust) · lsquic
  (LiteSpeed/H2O) · aioquic (Python) · msquic · haproxy.
- **Network** (fault-injecting UDP relay, extend `lossproxy.go`): clean · loss
  {1,5,10,20,30}% · reorder · latency+jitter · dup · bit-corruption · low-MTU ·
  throttle · combos.
- **Payload:** 0 · 1 · MTU±1 · 16 KiB · 1 MiB · 10 MiB · 100 MiB · frame/window
  boundaries.
- **Connection lifetime:** single-shot · long-lived (10³–10⁶ sequential
  requests).

## 4. Suites (P0 ship-next · P1 important · P2 nightly · NH needs-new-harness)

### A — HTTP semantics
A1 methods (GET/HEAD/POST/PUT/DELETE/PATCH/OPTIONS) P1 · **A2 HEAD → empty body,
CL header P0** · **A3 status pass-through 204/301/304/400/404/405/500/503 P0** ·
**A4 Content-Length match P0** · A5 CL mismatch → `ErrH3Message` P1·NH · **A6
sizes 0/1/MTU±1/16K/1M/10M P0** · A7 request-body sizes P1 · A8 request headers
P1 · A9 response trailers P1·NH · A10 1xx (100/103) P1.

### B — QPACK / headers (highest interop risk)
**B1 server dynamic-table on → client cap 0 honored P0** · **B2 server ignores
cap 0 → clean QPACK_DECOMPRESSION_FAILED P0·NH** · B3 field section near
MAX_FIELD_SECTION_SIZE P1 · B4 many/large headers P1 · B5 all 99 static entries
P1 · B6 GREASE settings/frames P1.

### C — QUIC transport & flow control
**C1 response > stream window (256 KiB) P0** · **C2 > conn window (1 MiB) P0** ·
C3 slow consumer P1·NH · C4 peer MAX_STREAMS P1 · C5 low-MTU P2·NH · **C6
explicit key update P0** · C7 CID rotation P1·NH.

### D — Stream lifecycle
D1/D2 concurrency — **blocked by sequential API (see §1)** · **D3 server
RESET_STREAM mid-response → StreamResetError + Retryable() P0·NH** · D4 server
STOP_SENDING → still read response P1·NH · D5 ctx cancel mid-response P1 · **D6
long-lived N sequential requests → memory flat (validates #166) P0**.

### E — Connection lifecycle
E1 GOAWAY P1·NH · E2 idle timeout P1 · E3 Retry P1·NH · E4 Version Negotiation
P2·NH · E5 graceful Close P0 · E6 abrupt server close → PeerClosedError P1·NH.

### F — Resilience (extend `lossproxy.go`)
**F1 loss sweep 1/5/10/20/30% P0** · **F2 reordering P0·NH** · F3 latency+jitter
P1·NH · F4 bit corruption P1·NH · F5 duplication P1·NH · F6 tail loss P1·NH · F7
combined P2·NH.

### G — Negative / server misbehavior (fault-injecting server)
G1 malformed frame → correct H3 error P1·NH · G2 truncated final frame →
H3_FRAME_ERROR P1·NH · G3 forbidden frames on request stream P1·NH · G4 DATA
before HEADERS / after trailers P1·NH · G5 message-rule violation → ErrH3Message
P1·NH.

### H — Resource-bound validation (#162–166)
**H1 no false-trip on normal real traffic P0** · H2 NCID flood → bound P2·NH ·
H3 oversized → ErrResponseTooLarge P2·NH · H4 gapped CRYPTO/STREAM flood P2·NH.

### I — TLS / crypto
I1 AES-128/256-GCM P1 · **I2 ChaCha20-only → documented graceful failure P1·NH**
· I3 real cert verification (no InsecureSkipVerify) P1·NH · I4 ALPN h3 / SNI P0 ·
I5 session tickets / post-handshake CRYPTO P2.

### J — Performance / scale (nightly)
J1 sustained throughput 100 MiB+ P2 · J2 request rate/burst P2 · J3 soak (hours)
P2.

## 5. Coverage

- **Have:** A1(GET/POST), A6(16K/1M), F1(10%), E5(partial), I4(implicit). 2
  servers, clean net.
- **Batch 1 (this iteration):** A2 (HEAD, Content-Length-mismatch tolerance) ·
  A3 *partial* (204/304/404/500 of the 8 listed codes) · A4 (Content-Length ==
  reassembled body on `/big.txt`, and == body on 4xx/5xx) · D6 *functional-scale
  only* (reuse + retirement path; the leak bound is unit-tested in the quic
  package, #166). See [`http3/interop_test.go`](../http3/interop_test.go).
- **Next:** extend `lossproxy.go` (F-sweep + reorder); fault-injecting H3 server
  (B2, D3/D4, G*, E1/E3); add quiche/lsquic servers.

## 6. Harness needed

1. Config-driven fault relay (loss/reorder/dup/jitter/corrupt/MTU) — from
   `lossproxy.go`.
2. Fault-injecting H3 server (malformed frames, dynamic QPACK, resets, trailers,
   1xx, GOAWAY, retry on command) — highest-leverage asset.
3. More server images (quiche, lsquic, aioquic, msquic).
4. Memory/RSS probe for D6/J3.
5. Align with the community **QUIC Interop Runner** matrix.

## 7. CI vs nightly

- **PR CI (< 5 min, hermetic):** A/B (P0/P1), C1/C2/C6, D6, F1(one level) —
  Caddy + nginx.
- **Nightly:** full server matrix, F-sweep, D6 long, H2–H4, I2/I3, J*.

## 8. Exit criteria & flake policy

- Entry: handshake succeeds; fixtures byte-exact.
- Exit (PR): P0/P1 CI cases green on every matrix server.
- Flake: network-condition suites — bounded timeouts, deterministic relay seeds,
  ≤1 retry; functional suites 0-flake.
- Oracles are byte-exact or typed-error — never "status 200 and hope".
