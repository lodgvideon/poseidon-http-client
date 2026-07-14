# HTTP/3 (Phase G) — Design & Roadmap

**Status:** design proposal, for review. No code yet.
**Goal:** a from-scratch HTTP/3 client conformant to **RFC 9114**, which in
practice means conformance to the whole stack it sits on: **RFC 9000** (QUIC
transport), **RFC 9001** (QUIC-TLS), **RFC 9002** (loss detection / congestion
control), and **RFC 9204** (QPACK). No third-party QUIC library in production —
only Go stdlib (`crypto/tls` QUIC handshake primitives, Go 1.24) + `x/crypto`
for AEAD/HKDF, consistent with the project's "no `net/http`, no `x/net/http2`"
rule.

> **Why this is a program, not a feature.** RFC 9114 is only the HTTP-semantics
> mapping onto QUIC streams. Everything that makes it *work* — packets, keys,
> streams, flow control, loss recovery, header compression — lives in the four
> RFCs beneath it. HTTP/3 shares essentially **nothing** with poseidon's current
> TCP+TLS+HTTP/2 stack: different transport (UDP, not TCP), different compression
> (QPACK, not HPACK), different framing (QUIC-varint, not the fixed 9-byte h2
> header), different crypto integration (`tls.QUICConn` secret pump, not an
> opaque `*tls.Conn` ALPN pipe). This is a larger undertaking than everything the
> project has built so far, combined, and is delivered as a phased PR program.

## 1. What "strict RFC 9114 conformance" actually requires

| Layer | RFC | Role | Reuses existing poseidon code? |
|---|---|---|---|
| HTTP/3 semantics + framing | **9114** | request/response over QUIC streams; control stream; SETTINGS; GOAWAY | ❌ new framing (varint type/length) |
| **QPACK** field compression | **9204** | replaces HPACK; static table + optional dynamic table on 2 uni streams | ♻️ **prefix-int + Huffman reused from `hpack/`**; new static table + representations |
| **QUIC-TLS** | **9001** | TLS 1.3 as handshake/secret engine; Initial-secret derivation; AEAD packet + header protection; key update | ⚠️ `crypto/tls.QUICConn` drives TLS; we wire packet protection |
| **QUIC transport** | **9000** | UDP; packets; 3 PN spaces; streams; flow control; conn IDs; transport params | ❌ new (varint codec goes in `internal/bytesx`) |
| **loss detection + CC** | **9002** | RTT, packet/time-threshold loss, PTO, NewReno | ❌ new; no precedent in `conn/` (only `KeepaliveInterval`) |

**The dependency is total, bottom-up:** QUIC transport cannot read or write a
single byte without RFC 9001 packet protection; H3 cannot send a byte without a
working QUIC stream layer. The crypto substrate (G.5) therefore gates every
end-to-end test.

## 2. Architecture

Mirror poseidon's **A-layer (pure codec) / B-layer (connection)** split, one
package per protocol layer, exactly as `frame/` + `hpack/` sit under `conn/`:

```
internal/bytesx/   + ReadVarint/WriteVarint/VarintLen   (RFC 9000 §16; 2-bit-prefix 1/2/4/8-byte)
qpack/             A-layer: QPACK encoder/decoder        (RFC 9204) — pure codec, reuses hpack Huffman + prefix-int
quic/              A-layer codec  (packet/frame codec, header protection scratch) — pure, 0-alloc-gated
   + engine        NOT a pure codec: owns UDP socket, crypto, streams, flow control, loss recovery, timers
http3/             B-layer: connection — owns control + QPACK streams, SETTINGS, H3 framer, request/response
client/            new TransportH3 / NewH3Client wiring the http3 conn into the existing Client API
```

**What transfers from the existing codebase (discipline, not engine):**

- The **Handler-visitor read model** (`frame.Framer.ReadFrame` → `On<Frame>` with
  caller-owned zero-copy scratch) transfers almost verbatim to a `quic.FrameHandler`
  and an `http3.FrameHandler`.
- The **pure-function header codec + stateful Framer split** (`frame/header.go` vs
  `frame/framer.go`), the **narrow `streamWriter` interface** that tests fake, the
  **one-codec-instance-per-connection / not-goroutine-safe** contract.
- The **error model**: sentinel package-var errors (no `fmt.Errorf` on hot paths),
  typed error-code constants mirroring the RFC registry, and the two-tier
  **fatal `ConnError` vs non-fatal `StreamError`** split routed in the read loop.
  H3 maps this onto QUIC's mechanisms: connection errors → `CONNECTION_CLOSE`,
  stream errors → `RESET_STREAM`/`STOP_SENDING` carrying the H3 code.

**What must be rebuilt (the structural mismatch):** poseidon's B-layer assumes a
`net.Conn` (reliable, ordered byte stream) + **one blocking reader goroutine** +
a `bufio.Writer` flushed under a mutex. QUIC is UDP datagrams with per-packet
crypto, multiplexed streams, timers, loss recovery, and pacing. The
`readerLoop`/`wmu`/flush engine **does not port**. Concretely:

- **`frame.Framer.ReadFrame`'s `io.ReadFull(header)` then `io.ReadFull(payload)`
  model is TCP-shaped and unusable.** One UDP datagram may coalesce multiple QUIC
  packets (different PN spaces/keys), each AEAD-protected, each containing many
  frames. The receive path is: `ReadFromUDP` → split datagram into packets by
  long-header Length → remove header protection → AEAD-open → parse frames → route
  by stream. Only the *write-side* framing and the *Handler dispatch* shape carry.
- **Most of `conn/`'s manual flow control disappears.** QUIC owns stream
  multiplexing, per-stream + connection flow control, ordering, and
  retransmission, so `WINDOW_UPDATE`/`acquireSendCredits`/`fcOutCond` at the H3
  layer are gone. H3 keeps only `SETTINGS_MAX_FIELD_SECTION_SIZE` + QPACK's
  capacity/blocked-streams knobs. There is **no H3 SETTINGS ACK and no H3 PING**
  (QUIC handles keepalive) — that h2 logic does not port.
- **A QUIC stream is the natural `Stream`**: an `io.Reader`/`io.Writer` with its
  own FIN and reset carrying an H3 application error code.

### 2.1 Handshake integration (the crypto seam)

Do **not** model this as "another `Dialer` returning a `net.Conn`" (today's
`dial.go`). QUIC uses TLS 1.3 as a handshake-message + secret provider only:

- `tls.QUICClient(&tls.QUICConfig{TLSConfig: cfg})` with `cfg.NextProtos=["h3"]`,
  SNI, `MinVersion=TLS1.3`. Drive it as an event loop: `Start(ctx)`, then
  `NextEvent()` until `QUICNoEvent`; feed inbound CRYPTO bytes with
  `HandleData(level, data)`; re-drain.
- **What `QUICConn` gives us:** per-direction, per-level traffic secrets
  (`QUICSetReadSecret`/`QUICSetWriteSecret`), TLS handshake bytes to put in CRYPTO
  frames (`QUICWriteData`), transport-parameter plumbing (the `0x39` extension),
  cipher suite, ALPN result, handshake-complete — and it refuses prohibited
  features (never emits a TLS KeyUpdate).
- **What we still wire (RFC 9001):** (1) **all Initial-level keys** — `QUICConn`
  emits none; derive from the client-chosen DCID + the fixed v1 salt
  `0x38762cf7…`; (2) the `"quic key"`/`"quic iv"`/`"quic hp"` HKDF-Expand-Label
  step on every secret; (3) per-packet **AEAD** seal/open; (4) **header
  protection**; (5) **key update** via `"quic ku"`. Note `ev.Secret`/`ev.Data`
  alias TLS-internal memory — copy before the next `NextEvent()`.

## 3. The minimal-conformant-client strategy (how to get to a working request fast)

RFC 9114/9204 are generous about what a *client* may omit. The design deliberately
targets the smallest fully-conformant surface first, then layers optimizations.
Each choice below is spec-legal, not a shortcut:

- **QPACK static-table-only.** Advertise `SETTINGS_QPACK_MAX_TABLE_CAPACITY=0`
  and `SETTINGS_QPACK_BLOCKED_STREAMS=0`. This **contractually forbids the server**
  from using dynamic-table insertions or blocking against us, so our decoder never
  maintains a dynamic table and never blocks. Our encoder emits the constant 2-byte
  section prefix `0x00 0x00` (Required Insert Count = 0, Base = 0) + indexed-static
  / literal field lines only. Result: the whole QPACK path is allocation-free and
  the encoder/decoder streams stay near-idle (but must still be *opened*).
- **Zero-length client Connection ID.** Sidesteps CID rotation
  (`NEW/RETIRE_CONNECTION_ID`) entirely for the first client.
- **No 0-RTT, no server push, no connection migration** (send
  `disable_active_migration`), **no NEW_TOKEN storage.** All spec-optional for a
  client; each is a later phase.
- **NewReno congestion control, named-and-stubbed** with a conservative fixed
  window for bring-up (RFC 9002 §7 phases stubbed) — a fixed large window is fine
  on a controlled test link; a real controller is required before open-internet use
  and is a dedicated phase.
- **`max_datagram_size` pinned at 1200** (no PMTU discovery yet). Pad every
  Initial-bearing datagram to ≥1200 (anti-amplification).

This yields a client that completes real request/response exchanges against a
conformant H3 server while implementing only: QUIC v1 handshake + Initial/
Handshake/1-RTT packet protection, the mandatory frames, per-space ACK + loss/PTO,
both flow-control levels, the control + QPACK streams, static QPACK, and the H3
request/response mapping.

## 4. RFC coverage map (the conformance spine)

Authoritative wire constants the implementation pins from the IANA registries
(not from memory). Full per-section matrices go in `docs/RFC_COVERAGE.md` as each
phase lands; the load-bearing constants:

- **QUIC varint (9000 §16):** 2-bit first-byte prefix `00/01/10/11` → 1/2/4/8-byte
  big-endian, ranges 0..63 / 0..16383 / 0..2³⁰-1 / 0..2⁶²-1. Reject non-minimal →
  `FRAME_ENCODING_ERROR (0x07)`.
- **QUIC packets:** long-header types Initial=0x0, 0-RTT=0x1, Handshake=0x2,
  Retry=0x3; short-header = 1-RTT. Version v1 = `0x00000001`. Three PN spaces
  (Initial/Handshake/App) each with independent PNs, keys, ACK ranges, loss/PTO
  timers.
- **QUIC frames (subset a client needs):** PADDING=0x00, PING=0x01, ACK=0x02/0x03,
  RESET_STREAM=0x04, STOP_SENDING=0x05, CRYPTO=0x06, STREAM=0x08–0x0f, MAX_DATA=0x10,
  MAX_STREAM_DATA=0x11, MAX_STREAMS=0x12/0x13, CONNECTION_CLOSE=0x1c/0x1d,
  HANDSHAKE_DONE=0x1e, NEW_CONNECTION_ID=0x18. Initial/Handshake packets allow only
  PADDING/PING/ACK/CRYPTO/CONNECTION_CLOSE.
- **QUIC-TLS (9001):** initial_salt v1 `0x38762cf7f55934b34d179ae6a4c80cadccbb7f0a`;
  labels `"client in"`/`"server in"`, `"quic key"`/`"quic iv"`(len 12)/`"quic hp"`,
  `"quic ku"`; AEAD nonce = iv XOR left-padded full PN; header-protection sample at
  `pn_offset+4` len 16; `quic_transport_parameters` TLS ext = `0x39`; forbid
  `TLS_AES_128_CCM_8_SHA256`.
- **QPACK (9204):** encoder stream type 0x02, decoder stream type 0x03; **99-entry
  0-based** static table (Appendix A — *not* HPACK's 61-entry 1-based table);
  static-only section prefix = `0x00 0x00`; error `QPACK_DECOMPRESSION_FAILED
  (0x0200)`; reuses RFC 7541 prefix-int + Huffman verbatim.
- **HTTP/3 (9114):** uni stream types Control=0x00, Push=0x01, QPACK-Enc=0x02,
  QPACK-Dec=0x03. Frames DATA=0x00, HEADERS=0x01, CANCEL_PUSH=0x03, SETTINGS=0x04,
  PUSH_PROMISE=0x05, GOAWAY=0x07, MAX_PUSH_ID=0x0d. **Reserved-h2 (hard error
  `H3_FRAME_UNEXPECTED 0x0105`):** 0x02/0x06/0x08/0x09; **GREASE (ignore):**
  0x1f·N+0x21. Settings `MAX_FIELD_SECTION_SIZE=0x06`, `QPACK_MAX_TABLE_CAPACITY=0x01`,
  `QPACK_BLOCKED_STREAMS=0x07`; reserved-h2 settings 0x02–0x05 → `H3_SETTINGS_ERROR
  (0x0109)`. Error codes 0x0100–0x0110. Request pseudo-headers `:method :scheme
  :authority :path`; response `:status`. Request streams = client bidi IDs 0,4,8,…

## 5. Phased roadmap

Dependency-ordered. Pure codecs first (low-risk, reuse existing patterns,
0-alloc-gated, testable without networking); the crypto + transport engine is the
hard core; the H3 mapping and integration finish it. Each phase = its own PR
stack with conformance tests + `docs/RFC_COVERAGE.md` rows.

| Phase | Deliverable | RFC | Gate |
|---|---|---|---|
| **G.1** | *This design doc + roadmap* | — | review |
| **G.2** | `internal/bytesx` QUIC varint (`ReadVarint`/`WriteVarint`/`VarintLen`) | 9000 §16 | 0-alloc |
| **G.3** | `qpack/` — static-table-only **encoder** + full **decoder** (reuse `hpack/` Huffman + prefix-int; new 99-entry static table + 5 field-line reps + section prefix) | 9204 | 0-alloc (static path) |
| **G.4** | `quic/` **codec** — long/short packet headers, frame parse/write, PN encode/decode; Handler-visitor dispatch | 9000 | 0-alloc (pure codec) |
| **G.5** | `quic/` **crypto** — `tls.QUICConn` event loop, Initial-secret derivation, HKDF-Expand-Label, AEAD packet protection, header protection, key update | 9001 | unit vs test vectors (RFC 9001 App. A) |
| **G.6** | `quic/` **transport engine** — UDP I/O, connection establishment, PN-space state, streams + both flow-control levels, ACK generation, RTT/loss/PTO, NewReno stub, idle timeout, CONNECTION_CLOSE | 9000/9002 | engine (exempt from 0-alloc, like `conn/`) |
| **G.7** | `http3/` — control stream + SETTINGS, H3 frame codec, request/response mapping, wire QPACK enc/dec streams, malformed-message detection, GOAWAY, error routing | 9114 | conformance |
| **G.8** | `client/` `TransportH3` + `NewH3Client`; **Docker H3 conformance peer** (nginx-quic / Caddy) via the existing `it-*` harness; interop | 9114 | integration + coverage |
| **G.9** *(deferred)* | QPACK **dynamic table** (both directions + blocking), **server push**, **0-RTT**, **connection migration**, production **congestion control**, PMTU discovery | 9204/9114/9000 | per-feature |

Rough sequencing note: G.2–G.4 are independent pure codecs and could be built in
parallel; G.5 depends on G.4's packet layout; G.6 depends on G.4+G.5; G.7 depends
on G.6+G.3; G.8 depends on G.7.

## 6. Key risks & mitigations

Drawn from the RFC extractions; these are the traps that most threaten conformance.

1. **Per-space discipline (the classic QUIC bug).** Separate PN counters, keys,
   ACK ranges, and loss/PTO timers for Initial/Handshake/App. *Mitigation:* a
   `pnSpace` struct instantiated three times; never a shared counter; ACK
   generation keyed by space.
2. **Header-protection sample chicken-and-egg + AEAD nonce/AAD.** Sample at
   `pn_offset+4` assuming a 4-byte PN, unmask byte 0 to learn the true PN length,
   then unmask exactly that many bytes; nonce uses the **full reconstructed** PN,
   AAD = header through the unprotected PN; protect-then-HP on send, HP-then-open
   on receive. *Mitigation:* one well-tested `protect.go` with RFC 9001 App. A
   test vectors as the G.5 gate; no packet round-trips until it passes.
3. **The two integer codecs must not be conflated.** QUIC transport varint (2-bit
   prefix) ≠ HPACK/QPACK N-bit prefix integer. *Mitigation:* separate,
   clearly-named helpers in different packages (`bytesx.Varint` vs
   `hpack`/`qpack` prefix-int).
4. **Wrong H3/QPACK table & constants.** QPACK static table is 99-entry 0-based
   (not HPACK's 61-entry 1-based); H3 GOAWAY=0x07, MAX_PUSH_ID=0x0d, reserved-h2
   ≠ GREASE. *Mitigation:* pin every constant from the IANA registry in a single
   `const` block per package with the RFC section cited; conformance tests assert
   the wire bytes.
5. **No in-process H3 test peer (the coverage gap).** `httptest`+`EnableHTTP2`
   has no H3 analogue, and the from-scratch rule forbids quic-go in production.
   *Mitigation:* stand up a **real H3 server (nginx-quic / Caddy / nghttp3) in the
   existing Docker `it-*` harness** as the peer — planned early (G.8 scaffolding
   can start alongside G.6) so `http3/` can meet the 80% coverage gate. A
   *test-only* quic-go peer is a fallback if the Docker peer proves flaky.
6. **Bench-gate scoping.** The 0-B/op gate fits pure codecs (varint, QUIC/H3
   frames, static QPACK) but fights the packet engine (AEAD, packet assembly,
   ack-range structures). *Mitigation:* add only `quic/` (codec), `qpack/`,
   `internal/bytesx` to `bench-gate.yml`; the QUIC engine + `http3/` are **exempt
   like `conn/`** today.
7. **RFC-coverage gate blindness.** `scripts/rfc-coverage-gate.sh` hardcodes
   `for tag in RFC7540 RFC7541`. *Mitigation:* extend the loop with
   `RFC9000 RFC9001 RFC9002 RFC9114 RFC9204` in G.2, or conformance tests rot
   green.
8. **Retry re-derivation & handshake authentication.** A Retry changes the DCID
   *and* re-derives Initial keys; the client MUST verify the Retry integrity tag
   and validate the server's `original_destination_connection_id` (and
   `retry_source_connection_id`) against what it sent — a security check, not a
   nicety. *Mitigation:* explicit transport-parameter validation step; Retry
   handled in G.6 with its own conformance test.

## 7. Non-goals for the first working client (Phase G.1–G.8)

0-RTT / early data; server push; connection migration & path validation; active
CID rotation; NEW_TOKEN storage; QPACK dynamic table (either direction);
production congestion control; PMTU discovery; extended CONNECT / CONNECT-UDP /
HTTP datagrams; ALTSVC/ORIGIN analogues. All are RFC-optional for a client and
are Phase G.9+.

## 8. Open questions for review

1. **Congestion control depth for v1** — ship the NewReno stub (fixed window,
   test-link only) and defer a real controller to G.9, or build NewReno properly
   in G.6? (Affects whether the first client is open-internet-safe.)
2. **Test peer** — Docker nginx-quic/Caddy (best fit with `it-*`, real interop) vs
   a test-only quic-go peer (simpler, but a heavy dep even if test-scoped). Recommend
   Docker.
3. **`x/crypto` dependency** — AEAD/HKDF/ChaCha20 header protection want
   `golang.org/x/crypto` (stdlib-adjacent, but a new `require`). Acceptable under
   the "no `net/http`/`x/net/http2`" rule (it's crypto, not an HTTP stack), or
   hand-roll HKDF-Expand-Label on `crypto/hmac` to stay stdlib-only? Recommend
   `x/crypto`.
4. **Scope confirmation** — is the G.1–G.8 minimal-conformant client (static QPACK,
   no push/0-RTT/migration) the right v1 target, with G.9 as follow-on?

---

# Post-v0.9: Concurrent Multiplexing — reader-goroutine actor model

> Plan for lifting the HTTP/3 client from one-blocking-`Do`-at-a-time to real per-connection multiplexing. Verified against the code in `quic/` and `http3/`; every fix below is folded into the architecture, not appended as an afterthought. Four adversarial reviews were reconciled: the single-mutex core survives, but it shipped two deterministic stalls, one wire-conformance regression, one shutdown panic, and three concrete reentrancy deadlocks that this revision closes.

## 1. Motivation

Poseidon is a load generator; its whole reason to exist is to put many requests in flight on one connection. Today the H3 `Client` funnels every request through `c.poll` under an `inFlight` guard (`http3/client.go:160`, `ErrConcurrentUse`): a single `Do` owns the connection, blocks on it, and no second request can run until it returns. That defeats the point of HTTP/3 stream multiplexing. This plan replaces the caller-drives-the-engine model with a dedicated reader goroutine per connection plus channel-based per-stream wakeups, so N goroutines can call `Do` on one `Client` concurrently and share the QUIC connection the way the protocol intends.

## 2. Current model vs. target model

**Current (v0.9).** No connection locks (`quic/conn.go:38`). The single active `Do` calls `c.poll` in a loop; `Poll` = `flush → readWithPTO → recvDatagram → drainBuffered → flush` (`conn_recv.go:48`), all on the caller's goroutine, under the caller's request `ctx`. Receive, loss detection, PTO, idle timeout, key-update, ACK emission, and H3 control-stream servicing all ride that one goroutine. Correctness is trivial because there is exactly one goroutine; concurrency is impossible because there is exactly one goroutine. `inFlight` enforces that fact.

**Target.** One **reader goroutine** per connection owns the engine (`Poll` + H3 control servicing) for the connection's lifetime. Request goroutines (`Do`) seal and send their own frames and block on **per-stream channel wakeups** that compose with `ctx`. A single connection mutex `c.mu` guards all `Conn` mutable state and the wire, held only for CPU-bound processing bursts and **never across the blocking socket read**. That release-across-`pc.Read` is the entire point: a `Do` can seal and send a request while the reader is parked waiting for datagrams.

The design rests on one decision: because the QUIC receive path deeply mutates *send*-path state (cwnd, `bytesInFlight`, `sendPN`, sealer/phase, the streams map), the state does not partition, so H2's `wmu`-only model is insufficient. The minimal correct model is **one mutex covering everything, dropped only across the read**, plus channels for wakeups because a `Do` must `select` readiness against `ctx.Done()` — something `sync.Cond` cannot do.

## 3. The actor architecture

### 3.1 The reader goroutine (in the H3 `Client`, one per connection)

Spawned by `NewClient` **after** `Establish` finishes the handshake, so it never overlaps the single-goroutine handshake loop (`conn_recv.go:19`).

```go
func (c *Client) readLoop() {
    defer close(c.readerDone)
    for {
        if err := c.conn.Poll(c.connCtx); err != nil {   // QUIC: recv+decrypt+dispatch+loss+flush
            c.fatal(err); return
        }
        if err := c.serviceControl(); err != nil {        // H3: SETTINGS/GOAWAY/critical-stream + QPACK uni
            c.fatal(err); return
        }
    }
}
```

Reader responsibilities (everything the correctness contract keeps reader-side):
- **Sole caller of `Poll`** and thus of `recvDatagram`/`drainBuffered`/`flush`/`readWithPTO`. All of receive, loss detection, PTO probing, idle timeout, key-update commit/discard, and ACK emission live here (INV-4/5/6/7).
- **Services the H3 control stream** after each poll: accept uni streams, `routeUni`, `readControl` (SETTINGS → `maxFieldSection`, GOAWAY → `goaway`), `checkCriticalStreams`. The whole H3 control-state block (`client.go:147-152`: `pendingUni`, `control`, `controlReader`, `qpackEnc/Dec`, `settingsRead`) becomes **reader-exclusive** — no `Do` touches it, so it needs no lock. `serviceControl`/`readControl` run on the reader goroutine but **outside** `c.mu` (after `Poll` returns), calling the public `Recv`/`StopSending`; this is correct **only because `Poll` never returns holding `c.mu`** — stated as an explicit postcondition of the restructured `Poll` (§3.2).
- **Signals stream owners** of progress (§3.3).
- **Latches the terminal error** and wakes every blocked `Do` on exit via the single-close latch (§3.3).

`Poll` takes the **connection-lifetime** `connCtx`, not any request `ctx` — so a per-request cancel can no longer poke the shared socket read deadline (INV-4). The `readWithPTO` watchdog (`pto.go:203`) rides `connCtx` and fires only on `Close`.

### 3.2 The write mutex `c.mu sync.Mutex` (in `quic.Conn`)

One `sync.Mutex` (not RWMutex — nearly every critical section mutates) guards **all `Conn` mutable state and the wire**. Discipline mirrors H2's "public method locks, internal helpers assume locked":

- **Public, lock-taking wrappers**: `Poll`, `Send`, `OpenStream`/`OpenUniStream`, `Recv`, `Reset`, `StopSending`, `AcceptUniStream`, `CloseWithError`.
- **Internal, assume-held**: `recvDatagram`, `flush`, `flushControl`, `sealPacket`, `detectLost`, `onPTO`, `commitKeyUpdate`, `discardStaleKeys`, `writeAppFrames`, `onStreamConsumed`, `fail`, and every `On*` frame handler — plus the new `closeWithErrorLocked`, `resetLocked`, `stopSendingLocked` (§5, PR 2a).

**The one structural change to `Poll`** — the blocking `pc.Read` runs with `c.mu` released:

```
lock c.mu
flush()                                  // KEEP the leading flush (conn_recv.go:59): retransmits
                                         //   queued by reader-side detectLost must go out before we park
dl := lossDetectionDeadline()            // min(loss, idle); nothing in flight ⇒ idle scale
c.armedReadDeadline = dl                 // published under the lock (see re-arm, §4 INV-4)
SetReadDeadline(dl)
if ctx.Err() != nil { unlock; return }   // KEEP the arm→recheck guard (pto.go:230): a connCancel
                                         //   that fired before we armed must not be lost
unlock c.mu
n, err := pc.Read(pollBuf)               // BLOCKING, UNLOCKED — a Do may seal+send here
lock c.mu
now := clock()                           // RTT rule: timestamp AFTER reacquiring, so a Do's hold
                                         //   time does not inflate the ACK-delay/RTT sample
if isTimeout(err) { handleExpiry(now) }  // onPTO / detectLost / idleClose(now>=idleDeadline) / flush
else { recvDatagram; drainBuffered; discardStaleKeys; maybeBroadcastSendWindow(); flush() }
unlock c.mu
signalReadyForAdvancedStreams()          // non-blocking cap-1 pokes; may also run under the lock
```

Processing bursts hold `c.mu` for sub-millisecond CPU work (decrypt + dispatch); `pc.Write` under the lock is a non-blocking UDP send. Because every seal is under `c.mu`, INV-1 (unique `sendPN`), INV-2 (key-update commit vs. seal), INV-5 (cwnd/`bytesInFlight`/`retransQueue`), and INV-8 (single close) all follow from one invariant rather than eight fine-grained proofs.

**RTT-skew rule (from review, folded in):** `Do` critical sections must be **per-packet** — `Send` releases `c.mu` between `writeStreamFrame`/seal iterations and never holds it across a whole `sendAll`. Combined with "reader timestamps after reacquiring," this keeps ACK-delay/RTT skew at microseconds.

### 3.3 Stream wake mechanism (the full wake vocabulary)

A blocked `Do` waits on channels, not `sync.Cond`, precisely so it can `select` readiness against `ctx.Done()`. Two consumer helpers share one per-stream channel:

Primitives:
- **`s.ready chan struct{}` (cap 1, per `quic.Stream`)** — reader→Do progress signal, for both recv-readiness and send-unblock. Never closed (no send-on-closed panic). Producer uses non-blocking `select { case s.ready <- struct{}{}: default: }` (cap-1 coalescing; the reader never blocks on a slow consumer — the H2 `push` rule).
- **`c.streamCredit chan struct{}` (cap 1, per `quic.Conn`)** — signaled from `OnMaxStreams`; lets a `Do` blocked on the cumulative stream limit wake when the peer raises it (§5, PR 2d).
- **`c.done chan struct{}` + `c.closeErr` + `c.terminated bool`** — the single-close latch (below).
- **`Client.qpackReady chan struct{}` (broadcast, per `http3.Client`, Q4)** — the QPACK blocked-stream wake (RFC 9204 §2.1.3). Where the two cap-1 signals above hand a token to ONE waiter, a single insert-count advance can unblock ANY number of parked decodes, each waiting on a different Required Insert Count, so `signalQPACKReady` (called by the reader after `readQPACKEncoder` advances the insert count) **closes** the channel to wake all current waiters and installs a fresh one. A blocked `Do` captures the current channel under `qpackReadyMu`, re-reads the published insert count (level-triggered), and re-parks on the new channel if still short — close-then-replace closes the same lost-wakeup window the cap-1 signals do. The wait selects `qpackReady` / request `ctx.Done()` / `connCtx.Done()`, so it is bounded by the request and by teardown and never hangs; `qpackReadyMu` is a leaf lock never held across the select (R2).

**Producer (reader, under `c.mu`) — `signalReady(s)` is called from:**

| Event | Why it must wake a sender/reader | Source |
|---|---|---|
| `OnStream` / `OnResetStream` | response data / fin / reset arrived | recv-readiness |
| `OnMaxStreamData` | this stream's send credit rose | `blockStream` unblock |
| `OnMaxData` (broadcast all streams) | connection send credit rose | `blockConn` unblock |
| **`OnStopSending` → `resetLocked`** | flips `sendReset`; a parked sender's next `Send` returns `ErrStreamReset` and must fall through to reading the response | **(fix F4/F: was missing)** |
| **cwnd freed** — end-of-burst broadcast when `bytesInFlight` decreased (`onPacketAcked`/`removeInFlight`/`detectLost`/`discardSpace`) or `OnMaxData` processed | `blockCong` has **no frame-driven wake**; cwnd opens only via ACK/loss accounting, which otherwise signals nothing | **(fix F1/BREAK2/F2: the critical missing wake)** |

The cwnd broadcast is gated to avoid O(n) per frame: a burst sets `c.sendWindowGrew` when it frees flight or connection credit; `maybeBroadcastSendWindow()` at the end of the burst does one O(n) `signalReady` sweep over `c.streams` and clears the flag. Same cost class the design already accepts for `OnMaxData`, incurred once per window-growing burst rather than per packet.

**Consumers (Do), two helpers over the one channel:**

```go
// response read loop
func (s *Stream) WaitReadable(ctx context.Context) error {
    select {
    case <-s.ready:      return nil          // recheck predicate under c.mu after waking
    case <-ctx.Done():   return ctx.Err()    // per-request cancel — only this Do
    case <-s.conn.done:  return s.conn.closeErr
    }
}

// sendAll loop — owns the pacing-timer case internally
func (s *Stream) WaitSendable(ctx context.Context) error {
    s.conn.mu.Lock()
    var timer <-chan time.Time
    if s.sendBlock == blockPace {            // blockPace refills on WALL-CLOCK, no inbound event
        timer = time.After(s.conn.pacingRefillDelay())
    }
    s.conn.mu.Unlock()
    select {
    case <-s.ready:      return nil
    case <-timer:        return nil          // pacing bucket has refilled; retry Send
    case <-ctx.Done():   return ctx.Err()
    case <-s.conn.done:  return s.conn.closeErr
    }
}
```

`s.sendBlock blockKind` is recorded under `c.mu` during the short `Send` (from `grantable`'s second return, `send.go:96`), keeping the unexported `blockKind` inside `quic`. `blockCong`/`blockStream`/`blockConn` all resolve to an inbound-event wake via `s.ready`; only `blockPace` arms a timer. Two helpers over one channel is safe because a `Do` drives its stream **send-then-recv sequentially** (`roundTrip` sends, then loops reading) — the waits never overlap.

**Level-triggered, not edge:** after each wake, `Do` re-reads its predicate (`RecvState()` / `grantable` via a retried `Send`) **under `c.mu`** before deciding to block again. Cap-1 buffering + recheck closes the classic check-then-block lost-wakeup window: a token deposited between the `Do`'s unlock and its `select` sits in the buffer and is drained immediately; the channel send/receive supplies the needed happens-before.

**Single-close latch (fixes the double-`close(done)` panic + `closeErr` clobbering, F3/BREAK4/F5):**

```go
func (c *Conn) terminateLocked(err error) { // c.mu held
    if c.terminated { return }
    c.terminated = true
    if c.closeErr == nil { c.closeErr = err }
    close(c.done)
}
```

**Every** teardown path funnels through it — reader `fatal`, `Client.Close`, `fail → closeWithErrorLocked`, `idleClose` (`pto.go:188`), `statelessResetReceived` (`conn_recv.go:159`), and the AEAD-limit close inside `sealPacket` (`conn_recv.go:518`). First error wins; all later callers no-op. There are otherwise **five** independent closers, several of which race on every ordinary `Close`.

### 3.4 Reader-owned receive state vs. cross-path state under `c.mu`

**Reader-private (sole mutator, no contention):** `pollBuf`, `cryptoRecv`, `acks`, `largestRecv`/`haveRecv`, read-side `keys`/`ku` openers, `authFailures`, `rtt` samples, `lastActivity`. Covered by `c.mu` during the burst only for uniformity; the reader's *waiting* is spent in the unlocked `pc.Read`, so nobody contends these.

**Genuinely cross-path, requires `c.mu`** (both reader and `Do` touch): `sendPN`, `oneRTTSealer`/`ku.phase`/`appSendCount`, `cwnd`/`bytesInFlight`/`ssthresh`/`ccBytesAcked`, `retransQueue`, **`sent[spaceApp]`** (Do writes it via `sealPacket → onPacketSent`; reader reads it in loss detection — see the deferral correction in §6), `pendingCtrl`/`pathRespPending`, the `streams` map + `nextBidiStreamID`/`openedBidi`/`openedUni`, per-`Stream` `sendMax`/`sendOffset`/`finSent`/`sendReset`/`sendBlock` and the `recv` buffer, `connMax`/`connSent`/peer `InitialMaxStreams*`, `armedReadDeadline`, and `closed`/`terminated`/`peerClose`.

**Do-side recv accessors are in the lock discipline (fix F5):** `roundTrip`/`endOfResponse` read `Finished()`/`ResetReceived()`/`ResetCode()`, which read `recv.fin`/`recv.finalSize`/`len(recv.data)`/`len(recv.pending)` — all mutated by the reader under `c.mu` in `OnStream`/`OnResetStream`. Replace the three unlocked calls with **one locked snapshot**:

```go
func (s *Stream) RecvState() (finished, reset bool, code uint64) // takes c.mu
```

`Stream.ID()` stays lock-free (immutable after `OpenStream` returns). The `Recv` byte slice returned to the `Do` is safe unsynchronized: `insert`/`absorb` only extend past the returned prefix and `FrameReader.Feed` copies (`http3/stream.go:55`) — append-only-disjoint, verified not a race.

### 3.5 Ownership table & lock ordering

**Goroutine-safe (any `Do`, concurrently):** `Send`, `Recv`, `OpenStream`/`OpenUniStream`, `Reset`, `StopSending`, `CloseWithError`, `WaitReadable`, `WaitSendable`, `RecvState`, and H3 `Do`.
**Reader-owned (never a `Do`):** `Poll`, `serviceControl`, receive dispatch, loss/PTO/idle, key-update, ACK flush, the whole H3 control-state block.
**Writer-owned (Do):** the per-`Do` request/response `FrameReader` (already local), and — after PR 2d — a **stack-local** `qpack.Encoder`/`Decoder` per request.

Primitives: (1) `c.mu` — the one mutex, guards all state + wire; (2) `s.ready` cap-1; (3) `c.streamCredit` cap-1; (4) `c.done` + `c.closeErr` (happens-before via the channel close); (5) H3 `maxFieldSection atomic.Uint64` (init `^uint64(0)`); (6) H3 `goaway atomic.Uint64` (init `^uint64(0)` = "none", so `streamID >= load(goaway)` is false until a real GOAWAY lands — one atomic, no `haveGoaway` bool).

Ordering rules (the entire deadlock argument — there is effectively one lock):
- **R1 — `c.mu` is the only mutex.** Nothing to invert against; the channels/atomics are lock-free.
- **R2 — `c.mu` is never held across a wait.** Not across `pc.Read` (reader releases first), not across `WaitReadable`/`WaitSendable` (Do reads its predicate under `c.mu`, releases, *then* selects), not across `<-c.done`, **not across `<-readerDone` in `Close`** (fix F6 — see §4 lifecycle).
- **R3 — QUIC never up-calls H3 while holding `c.mu`.** The dependency is one-way (H3→QUIC); `signalReady` only pokes a channel.
- **R4 — the reader never blocks on state a `Do` holds.** Its only block is the unlocked `pc.Read`; `Do` critical sections are finite seal+write, no waiting.

## 4. How each receive-path invariant is preserved

- **INV-1 (unique `sendPN` / nonce uniqueness).** Every `spaceApp` seal is under `c.mu`; `sendPN[sp]`++ and the write are atomic under the lock, so no two goroutines share a packet number. `Do` seals only `spaceApp`; Initial/Handshake seals are pre-reader (`Establish`) or reader-side (probes). The `OnAck largest >= sendPN` guard (`conn_recv.go:670`) cannot false-positive because seal+write are one critical section.
- **INV-2 (key-update commit vs. seal).** `commitKeyUpdate` (`crypto_keyupdate.go:129`) swaps `oneRTTSealer`, zeroes `appSendCount`, flips `ku.phase` — under `c.mu`; `sealPacket`'s reads of the same three are under `c.mu`. They cannot interleave, so no packet is sealed with a phase bit disagreeing with its key. A `Do` sealing under the old phase just before the reader commits is legal (§6.2). Client stays a pure key-update **responder**, which is what makes this tractable under one lock.
- **INV-3 (flow-control credit ordering).** `Recv` runs on the `Do` goroutine under `c.mu`; after `onStreamConsumed` queues `MAX_STREAM_DATA`/`MAX_DATA` into `pendingCtrl`, `Recv` calls **`flushControl()` before releasing** — the consuming goroutine grants its own credit; the reader parked in `pc.Read` need not wake. Refund hysteresis lives in `onStreamConsumed` (`recvflow.go:25`), not in `Poll` cadence, so grant count is unchanged — just delivered without waiting for the next datagram. **`flushControl` emits only `pendingCtrl`/PATH_RESPONSE, never the pending ACK**, so ACK cadence stays reader-owned.
  - **PATH_RESPONSE 1200-byte padding preserved (fix BREAK3).** `OnPathChallenge` appends PATH_RESPONSE **into `pendingCtrl`** and sets `pathRespPending` (`conn_recv.go:1041`); the reader's `flush` consumes both together and passes `padTo1200` (`sealPacket:513`, the §8.2.2 expansion shipped in PR #153). Because a `Do`-triggered `flushControl` can fire between the reader queueing the response and its terminal flush, **`flushControl` replicates the padding**: `padPath, c.pathRespPending = c.pathRespPending, false` and passes `padPath` to `sealPacket`. Without this, the datagram goes out under 1200 bytes — an on-wire RFC 9000 §8.2.2 violation that regresses the existing conformance test.
- **INV-4 (loss/PTO accounting + the read deadline).** Loss detection and PTO stay reader-side under `c.mu`, reacquired after the unlocked `pc.Read` timeout. **The stale-deadline stall is closed (fix F2/BREAK1/F1):** `c.armedReadDeadline` is published under `c.mu` before parking. Every `Do`-side send epilogue (`writeAppFrames` tail, `flushControl` tail), still under `c.mu` after `sealPacket`:
  ```
  newDL := lossDetectionDeadline()
  if newDL.Before(c.armedReadDeadline) {
      SetReadDeadline(newDL); c.armedReadDeadline = newDL   // legal against a blocked Read
  }
  ```
  This shortens the parked reader's deadline from the idle scale (10 s, or a large negotiated `max_idle_timeout`) to the correct sub-second PTO when a `Do` puts a packet in flight. The reader treats the early timeout as a normal iteration: reacquire, recompute (now seeing the in-flight packet), run `onPTO`/`detectLost`/`flush`. The expiry branch keeps `idleClose` gated on `now >= idleDeadline`, so an early PTO wake never mis-fires an idle close before probing. `drainBuffered`'s past-deadline pokes (`conn_recv.go:103`) are reconciled by re-arming `armedReadDeadline` after the drain loop. `detectLost` still runs after `onAckRange` folds RTT within `recvDatagram` — ordering unchanged.
- **INV-5 (cwnd/`bytesInFlight`/`retransQueue` consistency).** All three mutate only through internal helpers under `c.mu`. The send-side *liveness* half is the critical missing piece the base design broke: `grantable` returns `blockCong` when `bytesInFlight >= cwnd` and `blockPace` on an empty bucket (`send.go:113-124`) with **no frame emitted**, and cwnd opens only via `onPacketAcked` (`cc.go:45`) / `detectLost` / `discardSpace` — none of which signal anything in the base plan. With `kInitialWindow = 12000` (`cc.go:10`), any request body over ~12 KB parks the first `Send` forever. **Fixed by the cwnd broadcast** (§3.3): the reader signals every stream after any burst that frees flight, and `blockPace` uses the `WaitSendable` timer. `OnStopSending` also signals so a STOP_SENDING-driven reset wakes its parked sender.
- **INV-6 (ACK cadence — now bounded deferral + piggyback, RFC 9000 §13.2.1).** The reader still holds `c.mu` across the whole `recvDatagram` + `drainBuffered`(≤512) + terminal `flush`, so a burst is still processed atomically. What changed (`perf(quic): defer+piggyback ACKs`): a burst no longer *always* emits an ACK datagram. Per §13.2.1, an Application-space (1-RTT) ACK is sent **immediately** only when the burst delivered an ack-eliciting packet **out of order** (a reorder below the top of the received range, or a gap above it) **or** the **2nd ack-eliciting packet since the last ACK** — both to drive the peer's loss detection. Otherwise the ACK is **deferred** up to our advertised **max_ack_delay** (we advertise none ⇒ the §18.2 default, 25 ms): the owed ACK rides the **next outbound STREAM packet** (`writeStreamFrame` prepends it via `buildACK` when it fits the 1200-byte datagram, then `acks[spaceApp].acked()`), a coincident credit-grant/probe packet (`flush` coalesces any owed ACK it is already building a packet for), or, failing both, fires on a **max_ack_delay timer**. The timer piggybacks the existing `armedReadDeadline` mechanism (INV-4): `c.ackDeadline` shortens the reader's read deadline, and `c.armedLossDeadline` (the pre-ACK loss/idle deadline) lets `handleExpiry` tell an ACK-only wake from a genuine loss/PTO/idle expiry, so **a deferred ACK never provokes a spurious PTO probe**. The decision fields (`immediate`, `elicitCount` on `ackTracker`; `ackDeadline` on `Conn`) are reader-owned; `Do`-side sends only *consume* an owed ACK by piggybacking it under `c.mu`. Ordering detection reuses `ackTracker`'s range set (largest-received + gaps). Deferral engages **only** on a transport that can arm a read deadline (real UDP); a deadline-less unit pipe cannot schedule the fallback timer, so it ACKs immediately — never a stall. Initial/Handshake spaces and the pre-1-RTT path are unchanged (immediate). **Wire change:** the request/response path drops from ~2 send syscalls/GET (STREAM out + ACK out) to ~1 (ACK piggybacked); ACK frequency under steady bulk transfer is governed by the 2nd-ack-eliciting rule, so it stays RFC-correct for the peer's loss recovery. **The loss-relay interop run is the wire gate** — under loss/reorder the immediate-ACK triggers must keep the peer's loss detection driven; a wrong cadence shows as spurious retransmits or stalls there, not in local unit tests. Credit grants still do not coalesce into a *dedicated* ACK packet from `flushControl` (that path remains ACK-free; the ACK stays reader/piggyback-owned).
- **INV-7 (key retention window).** `discardStaleKeys` stays in `Poll` (`conn_recv.go:72`), reader-side, under `c.mu`; the prev-key 3×PTO retention window is untouched. No send-side self-rotation introduced.
- **INV-8 (single close on `c.closed`).** The single CONNECTION_CLOSE frame gates on `c.closed` under `c.mu` inside `closeWithErrorLocked`; the `close(c.done)`/`closeErr` latch is `terminateLocked` (§3.3). Exactly one close frame, exactly one channel close, first error wins.
- **Verified-preserved without change:** coalesced-packet handling (whole datagram processed inside one `c.mu` hold), packet-number-space separation, AEAD sealer/opener state, RTT sampling (with the timestamp-after-reacquire + per-packet-critical-section rules folded into §3.2).

## 5. PR breakdown (2a–2d)

Each PR is independently shippable, keeps the 3-server interop matrix + RFC gates green, and is bisectable. `inFlight` is deleted only in **2d**.

### PR 2a — Introduce `c.mu`; add the `…Locked` internals; zero behavior change
**Scope.** Add `c.mu sync.Mutex`. Split every public entry point into a lock-taking wrapper over an assume-held `…Locked` internal. Thread the "holds `c.mu`" assumption through `recvDatagram`/`flush`/`sealPacket`/`detectLost`/`onPTO`/`commitKeyUpdate`/`writeAppFrames`/`onStreamConsumed`/`fail`/frame handlers. **Ship `closeWithErrorLocked`, `resetLocked`, `stopSendingLocked` from the start** and repoint the three in-tree reentrant up-calls to them (fix F4/BREAK5/F3):
- `OnStopSending → s.Reset` (`conn_recv.go:956`) → `resetLocked`
- `sealPacket → CloseWithError` at the AEAD limit (`conn_recv.go:518`) → `closeWithErrorLocked`
- `fail → CloseWithError` (`close.go:51`) → `closeWithErrorLocked`

`Establish` calls `fail` pre-reader; wrap its body under `c.mu` so `fail` is unconditionally an assume-held internal (one convention). `Poll` still takes the caller's ctx and holds `c.mu` for its whole body (single goroutine today ⇒ holding across the read is harmless).
**Acceptance gate.** The lock is uncontended, so `-race` proves nothing here — **the fault-server suite is the real gate** (it triggers the `fail`/STOP_SENDING/AEAD paths that self-deadlock without the `Locked` variants). Add a grep/`go vet`-style check that **no `…Locked` path calls a public wrapper** (the three above are the seed list). Full unit suite + interop matrix green.

### PR 2b — Wake vocabulary + close latch + `WaitReadable`/`WaitSendable`; additive, inert
**Scope.** Add `s.ready` (cap 1) + `signalReady` (non-blocking) and call it under `c.mu` from `OnStream`/`OnResetStream`/`OnMaxStreamData`/`OnMaxData`/**`OnStopSending`**, plus the **cwnd broadcast** (`c.sendWindowGrew` set by `onPacketAcked`/`removeInFlight`/`detectLost`/`discardSpace`/`OnMaxData`, swept at end of burst). Add `c.streamCredit` (signaled from `OnMaxStreams`). Add `c.done`/`c.closeErr`/`c.terminated` + `terminateLocked`. Record `s.sendBlock` in the short-`Send` path. Add `WaitReadable`, `WaitSendable` (owns the `blockPace` timer), `RecvState`. **Do not** spawn a reader or change `Do` — signals are produced but unconsumed (`Do` still inline-polls), so the PR is genuinely inert. The full wake mechanism lands here because it defines the `WaitSendable` contract.
**Key risk.** A blocking send on `ready` stalling the reader (must be `select…default`); an edge-triggered lost wake. Mitigation: cap-1 coalescing + level-triggered recheck, unit-tested in isolation.
**Test.** Unit: `signalReady` wakes a `WaitReadable`; cwnd-broadcast wakes a `WaitSendable`; `blockPace` timer fires with no signal; coalescing under repeated signals; `terminateLocked` wakes all and is idempotent under concurrent callers. `-race`. Interop unchanged (signals inert).

### PR 2c — Reader goroutine drives the engine; `Do` stops self-polling; still single-request (`inFlight` retained)
**Scope.** QUIC: restructure `Poll` per §3.2 (leading `flush` under lock → compute+publish `armedReadDeadline` → arm → **ctx recheck** → unlock → `pc.Read` → relock → timestamp → process). Move the `readWithPTO` watchdog to `connCtx`. Wire the **`Do`-side deadline re-arm** into `writeAppFrames`/`flushControl` tails (INV-4). Add `flushControl` **with PATH_RESPONSE padding** to the `Recv` path (INV-3, BREAK3). Route `idleClose`/`statelessResetReceived`/AEAD-close through `terminateLocked` (F5). H3: `NewClient` spawns `readLoop` on `connCtx`; `Do` drops its `c.poll` calls in `roundTrip`/`sendAll` and the Do-entry `serviceControl` (`client.go:243`); `roundTrip`'s loop becomes `Recv → parse → RecvState → WaitReadable(ctx)`; `sendAll` parks on `WaitSendable(ctx)` when `Send` returns short. Publish `maxFieldSection`/`goaway` as atomics. `Close` coordination: **latch the intended error via `terminateLocked` BEFORE `connCancel()`** (so the reader's `fatal(connCtx.Err())` finds the latch taken and in-flight `Do`s see the graceful error, not `context.Canceled`), then `closeWithErrorLocked` + `pc.Close()`, **release `c.mu`, then `<-readerDone`** (F6 — never wait holding the lock). `inFlight` **stays**: one `Do` at a time, so the only concurrency is reader ↔ single `Do` — small, bisectable blast radius.
**`NewClient` ordering (fix, review):** start the reader **before** `sendAll(SETTINGS)`, so a flow-control-blocked SETTINGS send is unblocked by the peer's `MAX_DATA` the reader processes — otherwise a startup deadlock.
**Test.** Unit: large upload where the peer blocks on our `MAX_DATA` completes (proves `flushControl` breaks INV-3); a >12 KB body completes (proves the cwnd broadcast); `Close` during an in-flight `Do` wakes it with the graceful error; per-request ctx cancel aborts *its* stream and the connection survives (rewrite `client_ctx_test.go`); PATH_RESPONSE-via-`flushControl` datagram is ≥1200 bytes. **New interop coverage required in-scope:** a plain large-body upload echo (≥256 KB, server reads all before responding) — the matrix has no completed-upload-past-cwnd test today, so without it the cwnd stall ships green. **Add a loss-relay run as a required 2c gate** — the stale-deadline stall is invisible without loss. `make test-race`; full interop matrix under `-race`.

### PR 2d — Enable real concurrency: stream-credit wake, per-request QPACK, remove `inFlight`
Commit order matters for bisect:
1. **`OnMaxStreams` stream-credit wake + GOAWAY/open atomicity (fix F4).** `OpenStream` on the cumulative `openedBidi` limit returns `ErrTooManyStreams` immediately today, and `OnMaxStreams` (`conn_recv.go:1007`) raises the cap but signals nothing — so a second wave of >100 concurrent `Do` against Caddy races the peer's incremental MAX_STREAMS. Make `OpenStream` waitable on `c.streamCredit` with `ctx` (documented backpressure: a `Do` may block on stream credit until the peer grants or ctx fires). Fold the GOAWAY gate **into `OpenStream` under `c.mu`** (gate-then-open atomically) so an `ErrGoAway`-rejected request never allocates a stream; on any post-open abandon, `Reset` the stream so `maybeRetire` reclaims the `c.streams` entry and `openedBidi` (closes the TOCTOU leak that is live under 2d concurrency + graceful shutdown).
2. **Per-request QPACK codecs.** Give `roundTrip` a **stack-local** `qpack.Encoder`/`Decoder` per call instead of shared `c.enc`/`c.dec`. `qpack.Encoder` is an empty struct (`qpack/encoder.go:10`) — trivially safe. `qpack.Decoder` holds shared Huffman scratch buffers whose emitted slices alias them (`qpack/decoder.go:10-13`), so per-request `Decoder` is **mandatory, not an optimization**. Static-only QPACK ⇒ stateless per request ⇒ lock-free. Cost: losing scratch reuse (per-request allocs, off the gated frame/hpack bench paths).
3. **Delete `inFlight` + `ErrConcurrentUse` + `client_concurrency_test.go`** (last commit, so a bisect between halves still has a defined single-request contract). Everything else is covered: `OpenStream`'s id/map mutation and every seal are under `c.mu` (2a); streams wake independently (2b/2c); control state is reader-owned (2c). `Do` is now safe from N goroutines.
**Test — the acid test.** New `TestConcurrent_*`: N (e.g. 64) concurrent `Do` on one `Client` against each interop server, under `make test-race`. **Must include bodies >12 KB** (validates the cwnd wake under contention) and **multiple waves whose cumulative bidi stream count exceeds the peer `initial_max_streams_bidi`** — 100 for Caddy/quic-go, 128 for nginx/aioquic — (validates the `OnMaxStreams` wake). Per-request cancel of one `Do` among many leaves the rest completing; `Close` mid-flight fails all in-flight `Do` cleanly with the graceful error; a **loss-relay run** with concurrent large uploads. Confirm the frame/hpack bench-gate (0 B/op) is untouched and no QUIC hot-path alloc regressions.

## 6. Deferred / out-of-scope

- **Fine-grained locking.** One `c.mu` until profiling shows contention. **Correction to the earlier deferral text (fix F7):** a future "split reader-private substate into a lock-free region" micro-opt may cover openers/ack-trackers/RTT/`lastActivity` **but must exclude `sent[spaceApp]`, `cwnd`/`bytesInFlight`, and `retransQueue`** — those are written by `Do` goroutines (`Send → sealPacket → onPacketSent`, `Reset → scrubResetStreamData`) and are genuinely cross-path, not reader-private. Splitting them out as stated would be an instant race.
- **Releasing `c.mu` mid-`drainBuffered`** to cut send latency during large receive bursts — safe (the only cross-batch invariant is the single terminal ACK flush) but deferred; batches are CPU-only and short.
- **Targeted (vs. broadcast) cwnd wake.** The end-of-burst O(n) `signalReady` sweep is per-window-growing-burst; a later refinement can signal only streams with unsent buffered intent. Deferred until profiling shows the sweep on the hot path at high stream fan-out.
- **QUIC self-initiated key update** — unchanged; the client stays a responder, which is exactly what keeps INV-2 tractable under one lock.
- **Do-that-never-reads backpressure** — no new mechanism: an unread response stops granting credit and the peer stalls that one stream via existing flow control; no connection-wide impact.
- **0-RTT, connection migration, multi-conn pool** — out of scope; this delivers per-connection multiplexing only.

## 7. Residual risks the reviewers raised that remain

Honestly, after folding the fixes:
1. **Pacing wake is timer-polled, not event-driven.** `blockPace` resolves via a `time.After(pacingRefillDelay)` in `WaitSendable`, so a purely pacing-limited sender wakes on a bounded timer rather than an exact event — a small approximation (occasional early recheck), not a stall. If `pacingRefillDelay` is mis-estimated low, it busy-rechecks; keep the estimate ≥ one token interval.
2. **O(n) cwnd broadcast cost at high concurrency.** For a load generator running hundreds of concurrent streams, the end-of-burst sweep is O(streams) roughly per RTT. Accepted as the same class as `OnMaxData`, but it is more frequent under bulk transfer; item 3 of §6 is the escape hatch if it shows up in a CPU profile.
3. **RTT-skew correctness depends on discipline, not structure.** The microsecond-skew guarantee relies on `Send` keeping per-packet critical sections and the reader timestamping after reacquiring `c.mu`. A future change that holds `c.mu` across multiple seals would silently inflate RTT/ACK-delay samples with no test failure. Enforce "Send is per-packet" in review; there is no compiler check.
4. **Stream-limit backpressure is a semantic choice, caller-visible.** Blocking `OpenStream` on `c.streamCredit` means a `Do` can wait indefinitely (until ctx) if the peer never raises `MAX_STREAMS` (e.g. never closes streams). The alternative — surfacing `ErrTooManyStreams` to the caller — pushes the wave-shaping decision up. This plan picks blocking-with-ctx and documents it; a caller that prefers fail-fast must set a request deadline.
5. **Wire-behavior diff, not a bug.** Credit grants decoupled from ACK packets (INV-6) produce more small ack-eliciting packets and more peer ACK traffic. Interop packet-count assertions will shift; call it out in PR descriptions so the diff is not mistaken for a regression.

**Files to change:** `quic/conn.go` (`c.mu`, `done`, `closeErr`, `terminated`, `armedReadDeadline`, `streamCredit`, `sendWindowGrew`), `quic/conn_recv.go` (`Poll` read-unlock restructure, `flushControl` with PATH_RESPONSE padding, `terminateLocked` routing), `quic/pto.go` (`readWithPTO` on `connCtx`, expiry gating), `quic/cc.go` + `quic/recvflow.go` (set `sendWindowGrew` on flight/credit release), `quic/send.go` (record `s.sendBlock`), `quic/stream.go` (`ready`, `signalReady`, `WaitReadable`, `WaitSendable`, `RecvState`, `resetLocked`/`stopSendingLocked`, `Recv` credit flush), `quic/close.go` (`closeWithErrorLocked`, single close via `terminateLocked`), `http3/client.go` (reader goroutine, `NewClient` reader-first, drop `inFlight`, per-request QPACK codecs, `Close` latch-before-cancel + release-before-`readerDone`, atomics, GOAWAY/open atomicity), `http3/control.go` (reader-owned servicing). **Tests:** rewrite `http3/client_ctx_test.go`; replace `http3/client_concurrency_test.go` with the true concurrency+`-race` suite; add a large-upload interop echo and a loss-relay gate to the 2c/2d matrices.
