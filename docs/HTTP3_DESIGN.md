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
