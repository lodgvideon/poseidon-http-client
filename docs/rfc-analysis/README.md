# HTTP RFC Analysis & Implementation Reconciliation

Deep, from-scratch analysis of the HTTP specifications this client implements —
**HTTP/1.1** (`http1/` + `client/`) and **HTTP/2** (`conn/` + `frame/` + `hpack/`) —
re-deriving every normative rule from raw RFC text and reconciling the code.

Both stacks were originally built against **obsolete** RFCs (HTTP/1.1 →
RFC 2616, 1999; HTTP/2 → RFC 7540, 2015). For each, this analysis re-extracts
the rules, produces the **current slice** (HTTP/1.1 → RFC 9110 + 9112;
HTTP/2 → RFC 9113 + HPACK RFC 7541), and diffs old→current. For **both** it
distills a client checklist and audits the code: HTTP/1.1 (PASS 101 · N/A 142 ·
actionable 13) and HTTP/2 (PASS 166 · FAIL 19 · PARTIAL 19 · UNTESTED 11 ·
REVIEW 17 over 232 obligations).

## Method (how these were produced)

Every fact is extracted from the **raw RFC text fetched from rfc-editor.org**
(never from model recall), then **each fact independently verified twice** by a
default-refute pass that re-reads the source bytes and checks the quote is
verbatim, the normative level (MUST/SHOULD/MAY) is right, and the audience
(client/server/proxy) is right. The reconciliation adds an **adversarial**
layer: every non-PASS judgement is re-checked by two verifiers that try to
prove the code actually complies.

Extraction ran on the `fable` model; verification and code-judging on `opus`.
All artifacts are regenerable from the workflow journals via the scripts in the
session scratchpad (`parse-*.ps1`).

## HTTP/1.1 artifacts

| File | What | Size | Verification |
|------|------|------|--------------|
| [RFC2616_FACTS.md](RFC2616_FACTS.md) | Normative facts of the **obsolete** spec the client was built to | 1322 facts | 1317 confirmed, 5 cosmetic disputes |
| [RFC9110_9112_FACTS.md](RFC9110_9112_FACTS.md) | Normative facts of the **current 2026** HTTP/1.1 slice (RFC 9110 semantics + 9112 message syntax) | 1208 facts | 1206 confirmed, 2 cosmetic disputes |
| [HTTP1_RFC_DELTA.md](HTTP1_RFC_DELTA.md) | Where a client built to **2616 diverges** from the current spec, keyed by client impact | 196 deltas | 193 confirmed, 3 `change_type`-label disputes |
| [HTTP1_CLIENT_CHECKLIST.md](HTTP1_CLIENT_CHECKLIST.md) | The **256 client-relevant obligations** (163 MUST-family, 93 SHOULD-family) distilled from the current slice | 256 items | derived from verified facts |
| [HTTP1_RECONCILIATION.md](HTTP1_RECONCILIATION.md) | The implementation **audited** against the checklist | 256 judged | PASS 101 · N/A 142 · actionable 13 |

**Disputes** across all catalogs are cosmetic: an extractor dropped an inline
`(Section X.Y)` cross-reference from a quote, or a delta's `change_type` enum is
mislabelled (e.g. `relaxed` vs `removed`). Each is flagged **VERIFY** inline
with the correction. No substantive normative rule is affected.

## HTTP/2 artifacts

Target packages: `frame/` (frame codec), `conn/` (connection engine — streams,
flow control, SETTINGS, GOAWAY, pseudo-headers, malformed), `hpack/` (RFC 7541).

| File | What | Size | Verification |
|------|------|------|--------------|
| [RFC7540_FACTS.md](RFC7540_FACTS.md) | Normative facts of the **obsolete** HTTP/2 spec the client was built to (tests are `TestConformance_RFC7540_*`) | 606 facts | 605 confirmed, 1 cosmetic dispute |
| [RFC9113_FACTS.md](RFC9113_FACTS.md) | Normative facts of the **current** HTTP/2 spec (RFC 9113, 2022; obsoletes 7540/8740) | 594 facts | 591 confirmed, 3 cosmetic disputes |
| [RFC7541_HPACK_FACTS.md](RFC7541_HPACK_FACTS.md) | Normative facts of **HPACK** (RFC 7541) → the `hpack/` codec | 167 facts | 166 confirmed, 1 cosmetic dispute |
| [HTTP2_RFC_DELTA.md](HTTP2_RFC_DELTA.md) | Where a client built to **7540 diverges** from 9113, keyed by client impact | 91 deltas | 90 confirmed, 1 `change_type`-label dispute |
| [HTTP2_CLIENT_CHECKLIST.md](HTTP2_CLIENT_CHECKLIST.md) | The **232 client-relevant obligations** (211 MUST-family, 21 SHOULD-family) distilled from RFC 9113 + 7541 | 232 items | derived from verified facts |
| [HTTP2_RECONCILIATION.md](HTTP2_RECONCILIATION.md) | The implementation **audited** against the checklist (judge + 2 adversarial verifiers) | 232 judged | PASS 166 · FAIL 19 · PARTIAL 19 · UNTESTED 11 · REVIEW 17 |

Change-type spread of the 91 deltas: 18 added, 14 clarified, 13 tightened,
12 removed, 9 unchanged-wording, 7 security-hardening, 7 moved, 6 deprecated,
4 relaxed, 1 default-changed.

### HTTP/2 headline findings (where a 7540-era client is now off)

- **Priority (RFC 7540 §5.3) is deprecated wholesale.** The dependency/weight
  tree, PRIORITY frame, and HEADERS priority fields still exist on the wire but
  9113 servers ignore them. Two client-side consequences: (a) exposing priority
  knobs (`SendHeadersWithPriority`, CLIENT_GUIDE §4) is a no-op against modern
  servers — misleading for a load generator; (b) the 7540 **MUST** that a
  self-dependent stream is a `PROTOCOL_ERROR` was **removed** — a client still
  enforcing it now RST-kills streams a 9113 peer considers legal. Receiving
  PRIORITY as a no-op (`OnPriority`) is now spec-blessed, not lazy.
- **h2c in-band Upgrade removed.** Cleartext HTTP/2 is prior-knowledge only in
  9113; never offer `Upgrade: h2c` / `HTTP2-Settings`.
- **Field validation added (9113 §8.2.1)** and pseudo-header / trailer /
  connection-specific-header rules tightened; malformed = **stream** error
  (`RST_STREAM(PROTOCOL_ERROR)`), not a connection error.
- **TLS 1.3 rules added (9113 §9.2.3)**, absorbing RFC 8740.
- **421 (Misdirected Request)** definition moved out to RFC 9110 §15.5.20.

The HPACK catalog pins the security-critical decoder rules (integer-overflow
and octet-length caps → decoding error, Huffman EOS/over-long-padding → error,
index-out-of-range → `COMPRESSION_ERROR`) with per-file reconcile targets
(`hpack/integer.go`, `hpack/huffman_fsm.go`).

## HTTP/1.1 headline findings

### Delta 2616 -> 9110/9112 (what "our understanding" got outdated on)

The load-bearing changes a 2616-era client gets wrong, all now framed as
**security** issues (request smuggling / response splitting):

- **TE + Content-Length together**: 2616 said "ignore Content-Length"; 9112 says
  this is a smuggling signal — treat as error / never reuse the connection.
- **Invalid or conflicting Content-Length**: 2616 said "notify the user"; 9112
  makes it an **unrecoverable** framing error (close, discard).
- **Non-chunked-final Transfer-Encoding**: must read-until-close, not chunk-parse;
  request side must be `400` + close.
- **`identity` transfer-coding removed**; **`multipart/byteranges` self-delimiting
  framing removed**; **obs-fold deprecated** (must reject or replace with SP).
- **HTTP/1.0 message carrying Transfer-Encoding**: framing is faulty -> close.
- Caching (2616 §13) **moved out** to RFC 9111; **Warning** header removed;
  **DNS-TTL MUST** dropped; **userinfo in http(s) URIs** deprecated.

### Reconciliation (implementation vs current spec)

- **101 PASS** (implemented + tested), **142 NOT_APPLICABLE** (out of this
  low-level client's scope — caching, redirect-following, proactive content
  negotiation, proxy roles — or the caller's responsibility).
- **13 actionable**, clustered in connection management, all SHOULD-level or
  split-verdict — **no hard-MUST failure**:
  - `FAIL` 8-9-46 / 8-9-47 (SHOULD): no concurrent read while uploading a body,
    so an early error response mid-upload is not noticed and the upload is not
    aborted (single-goroutine exchange design).
  - `FAIL` 8-9-14 (SHOULD): idle pooled connections are not monitored, so
    unsolicited/CRLF-noise bytes are not discarded proactively.
  - `PARTIAL` 6-16 (SHOULD): a response with an HTTP minor version > 1.1 and no
    `Connection` header is treated as close instead of persistent.
  - `PARTIAL` 8-9-43 (SHOULD): a peer FIN on an idle conn is discovered only on
    next use (recovered via retry), not by proactive monitoring.
  - `REVIEW` 8-9-71 (MUST): TLS `close_notify` is sent by `crypto/tls` on close
    but no test asserts it.
  - Remaining `REVIEW` items are verifier-split (deliberate design vs literal
    obligation) — see the table.

Most of the actionable set are **deliberate simplifications** typical of a
low-level load-generation client, not defects. The full per-item evidence
(file:line) is in [HTTP1_RECONCILIATION.md](HTTP1_RECONCILIATION.md).

## Scope note

RFC 9111 (caching) is intentionally **out of scope** — this client does not
cache. If a cache is ever added, RFC 9111 must be analysed the same way.
