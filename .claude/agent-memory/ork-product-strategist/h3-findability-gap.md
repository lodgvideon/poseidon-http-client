---
name: h3-findability-gap
description: Strategic assessment of the HTTP/3 orphaned-stack / findability gap in poseidon-http-client (2026-07-14)
metadata:
  type: project
---

The H3 stack (quic/ + http3/ + qpack/, ~39% of codebase, shipped v0.9.0) is architecturally
separate from the public `client` package — no TransportH3, `client.Do()` cannot speak H3.
Prior framing treated this as a surprising "expectation gap" bug. Assessment says otherwise.

**Why:** The README ALREADY communicates the split extremely well — a dedicated "Two clients,
one philosophy" section, a maturity comparison table, an honest "HTTP/3 scope and known
limitations" list, and the first H3 example uses `http3.Dial` (not `client.Do`). A user reading
the README does NOT form the wrong expectation. So the user-facing expectation gap is LOW/MED,
not high. The H3 client also lacks pooling/concurrency/retries, so a `TransportH3` wired under
`client.Do()` would be a degraded, half-feature experience — unifying now is premature.

**How to apply:** For future product/roadmap calls on this repo:
1. Do NOT recommend building TransportH3 yet — gate it behind H3 gaining concurrent-in-flight +
   pooling (currently blocking/sequential, one req per conn). Unify the API only once the H3
   engine reaches load-gen parity, else you ship a trap.
2. The sharp, cheap wins are DOCS/CONTRIBUTOR findability, not code:
   - CLAUDE.md "Phase status" is STALE — says "Phase B complete, next Phase C" while Phase C
     (public client) AND Phase G (HTTP/3, v0.9.0) already shipped. Its architecture diagram
     omits client/, quic/, http3/, qpack/. This is the real findability defect — a new
     contributor reading CLAUDE.md would not know H3 or the public client exist. Verify current
     state before acting (memory decays); staleness itself is derivable from repo.
   - No H3 usage guide paralleling docs/CLIENT_GUIDE.md (only HTTP3_DESIGN.md, a design doc).
   - No protocol-selection guide beyond the README table; no migrate-from-net/http guide.

**Value prop / niche:** zero-dependency, zero-alloc, RFC-auditable H2+H3 client with load-gen
ergonomics (pool/discovery/rate-limit/hooks/metrics — H2 only). Niche is REAL but NARROW:
authors of load generators / benchmarking / conformance harnesses who need GC-pressure control
and fine-grained stream/flow-control. Competes below quic-go (production, but deps +
net/http-shaped http.RoundTripper, allocating) and beside fasthttp (H1-focused). Watch threat:
Go stdlib moving toward official HTTP/3 — narrows H3 novelty but not the zero-alloc/load-gen angle.
