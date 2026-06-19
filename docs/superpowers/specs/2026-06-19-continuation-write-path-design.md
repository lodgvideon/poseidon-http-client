# CONTINUATION write-path — design

**Date:** 2026-06-19
**Branch:** `claude/continuation-write-path`
**RFC:** 7540 §6.2 (HEADERS), §6.10 (CONTINUATION)

## Problem

`conn.writeHeadersWithPriority` (conn/conn.go:418) always emits a single
HEADERS frame with `EndHeaders: true` carrying the full HPACK block:

```go
block := c.enc.EncodeBlock(*buf, fields)
err := c.fr.WriteHeaders(frame.WriteHeadersParams{
    StreamID:      s.id,
    BlockFragment: block,
    EndHeaders:    true,
    EndStream:     endStream,
    PadLength:     c.opts.Padding.ForHeaders(),
    Priority:      prio,
})
```

When the encoded block exceeds the peer's `SETTINGS_MAX_FRAME_SIZE` the frame
is oversized — the peer rejects the connection with `FRAME_SIZE_ERROR`. The
codec already enforces its own cap: `WriteHeaders` returns `ErrFrameTooLarge`
when `totalLen > f.maxReadFrameSize` (framer.go:222), which defaults to 16384
and is never raised by conn. So today any request whose header block exceeds
~16 KiB fails locally before it even reaches the wire.

This matters for a load generator: large bearer tokens, fat cookies, or gRPC
metadata routinely push header blocks past 16 KiB.

## Goal

Split an oversized HPACK block into one HEADERS frame plus N CONTINUATION
frames (RFC 7540 §6.2 / §6.10), so arbitrarily large header sets transmit
correctly. Normal-sized requests keep byte-for-byte identical behavior.

## Non-goals

- Changing the frame-layer codec. `Framer.WriteContinuation` already exists.
- Raising `maxReadFrameSize`. The framer's hard cap stays 16384; with default
  settings this is consistent. Raising `opts.Settings.MaxFrameSize` >16384
  without a matching `SetMaxReadFrameSize` is a pre-existing inconsistency,
  not addressed here.
- Receive-side CONTINUATION handling — `OnContinuation` already exists and is
  tested.

## Change surface

A single function: `conn.writeHeadersWithPriority` (conn/conn.go:418). It
serves both the initial request HEADERS path (`SendHeaders` /
`SendHeadersWithPriority`) and the request-trailer HEADERS path (trailers go
through `SendHeaders` with `endStream=true`), so one fix covers both.

- **No signature/interface change.** The `streamWriter` interface
  (stream.go:69-70) is untouched; fakes and callers compile unchanged.
- **No frame-layer change.** The 0-alloc codec bench gate is unaffected.

## Mechanism

After `EncodeBlock`, compute the per-frame budget. If the block fits the
first-frame budget, emit a single HEADERS frame exactly as today (zero
behavior change for normal requests). Otherwise:

1. **Frame 1 — HEADERS:** `EndHeaders=false`, `EndStream=endStream`,
   `PadLength=c.opts.Padding.ForHeaders()`, `Priority=prio`,
   `BlockFragment=block[:budget0]`.
2. **Frames 2..n — CONTINUATION:** `WriteContinuation(s.id, endHeaders, frag)`,
   each fragment up to `maxFrame` bytes, no padding / priority / end_stream.
   The **last** CONTINUATION sets `endHeaders=true`.

The entire sequence runs under the already-held `c.wmu` write mutex, which
guarantees no frame from any other stream interleaves between the HEADERS and
its CONTINUATION frames (RFC §6.10 hard requirement).

### Frame-size budget

Mirror `writeData` (conn.go:471-477):

```
maxFrame = min(peer SETTINGS_MAX_FRAME_SIZE, our opts.Settings.MaxFrameSize)
if maxFrame <= 0 { maxFrame = 16384 }
```

First-HEADERS block budget accounts for the in-payload overhead that
`WriteHeaders` adds to `totalLen` (framer.go:215-221):

```
padOverhead      = 0; if padLen > 0 { padOverhead = 1 + int(padLen) }
priorityOverhead = 0; if prio != nil { priorityOverhead = 5 }
budget0          = maxFrame - padOverhead - priorityOverhead
```

CONTINUATION frames carry up to `maxFrame` bytes each (no pad/priority, so no
overhead subtraction). `WriteContinuation` does not check `maxReadFrameSize`,
so conn must keep each fragment ≤ `maxFrame` itself.

Edge: if `budget0 <= 0` (pathological tiny `maxFrame` with priority+padding),
fall back to `budget0 = 1` so progress is always made. In practice `maxFrame`
is ≥16384 and overhead ≤ 261 bytes, so this never triggers on real configs;
it only guards against a misconfigured `MaxFrameSize`.

### Flag rules (RFC §6.2 / §6.10)

| Flag        | HEADERS (frame 1) | CONTINUATION (2..n) |
|-------------|-------------------|---------------------|
| END_STREAM  | = `endStream`     | never               |
| END_HEADERS | `false` if split  | `true` only on last |
| PADDED      | = padding config  | never               |
| PRIORITY    | = `prio != nil`   | never               |

### Buffer safety

`block` aliases the `encBufPool` buffer. Both `WriteHeaders` and
`WriteContinuation` fully consume their `BlockFragment` before returning — the
fast path copies into `f.writeBuf` (framer.go:226-234); the slow path writes
the fragment via `f.w.Write` before return (framer.go:259). The framer never
retains the slice. Slicing `block` into fragments across successive calls is
therefore safe. The buffer returns to `encBufPool` after the final frame.

## Pseudocode

```go
block := c.enc.EncodeBlock(*buf, fields)

maxFrame := frameBudget(c)              // min(peer, our), floor 16384
padLen := c.opts.Padding.ForHeaders()
budget0 := maxFrame - padOverhead(padLen) - priorityOverhead(prio)
if budget0 <= 0 { budget0 = 1 }

if len(block) <= budget0 {
    err = c.fr.WriteHeaders(... BlockFragment: block, EndHeaders: true ...)
} else {
    err = c.fr.WriteHeaders(... BlockFragment: block[:budget0],
                                EndHeaders: false ...)   // pad+prio+endStream here
    if err == nil {
        rest := block[budget0:]
        for len(rest) > 0 {
            n := min(len(rest), maxFrame)
            last := n == len(rest)
            err = c.fr.WriteContinuation(s.id, last, rest[:n])
            if err != nil { break }
            rest = rest[n:]
        }
    }
}
*buf = block[:0]
encBufPool.Put(buf)
```

`bumpFramesSent()` is called once after a successful emission (matching current
behavior — it counts the logical HEADERS send, not per-wire-frame).

## Tests

### Unit (wire-byte)

Wire `c.fr` to a `bytes.Buffer` (per CLAUDE.md "wire-byte assertions" pattern),
force a small `MaxFrameSize`, send a field set whose encoded block exceeds it,
parse the produced bytes, and assert:

- `TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation` — frame 1 is
  HEADERS with END_HEADERS=0; END_STREAM matches the call; frames 2..n are
  CONTINUATION; the last has END_HEADERS=1; reassembled block decodes back to
  the original fields.
- `TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding` — padding and
  priority appear only on the HEADERS frame, never on CONTINUATION.
- `TestConn_WriteHeaders_BlockFitsExactly_SingleFrame` — block length exactly
  equal to `budget0` produces one HEADERS frame (END_HEADERS=1), zero
  CONTINUATION frames (boundary).

### Integration (h2 server)

- `TestIntegration_LargeHeaders_SplitAcrossContinuation` — httptest
  `net/http2.Server` peer; issue a request with ~50 headers of ~1 KiB each
  (>16384 total block); assert the server reassembles and responds 200, and
  that the echoed/observed header count matches.

### RFC trace

Add rows to `docs/RFC_COVERAGE.md` under RFC 7540:

| §6.2  | Conformance | TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation (conn) |
| §6.10 | Conformance | TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding (conn) |

## Allocation profile

`writeHeadersWithPriority` is not on the 0-alloc gate (the client path
allocates by design). The split loop adds no per-call heap allocation — it only
re-slices the existing `block`. `encBufPool` usage is unchanged.

## Acceptance criteria

1. Normal requests (block ≤ budget) emit byte-identical frames to today.
2. Oversized blocks split into HEADERS + CONTINUATION per the flag table.
3. `go test -race ./conn/... ./frame/...` passes.
4. `make bench` 0-alloc gate on frame + hpack unchanged.
5. `make lint` clean.
6. New conformance rows present in `docs/RFC_COVERAGE.md`.
