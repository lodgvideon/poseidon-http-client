#!/usr/bin/env python3
"""Behaviour gate for the BASE pool tier: client/pool.go, h1_pool.go, h3_pool.go.

Sibling of scripts/pool-behaviour-gate.py, which covers the MANAGED tier. Kept
separate on purpose: different tier, different evaluator, and the managed gate
stays untouched as the thing that grades managed-tier work.

Each case reverses one DELIBERATE divergence between the three pool actors and
asserts a named test notices. These are the decisions a unification would
plausibly tidy away — the three semantically unrelated `active == 0` guards
and the two opposite tick orders among them — each documented in place.

A survey before this gate existed found SIX of twelve such decisions defended
by nothing at all. Four are now covered by tests written for that survey; the
other two are recorded below the case table as un-gateable, with the reason.

Two rules this gate follows that a naive mutation harness does not:

  * Every case runs ONLY its own test, via -run. A whole-suite run is not
    evidence here: TestIT_H2_StreamedDownload_RetentionStaysBounded flakes on
    a loaded machine, and it has already produced a false CAUGHT on an
    unrelated h1 mutation. A case is caught by the test that names the
    behaviour or it is not caught.

  * Files are restored from an in-memory copy, never `git checkout`. The
    harness and any work in progress share a file, and git cannot tell them
    apart.

Exit status is non-zero if any case goes uncaught.
"""

import re
import subprocess
import sys
import pathlib

REPO = pathlib.Path(__file__).resolve().parent.parent

# (name, what it pins, file, enclosing func, old lines, new lines, test filter)
CASES = [
    (
        "h1-tick-order",
        "h1 sweeps idle BEFORE dead; the reverse attributes like its siblings",
        "client/h1_pool.go", "func (p *h1Pool) handleTick(",
        ["\trs.conns = p.evictIdle(rs.conns)", "\trs.conns = p.evictDead(rs.conns)"],
        ["\trs.conns = p.evictDead(rs.conns)", "\trs.conns = p.evictIdle(rs.conns)"],
        "TestH1Pool_Tick_SweepsIdleBeforeDead",
    ),
    (
        "h2-tick-order",
        "h2 sweeps dead BEFORE idle so a GOAWAY'd idle conn is not filed as inactivity",
        "client/pool.go", "func (p *Pool) handleTick(",
        ["\trs.conns = p.evictDead(rs.conns)", "\trs.conns = p.evictIdle(rs.conns)"],
        ["\trs.conns = p.evictIdle(rs.conns)", "\trs.conns = p.evictDead(rs.conns)"],
        "TestPool_TickAttributesGoAwayBeforeIdle",
    ),
    (
        "h3-tick-order",
        "h3 sweeps dead BEFORE idle, same attribution reason as h2",
        "client/h3_pool.go", "func (p *h3Pool) handleTick(",
        ["\trs.conns = p.evictDead(rs.conns)", "\trs.conns = p.evictIdle(rs.conns)"],
        ["\trs.conns = p.evictIdle(rs.conns)", "\trs.conns = p.evictDead(rs.conns)"],
        "TestH3Pool_Tick_AttributesGoAwayBeforeIdle",
    ),
    (
        "h2-evictdead-drain-guard",
        "h2 must not evict a GOAWAY'd conn still carrying streams (RFC 7540 6.8)",
        "client/pool.go", "func (p *Pool) evictDead(",
        ["\t\tif !mc.c.IsAlive() && mc.active == 0 {"],
        ["\t\tif !mc.c.IsAlive() {"],
        "TestConformance_RFC7540_Sec6_8_PoolDrainsInflightOnGoAway",
    ),
    (
        "h3-dead-arm-unguarded",
        "h3's Dead arm is deliberately NOT guarded on active==0; the QUIC reader is gone",
        "client/h3_pool.go", "func h3RetireReason(",
        ["\tcase !mc.cl.Alive():"],
        ["\tcase !mc.cl.Alive() && mc.active == 0:"],
        "TestH3Pool_Tick_EvictsDeadConnStillCarryingStreams",
    ),
    (
        "h3-pick-skips-goaway",
        "h3 stops selecting a GOAWAY'd conn before it is retired (RFC 9114 5.2)",
        "client/h3_pool.go", "func (p *h3Pool) pickLeastLoaded(",
        ["\t\tif !mc.cl.Alive() || mc.cl.GoingAway() {"],
        ["\t\tif !mc.cl.Alive() {"],
        "TestConformance_RFC9114_Sec52_PoolEvictsGoAwayConn",
    ),
    (
        "h3-close-attributes-goaway",
        "closing the pool reports a GOAWAY'd conn as CloseGoAway, not as an operator close",
        "client/h3_pool.go", "func (p *h3Pool) handleClose(",
        ["\t\tif mc.cl.GoingAway() {"],
        ["\t\tif false {"],
        "TestH3Pool_Close_AttributesGoAway",
    ),
    (
        "h1-warmup-holds",
        "h1 warmup holds every conn then releases; releasing inline collapses onto one socket",
        "client/h1_pool.go", "func (p *h1Pool) warmup(",
        ["\t\theld = append(held, mc)"],
        ["\t\tp.release(mc, true)"],
        "TestH1PoolTransport_Warmup_PreDials",
    ),
    (
        "h1-release-keepalive",
        "h1's release verdict reads the MESSAGE (keepAlive), not the conn",
        "client/h1_pool.go", "func (p *h1Pool) handleRelease(",
        ["\tif !msg.keepAlive {"],
        ["\tif false {"],
        "TestConformance_RFC9112_Sec6_3_LatePoisonNotPooled",
    ),
    (
        "h1-acquire-residue",
        "h1 rejects a checked-out conn carrying unsolicited bytes (response-queue poisoning)",
        "client/h1_pool.go", "func (p *h1Pool) acquire(",
        ["\t\tif !mc.c.HasResidue() && (time.Since(mc.lastUsed) <= h1ProbeIdleAfter || mc.c.ProbeIdle()) {"],
        ["\t\tif time.Since(mc.lastUsed) <= h1ProbeIdleAfter || mc.c.ProbeIdle() {"],
        "TestConformance_RFC9112_Sec6_3_FastReusePoisonNotPooled_Pool",
    ),
    (
        "warmup-respects-backoff",
        "a warmup must not defeat the dial backoff a failing peer earned",
        "client/pool.go", "func (p *Pool) handleWarmup(",
        ["\tif inDialBackoff(rs.lastDialErrAt, p.opts.DialBackoff) {"],
        ["\tif false {"],
        "TestPool_Warmup_DoesNotDefeatDialBackoff",
    ),
    (
        "h1-sweep-reservation",
        "a conn the sweep reserved is capacity that exists; dialling over it opens a socket the pool owns",
        "client/h1_pool.go", "func (p *h1Pool) ensureDialForWaiters(",
        ["\tif len(rs.waiters) <= h1CountReservedIdle(rs.conns) {", "\t\treturn", "\t}"],
        ["\tif false {", "\t\treturn", "\t}"],
        "TestH1Pool_HealthSweep_DoesNotDialOverAReservedConn",
    ),
]


# Behaviours deliberately NOT gated, with the reason, so the next reader does
# not mistake their absence for an oversight:
#
#   * The h1 health probe running on its OWN GOROUTINE. What the test pins is
#     that acquire latency does not scale with pool size, and probing inline
#     but CONCURRENTLY keeps that flat too -- measured. No single-site edit
#     violates the property, so there is no honest mutation. Off-actor is a
#     design choice (the actor does no I/O at all) that measured better, not
#     something this suite proves.
#
#   * h1's serveWaiters not stamping lastUsed. Reached only from handleRelease,
#     which stamps it one line earlier, and from handleDialDone, where the conn
#     was just dialled -- so time.Since(lastUsed) is ~0 either way and the
#     checkout probe gate behaves identically. Looks like a consistency choice
#     rather than an observable behaviour.
#
#   * h2 alone re-serving waiters on tick. Needs a peer that RAISES
#     SETTINGS_MAX_CONCURRENT_STREAMS mid-connection; net/http2's server will
#     not do that on demand, so it wants a hand-rolled h2 peer.

def build(lines):
    """CRLF-safe multi-line pattern."""
    return re.compile(r"\r?\n".join(re.escape(l) for l in lines))


def enclosing(src, sig):
    m = re.search(re.escape(sig), src)
    if not m:
        return None
    e = re.compile(r"\r?\n\}").search(src, m.end())
    return (m.start(), e.end()) if e else None


def run_case(name, pins, path, sig, old, new, test):
    p = REPO / path
    original = open(p, "r", encoding="utf-8", newline="").read()

    span = enclosing(original, sig)
    if span is None:
        return "ANCHOR", "signature not found: " + sig
    lo, hi = span
    seg = original[lo:hi]
    pat = build(old)
    hits = len(pat.findall(seg))
    if hits != 1:
        return "ANCHOR", "pattern matched %d times inside %s (want 1)" % (hits, sig)

    nl = "\r\n" if "\r\n" in original else "\n"
    mutated = original[:lo] + pat.sub(lambda _: nl.join(new), seg, count=1) + original[hi:]
    try:
        open(p, "w", encoding="utf-8", newline="").write(mutated)
        r = subprocess.run(
            ["go", "test", "./client/", "-run", "^" + test + "$", "-count=1", "-timeout", "300s"],
            cwd=REPO, capture_output=True, text=True,
        )
        out = r.stdout + r.stderr
        if "no tests to run" in out or "[build failed]" in out:
            return "ANCHOR", "test filter matched nothing or the mutation broke the build: " + test
        return ("CAUGHT" if r.returncode != 0 else "MISSED"), test
    finally:
        # From the copy, never from git: the harness and any work in progress
        # share this file and git cannot tell them apart.
        open(p, "w", encoding="utf-8", newline="").write(original)


def main():
    caught = missed = broken = 0
    for name, pins, path, sig, old, new, test in CASES:
        verdict, detail = run_case(name, pins, path, sig, old, new, test)
        print("  %-9s %-28s %s" % (verdict, name, detail), flush=True)
        if verdict == "CAUGHT":
            caught += 1
        elif verdict == "MISSED":
            missed += 1
            print("            ^ nothing notices: %s" % pins)
        else:
            broken += 1

    print()
    print("caught %d/%d" % (caught, len(CASES)))
    if broken:
        print("%d case(s) could not be applied — the anchor drifted; fix before trusting this run" % broken)
    return 1 if (missed or broken) else 0


if __name__ == "__main__":
    sys.exit(main())
