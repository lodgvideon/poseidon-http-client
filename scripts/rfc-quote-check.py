#!/usr/bin/env python3
"""Check that every RFC quotation in the source is verbatim in the RFC.

A quotation here means a span in double quotes that the surrounding comment
presents as coming from a numbered section — `RFC 9112 §6.3 rule 5: "..."`.
Prose that merely contains quotes, test failure messages and wire fixtures are
not quotations and are not checked; the point is to catch text that CLAIMS to be
the spec and is not.

Why this exists: this repo shipped four altered or invented quotations into
production comments and test names. Each read as authoritative, each was written
from memory, and one of them ("the sender MUST close the connection after
responding", attributed to RFC 9112 §6.3 rule 3) does not appear in RFC 9112 at
all — the real sentence is §6.1 and binds a server, not a client. A citation the
next reader trusts is worse than none, because it stops them checking.

Usage:
    python scripts/rfc-quote-check.py [paths...]     # default: http1/ conn/ http3/

RFC texts are fetched to a cache dir on first use. Offline: pre-populate
<cache>/rfcNNNN.txt.
"""

import os
import re
import sys
import glob
import urllib.request

CACHE = os.environ.get("RFC_CACHE", os.path.join(os.path.dirname(__file__), ".rfc-cache"))
RFCS = ("9112", "9110", "2616", "7540", "7541", "9114", "9204", "9000", "8336")

# A quotation is a citation, a colon, then a quoted span. The optional RFC number
# pins which document to check against; a bare "§6.3: ..." is checked against all.
CITE = re.compile(r'(?:RFC\s*(\d{4})\s*)?§[0-9][0-9.]*[^:"]{0,40}:\s*"([^"]{25,600})"')

# A Go format verb, which marks a string as a message rather than a quotation.
# Testing for a bare "%" instead skipped any quotation containing a percent sign.
FMT_VERB = re.compile(r"%[-+# 0-9.\[\]*]*[vTtbcdoOqxXUeEfFgGsp%w]")


def norm(t):
    return re.sub(r"\s+", " ", t).strip()


def variants(text):
    """Both readings of an RFC's line-wrapped text.

    RFC .txt files wrap at 72 columns and break words across lines on a hyphen:

        ... after receiving a final (non-
        informational) status code ...

    Collapsing whitespace gives "(non- informational)", which no correctly
    written quotation will ever match — so a checker that only does that reports
    every such quote as fabricated. Rejoining every "- " instead corrupts genuine
    hyphenated compounds that merely straddle a line. Neither reading is right on
    its own, so a quote counts as found if it appears in EITHER.

    Getting this wrong is not a small thing: a checker with false positives is
    worse than no checker, because the first response to one is to switch it off.
    """
    collapsed = norm(text)
    return (collapsed, collapsed.replace("- ", "-"))


class CorpusError(Exception):
    """An RFC text could not be obtained, so nothing can be checked against it."""


def load(num):
    """Return both readings of RFC `num`, or raise.

    Raising rather than returning None is the point. This used to warn and hand
    back None, and main() then skipped the whole check when the corpus came back
    empty — so a network blip on the CI runner turned the gate into a no-op that
    printed nothing and exited 0. A gate that cannot fail is decoration, and one
    that quietly stops gating when the network hiccups is worse than decoration,
    because its green tick still gets believed.
    """
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"rfc{num}.txt")
    if not os.path.exists(path):
        url = f"https://www.rfc-editor.org/rfc/rfc{num}.txt"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                body = r.read()
        except Exception as e:
            raise CorpusError(f"could not fetch RFC {num} from {url}: {e}") from e
        if len(body) < 10000:
            raise CorpusError(f"RFC {num} fetched but is only {len(body)} bytes; refusing to trust it")
        with open(path, "wb") as f:
            f.write(body)
    with open(path, encoding="utf-8", errors="replace") as f:
        text = f.read()
    if len(text) < 10000:
        raise CorpusError(f"cached RFC {num} is only {len(text)} bytes; delete {path} and retry")
    return variants(text)


def discover_dirs():
    """Every directory in the tree that holds Go source.

    The scope used to be a hand-maintained list — `http1 conn http3` — which
    drifted: client/ was never scanned, and a fabricated RFC 7540 §6.8
    quotation sat in it unchecked. A path list a human keeps in sync with the
    package layout is exactly the kind of thing this whole script exists to
    stop trusting. So the default scope is now the tree itself: any dir with a
    .go file, minus the ones no citation should live in.
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    skip = {".git", "vendor", "testdata", "node_modules", "website"}
    dirs = set()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip and not d.startswith(".")]
        if any(f.endswith(".go") for f in filenames):
            dirs.add(os.path.relpath(dirpath, root))
    return sorted(dirs)


def main():
    # Explicit args override discovery (handy for `... http1` while iterating);
    # with none, scan every Go dir so the scope cannot silently drift.
    paths = sys.argv[1:] or discover_dirs()
    try:
        corpus = {n: load(n) for n in RFCS}
    except CorpusError as e:
        # Fail CLOSED. Every RFC in RFCS is needed to judge the citations in this
        # tree, so a missing one means "unknown", not "fine".
        print(f"error: {e}", file=sys.stderr)
        print("refusing to pass a quotation check with an incomplete corpus", file=sys.stderr)
        return 2

    ok = bad = 0
    for p in paths:
        for f in sorted(glob.glob(os.path.join(p, "*.go"))):
            # Join wrapped comment lines so a quotation split across `//` lines
            # is seen whole.
            joined = re.sub(r"\n\s*//", " ", open(f, encoding="utf-8").read())
            for m in CITE.finditer(joined):
                which, q = m.group(1), m.group(2)
                # Skip things that are not prose: Go format strings and wire
                # fixtures. Matching a bare "%" skipped any quotation that merely
                # contained a percent sign, so the test is for an actual verb.
                if FMT_VERB.search(q) or "\\r" in q or "\\n" in q:
                    continue
                # An ellipsis is a legitimate elision: every piece must appear,
                # but not necessarily adjacently.
                parts = [norm(x) for x in q.split("...") if len(norm(x).split()) >= 4]
                if not parts:
                    continue
                if which is not None and which not in corpus:
                    # A citation naming an RFC outside RFCS. Checking it against
                    # the other documents would report a correct quotation as
                    # fabricated, so add the RFC to RFCS instead of guessing.
                    print(f"error: {f} cites RFC {which}, which is not in the corpus")
                    bad += 1
                    continue
                pool = [corpus[which]] if which else list(corpus.values())
                if any(all(part in v for part in parts) for vs in pool for v in vs):
                    ok += 1
                else:
                    bad += 1
                    print(f"NOT VERBATIM  {f}")
                    print(f"    claims: {q[:160]}")
    print(f"\nRFC quotations: {ok + bad} checked, {ok} verbatim, {bad} not")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
