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
RFCS = ("9112", "9110", "2616", "7540", "7541", "9114", "9204", "9000")

# A quotation is a citation, a colon, then a quoted span. The optional RFC number
# pins which document to check against; a bare "§6.3: ..." is checked against all.
CITE = re.compile(r'(?:RFC\s*(\d{4})\s*)?§[0-9][0-9.]*[^:"]{0,40}:\s*"([^"]{25,600})"')


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


def load(num):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"rfc{num}.txt")
    if not os.path.exists(path):
        url = f"https://www.rfc-editor.org/rfc/rfc{num}.txt"
        try:
            with urllib.request.urlopen(url, timeout=30) as r, open(path, "wb") as f:
                f.write(r.read())
        except Exception as e:
            print(f"warn: could not fetch RFC {num}: {e}", file=sys.stderr)
            return None
    with open(path, encoding="utf-8", errors="replace") as f:
        return variants(f.read())


def main():
    paths = sys.argv[1:] or ["http1", "conn", "http3"]
    corpus = {n: v for n in RFCS if (v := load(n))}
    if not corpus:
        print("no RFC texts available; skipping", file=sys.stderr)
        return 0

    ok = bad = 0
    for p in paths:
        for f in sorted(glob.glob(os.path.join(p, "*.go"))):
            # Join wrapped comment lines so a quotation split across `//` lines
            # is seen whole.
            joined = re.sub(r"\n\s*//", " ", open(f, encoding="utf-8").read())
            for m in CITE.finditer(joined):
                which, q = m.group(1), m.group(2)
                if any(x in q for x in ("%", "\\r", "\\n")):
                    continue  # format string or wire fixture, not a quotation
                # An ellipsis is a legitimate elision: every piece must appear,
                # but not necessarily adjacently.
                parts = [norm(x) for x in q.split("...") if len(norm(x).split()) >= 4]
                if not parts:
                    continue
                pool = [corpus[which]] if which in corpus else list(corpus.values())
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
