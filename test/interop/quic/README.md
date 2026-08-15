# quic-interop-runner endpoint

The client endpoint of the [quic-interop-runner][runner] matrix. This directory
holds everything the image is built from; publishing it and opening the upstream
PR are manual steps and are described at the bottom.

Issue: [#555](https://github.com/lodgvideon/poseidon-http-client/issues/555),
stage E5 of `docs/NETWORK_EMULATION_PLAN.md`.

[runner]: https://github.com/quic-interop/quic-interop-runner

## What is here

| File | What it is |
|---|---|
| `main.go` | Environment handling, the support table, the exit codes |
| `hq09.go` | HTTP/0.9 over QUIC (ALPN `hq-interop`) — every case except `http3` |
| `h3.go` | HTTP/3 (ALPN `h3`) — the `http3` case |
| `run_endpoint.sh` | Container entrypoint: `/setup.sh`, wait for the simulator, run the client |
| `Dockerfile` | Cross-compiling builder plus the runner's mandated base image |

It is a nested Go module. The root module cannot host it: this is a `main`
package with no tests, and CI enforces a per-package statement-coverage floor of
80 over the root `./...`. `examples/` is already excluded from that run for the
same reason, and widening the exclusion is worse than staying outside it. The
`replace` in `go.mod` points at the parent, so it always builds against this
tree — `make lint` and `make tidy` iterate over it, and the Docker build compiles
it from source, so an API change here fails loudly instead of drifting.

Nothing is added to the public API: `test/interop/quic` is `package main`.

## Building

```sh
make qns-image          # linux/amd64, loaded into the local image store
make qns-image-multi    # linux/amd64 + linux/arm64, build only
```

Both build from the repository root, because the nested module's `replace` needs
the parent in the context. Equivalent by hand:

```sh
docker buildx build --platform linux/amd64 --load \
  -f test/interop/quic/Dockerfile -t poseidon-interop:local .
```

The builder stage is pinned to `$BUILDPLATFORM` and cross-compiles with
`GOARCH=$TARGETARCH`, so the arm64 image is produced by the native toolchain
rather than under qemu — the arm64 build is no slower than the amd64 one.

## Running the runner locally

The runner needs Python 3 with `requirements.txt` installed and Wireshark 4.5.0
or newer on `PATH`; `run.py -r` maps an implementation name to a locally built
tag, so nothing has to be published to test:

```sh
git clone https://github.com/lodgvideon/quic-interop-runner
cd quic-interop-runner
pip install -r requirements.txt
python3 run.py -c poseidon -s quic-go \
  -r poseidon=poseidon-interop:local \
  -t handshake,transfer,http3
```

The entry must already exist in `implementations_quic.json` — `-r` replaces the
image of a known name, it does not add one. The branch on the fork carries it.

Results land in `logs_<timestamp>/<server>_<client>/<testcase>/`, with the
client's own log at `client/client.log`.

## The support table

`TESTCASE` dispatch lives in one map in `main.go`, `support`. Every string the
runner can send appears in it exactly once, either with a function to run or with
the reason it exits 127. A name that is absent — the compliance check sends a
random slug — also exits 127.

Supported: `handshake`, `transfer`, `retry`, `multiconnect`, `http3`.

Declined, with reasons in the table: `chacha20`, `keyupdate`, `resumption`,
`zerortt`, `ecn`, `v2`, `versionnegotiation`.

A matrix cell and its `TESTCASE` are not the same string. `transfer` is what the
client receives for multiplexing, blackhole, amplificationlimit, transferloss,
transfercorruption, ipv6, the two rebind cases, connectionmigration, goodput and
crosstraffic; `multiconnect` covers handshakeloss and handshakecorruption;
`handshake` also covers longrtt. So those cells cannot be declined even where the
feature is missing — `connectionmigration` in particular arrives as a plain
`transfer` and simply fails, because the client never migrates.

## Publishing — the maintainer's handoff

**Nothing below has been done.** No image exists on any registry, no upstream PR
is open, and no registry credentials were ever handled by the work that produced
this directory. Every step is the maintainer's, run under **his own** Docker Hub
account, in this order.

Read [Verification runs](#verification-runs) first — step 0 is a judgement call,
not a command.

### Step 0 — decide whether to publish at all

Issue [#555](https://github.com/lodgvideon/poseidon-http-client/issues/555) says
registration "records the value; it does not authorise the publication", and that
appearing on the public matrix makes our failures public too. The local numbers
below are **PARTIAL** — but the decision no longer rests on them alone: the
`quic-interop` CI gate now runs the same matrix on GitHub-hosted runners against
two servers, and it clears `transfer` while leaving the handshake-cap cells
unasserted. So the one failure that is plausibly ours is the handshake cap, and
it is the one that reproduces off this host. See
[What CI settled](#what-ci-settled). Publishing is still a decision at this
point, not a formality.

### Step 1 — log in to your own Docker Hub account

```sh
docker login                       # your own account; nothing here assumes one
```

Substitute your namespace for `<dockerhub-user>` in every command below. The
placeholder is deliberate — no account name is baked into this repository.

### Step 2 — build and push the multi-platform image

Multi-platform is required by the online runner; a single-arch image is rejected.
`--push` is what makes buildx assemble the manifest list, so build and push are
one command:

```sh
docker buildx build --pull --push \
  --platform linux/amd64,linux/arm64 \
  -f test/interop/quic/Dockerfile \
  -t <dockerhub-user>/poseidon-interop:latest .
```

Run it from the repository root — the nested module's `replace` needs the parent
in the build context.

### Step 3 — verify the manifest carries both platforms

```sh
docker buildx imagetools inspect <dockerhub-user>/poseidon-interop:latest
```

Both `linux/amd64` and `linux/arm64` must appear before the upstream PR is
opened. The multi-arch manifest was built and inspected locally during
verification and **was** correct on both platforms — but it was never pushed, so
this has to be re-confirmed against the pushed tag.

The runner pulls `:latest` on every run, so re-pushing that tag is how a later fix
reaches the matrix.

### Step 4 — open the upstream PR with this entry

Append to the object in `implementations_quic.json` in
[quic-interop/quic-interop-runner][runner]. Three fields, all required, no
optional ones — paste-ready:

```json
  "poseidon": {
    "image": "<dockerhub-user>/poseidon-interop:latest",
    "url": "https://github.com/lodgvideon/poseidon-http-client",
    "role": "client"
  }
```

Only `image` is yours to decide — it has to match what was pushed in step 2.
`role` must be `client`: the runner builds its client and server lists from this
field, so a `client` entry is never asked to serve and its server-side compliance
check is never run. `chrome` is the existing client-only precedent.

**The entry already exists on the fork.** Branch `poseidon-interop-entry` of
`https://github.com/lodgvideon/quic-interop-runner` carries it, spelled
`lodgvideon/poseidon-interop:latest`. If step 2 pushed to a different namespace,
change that string on the fork branch before opening the PR from it.

## Verification runs

Three sources, and they do not weigh the same. Two are local runs on one WSL2
host; the third is the `quic-interop` CI gate, which drives the same runner on
GitHub-hosted ubuntu-24.04. **Read [What CI settled](#what-ci-settled) before
quoting any local number.** It closes one of the two questions the local runs
left open and confirms the other, in opposite directions.

### What CI settled

`.github/workflows/quic-interop.yml` runs the pinned runner against `quic-go`
and `ngtcp2` and compares every cell against `.github/interop/expected.json`.
That is a second host, and on one cell it disagrees with this one.

**`transfer` is not flaky in CI. The local intermittency is a host artefact.**
Over the workflow's first ten runs, on 2026-08-15 while #679 was in review —
four full-matrix legs and six pull-request legs, two servers each — `transfer`
was observed 20 times and came
back `succeeded` 18 times. Both exceptions come from one pull-request leg, in
which `handshake` and `retry` came back `failed` against *both* servers and the
whole leg finished in 42 s: the endpoint completed no handshake at all there,
which is not the mid-transfer stall described below. **Not one mid-transfer
stall on a GitHub runner.** `expected.json` therefore asserts
`transfer: succeeded` unconditionally, and `transfer` is one of the three cells
in the pull-request leg, so a real regression would surface on every PR.

The shortfall recorded below — 10 of 14, and 3 of 5 in the earlier sweep — is
thus a property of **this host**, and the mechanism is the clock step in
[the confound](#the-confound-which-is-not-optional-reading). The numbers are
kept because they are what the host did; they are not evidence about the client.

**What CI did not settle: the handshake cap.** The opposite result, and the
reason the local reading of `multiconnect` should not be written off.
`handshakeloss` and `handshakecorruption` are both declared NOT ASSERTED in
`expected.json` because they were measured probabilistic *on GitHub runners
too* — F/S/S and S/F/F against quic-go across three baseline runs. That
reproduces off this host, so it is ours.

### The three-server run — verdict PARTIAL

The five supported test cases against `quic-go`, `ngtcp2` and `aioquic` as
servers. This is the broader and more recent of the two runs.

| Case | Result |
|---|---|
| `handshake` | pass 3/3 servers |
| `retry` | pass 3/3 servers |
| `http3` | pass on quic-go and ngtcp2; fail on aioquic |
| `transfer` | **10 of 14 runs** — quic-go 4/6, ngtcp2 6/7, aioquic 0/1. Host artefact: 18 of the first 20 CI observations were clean, see [What CI settled](#what-ci-settled) |
| `multiconnect` | pass on ngtcp2; **fail on quic-go and aioquic** |

All seven declined cases recorded **UNSUPPORTED**, not FAILED — the exit-127
contract works as intended.

Three readings that the table does not carry:

- The `http3` failure on aioquic is a harness artefact, not a protocol result:
  that server booted **40 s after the client dialled**, so there was nothing to
  talk to.
- The `multiconnect` failures are a **10 s handshake timeout hit on 7 of 50
  connections**. That is `defaultHandshakeTimeout` in `quic/pto.go`, our own per-
  connection cap — so unlike the idle-timeout stalls, **this one may well be
  ours rather than the host's.** It is the reason the verdict is PARTIAL rather
  than pass-with-noise, and it should not be written off as environmental. CI
  has since backed that reading: the two cells that hit this cap are
  probabilistic on GitHub runners too, which is why `expected.json` declares
  them not-asserted rather than passing.
- The multi-arch manifest (amd64 + arm64) was built and inspected in this run.
  **Nothing was pushed to any registry.**

### The confound, which is not optional reading

**Every idle-timeout failure in this run shows a VM-wide backwards clock step in
_both_ endpoints' logs** — ours and the peer's. A backwards wall clock breaks
exactly the timers these failures are attributed to. Reliability therefore
**cannot be settled on this WSL2 host**, and re-running here produces more of the
same evidence rather than better evidence. It needed a second host.

It has one now. The `quic-interop` gate runs the same runner on GitHub-hosted
ubuntu-24.04, and it resolves the `transfer` shortfall as **the host's, not
ours** — see [What CI settled](#what-ci-settled). What that does *not* cover is
the `multiconnect` failure above, which fails on a timeout of ours, on a count,
rather than on an idle timer, and which reproduces on GitHub runners.

### The earlier full-matrix sweep, against quic-go only

The whole non-measurement matrix against a single server. Superseded on the five
cases above, kept because it is the only run that exercised the wider cells.

| Result | Cases |
|---|---|
| Pass | handshake, longrtt, retry, http3, multiplexing, blackhole, amplificationlimit, handshakeloss, transferloss, transfercorruption, ipv6, rebind-port, rebind-addr |
| Intermittent | transfer — 3 of 5 runs; host artefact, see [What CI settled](#what-ci-settled) |
| Unsupported (exit 127) | chacha20, resumption, zerortt, keyupdate, ecn, v2 — and connectionmigration, which the runner declined on its own rather than failing |
| Fail | handshakecorruption — in this sweep. Not deterministic: in CI it is neither reliably pass nor reliably fail |

Three of those are worth reading rather than counting.

**`transfer` was intermittent here — 3 passes and 2 failures in 5 runs**, and
because `transfer` is the `TESTCASE` behind a dozen cells it looked like the
biggest risk to a green public matrix. A passing run takes 9.0 s almost to the
tenth, so the transfer itself is deterministic; a failing one stalls partway and
both endpoints then give up on their idle timers — the client reports `quic: idle
timeout` and quic-go's server reports "no recent network activity". In the one
failure captured in detail the container's wall clock also jumped about 6 seconds
backwards mid-transfer (the log timestamps run backwards), which is a WSL2 /
Docker Desktop artifact rather than anything QUIC does. That was a correlation,
not a diagnosis, and it has since been re-run off this host: 18 of the first 20
CI observations clean, and no mid-transfer stall at all. The correlation was the
diagnosis — this is the harness. See [What CI settled](#what-ci-settled).

`handshakecorruption` is 50 sequential handshakes under 30% packet corruption
inside a 300 s budget. It fails at a specific point — "connection 13/50:
handshake: context deadline exceeded" — which is `defaultHandshakeTimeout`, the 10 s
per-connection cap in `quic/pto.go`, not the runner's budget. Its sibling
`handshakeloss` — the same 50 handshakes under 30% packet *loss* — finishes in
65 s and passes, so the gap looked specific to corruption, where a damaged packet
is discarded and only a probe timeout recovers it. CI says the split is not that
clean: both cells are probabilistic there, `handshakeloss` included, which is why
neither is asserted. Read this run as one sample of a coin flip, not as a
deterministic failure.

`blackhole` failed once, before it passed: `wait-for-it.sh sim:57832` timed out
after 30 s and the entrypoint exited 124, so the client never ran at all. It has
passed on every attempt since.

`multiplexing` failed once too, and that one was a real bug in this client, now
fixed — see the comment on the receive loop in `hq09.go`. One file out of 1999
came back empty because the loop drained the stream before reading its state.

## Known gaps

- **The ClientHello is pinned to X25519.** Go's default also offers
  X25519MLKEM768, and this stack puts the whole ClientHello into one Initial
  packet, which then exceeded the path MTU and was IP-fragmented — RFC 9000 §14
  forbids that, and the simulator dropped it. `main.go` documents the
  measurement. The library-side fix is to split a large ClientHello across
  Initial packets.
- **No qlog.** `QLOGDIR` is read and reported in the log, and the directory stays
  empty. The runner does not require it.
- **`connectionmigration` cannot be declined** (see above). It came back
  UNSUPPORTED locally rather than red, but the client does not migrate, so do not
  count on that.
- **The handshake's 10 s cap is no longer hard — but this endpoint still uses
  it.** The cap (`defaultHandshakeTimeout`, `quic/pto.go`) is what the quic-go-only
  sweep hit on the 13th of 50 `handshakecorruption` connections, and what the
  three-server run hit on 7 of 50 in `multiconnect`. It is now a per-connection
  option, `quic.WithHandshakeTimeout`, so this client *can* be given a larger
  budget for the lossy cells — and deliberately is not, yet, because the raise
  could not be shown to help here.

  Six `handshakecorruption` runs against quic-go on the WSL2 host, three at the
  10 s default and three with a 60 s bound, came back **1/3 and 0/3**. The
  failures do not share a mechanism: one baseline run blew the runner's 300 s
  budget having completed 31 of 50 connections, another completed **4 in 338 s**
  — about 85 s per connection, which nothing in a handshake capped at 10 s can
  produce — while the raised arm failed once on a handshake that exhausted even
  the 60 s bound (connection 17/50), once on a plain read timeout mid-download
  (connection 7/50), and once on the 300 s budget again. Five failures, four
  distinct mechanisms, only one of them the handshake bound. Those are host
  stalls landing on whichever timer happens to be armed, an order of magnitude
  larger than the effect under test, so **the arms are not distinguishable here
  and the 60 s trial is not evidence either way.** Setting the option for this
  endpoint belongs on a machine that hosts the runner natively; the library-side
  behaviour it depends on is pinned by
  `TestConn_HandshakeTimeout_RescuesALiveHandshake` instead, in fake time, where
  it is exactly reproducible.
- **`transfer` stalls intermittently *on this host*, and only on this host.** 10
  of 14 runs across three servers, 3 of 5 in the earlier sweep. It decides a
  dozen matrix cells, so it read as the biggest risk to a green public matrix.
  It is not one: on GitHub-hosted runners the same cell came back `succeeded` in
  18 of the first 20 observations, with no mid-transfer stall in any of them, and
  `expected.json` asserts it unconditionally on every pull request. The local
  shortfall is the clock step in the confound above, not a client bug — the full
  count is in [What CI settled](#what-ci-settled). This entry stays as a gap in
  the *local verification*, not in the client: nothing recorded here has been
  reproduced on a host whose wall clock does not step backwards.
