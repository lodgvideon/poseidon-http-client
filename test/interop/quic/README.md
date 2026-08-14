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

## Publishing — the manual steps

Neither of these is done by the repository or by CI.

### 1. Push the image to Docker Hub

Multi-platform is required by the online runner; a single-arch image is rejected.
`--push` is what makes buildx assemble the manifest list, so build and push are
one command:

```sh
docker login                       # your Docker Hub account
docker buildx build --pull --push \
  --platform linux/amd64,linux/arm64 \
  -f test/interop/quic/Dockerfile \
  -t <dockerhub-user>/poseidon-interop:latest .
```

Verify the manifest carries both platforms before opening the PR:

```sh
docker buildx imagetools inspect <dockerhub-user>/poseidon-interop:latest
```

The runner pulls `:latest` on every run, so re-pushing that tag is how a fix
reaches the matrix.

### 2. The `implementations_quic.json` entry

Append to the object in the upstream repository. Three fields, all required, no
optional ones:

```json
  "poseidon": {
    "image": "<dockerhub-user>/poseidon-interop:latest",
    "url": "https://github.com/lodgvideon/poseidon-http-client",
    "role": "client"
  }
```

Only `image` is yours to decide — it has to match what was pushed in step 1.
`role` must be `client`: the runner builds its client and server lists from this
field, so a `client` entry is never asked to serve and its server-side compliance
check is never run. `chrome` is the existing client-only precedent.

The entry is already on the fork at
`https://github.com/lodgvideon/quic-interop-runner`, branch
`poseidon-interop-entry`, spelled `lodgvideon/poseidon-interop:latest`. Change
that string if the image goes somewhere else.

## Observed locally

Against `quic-go` as the server, the whole non-measurement matrix, on one
machine. Not a substitute for the public matrix, which runs against every other
implementation, but it is what the client has actually been through.

| Result | Cases |
|---|---|
| Pass | handshake, longrtt, retry, http3, multiplexing, blackhole, amplificationlimit, handshakeloss, transferloss, transfercorruption, ipv6, rebind-port, rebind-addr |
| Intermittent | transfer — 3 of 5 runs |
| Unsupported (exit 127) | chacha20, resumption, zerortt, keyupdate, ecn, v2 — and connectionmigration, which the runner declined on its own rather than failing |
| Fail | handshakecorruption |

Three of those are worth reading rather than counting.

**`transfer` is intermittent — 3 passes and 2 failures in 5 runs**, which is the
biggest risk to a green public matrix, because `transfer` is the `TESTCASE`
behind a dozen cells. A passing run takes 9.0 s almost to the tenth, so the
transfer itself is deterministic; a failing one stalls partway and both endpoints
then give up on their idle timers — the client reports `quic: idle timeout` and
quic-go's server reports "no recent network activity". In the one failure
captured in detail the container's wall clock also jumped about 6 seconds
backwards mid-transfer (the log timestamps run backwards), which is a WSL2 /
Docker Desktop artifact rather than anything QUIC does. That is a correlation,
not a diagnosis: it has to be re-run on a machine hosting the runner natively
before anyone decides whether this is a client bug or the harness.

`handshakecorruption` is 50 sequential handshakes under 30% packet corruption
inside a 300 s budget. It fails at a specific point — "connection 13/50:
handshake: context deadline exceeded" — which is `handshakeTimeout`, the 10 s
per-connection cap in `quic/pto.go`, not the runner's budget. Its sibling
`handshakeloss` — the same 50 handshakes under 30% packet *loss* — finishes in
65 s and passes, so the gap is specific to corruption, where a damaged packet is
discarded and only a probe timeout recovers it.

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
- **The handshake has a hard 10 s cap** (`handshakeTimeout`, `quic/pto.go`), and
  `handshakecorruption` hits it on the 13th of 50 connections.
- **`transfer` stalls intermittently** — see above. This one should be settled
  before the image is published, because it decides a dozen cells.
