# HTTP/3 interop test

Validates the from-scratch HTTP/3 client (`http3.Dial` → `Client.Do`) against a
**matrix of two real, independent HTTP/3 server stacks** over real UDP:

- [Caddy](https://caddyserver.com/) — `quic-go` (Go), and
- [nginx](https://nginx.org/) — its own C QUIC implementation.

Each scenario runs against every server as a subtest (`TestInterop_GET/caddy`,
`TestInterop_GET/nginx`, …), so cross-implementation interop bugs — QPACK, flow
control, connection-ID rotation, key-update timing — surface, not just one
vendor's behaviour.

## Run

```bash
make h3-interop
```

or directly:

```bash
# `run` (not `up --abort-on-container-exit`): the one-shot fixture/cert init
# services exiting must not tear the stack down before the runner executes.
docker compose -f test/integration/http3/docker-compose.yml run --rm runner
docker compose -f test/integration/http3/docker-compose.yml down -v
```

Expected output:

```
runner-1  | HTTP/3 response: status=200 body="hello from http3"
runner-1  | --- PASS: TestInterop_GET
```

## Why a container runner (not the host)

The `runner` service runs the Go client **inside a container** on the same
Docker network as the servers, rather than on the host against a published port.
Docker Desktop on Windows does not reliably forward host UDP to a published
container port, so a host-run QUIC client cannot reach the server;
container-to-container UDP works on every platform. The server matrix is injected
via `H3_INTEROP_ADDRS` (`caddy=h3caddy:443,nginx=h3nginx:443`); the single-server
`H3_INTEROP_ADDR` is still honoured (the loss harness uses it). nginx is gated on
container start rather than a healthcheck — its image ships no `curl`/`wget` — so
the runner retries the dial until nginx is serving.

## What it exercises

The full stack end to end against two strict, independent implementations
(Caddy on `quic-go`, nginx on its own C stack): the QUIC v1 + TLS 1.3 handshake,
transport parameters, 1-RTT keys, the HTTP/3 control stream + SETTINGS, a
QPACK-encoded request, and the decoded response HEADERS + DATA.

`http3/interop_test.go` is guarded by `//go:build interop`, so it is excluded
from the default build and CI (which have no server) and only runs under the
compose harness.
