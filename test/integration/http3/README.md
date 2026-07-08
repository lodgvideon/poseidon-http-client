# HTTP/3 interop test

Validates the from-scratch HTTP/3 client (`http3.Dial` → `Client.Do`) against a
real [Caddy](https://caddyserver.com/) HTTP/3 server over real UDP.

## Run

```bash
make h3-interop
```

or directly:

```bash
docker compose -f test/integration/http3/docker-compose.yml \
  up --abort-on-container-exit --exit-code-from runner
docker compose -f test/integration/http3/docker-compose.yml down -v
```

Expected output:

```
runner-1  | HTTP/3 response: status=200 body="hello from http3"
runner-1  | --- PASS: TestInterop_GET
```

## Why a container runner (not the host)

The `runner` service runs the Go client **inside a container** on the same
Docker network as Caddy, rather than on the host against a published port. Docker
Desktop on Windows does not reliably forward host UDP to a published container
port, so a host-run QUIC client cannot reach the server; container-to-container
UDP works on every platform. The test address is injected via `H3_INTEROP_ADDR`
(`h3caddy:443`).

## What it exercises

The full stack end to end against a strict, independent implementation
(Caddy uses `quic-go`): the QUIC v1 + TLS 1.3 handshake, transport parameters,
1-RTT keys, the HTTP/3 control stream + SETTINGS, a QPACK-encoded request, and
the decoded response HEADERS + DATA.

`http3/interop_test.go` is guarded by `//go:build interop`, so it is excluded
from the default build and CI (which have no server) and only runs under the
compose harness.
