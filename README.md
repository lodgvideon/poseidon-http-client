# poseidon-http-client

A low-level, zero-allocation HTTP/2 client codec for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from scratch
without `net/http` or `golang.org/x/net/http2`.

**Status:** Phase A — frame codec + HPACK. See [design](docs/superpowers/specs/2026-05-02-poseidon-frame-layer-design.md).

## Phases

- **A — Frame layer + HPACK** *(in progress)*: codec only, no networking.
- **B — Connection layer**: TLS, ALPN, stream state machine, flow control.
- **C — Client + pool + discovery + stats**: public API for load generators.

## Local development

Requirements: Go 1.24, `golangci-lint` v1.62.

```bash
make tidy
make lint
make test-race
make bench
```

Optional pre-commit hook:

```bash
git config core.hooksPath .githooks
```

## License

MIT — see [LICENSE](LICENSE).
