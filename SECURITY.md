# Security Policy

## Supported versions

Only the latest tagged release receives security fixes. Older tags are not
patched; upgrade to the newest release before reporting an issue against it.

The library requires Go 1.25 or later. Security-relevant fixes in the Go
standard library (in particular `crypto/tls`) reach you through your Go
toolchain, not through this module — keep your toolchain current. See
[Go toolchain advisories](#go-toolchain-advisories) for the patch floor that
currently matters.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub Security Advisories: open the
repository's **Security** tab and use **Report a vulnerability**. Do not open
a public issue for a security problem — public issues disclose the bug before
a fix exists.

Include what you can: affected package (`frame`, `hpack`, `qpack`, `conn`,
`quic`, `http3`, `client`), a reproduction or packet trace, and the impact you
see (crash, memory growth, protocol confusion, data exposure).

## Scope

poseidon-http-client implements security-sensitive protocols from scratch:

- HTTP/2 framing and HPACK (RFC 7540, RFC 7541)
- QUIC (RFC 9000, 9001, 9002), including packet protection
- HTTP/3 and QPACK (RFC 9114, RFC 9204)

The TLS 1.3 handshake itself is **not** implemented here — it uses the Go
standard library `crypto/tls`. ChaCha20-Poly1305 packet protection uses
`golang.org/x/crypto`. Vulnerabilities in `crypto/tls` or `golang.org/x/crypto`
should be reported to the Go project; vulnerabilities in this module's framing,
header compression, QUIC, or HTTP/3 code belong here.

Parsers for untrusted input (frame, HPACK, QPACK, QUIC packets) are fuzzed and
covered by RFC-keyed conformance tests, but the library has not had a formal
third-party security audit.

## Go toolchain advisories

This library targets the **Go 1.25 line** (`go.mod` declares `go 1.25.0`), so
the patch releases that matter here are the 1.25 ones.

**Build with go1.25.13 or later.** As of 2026-08-14 that is the floor which
clears every standard-library advisory `govulncheck ./...` reports as reachable
from this module — scanned on go1.25.0, that is 20 advisories, the oldest of
them fixed in 1.25.2. The three most recent:

| Advisory | Standard-library package | Fixed on 1.25 | Fixed on 1.26 |
|---|---|---|---|
| [GO-2026-6090](https://pkg.go.dev/vuln/GO-2026-6090) | `crypto/tls` — unbounded post-handshake handshake messages | 1.25.13 | 1.26.6 |
| [GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972) | `encoding/asn1` — stack exhaustion on deeply nested input | 1.25.13 | 1.26.6 |
| [GO-2026-5856](https://pkg.go.dev/vuln/GO-2026-5856) | `crypto/tls` — Encrypted Client Hello privacy leak | 1.25.12 | 1.26.5 |

If you build on the 1.26 line instead, the equivalent floor is **go1.26.6**.
Do not treat go1.26.5 as patched: it carries the ECH fix only, and still
contains GO-2026-6090 and GO-2026-5972.

### GO-2026-5856 is flagged by scanners, but is not exploitable here

`govulncheck` reports GO-2026-5856 as reachable, because it resolves symbols
and every TLS dial in this library enters `crypto/tls`. The privacy leak itself
requires Encrypted Client Hello to be *configured*, which poseidon-http-client
never does — so the advisory is not exploitable via poseidon. Upgrading the
toolchain clears it from scanner output; it is not a live exposure.

## Disclaimer

This is young software, at its first release. It is provided **as is**, without
warranty of any kind, under the MIT license — use at your own risk. It has been
conformance-tested, fuzzed, and interop-verified against independent server
implementations, but it has not been audited by a third party. Do not deploy it
in security-critical production without your own review.
