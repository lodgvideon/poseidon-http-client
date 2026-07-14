# Security Policy

## Supported versions

Only the latest tagged release receives security fixes. Older tags are not
patched; upgrade to the newest release before reporting an issue against it.

The library requires Go 1.25 or later. Security-relevant fixes in the Go
standard library (in particular `crypto/tls`) reach you through your Go
toolchain, not through this module — keep your toolchain current.

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

## GO-2026-5856 (Encrypted Client Hello in crypto/tls)

Vulnerability scanners may flag GO-2026-5856, an Encrypted Client Hello (ECH)
advisory in the Go standard library's `crypto/tls`. poseidon-http-client does
not enable ECH — the vulnerable code path is not reachable through this
library, so the advisory is not exploitable via poseidon.

To clear the advisory entirely (including in `govulncheck` output), build with
Go 1.26.5 or later, which contains the upstream fix.

## Disclaimer

This is young software, at its first release. It is provided **as is**, without
warranty of any kind, under the MIT license — use at your own risk. It has been
conformance-tested, fuzzed, and interop-verified against independent server
implementations, but it has not been audited by a third party. Do not deploy it
in security-critical production without your own review.
