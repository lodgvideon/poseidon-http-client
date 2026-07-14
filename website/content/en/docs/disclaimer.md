---
title: Disclaimer
weight: 6
---

# Disclaimer

poseidon-http-client is young software. This is its first release. Read this page before depending on it.

## No warranty

The library is licensed under MIT and is provided **as is**, without warranty of any kind. You use it at your own risk. See the [LICENSE](https://github.com/lodgvideon/poseidon-http-client/blob/main/LICENSE) file for the exact terms.

## Security-sensitive code, no third-party audit

This library implements security-sensitive protocols from scratch: the QUIC transport (RFC 9000/9001/9002), TLS 1.3 record protection for QUIC, HPACK (RFC 7541), and QPACK (RFC 9204). It does not reuse `net/http`, `quic-go`, or any other established protocol implementation for these paths.

What has been done:

- ~200 conformance tests keyed to RFC sections, gated in CI.
- Fuzzing of the wire parsers.
- Interop verification against three independent HTTP/3 server implementations (Caddy/quic-go, nginx, aioquic) over real UDP.
- The full test suite runs under the Go race detector.

What has **not** been done: a formal third-party security audit. Testing and fuzzing reduce the defect rate; they do not prove the absence of vulnerabilities in a from-scratch protocol stack.

## Before production use

Do not treat this library as a drop-in replacement for `net/http` in security-critical systems. If your deployment handles untrusted peers, sensitive data, or is otherwise security-critical, review the code paths you depend on yourself — or wait until the project has more production history — before adopting it.

For load generation, testing, and tooling against infrastructure you control, the risk profile is lower. That is the primary use case this library is built for.

## Reporting vulnerabilities

If you find a security issue, report it privately as described in [SECURITY.md](https://github.com/lodgvideon/poseidon-http-client/blob/main/SECURITY.md). Do not open a public GitHub issue for vulnerabilities.
