---
title: poseidon-http-client
type: docs
---

# poseidon-http-client

一个 Go 语言的底层 HTTP 客户端，从零实现了 HTTP/1.1、HTTP/2 和 HTTP/3 —— 自研的帧编解码、HPACK、QPACK，以及一套从零编写的 QUIC 协议栈。它不使用 `net/http`，也不含任何第三方协议代码；四个直接依赖 —— `golang.org/x/net`、`golang.org/x/crypto`（用于 ChaCha20-Poly1305）、`github.com/andybalholm/brotli`、`github.com/klauspost/compress`（用于 zstd）—— 都是加密与压缩原语，TLS 1.3 来自标准库。三个协议版本共用同一套请求 API：`Do` 和 `DoStream`。它面向压测工具（load generator）以及需要精细控制连接、流和流量控制的场景 —— 不是 `net/http` 的通用替代品。

MIT 许可证。要求 Go 1.25。源码：[github.com/lodgvideon/poseidon-http-client](https://github.com/lodgvideon/poseidon-http-client)。

## 为什么选 poseidon

- **一个客户端，三个协议版本** —— HTTP/1.1、/2、/3 走同一套 `Do`/`DoStream` API；Go 标准库没有 HTTP/3。
- **没有第三方协议代码** —— QUIC、HTTP/3、QPACK、HTTP/2、HPACK 和 HTTP/1.1 全部在本模块内实现；没有 `quic-go`，没有 `nghttp2`，没有 cgo。四个直接依赖是加密与压缩原语（ChaCha20-Poly1305、Brotli、zstd）—— 这是有意的取舍：手写 AEAD 或解压器是安全隐患，不是卖点。
- **零分配线路编解码器** —— HTTP/2（帧、HPACK）与 HTTP/3（QPACK、HTTP/3 帧、QUIC 帧及包头）的编解码均达到 0 B/op、0 allocs/op，由 CI 基准门禁强制保证。
- **精细控制** —— 流、流量控制窗口、SETTINGS、连接池策略、拥塞控制（NewReno 或 BBR）；这些都是 `net/http` 不暴露的旋钮。
- **内置压测所需的功能** —— 连接池、DNS 服务发现、重试、限流、钩子与指标，三个协议版本共用。
- **一致性测试覆盖** —— 约 200 个按 RFC 章节对应的一致性测试，一个覆盖 3 个服务器（Caddy、nginx、aioquic）的 HTTP/3 真实 UDP 互通矩阵，线上解析器经过模糊测试，全程开启 `-race`。

## 指南

- [快速上手]({{< relref "/docs/getting-started" >}})
- [HTTP/1.1]({{< relref "/docs/http1" >}})
- [HTTP/2]({{< relref "/docs/http2" >}})
- [HTTP/3]({{< relref "/docs/http3" >}})
- [功能与优势]({{< relref "/docs/features" >}})
- [免责声明]({{< relref "/docs/disclaimer" >}})

{{< hint warning >}}
**年轻的软件。**这是首个发布版本。它从零实现了安全敏感的协议（TLS 1.3、QUIC、HPACK/QPACK），尚未经过第三方安全审计。按现状提供，使用风险自负（MIT —— 不提供任何担保）。部署前请阅读[免责声明]({{< relref "/docs/disclaimer" >}})。
{{< /hint >}}
