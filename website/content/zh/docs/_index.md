---
title: 文档
weight: 1
bookCollapseSection: false
---

# 文档

poseidon-http-client 是一个 Go 语言的底层 HTTP 客户端，从零实现了 HTTP/1.1、HTTP/2 和 HTTP/3——自带帧编解码、HPACK、QPACK 和 QUIC 协议栈，不依赖 `net/http`，也不使用任何第三方协议库。三个协议版本共用同一套 `Do`/`DoStream` API：你只需选择传输层，请求代码保持不变。本文档涵盖安装说明、每个协议各一页（附经过验证的示例）、各传输层共享的功能（连接池、服务发现、重试、限流、钩子、指标），以及一份免责声明——在任何安全敏感场景使用首个发布版本之前，请务必阅读。

- [快速上手](getting-started/) — 安装、环境要求、第一个请求。
- [HTTP/1.1](http1/) — 单连接传输、TLS 拨号器配置、ALPN 回退。
- [HTTP/2](http2/) — 单连接、连接池及服务发现托管客户端；流式传输与流量控制。
- [HTTP/3](http3/) — 基于 QUIC 的客户端、动态 QPACK、NewReno/BBR 拥塞控制。
- [功能特性](features/) — 请求 API、连接池、Resolver/Selector 服务发现、重试、限流、钩子、指标。
- [免责声明](disclaimer/) — 这里的"首个发布版本"意味着什么，以及如何报告安全漏洞。
