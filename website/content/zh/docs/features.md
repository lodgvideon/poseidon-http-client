---
title: 功能与优势
weight: 5
---

# 功能与优势

## 支持矩阵

三个协议版本共用同一套请求 API：`client.Do` / `client.DoStream`，配合由调用方持有、可复用的 `client.Response`。不同之处在于各传输层暴露了多少协议能力。

| 协议 | 实现 | 构造函数 | 单连接并发请求 | 连接池 | 服务发现 | 主要能力 |
|---|---|---|---|---|---|---|
| HTTP/1.1 | 从零实现 | `NewClient` 搭配 `TransportH1SingleConn` | 否 —— 单连接，请求串行执行（无 pipelining） | 无 | 无 | 作为 ALPN 回退目标：服务端不提供 h2 时，`TransportALPN` 自动选择 HTTP/1.1 |
| HTTP/2 | RFC 7540 + HPACK（RFC 7541），从零实现 | `NewSingleConnClient`、`NewPoolClient`、`NewManagedClient` | 是 —— 流多路复用，受 `MAX_CONCURRENT_STREAMS` 限制 | `NewPoolClient`（按主机建池、选取负载最低的流、空闲连接回收） | `NewManagedClient`（Resolver + Selector） | `DoStream` 与请求 trailer；流量控制；动态 SETTINGS；GOAWAY 排空；PING 保活；服务端推送（PUSH_PROMISE）；请求优先级；扩展 CONNECT（RFC 8441，基于 H2 的 WebSocket）；CONTINUATION；HTTP CONNECT 代理拨号器；h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC（RFC 9000/9001/9002）+ QPACK（RFC 9204），从零实现 | `NewH3Client`、`NewH3PoolClient`、`NewManagedH3Client` | 是 —— 单条 QUIC 连接上并发在途请求 | `NewH3PoolClient`（多连接池） | `NewManagedH3Client` | `DoStream`；双向动态 QPACK（编码 + 解码）；全部 TLS 1.3 AEAD（AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305）；拥塞控制默认 NewReno，可选启用 BBR；Linux 上支持 GSO 批量发送、GRO 批量接收、有界 ACK 合并 |

HTTP/1.1 支持有意保持精简 —— 它的存在是为了让一次压测能用同一套代码打遍三个协议版本的同一目标，同时作为 `TransportALPN` 的回退。HTTP/2 和 HTTP/3 才是功能完整的传输层。

在 HTTP/3 上启用 BBR：

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## 为什么选 poseidon

**一个客户端，三个协议版本。** HTTP/1.1、HTTP/2、HTTP/3 走同一套 `Do`/`DoStream` API。Go 标准库没有 HTTP/3；多数技术栈靠一个独立的库、一套独立的 API 来补上。而在这里，把压测从 h2 切到 h3 只是换一个构造函数，不必重写。

**从零实现，几乎零依赖。** 没有 `quic-go`，没有 `nghttp2`，没有 cgo。直接依赖只有 `golang.org/x/net` 和 `golang.org/x/crypto`（后者仅用于 ChaCha20-Poly1305 包保护）；TLS 1.3 握手使用标准库 `crypto/tls`。协议代码全部在本模块内 —— 可审计、暴露面小，没有传递依赖带来的供应链膨胀。

**零分配编解码。** Frame 和 HPACK 的编解码稳定在 0 B/op、0 allocs/op，CI 中的基准门禁在指标退化时直接使构建失败。压测工具在高请求速率下，每帧的内存分配会直接体现为 GC 压力；这套编解码不产生任何分配。`frame`、`hpack`、`qpack` 三个包都可以独立使用。

**细粒度控制。** 直接操作流、流控窗口、SETTINGS、连接池策略、拥塞控制（NewReno 或 BBR）和发包节奏 —— 这些都是 `net/http` 藏在其 transport 背后的旋钮。如果你的工具需要压住某个窗口不放、钉死流并发数、或测量某个拥塞控制器的效果，这些控制杆都是暴露出来的。

**压测所需的功能内置。** 连接池、DNS 服务发现（Resolver/Selector）、可选启用的幂等请求有界重试、令牌桶限速（`WithRateLimit`）、生命周期钩子（`Client.Hooks`）以及指标（`Client.MetricsSnapshot()`、`Client.PoolStats()`）。全部在 HTTP/2 和 HTTP/3 之间共享 —— 配置一次，不必按协议各配一遍。

**经过一致性测试。** 约 200 个一致性测试逐条对应到具体 RFC 章节，并在 CI 中设卡。三服务端 HTTP/3 互通矩阵（Caddy/quic-go、nginx/C、aioquic/Python）在真实 UDP 上运行。线上格式解析器经过模糊测试。整个测试套件在 `-race` 下运行。

## 与 net/http 对比

`net/http` 是开箱即用的标准客户端。它无需配置就能处理重定向、Cookie、来自环境变量的代理，以及 HTTP/1.1 与 HTTP/2 的协商。poseidon 拿这份便利换取控制力：它加上了 HTTP/3、零分配编解码和压测工具链，代价是你要按目标逐个构造客户端、自己管理响应对象。想要通用的 Web 客户端，用 `net/http`；要做压测工具，或者需要 HTTP/3 加上细粒度控制，用 poseidon。

## 与 quic-go 对比

`quic-go` 是成熟且被广泛使用的 QUIC 与 HTTP/3 库，服务端和客户端都覆盖。poseidon 重新实现 QUIC 是为了保持零依赖并专注于压测场景。它更年轻、范围更窄：只做客户端，没有服务端。如果你需要久经考验的 QUIC 协议栈或需要服务端，`quic-go` 是既定的选择。

## 1.0 的非目标

以下能力在本版本中有意不做：

- **0-RTT / 会话恢复。** 客户端从不发起。
- **QUIC 连接迁移。** 不发起。
- **HTTP/3 服务端推送。** 不参与。

对端提供这些能力时，客户端只是不予使用 —— 不会出错。遇到不支持的 TLS 密码套件时，会以类型化错误 `ErrCryptoSuite` 干净地失败；不会挂起，也不会 panic。
