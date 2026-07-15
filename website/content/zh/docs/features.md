---
title: 功能与优势
weight: 5
---

# 功能与优势

## 支持矩阵

三个协议版本共用同一套请求 API：`client.Do` / `client.DoStream`，配合由调用方持有、可复用的 `client.Response`。压缩能力同样共享：响应端对 gzip、deflate、br、zstd 四种编码都能解码（客户端通告 `accept-encoding: gzip, deflate, br, zstd`），请求端可用 `Request.CompressBody` 压缩请求体 —— 见下文[压缩](#压缩)。不同之处在于各传输层暴露了多少协议能力。

| 协议 | 实现 | 构造函数 | 单连接并发请求 | 连接池 | 服务发现 | 主要能力 |
|---|---|---|---|---|---|---|
| HTTP/1.1 | 从零实现 | `NewH1Client`、`NewH1PoolClient`、`NewManagedH1Client` | 否 —— 每条连接同一时刻只承载一个交换（无 pipelining） | `NewH1PoolClient`（独占借出式连接池：`MaxConnsPerHost` 即请求并发数） | `NewManagedH1Client`（Resolver + Selector） | Keep-alive 连接复用；请求体可流式发送（`Request.BodyReader`，长度未知时用 chunked），但响应总是缓冲进 `Response.Body` —— `DoStream` 和 `BodyStream` 返回错误；作为 ALPN 回退目标：服务端不提供 h2 时，`TransportALPN` 自动选择 HTTP/1.1 |
| HTTP/2 | RFC 7540 + HPACK（RFC 7541），从零实现 | `NewSingleConnClient`、`NewPoolClient`、`NewManagedClient` | 是 —— 流多路复用，受 `MAX_CONCURRENT_STREAMS` 限制 | `NewPoolClient`（按主机建池、选取负载最低的流、空闲连接回收） | `NewManagedClient`（Resolver + Selector） | `DoStream` 与请求 trailer；流量控制；动态 SETTINGS；GOAWAY 排空；PING 保活；服务端推送（PUSH_PROMISE）；请求优先级；扩展 CONNECT（RFC 8441，基于 H2 的 WebSocket）；CONTINUATION；HTTP CONNECT 代理拨号器；h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC（RFC 9000/9001/9002）+ QPACK（RFC 9204），从零实现 | `NewH3Client`、`NewH3PoolClient`、`NewManagedH3Client` | 是 —— 单条 QUIC 连接上并发在途请求 | `NewH3PoolClient`（多连接池） | `NewManagedH3Client` | `DoStream`；双向动态 QPACK（编码 + 解码）；全部 TLS 1.3 AEAD（AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305）；拥塞控制默认 NewReno，可选启用 BBR；Linux 上支持 GSO 批量发送、GRO 批量接收、有界 ACK 合并 |

HTTP/1.1 与另外两个协议版本拥有同样的构造函数组合，但它的连接池在性质上不同。HTTP/1.1 没有多路复用：单条连接意味着请求严格串行，所以没有连接池时客户端根本无法产生 HTTP/1.1 负载。连接池以独占借出的方式分发连接 —— 每条连接同一时刻只承载一个交换 —— 因此 `MaxConnsPerHost` 就是请求并发数；`MaxStreamsPerConn` 不适用。请求发现所有连接都在忙时，会等待其中一条空出，等待受请求 context 约束。连接保持存活并被复用；遇到 `Connection: close`、连接失效或交换出错时，该连接被丢弃并重新拨号。Pipelining 是有意不实现的。拨号器不得提供 `h2` ALPN 令牌 —— 使用纯 TCP 拨号器，或 `NextProtos` 只含 `"http/1.1"` 的 TLS 拨号器。

在 HTTP/3 上启用 BBR：

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## 压缩

压缩在 HTTP/1.1、HTTP/2、HTTP/3 上的行为完全一致。

**响应。** 客户端通告 `accept-encoding: gzip, deflate, br, zstd`，四种编码都能解码，解码器复用自对象池。调用方自行提供的 accept-encoding 头优先；`Request.DisableDecompression` 同时抑制该头和解码。解压炸弹防护会拒绝膨胀超过 `MaxDecompressedSize`（默认 10 MiB）的响应体，返回 `ErrBodyTooLarge`；zstd 窗口上限为 8 MiB。`Content-Encoding` 匹配不区分大小写（RFC 9110 §8.4.1）。

**请求。** 设置 `Request.CompressBody`，客户端会压缩请求体并自行设置 `content-encoding`：

```go
var resp client.Response
err := c.Do(ctx, &client.Request{
    Method: "POST", Path: "/ingest",
    Body:   payload,
    CompressBody: client.EncodingZstd, // client sets content-encoding itself
}, &resp)
```

可用值为 `EncodingGzip`、`EncodingDeflate`、`EncodingBrotli`、`EncodingZstd`。零值 `EncodingIdentity` 原样发送请求体 —— 不启用的调用方不付任何代价（0 次分配）。手动设置 `content-encoding` 的含义不变，仍表示“这个请求体已经编码过”，请求体不会被改动（RFC 9110 §8.4 —— Content-Encoding 描述请求体，而不是一条指令）。同时设置 `CompressBody` 和手动的 `content-encoding` 会返回 `ErrConflictingContentEncoding`。content-length：缓冲请求体取压缩后的大小；流式请求体则省略（HTTP/1.1 随之改用 chunked 传输编码）。

## 为什么选 poseidon

**一个客户端，三个协议版本。** HTTP/1.1、HTTP/2、HTTP/3 走同一套 `Do`/`DoStream` API。Go 标准库没有 HTTP/3；多数技术栈靠一个独立的库、一套独立的 API 来补上。而在这里，把压测从 h2 切到 h3 只是换一个构造函数，不必重写。

**没有第三方协议代码。** 每一个协议栈都在本模块内从零实现：QUIC（RFC 9000/9001/9002）、HTTP/3（RFC 9114）、QPACK（RFC 9204）、HTTP/2 帧层（RFC 7540）与 HPACK（RFC 7541），以及 HTTP/1.1。没有 `quic-go`，没有 `nghttp2`，没有 `net/http`，没有 cgo。TLS 1.3 握手使用标准库 `crypto/tls`。四个直接依赖都是加密与压缩基础件，是有意引入而非自己手写：`golang.org/x/net`、`golang.org/x/crypto`（ChaCha20-Poly1305 包保护）、`github.com/andybalholm/brotli`、`github.com/klauspost/compress`（zstd）。重新实现 Poly1305 或 Brotli 只会带来安全风险而没有任何好处 —— Brotli 需要 122 KB 的静态字典外加 121 种变换，klauspost 的 zstd 背后有多年的模糊测试积累，而解压器正是首要的攻击面。所以边界很清楚：协议代码全部是我们自己的，在一个模块内可审计；加密与压缩基础件是借来的，因为在这里借用才是更稳妥的工程选择。

**零分配编解码。** 整套线上格式编解码在两个协议版本上都稳定在 0 B/op、0 allocs/op —— HTTP/2（frame 与 HPACK 的编解码）和 HTTP/3（QUIC 帧与包头的解析和序列化、HTTP/3 帧、QPACK 字段区段）—— CI 中的基准门禁在指标退化时直接使构建失败。压测工具在高请求速率下，每帧的内存分配会直接体现为 GC 压力；这套编解码不产生任何分配。`frame`、`hpack`、`qpack` 三个包都可以独立使用。有一条如实的边界：QUIC 的包发送路径（构造并加密出站包）是低分配而非零分配 —— 零分配的说法只针对编解码，不涵盖整个请求。

**细粒度控制。** 直接操作流、流控窗口、SETTINGS、连接池策略、拥塞控制（NewReno 或 BBR）和发包节奏 —— 这些都是 `net/http` 藏在其 transport 背后的旋钮。如果你的工具需要压住某个窗口不放、钉死流并发数、或测量某个拥塞控制器的效果，这些控制杆都是暴露出来的。

**压测所需的功能内置。** 连接池、DNS 服务发现（Resolver/Selector）、可选启用的幂等请求有界重试、令牌桶限速（`WithRateLimit`）、生命周期钩子（`Client.Hooks`）以及指标（`Client.MetricsSnapshot()`、`Client.PoolStats()`）。全部在 HTTP/1.1、HTTP/2、HTTP/3 之间共享 —— 配置一次，不必按协议各配一遍。

**经过一致性测试。** 约 200 个一致性测试逐条对应到具体 RFC 章节，并在 CI 中设卡。三服务端 HTTP/3 互通矩阵（Caddy/quic-go、nginx/C、aioquic/Python）在真实 UDP 上运行。线上格式解析器经过模糊测试。整个测试套件在 `-race` 下运行。

## 与 net/http 对比

`net/http` 是开箱即用的标准客户端。它无需配置就能处理重定向、Cookie、来自环境变量的代理，以及 HTTP/1.1 与 HTTP/2 的协商。poseidon 拿这份便利换取控制力：它加上了 HTTP/3、零分配编解码和压测工具链，代价是你要按目标逐个构造客户端、自己管理响应对象。想要通用的 Web 客户端，用 `net/http`；要做压测工具，或者需要 HTTP/3 加上细粒度控制，用 poseidon。

## 与 quic-go 对比

`quic-go` 是成熟且被广泛使用的 QUIC 与 HTTP/3 库，服务端和客户端都覆盖。poseidon 重新实现 QUIC，是为了把所有协议代码留在本模块内，并保持对压测场景的专注。它更年轻、范围更窄：是一个 HTTP 客户端。`quic` 包确实暴露了服务端角色（`Listen` / `Accept`、接受流）—— 它的存在是为了在测试中给客户端一个真实的对端 —— 但其上没有 HTTP/3 服务端。如果你需要久经考验的 QUIC 协议栈或 HTTP/3 服务端，`quic-go` 是既定的选择。

## 1.0 的非目标

以下能力在本版本中有意不做：

- **0-RTT / 会话恢复。** 客户端从不发起。
- **QUIC 连接迁移。** 不发起。
- **HTTP/3 服务端推送。** 不参与。

对端提供这些能力时，客户端只是不予使用 —— 不会出错。遇到不支持的 TLS 密码套件时，会以类型化错误 `ErrCryptoSuite` 干净地失败；不会挂起，也不会 panic。
