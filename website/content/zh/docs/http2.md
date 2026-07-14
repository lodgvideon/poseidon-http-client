---
title: HTTP/2
weight: 3
---

# HTTP/2

HTTP/2 客户端从零实现 RFC 7540 与 HPACK（RFC 7541）——不依赖 `net/http`，也不依赖 `golang.org/x/net/http2`。三个构造函数覆盖常见的连接拓扑：单连接、按主机的连接池、由 resolver 驱动的多后端。三者返回的都是 `*client.Client`，共用同一套 `Do` / `DoStream` API。

## 单连接

`client.NewSingleConnClient` 持有一条连接，断开后自动重拨。以下是 `examples/http2/main.go`：

```go
// Command http2-example issues a single HTTP/2 GET with the poseidon client.
// TransportSingleConn (the default) negotiates h2 over TLS and reuses one
// connection with automatic redial.
//
//	go run ./examples/http2
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	c, err := client.NewSingleConnClient(
		"www.cloudflare.com:443",
		&conn.TLSDialer{Config: &tls.Config{
			ServerName: "www.cloudflare.com",
			NextProtos: []string{"h2"},
		}},
	)
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Response is caller-owned and reusable; Reset() clears it between requests.
	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/2 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

`Response` 由调用方持有：分配一次，请求之间调用 `resp.Reset()` 复用。`client.GET(path)` 是简写；需要完全控制时直接构造 `client.Request{Method, Scheme, Authority, Path, BodyMode}`。

## 连接池

`client.NewPoolClient` 维护按主机的连接池。每个请求分配给负载最低的连接，最多拨号 `MaxConnsPerHost` 条连接，并在健康检查周期到达时淘汰空闲连接。

```go
c, err := client.NewPoolClient("api.example.com:443", dialer,
	client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		HealthCheckPeriod: 30 * time.Second,
	})
```

`MaxStreamsPerConn` 是软上限；实际生效的限制取该值与对端 `SETTINGS_MAX_CONCURRENT_STREAMS` 的较小者。`c.Warmup(n)` 预拨连接；`c.PoolStats()` 报告实时计数；`c.Shutdown(timeout)` 平滑排空。

## 流式请求

`DoStream` 在响应 HEADERS 到达后立即返回。调用方循环调用 `Recv` 读取 DATA、trailer 和 reset 事件，并且必须调用 `Close`。

```go
var sr client.StreamResponse
if err := c.DoStream(ctx, client.GET("/events"), &sr); err != nil {
	log.Fatal(err)
}
defer func() { _ = sr.Close() }() // mandatory: releases the stream slot

for {
	ev, err := sr.Recv(ctx)
	if errors.Is(err, client.ErrStreamEnded) {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
	if ev.Type == client.EventData {
		// ev.Data aliases a pooled buffer recycled on the next Recv;
		// use ev.DataCopy() to retain the bytes.
		fmt.Printf("chunk: %d bytes\n", len(ev.Data))
	}
}
```

相关形式：

- `c.Stream(ctx, req, fn)` — 回调形式，流总是替你关闭。
- `req.BodyMode = client.BodyStream` 搭配普通 `Do` — 响应体在 `resp.BodyReader` 上按需到达（一个 `io.ReadCloser`，记得关闭）。
- `req.BodyReader` — 从 `io.Reader` 流式发送请求体，在流量控制下切分为 DATA 帧。
- `sr.WaitTrailers(ctx)` — 排空响应体并返回响应 trailer（例如 gRPC 的 `grpc-status`）。

## 服务发现

`client.NewManagedClient` 接受一个 `Resolver`（有哪些后端）和一个 `Selector`（下一个请求发给谁）。每个解析出的地址对应一个子池。

```go
resolver := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 443},
	client.Address{Host: "10.0.0.2", Port: 443},
)
c, err := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

`client.DNSResolver(host, port, client.DNSOptions{TTL: 30 * time.Second})` 按 TTL 重新解析 A/AAAA 记录；连接池会拨号新增的后端并排空被移除的后端。可用的 Selector：`RoundRobin()`、`Random(rng)`、用于会话亲和的 `Hash(keyFn)`。`Resolver` 是一个接口——实现 `Resolve`/`Watch` 即可接入自己的服务发现。

## 弹性

重试通过 `Retryer` 显式开启。它在瞬时故障（REFUSED_STREAM、GOAWAY、拨号错误）时重发幂等请求，采用指数退避加抖动，次数受 `MaxAttempts` 限制。

```go
r := c.Retryer(client.RetryOptions{
	MaxAttempts: 5,
	IsRetryable: func(err error, resp *client.Response) bool {
		return err == nil && resp != nil && resp.Status == 503
	},
})
var resp client.Response
err = r.Do(ctx, client.GET("/v1/health"), &resp)
```

非幂等方法不会被重试，除非设置 `req.Idempotency = client.ForceIdempotent`（应配合幂等键使用）。令牌桶限流是构造函数选项——`client.WithRateLimit(100, 20)` 限制每秒 100 个请求，突发最多 20。按请求设置超时：设 `req.Timeout`；到期时 `Do` 以 `context.DeadlineExceeded` 失败，流以 `RST_STREAM(CANCEL)` 重置。

## 可观测性

`WithHooks` 安装生命周期回调；`MetricsSnapshot` 返回计数器与延迟直方图的一份冻结视图。

```go
hooks := &client.Hooks{
	OnRequestComplete: func(ev client.RequestCompleteEvent) {
		log.Printf("%s %s -> %d in %s (%d B sent, %d B recv)",
			ev.Method, ev.Path, ev.Status, ev.Latency,
			ev.BytesSent, ev.BytesRecv)
	},
}
c, err := client.NewSingleConnClient(addr, dialer, client.WithHooks(hooks))
// ...
snap := c.MetricsSnapshot()
fmt.Println(snap.Counters.Responses2xx, snap.Latency.Request.Quantile(0.99))
```

其他钩子：`OnRequestStart`、`OnRetry`、`OnDial`、`OnConnClose`、`OnResolverUpdate`。钩子不得阻塞。`c.PoolStats()` 暴露连接池实时状态，可用于 `/debug` 端点。

## 高级协议特性

**服务器推送（RFC 7540 §8.2）。** 用 `WithPushHandler` 注册处理器，即在 SETTINGS 中启用推送。客户端把每个被推送的流排空到一个 `Response`，再交给回调。未注册处理器时，PUSH_PROMISE 是协议错误。

```go
push := func(ctx context.Context, promised []conn.HeaderField, resp *client.Response, err error) {
	if err != nil {
		log.Printf("push failed: %v", err)
		return
	}
	log.Printf("pushed -> %d (%d bytes)", resp.Status, len(resp.Body))
}
c, err := client.NewSingleConnClient(addr, dialer, client.WithPushHandler(push))
```

**扩展 CONNECT（RFC 8441）。** 在单条 HTTP/2 流上隧道 WebSocket（或任何协议）。服务器须通告 `SETTINGS_ENABLE_CONNECT_PROTOCOL=1`。

```go
req := &client.Request{
	Method:   "CONNECT",
	Protocol: "websocket",
	Path:     "/chat",
	BodyMode: client.BodyStream,
}
var sr client.StreamResponse
err = c.DoStream(ctx, req, &sr)
```

**H2C（prior knowledge）。** 不经 TLS 与 ALPN 的明文 HTTP/2：使用 `PlaintextDialer` 并把默认 scheme 设为 `http`。

```go
c, err := client.NewSingleConnClient("localhost:8080",
	&conn.PlaintextDialer{},
	client.WithDefaultScheme("http"))
```

同样支持：请求优先级（`req.Priority = &frame.Priority{...}`，RFC 7540 §5.3）、请求 trailer（`req.TrailerFunc`）、HTTP CONNECT 代理拨号器（`conn.ProxyTLSDialer`）。

## 负载生成器示例

`examples/loadgen` 是一个单文件的完整 HTTP/2 负载生成器：池化客户端、N 个 worker goroutine、可选的全局 QPS 上限，以及基于 `MetricsSnapshot` 生成的汇总。

```bash
go run ./examples/loadgen -url https://localhost:8443/ \
    -conns 4 -workers 64 -duration 30s -rps 5000
```
