---
title: 快速上手
weight: 1
---

# 快速上手

poseidon-http-client 是一个 Go 语言的底层 HTTP 客户端。它从零实现了 HTTP/1.1、HTTP/2 和 HTTP/3——自带帧编解码、HPACK、QPACK 和 QUIC 协议栈——不依赖 `net/http`，也不依赖任何第三方协议库。它面向压测工具和需要直接控制连接、流和流量控制的场景。

## 安装

要求 Go 1.25 或更高版本。

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## 请求模型

三个协议版本共用同一套 API。先用构造函数创建客户端，再调用 `Do`，传入 context、请求对象，以及一个由调用方持有的 `*client.Response`：

```go
c, err := client.NewSingleConnClient(addr, dialer)   // or any constructor above
defer c.Close()
resp := &client.Response{}                            // caller-owned, reusable
err = c.Do(ctx, client.GET("/path"), resp)            // resp.Status, resp.Body
resp.Reset()                                          // reuse for the next request
```

与 `net/http` 有两处不同：

- **响应对象由调用方持有。** 你只分配一个 `client.Response`，传给 `Do`，读取 `resp.Status`（`int`）和 `resp.Body`（`[]byte`），然后调用 `resp.Reset()` 供下一次请求复用。每次调用不会分配新的响应对象——在请求循环中，一个 `Response` 可以贯穿所有迭代。
- **请求是值，不是指向传输层的指针。** `client.GET(path)` 构造一个 GET 请求。需要完全控制时，直接构造 `client.Request{Method, Scheme, Authority, Path, BodyMode}`。

## 第一个 HTTP/2 请求

下面是仓库中的 `examples/http2/main.go`，原样未改。它向一个公开的 HTTP/2 端点发起一次 GET：

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

这里由 dialer 决定协议：`conn.TLSDialer` 配置 `NextProtos: []string{"h2"}`，在 ALPN 协商中只提供 HTTP/2。客户端保持一条连接，断开后自动重拨。

## 选择协议

调用哪个构造函数，就决定了使用哪个协议版本：

| 构造函数 | 协议 | 连接模型 |
|---|---|---|
| `client.NewH1Client(addr, dialer, opts...)` | HTTP/1.1 | 单连接，请求串行 |
| `client.NewH1PoolClient(addr, dialer, pool, opts...)` | HTTP/1.1 | 独占式连接池，每条连接同时只处理一个请求 |
| `client.NewManagedH1Client(resolver, dialer, opts...)` | HTTP/1.1 | 服务发现（Resolver + Selector） |
| `client.NewSingleConnClient(addr, dialer, opts...)` | HTTP/2 | 单连接，自动重拨 |
| `client.NewPoolClient(addr, dialer, pool, opts...)` | HTTP/2 | 按主机的连接池 |
| `client.NewManagedClient(resolver, dialer, opts...)` | HTTP/2 | 服务发现（Resolver + Selector） |
| `client.NewH3Client(addr, tlsConfig, opts...)` | HTTP/3 | 单条 QUIC 连接 |
| `client.NewH3PoolClient(addr, tlsConfig, pool, opts...)` | HTTP/3 | 多连接 QUIC 池 |
| `client.NewManagedH3Client(resolver, tlsConfig, opts...)` | HTTP/3 | 基于 QUIC 的服务发现 |

说明：

- HTTP/2 的构造函数要求 dialer 的 `NextProtos` 提供 `"h2"`。`client.TransportALPN` 在服务器不支持 h2 时回退到 HTTP/1.1。
- HTTP/3 的构造函数直接接收 `*tls.Config`；传输层是基于 UDP 的 QUIC。
- 所有构造函数返回的客户端都提供相同的 `Do` / `DoStream` 方法。

## 下一步

每个协议都有独立的指南，包含完整的已验证示例、流式传输、连接池，以及该版本特有的选项：

- [HTTP/1.1 指南](/docs/http1/)
- [HTTP/2 指南](/docs/http2/)
- [HTTP/3 指南](/docs/http3/)
