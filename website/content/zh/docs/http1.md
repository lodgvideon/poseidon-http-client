---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

当目标服务器不支持 HTTP/2，或者你想专门测试它的 HTTP/1.1
路径、必须阻止 ALPN 协商把连接升级到 h2 时，使用 HTTP/1.1。

HTTP/1.1 拥有与 HTTP/2、HTTP/3 相同的一套构造函数：

| 构造函数 | Transport | 形态 |
|---|---|---|
| `NewH1Client(addr, dialer, opts...)` | `TransportH1SingleConn` | 一条连接，自动重拨。请求串行执行。 |
| `NewH1PoolClient(addr, dialer, pool, opts...)` | `TransportH1Pool` | 最多 `MaxConnsPerHost` 条连接。这个数字就是请求并发度。 |
| `NewManagedH1Client(resolver, dialer, opts...)` | `TransportH1Managed` | Resolver + Selector 服务发现；按地址各建一个 HTTP/1.1 子连接池分摊请求。 |

## 强制使用 HTTP/1.1

固定协议需要两点：

1. 一个 HTTP/1.1 transport——上表中的任一构造函数，或在
   `client.ClientOptions` 中设置对应的 `Transport` 值。
2. 一个不提供 `h2` 的 dialer：纯 TCP dialer
   （`&conn.PlaintextDialer{}`），或 `NextProtos` 只提供
   `"http/1.1"` 的 TLS dialer，这样服务器在握手时无法选中 `h2`。

```go
// Command http1-example issues a single HTTP/1.1 request with the poseidon
// client. HTTP/1.1 is reached by pinning the transport to TransportH1SingleConn
// and offering only the "http/1.1" ALPN token, so the connection never upgrades
// to HTTP/2.
//
//	go run ./examples/http1
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
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "example.com:443",
		Transport: client.TransportH1SingleConn,
		ConnOpts: conn.ConnOptions{
			// Offer only http/1.1 so the server cannot select h2.
			Dialer: &conn.TLSDialer{Config: &tls.Config{
				ServerName: "example.com",
				NextProtos: []string{"http/1.1"},
			}},
		},
	})
	if err != nil {
		log.Fatalf("build client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/"), resp); err != nil {
		log.Fatalf("GET /: %v", err)
	}
	fmt.Printf("HTTP/1.1 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

`client.NewH1Client(addr, dialer)` 一行调用即可构建同样的单连接
客户端。请求 API 与 HTTP/2、HTTP/3 使用的 `Do` / `client.GET` /
`client.Response` 完全相同。因此在不同协议版本之间切换测试目标，
只需换一个构造函数，不用重写代码。

## 一条连接意味着串行请求

HTTP/1.1 没有多路复用：一条连接同一时刻只承载一个请求/响应
交换。Pipelining 是有意不支持的。因此 `TransportH1SingleConn`
串行发送请求——上一个请求完成后才写入下一个，第二个并发的 `Do`
会等待第一个结束。在单条连接上，HTTP/1.1 负载是严格串行的。
需要并发时，请使用连接池。

## 连接池

```go
c, err := client.NewH1PoolClient("example.com:443",
	&conn.TLSDialer{Config: &tls.Config{
		ServerName: "example.com",
		NextProtos: []string{"http/1.1"},
	}},
	client.PoolOptions{MaxConnsPerHost: 32},
)
if err != nil {
	log.Fatalf("build client: %v", err)
}
defer func() { _ = c.Close() }()
// Up to 32 requests in flight. The 33rd waits for a connection to free.
```

与 HTTP/2、HTTP/3 的连接池不同，这是一个独占签出
（exclusive-checkout）式连接池。H2/H3 连接池把同一条连接交给
许多并发流、并挑选负载最低的一条；HTTP/1.1 连接池则在一次交换
期间把连接从空闲集合中取出，交换结束后归还。由此带来几个结果：

- `MaxConnsPerHost` **就是**请求并发度。它是这里唯一起作用的
  容量参数，连接池拨号数永远不会超过它。
- `MaxStreamsPerConn` 对 HTTP/1.1 不适用，会被忽略
  （`PoolOptions` 与 H2/H3 连接池共用；这里每条连接的上限
  恒为 1）。
- 当所有连接都在忙时，请求会等待某条连接空出，等待受其 ctx
  约束（若设置了 `PoolOptions.AcquireTimeout`，也受其约束）。
  请求绝不会被排到一条正忙的连接上串行执行。

连接在多次交换之间保持存活并被复用。当响应表明连接不会保持
（`Connection: close`，或没有 keep-alive 的 HTTP/1.0）、对端
关闭了套接字、或任何一次交换出错时，连接会被丢弃并重新拨号，
而不是复用。空闲淘汰与健康检查扫描与 H2/H3 连接池一致。

对于多地址目标，`NewManagedH1Client(resolver, dialer)` 为每个
解析出的地址运行一个独占签出子连接池；此时 `MaxConnsPerHost`
设定的是每个地址的并发度。

## 流式传输

HTTP/1.1 不支持响应流式传输：响应总是完整缓冲到
`Response.Body` 中。在 HTTP/1.1 transport 上调用 `DoStream`
会返回错误，`Do` 搭配 `Request.BodyMode: client.BodyStream`
也一样。这是有意为之——需要流式读取响应时，请使用 HTTP/2 或
HTTP/3。请求体则可以流式发送：当长度事先未知时，
`Request.BodyReader` 会以 `Transfer-Encoding: chunked` 发送，
`Request.CompressBody` 发送压缩的流式请求体走的也是这条路径。

## 自动回退

如果只是目标服务器恰好不支持 HTTP/2，并不需要专门使用
HTTP/1.1 transport。`client.TransportALPN` 通过 ALPN
协商，在服务器不提供 `h2` 时会自动回退到 HTTP/1.1。只有当你需要
确保连接始终停留在 HTTP/1.1 上时，才固定 transport。
