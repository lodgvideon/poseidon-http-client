---
title: HTTP/3
weight: 4
---

# HTTP/3

HTTP/3 客户端（RFC 9114）运行在本仓库自行实现的 QUIC 协议栈（RFC 9000/9001/9002）和 QPACK 编解码器（RFC 9204）之上——不依赖 `quic-go`，不使用 cgo。只有 TLS 1.3 握手本身来自标准库 `crypto/tls`；数据包保护、丢包恢复、拥塞控制和流量控制均为 poseidon 自己的代码。

请求 API 与 HTTP/2 完全相同，仍是 `Do` / `DoStream`。`client.Request` 和 `client.Response` 在不同传输层之间没有任何区别；把一个压测从 H2 切换到 H3，只需换一个构造函数。

## 示例

`examples/http3/main.go`：

```go
// Command http3-example issues a single HTTP/3 GET over QUIC with the poseidon
// client, and shows how to opt into BBR congestion control.
//
//	go run ./examples/http3
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

func main() {
	// The simple path: one QUIC connection, buffered response.
	//
	//	c, err := client.NewH3Client("www.cloudflare.com:443",
	//	    &tls.Config{ServerName: "www.cloudflare.com"})
	//
	// Below uses NewClient so we can also select BBR congestion control.
	c, err := client.NewClient(client.ClientOptions{
		Addr:      "www.cloudflare.com:443",
		Transport: client.TransportH3,
		TLSConfig: &tls.Config{ServerName: "www.cloudflare.com"},
		H3ConnOptions: []quic.ConnOption{
			quic.WithCongestionControl(quic.CCBBR), // default is NewReno
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
	fmt.Printf("HTTP/3 %d — %d bytes\n", resp.Status, len(resp.Body))
}
```

## 构造函数

三个构造函数，与 HTTP/2 的一一对应：

```go
// One QUIC connection. Buffered (Do) and streaming (DoStream) requests.
client.NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error)

// Pool of QUIC connections to one host.
client.NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error)

// Service discovery: a Resolver supplies addresses, requests spread across them.
client.NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error)
```

对于这些构造函数没有暴露的设置——比如拥塞控制——请改用 `client.NewClient(client.ClientOptions{...})` 并设置 `Transport: client.TransportH3`，如上面的示例所示。

## 并发请求

一条 HTTP/3 连接可以同时承载多个在途请求，每个请求走独立的 QUIC 流。可以从多个 goroutine 并发地对同一个客户端调用 `Do` / `DoStream`；池化和托管两种变体还会进一步把流分散到多条连接上。

## 密码套件

QUIC 数据包保护支持全部三种 TLS 1.3 AEAD 套件：AES-128-GCM、AES-256-GCM 和 ChaCha20-Poly1305。由服务器在握手时选择，客户端无需任何配置。若协商出此范围之外的套件（例如 TLS_AES_128_CCM_8_SHA256），会在密钥安装阶段以类型化错误 `quic.ErrCryptoSuite` 失败——不会挂起，不会 panic。

## QPACK

头部压缩在两个方向上都使用动态表 QPACK：客户端针对动态表编码请求头，并在 decoder 流上解码服务器的插入。这一切自动完成，没有需要调节的开关。

## 内存分配

HTTP/3 线路编解码器是零分配的：QUIC 帧和数据包头、HTTP/3 帧以及 QPACK 字段区段的编解码均为 0 B/op、0 allocs/op。对 HTTP/2 帧和 HPACK 编解码器施加这一约束的同一个 CI 基准门禁，同样覆盖 `qpack`、`quic` 和 `http3` 包。QUIC 数据包发送路径是例外：构建并加密封装一个出站数据包会产生少量、有上界的分配。因此一次 HTTP/3 请求是低分配，而非零分配。

## 拥塞控制

默认是 NewReno。BBR 可以按需启用：

```go
H3ConnOptions: []quic.ConnOption{
	quic.WithCongestionControl(quic.CCBBR),
},
```

BBR 的实现正确且经过测试，但它相对 NewReno 的吞吐优势只在存在真实排队延迟的瓶颈 WAN 路径上才体现得出来。在局域网或 loopback 上测不出差异。如果无法在目标路径上做基准测试，就保持 NewReno 默认值。

## Linux 批量 I/O

在 Linux 上，QUIC 传输层使用 GSO（generic segmentation offload）在一次系统调用中把成批的出站数据包交给内核，并用 GRO 接收合并后的批次。两者都是自动的——无需配置，无需 build tag。在其他平台上，客户端每次系统调用只收发一个数据报。

## 非目标

1.0 有意不包含以下功能：

- **0-RTT / 会话恢复** —— 每条连接都执行完整握手。
- **QUIC 连接迁移** —— 连接绑定在建立时所用的路径上。
- **HTTP/3 服务器推送** —— 从不启用。

客户端从不主动发起以上任何一项。对端即使提供这些能力，也只是不被使用；不会失败，也不会产生错误。该领域唯一的硬性失败是不支持的密码套件，它会返回上文所述的类型化错误 `quic.ErrCryptoSuite`。
