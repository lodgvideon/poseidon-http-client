---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

当目标服务器不支持 HTTP/2，或者你想专门测试它的 HTTP/1.1
路径、必须阻止 ALPN 协商把连接升级到 h2 时，使用 HTTP/1.1。

## 强制使用 HTTP/1.1

固定协议需要两步：

1. 在 `client.ClientOptions` 中设置 `Transport: client.TransportH1SingleConn`。
2. 使用一个 `NextProtos` 只提供 `"http/1.1"` 的 TLS
   dialer，这样服务器在握手时无法选中 `h2`。

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

请求 API 与 HTTP/2、HTTP/3 使用的 `Do` / `client.GET` /
`client.Response` 完全相同。因此在不同协议版本之间切换测试目标，
只需换一个构造函数，不用重写代码。

## 请求串行化

`TransportH1SingleConn` 只维护一条连接，并在其上串行发送请求：
上一个请求完成后才写入下一个。不支持 pipelining。要对 HTTP/1.1
服务器施加并发负载，请运行多个客户端实例。

## 自动回退

如果只是目标服务器恰好不支持 HTTP/2，并不需要
`TransportH1SingleConn`。`client.TransportALPN` 通过 ALPN
协商，在服务器不提供 `h2` 时会自动回退到 HTTP/1.1。只有当你需要
确保连接始终停留在 HTTP/1.1 上时，才固定 transport。
