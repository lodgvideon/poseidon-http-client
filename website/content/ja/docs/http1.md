---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

対象サーバーが HTTP/2 に対応していない場合、あるいはサーバーの HTTP/1.1 経路を狙って試験したい場合に HTTP/1.1 を使います。後者では、ALPN ネゴシエーションによって接続が h2 に格上げされるのを防ぐ必要があります。

## HTTP/1.1 への固定

プロトコルの固定には次の 2 点が必要です。

1. `client.ClientOptions` で `Transport: client.TransportH1SingleConn` を指定する。
2. TLS ダイヤラの `NextProtos` に `"http/1.1"` だけを載せ、ハンドシェイク中にサーバーが `h2` を選べないようにする。

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

リクエスト API は HTTP/2・HTTP/3 と同じ `Do` / `client.GET` / `client.Response` です。試験対象のプロトコルバージョンを切り替えるときは、コンストラクタを変えるだけで済み、書き直しは要りません。

## 直列実行

`TransportH1SingleConn` は 1 本の接続上でリクエストを直列に実行します。各リクエストが完了してから次のリクエストが書き込まれます。パイプライニングはありません。HTTP/1.1 サーバーに並行負荷をかけたい場合は、クライアントを複数動かしてください。

## 自動フォールバック

HTTP/2 非対応のサーバーと通信するだけなら `TransportH1SingleConn` は不要です。`client.TransportALPN` は ALPN でネゴシエートし、サーバーが `h2` を提示しなければ自動的に HTTP/1.1 へフォールバックします。トランスポートを固定するのは、接続が確実に HTTP/1.1 のまま維持されるという保証が欲しいときだけです。
