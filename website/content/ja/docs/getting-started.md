---
title: はじめに
weight: 1
---

# はじめに

poseidon-http-client は Go 向けの低レベル HTTP クライアントです。HTTP/1.1、HTTP/2、HTTP/3 をフレーミング、HPACK、QPACK、QUIC スタックまで含めてすべて自前で実装しており、`net/http` にもサードパーティのプロトコルライブラリにも依存しません。コネクション、ストリーム、フロー制御を直接扱う必要がある負荷生成ツールやテストツールのために作られています。

## インストール

Go 1.25 以降が必要です。

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## リクエストモデル

3 つのプロトコルバージョンはすべて同じ API を共有します。コンストラクタでクライアントを作り、context・リクエスト・呼び出し側が所有する `*client.Response` を渡して `Do` を呼びます。

```go
c, err := client.NewSingleConnClient(addr, dialer)   // or any constructor above
defer c.Close()
resp := &client.Response{}                            // caller-owned, reusable
err = c.Do(ctx, client.GET("/path"), resp)            // resp.Status, resp.Body
resp.Reset()                                          // reuse for the next request
```

`net/http` と異なる点は 2 つあります。

- **レスポンスは呼び出し側が所有します。** `client.Response` を 1 つ確保して `Do` に渡し、`resp.Status`（`int`）と `resp.Body`（`[]byte`）を読んだら、`resp.Reset()` を呼んで次のリクエストで再利用します。呼び出しごとにレスポンスオブジェクトが確保されることはなく、リクエストループでは 1 つの `Response` を全イテレーションで使い回せます。
- **リクエストは値であり、トランスポート内部へのポインタではありません。** `client.GET(path)` で GET リクエストを組み立てられます。細かく制御したい場合は `client.Request{Method, Scheme, Authority, Path, BodyMode}` を直接構築してください。

## HTTP/2 での最初のリクエスト

以下はリポジトリの `examples/http2/main.go` そのままです。公開されている HTTP/2 エンドポイントに GET を 1 回発行します。

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

ここでプロトコルを決めているのはダイヤラーです。`conn.TLSDialer` に `NextProtos: []string{"h2"}` を指定すると、ALPN ネゴシエーションで HTTP/2 だけを提示します。クライアントはコネクションを 1 本維持し、切断されたら自動で再接続します。

## プロトコルの選び方

どのコンストラクタを呼ぶかでプロトコルバージョンが決まります。

| コンストラクタ | プロトコル | コネクションモデル |
|---|---|---|
| `client.NewH1Client(addr, dialer, opts...)` | HTTP/1.1 | 1 コネクション、リクエストは直列実行 |
| `client.NewH1PoolClient(addr, dialer, pool, opts...)` | HTTP/1.1 | 排他チェックアウト方式のプール、1 コネクションにつき 1 リクエスト |
| `client.NewManagedH1Client(resolver, dialer, opts...)` | HTTP/1.1 | サービスディスカバリ（Resolver + Selector） |
| `client.NewSingleConnClient(addr, dialer, opts...)` | HTTP/2 | 1 コネクション、自動再接続 |
| `client.NewPoolClient(addr, dialer, pool, opts...)` | HTTP/2 | ホスト単位のコネクションプール |
| `client.NewManagedClient(resolver, dialer, opts...)` | HTTP/2 | サービスディスカバリ（Resolver + Selector） |
| `client.NewH3Client(addr, tlsConfig, opts...)` | HTTP/3 | 1 QUIC コネクション |
| `client.NewH3PoolClient(addr, tlsConfig, pool, opts...)` | HTTP/3 | 複数 QUIC コネクションのプール |
| `client.NewManagedH3Client(resolver, tlsConfig, opts...)` | HTTP/3 | QUIC 上のサービスディスカバリ |

補足:

- HTTP/2 のコンストラクタでは、ダイヤラーの `NextProtos` に `"h2"` を含める必要があります。`client.TransportALPN` は、サーバーが h2 を提示しない場合に HTTP/1.1 へフォールバックします。
- HTTP/3 のコンストラクタは `*tls.Config` を直接受け取ります。トランスポートは UDP 上の QUIC です。
- どのコンストラクタも、同じ `Do` / `DoStream` メソッドを持つクライアントを返します。

## 次のステップ

プロトコルごとに個別のガイドがあり、検証済みのサンプルコード全体、ストリーミング、プーリング、そのバージョン固有のオプションを解説しています。

- [HTTP/1.1 ガイド](/docs/http1/)
- [HTTP/2 ガイド](/docs/http2/)
- [HTTP/3 ガイド](/docs/http3/)
