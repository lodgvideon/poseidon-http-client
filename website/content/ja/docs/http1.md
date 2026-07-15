---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

対象サーバーが HTTP/2 に対応していない場合、あるいはサーバーの HTTP/1.1 経路を狙って試験したい場合に HTTP/1.1 を使います。後者では、ALPN ネゴシエーションによって接続が h2 に格上げされるのを防ぐ必要があります。

HTTP/1.1 には HTTP/2・HTTP/3 と同じコンストラクタ群があります。

| コンストラクタ | トランスポート | 形態 |
|---|---|---|
| `NewH1Client(addr, dialer, opts...)` | `TransportH1SingleConn` | 接続 1 本、自動再ダイヤル。リクエストは直列。 |
| `NewH1PoolClient(addr, dialer, pool, opts...)` | `TransportH1Pool` | 最大 `MaxConnsPerHost` 本。この値がそのままリクエスト並行度。 |
| `NewManagedH1Client(resolver, dialer, opts...)` | `TransportH1Managed` | Resolver + Selector によるサービスディスカバリ。解決したアドレスごとに HTTP/1.1 サブプールを展開。 |

## HTTP/1.1 への固定

プロトコルの固定には次の 2 点が必要です。

1. HTTP/1.1 トランスポート — 上記のいずれかのコンストラクタ、または `client.ClientOptions` の対応する `Transport` 値を指定する。
2. `h2` を提示しないダイヤラ — 素の TCP ダイヤラ（`&conn.PlaintextDialer{}`）、または `NextProtos` に `"http/1.1"` だけを載せた TLS ダイヤラを使い、ハンドシェイク中にサーバーが `h2` を選べないようにする。

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

`client.NewH1Client(addr, dialer)` を使えば、同じ単一接続クライアントを 1 回の呼び出しで構築できます。リクエスト API は HTTP/2・HTTP/3 と同じ `Do` / `client.GET` / `client.Response` です。試験対象のプロトコルバージョンを切り替えるときは、コンストラクタを変えるだけで済み、書き直しは要りません。

## 接続 1 本ならリクエストは直列

HTTP/1.1 には多重化がありません。1 本の接続が同時に運べるのは 1 組のリクエスト/レスポンス交換だけです。パイプライニングは意図的に非対応としています。そのため `TransportH1SingleConn` はリクエストを直列に実行します。各リクエストが完了してから次のリクエストが書き込まれ、2 つ目の並行 `Do` は 1 つ目の完了を待ちます。接続 1 本の上では、HTTP/1.1 の負荷は厳密に直列です。並行実行にはプールを使ってください。

## プール接続

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

HTTP/2・HTTP/3 のプールと違い、これは排他チェックアウト方式のプールです。H2/H3 のプールは同じ接続を多数の並行ストリームに割り当て、負荷が最も低い接続を選びます。HTTP/1.1 のプールは、交換の間だけ接続をアイドル集合から取り出し、完了時に戻します。この方式の帰結は次のとおりです。

- `MaxConnsPerHost` が**そのまま**リクエスト並行度です。ここで意味を持つサイズ調整はこれだけで、プールがこの値を超えてダイヤルすることはありません。
- `MaxStreamsPerConn` は HTTP/1.1 には適用されず、無視されます（`PoolOptions` は H2/H3 のプールと共用ですが、ここでの接続あたり上限は常に 1 です）。
- すべての接続が使用中の場合、リクエストは接続が空くまで待ちます。待ち時間は ctx（および設定していれば `PoolOptions.AcquireTimeout`）で制限されます。使用中の接続に直列化されることはありません。

接続は交換をまたいで維持・再利用されます。レスポンスが接続を維持しないと示した場合（`Connection: close`、または keep-alive のない HTTP/1.0）、相手がソケットを閉じた場合、交換中にエラーが起きた場合は、再利用せず破棄して再ダイヤルします。アイドル接続の退避とヘルスチェックの巡回は H2/H3 のプールと同じです。

複数アドレスが対象なら、`NewManagedH1Client(resolver, dialer)` が解決したアドレスごとに排他チェックアウト方式のサブプールを 1 つずつ動かします。このとき `MaxConnsPerHost` はアドレスあたりの並行度になります。

## ストリーミング

HTTP/1.1 ではレスポンスのストリーミングは使えません。レスポンスは常に `Response.Body` にバッファリングされます。HTTP/1.1 トランスポートで `DoStream` を呼ぶとエラーになり、`Request.BodyMode: client.BodyStream` を指定した `Do` も同様です。これは意図的な仕様です。レスポンスをストリーミングしたい場合は HTTP/2 か HTTP/3 を使ってください。一方、リクエストボディはストリーミングできます。`Request.BodyReader` は、長さが事前に分からない場合 `Transfer-Encoding: chunked` で送信されます。`Request.CompressBody` が圧縮ストリーミングボディを送るのもこの仕組みです。

## 自動フォールバック

HTTP/2 非対応のサーバーと通信するだけなら HTTP/1.1 トランスポートは不要です。`client.TransportALPN` は ALPN でネゴシエートし、サーバーが `h2` を提示しなければ自動的に HTTP/1.1 へフォールバックします。トランスポートを固定するのは、接続が確実に HTTP/1.1 のまま維持されるという保証が欲しいときだけです。
