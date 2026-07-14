---
title: HTTP/3
weight: 4
---

# HTTP/3

HTTP/3 クライアント（RFC 9114）は、本リポジトリで実装した QUIC スタック（RFC 9000/9001/9002）と QPACK コーデック（RFC 9204）の上で動作します。`quic-go` も cgo も使いません。標準ライブラリ `crypto/tls` に依存するのは TLS 1.3 ハンドシェイクのみで、パケット保護、ロスリカバリ、輻輳制御、フロー制御はすべて poseidon 自身のコードです。

リクエスト API は HTTP/2 と同じ `Do` / `DoStream` です。`client.Request` と `client.Response` はトランスポートが変わっても同一なので、負荷試験を H2 から H3 に切り替えるのに必要な変更はコンストラクタだけです。

## 使用例

`examples/http3/main.go`:

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

## コンストラクタ

HTTP/2 側と対になる 3 つのコンストラクタがあります。

```go
// One QUIC connection. Buffered (Do) and streaming (DoStream) requests.
client.NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error)

// Pool of QUIC connections to one host.
client.NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error)

// Service discovery: a Resolver supplies addresses, requests spread across them.
client.NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error)
```

輻輳制御など、これらのコンストラクタが公開していない設定が必要な場合は、上の例のように `client.NewClient(client.ClientOptions{...})` で構築し、`Transport: client.TransportH3` を指定してください。

## 並行リクエスト

1 本の HTTP/3 コネクションで複数のリクエストを同時に処理できます。各リクエストは独立した QUIC ストリームに載ります。同一クライアントに対して複数の goroutine から `Do` / `DoStream` を呼んで構いません。プール版・マネージド版では、さらにストリームが複数のコネクションに分散されます。

## 暗号スイート

QUIC のパケット保護では TLS 1.3 の AEAD スイート 3 種すべてに対応しています: AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305。どれを使うかはハンドシェイク時にサーバーが選ぶため、クライアント側の設定は不要です。この 3 種以外のスイート（例: TLS_AES_128_CCM_8_SHA256）は鍵インストールの時点で型付きエラー `quic.ErrCryptoSuite` として失敗します。ハングもパニックもしません。

## QPACK

ヘッダー圧縮には双方向の動的テーブル QPACK を使います。クライアントは動的テーブルを参照してリクエストヘッダーをエンコードし、デコーダーストリーム経由でサーバーからのテーブル挿入をデコードします。すべて自動で行われ、調整項目はありません。

## 輻輳制御

デフォルトは NewReno です。BBR はオプトインで有効化できます。

```go
H3ConnOptions: []quic.ConnOption{
	quic.WithCongestionControl(quic.CCBBR),
},
```

BBR は正しく実装されテスト済みですが、NewReno に対するスループット上の優位は、実際のキューイング遅延を伴うボトルネック付き WAN 経路でしか現れません。LAN やループバックでは差は測定できません。対象経路でベンチマークを取れないなら、デフォルトの NewReno のままにしてください。

## Linux でのバッチ I/O

Linux では QUIC トランスポートが GSO（generic segmentation offload）を使い、送信パケットをまとめて 1 回のシステムコールでカーネルに渡します。受信側は GRO で結合済みのバッチを受け取ります。どちらも自動で有効になり、設定もビルドタグも不要です。他のプラットフォームでは、送受信ともデータグラム 1 個につき 1 システムコールです。

## 非目標

1.0 では次を意図的にスコープ外としています。

- **0-RTT / セッション再開** — すべてのコネクションでフルハンドシェイクを行います。
- **QUIC コネクションマイグレーション** — コネクションは確立時の経路に固定されます。
- **HTTP/3 サーバープッシュ** — 常に無効です。

クライアントがこれらを開始することはありません。ピアが提示してきても単に応じないだけで、何も失敗せず、エラーも発生しません。この領域で唯一のハードな失敗は未対応の暗号スイートで、その場合は前述の型付きエラー `quic.ErrCryptoSuite` が返ります。
