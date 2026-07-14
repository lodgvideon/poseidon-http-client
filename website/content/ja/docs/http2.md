---
title: HTTP/2
weight: 3
---

# HTTP/2

HTTP/2 クライアントは RFC 7540 と HPACK（RFC 7541）をスクラッチで実装しています。`net/http` も `golang.org/x/net/http2` も使いません。コンストラクタは 3 つあり、典型的な構成をカバーします。単一コネクション、ホストごとのプール、リゾルバ駆動の複数バックエンドです。いずれも `*client.Client` を返し、同じ `Do` / `DoStream` API で使えます。

## 単一コネクション

`client.NewSingleConnClient` は 1 本のコネクションを保持し、切断時には自動で再ダイヤルします。以下は `examples/http2/main.go` です。

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

`Response` は呼び出し側が所有します。一度確保したら、リクエストの合間に `resp.Reset()` を呼んで再利用してください。`client.GET(path)` は省略記法です。細かく制御したい場合は `client.Request{Method, Scheme, Authority, Path, BodyMode}` を直接組み立てます。

## コネクションプーリング

`client.NewPoolClient` はホストごとのプールを維持します。各リクエストを最も負荷の低いコネクションに割り当て、`MaxConnsPerHost` を上限に新規ダイヤルし、ヘルスチェックのタイミングでアイドルなコネクションを破棄します。

```go
c, err := client.NewPoolClient("api.example.com:443", dialer,
	client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		HealthCheckPeriod: 30 * time.Second,
	})
```

`MaxStreamsPerConn` はソフトな上限です。実効値は、この値とピアの `SETTINGS_MAX_CONCURRENT_STREAMS` の小さい方になります。`c.Warmup(n)` でコネクションを事前にダイヤルでき、`c.PoolStats()` は現在の接続数を返し、`c.Shutdown(timeout)` はグレースフルにドレインします。

## ストリーミング

`DoStream` はレスポンスの HEADERS が届いた時点で戻ります。呼び出し側は `Recv` をループで回して DATA・トレーラ・リセットの各イベントを受け取り、最後に必ず `Close` を呼びます。

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

関連する形式:

- `c.Stream(ctx, req, fn)` — コールバック形式。ストリームのクローズは常に内部で行われます。
- `req.BodyMode = client.BodyStream` を通常の `Do` と組み合わせる — レスポンスボディは `resp.BodyReader`（`io.ReadCloser`。クローズが必要）から遅延して読み出されます。
- `req.BodyReader` — リクエストボディを `io.Reader` からストリーミングします。フロー制御に従って DATA フレームに分割されます。
- `sr.WaitTrailers(ctx)` — ボディを読み切ってレスポンストレーラを返します（gRPC の `grpc-status` など）。

## サービスディスカバリ

`client.NewManagedClient` は `Resolver`（どのバックエンドが存在するか）と `Selector`（次のリクエストをどれに送るか）を受け取ります。解決されたアドレスごとにサブプールを 1 つ持ちます。

```go
resolver := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 443},
	client.Address{Host: "10.0.0.2", Port: 443},
)
c, err := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

`client.DNSResolver(host, port, client.DNSOptions{TTL: 30 * time.Second})` は TTL ごとに A/AAAA レコードを再解決します。プールは新しいバックエンドにダイヤルし、消えたバックエンドをドレインします。Selector には `RoundRobin()`、`Random(rng)`、セッションアフィニティ用の `Hash(keyFn)` があります。`Resolver` はインターフェースなので、`Resolve`/`Watch` を実装すれば独自のディスカバリを組み込めます。

## 耐障害性

リトライは `Retryer` によるオプトインです。一時的な失敗（REFUSED_STREAM、GOAWAY、ダイヤルエラー）に対して冪等なリクエストを再発行します。指数バックオフとジッタ付きで、回数は `MaxAttempts` で制限されます。

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

冪等でないメソッドは、`req.Idempotency = client.ForceIdempotent` を設定しない限りリトライされません（設定する場合は冪等性キーと併用してください）。トークンバケット方式のレートリミットはコンストラクタのオプションです。`client.WithRateLimit(100, 20)` は 100 リクエスト/秒、バースト上限 20 に制限します。リクエスト単位のデッドラインは `req.Timeout` で設定します。期限切れになると `Do` は `context.DeadlineExceeded` で失敗し、ストリームは `RST_STREAM(CANCEL)` でリセットされます。

## 可観測性

`WithHooks` でライフサイクルコールバックを登録できます。`MetricsSnapshot` はカウンタとレイテンシヒストグラムの固定時点のビューを返します。

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

そのほかのフック: `OnRequestStart`、`OnRetry`、`OnDial`、`OnConnClose`、`OnResolverUpdate`。フック内でブロックしてはいけません。`c.PoolStats()` はプールの現在の状態を返すので、`/debug` エンドポイントに使えます。

## 高度なプロトコル機能

**サーバープッシュ（RFC 7540 §8.2）。** `WithPushHandler` でハンドラを登録すると、SETTINGS でプッシュが有効になります。クライアントはプッシュされた各ストリームを `Response` に読み切ってからコールバックに渡します。ハンドラがない場合、PUSH_PROMISE はプロトコルエラーです。

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

**Extended CONNECT（RFC 8441）。** WebSocket（あるいは任意のプロトコル）を単一の HTTP/2 ストリーム上でトンネルします。サーバー側が `SETTINGS_ENABLE_CONNECT_PROTOCOL=1` を広告している必要があります。

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

**H2C（prior knowledge）。** TLS も ALPN も使わない平文の HTTP/2 です。`PlaintextDialer` を使い、デフォルトのスキームを `http` に設定します。

```go
c, err := client.NewSingleConnClient("localhost:8080",
	&conn.PlaintextDialer{},
	client.WithDefaultScheme("http"))
```

このほか、リクエスト優先度（`req.Priority = &frame.Priority{...}`、RFC 7540 §5.3）、リクエストトレーラ（`req.TrailerFunc`）、HTTP CONNECT プロキシダイヤラ（`conn.ProxyTLSDialer`）に対応しています。

## 負荷生成の例

`examples/loadgen` は 1 ファイルで完結する HTTP/2 負荷生成ツールです。プール化したクライアント、N 個のワーカー goroutine、任意のグローバル QPS 上限、そして `MetricsSnapshot` から算出するサマリで構成されています。

```bash
go run ./examples/loadgen -url https://localhost:8443/ \
    -conns 4 -workers 64 -duration 30s -rps 5000
```
