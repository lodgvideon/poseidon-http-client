---
title: 機能と利点
weight: 5
---

# 機能と利点

## サポートマトリクス

3 つのプロトコルバージョンはすべて同じリクエスト API を共有する: `client.Do` / `client.DoStream` と、呼び出し側が所有し再利用できる `client.Response`。異なるのは、各トランスポートがプロトコルのどこまでを公開するかである。

| プロトコル | 実装 | コンストラクタ | 1 コネクションあたりの並行リクエスト | プーリング | サービスディスカバリ | 主な機能 |
|---|---|---|---|---|---|---|
| HTTP/1.1 | フルスクラッチ | `NewClient` + `TransportH1SingleConn` | 不可 — 1 コネクション、リクエストは直列実行(パイプラインなし) | なし | なし | ALPN フォールバック先: サーバーが h2 を提供しない場合、`TransportALPN` は自動的に HTTP/1.1 を選択する |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541)、フルスクラッチ | `NewSingleConnClient`、`NewPoolClient`、`NewManagedClient` | 可 — ストリーム多重化、`MAX_CONCURRENT_STREAMS` で制限 | `NewPoolClient`(ホスト単位のプール、最小負荷のストリーム選択、アイドル削除) | `NewManagedClient`(Resolver + Selector) | `DoStream` とリクエストトレーラ、フロー制御、動的 SETTINGS、GOAWAY ドレイン、PING キープアライブ、サーバープッシュ (PUSH_PROMISE)、リクエスト優先度、拡張 CONNECT(RFC 8441、H2 上の WebSocket)、CONTINUATION、HTTP CONNECT プロキシダイヤラー、h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204)、フルスクラッチ | `NewH3Client`、`NewH3PoolClient`、`NewManagedH3Client` | 可 — 1 本の QUIC コネクション上で複数リクエストを同時進行 | `NewH3PoolClient`(複数コネクションのプール) | `NewManagedH3Client` | `DoStream`、双方向の動的 QPACK(エンコード + デコード)、TLS 1.3 の全 AEAD(AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305)、輻輳制御はデフォルト NewReno でオプトインの BBR、Linux では GSO バッチ送信・GRO バッチ受信・上限付き ACK 集約 |

HTTP/1.1 のサポートは意図的に最小限にしてある。同じターゲットを 3 つのバージョンすべてで 1 つのコードベースから負荷試験できるようにするため、そして `TransportALPN` のフォールバック先として存在する。フル機能のトランスポートは HTTP/2 と HTTP/3 である。

HTTP/3 で BBR を有効にするには:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## poseidon を選ぶ理由

**1 つのクライアントで 3 つのプロトコルバージョン。** HTTP/1.1、HTTP/2、HTTP/3 を同じ `Do`/`DoStream` API で扱える。Go 標準ライブラリに HTTP/3 はなく、多くのスタックは別ライブラリ・別 API で後付けする。ここでは、負荷試験を h2 から h3 へ切り替えるのに必要なのはコンストラクタの変更であって、書き直しではない。

**フルスクラッチ実装、依存はほぼゼロ。** `quic-go` も `nghttp2` も cgo も使わない。直接依存は `golang.org/x/net` と `golang.org/x/crypto`(後者は ChaCha20-Poly1305 のパケット保護のみ)。TLS 1.3 ハンドシェイクは標準ライブラリの `crypto/tls` を使う。プロトコルコードはすべてこのモジュール内にある — 監査可能で、表面積が小さく、推移的依存によるサプライチェーンの肥大がない。

**ゼロアロケーションのコーデック。** フレームと HPACK のエンコード/デコードは 0 B/op、0 allocs/op で動作し、退行すると CI のベンチゲートがビルドを落とす。負荷生成器の高リクエストレートでは、フレームごとのアロケーションはそのまま GC 負荷として現れるが、このコーデックは一切寄与しない。`frame`、`hpack`、`qpack` の各パッケージは単体でも使える。

**細かい制御。** ストリーム、フロー制御ウィンドウ、SETTINGS、プーリングポリシー、輻輳制御(NewReno または BBR)、ペーシングに直接アクセスできる — `net/http` がトランスポートの内側に隠しているノブである。ウィンドウを閉じたまま保持する、ストリーム並行数を固定する、輻輳制御アルゴリズムの効果を測る、といった用途にレバーがそのまま公開されている。

**負荷生成向けの機能を標準装備。** コネクションプーリング、DNS サービスディスカバリ(Resolver/Selector)、冪等リクエストに対するオプトインの上限付きリトライ、トークンバケットのレート制限(`WithRateLimit`)、ライフサイクルフック(`Client.Hooks`)、メトリクス(`Client.MetricsSnapshot()`、`Client.PoolStats()`)。これらはすべて HTTP/2 と HTTP/3 で共通で、設定は 1 回で済む。プロトコルごとに繰り返す必要はない。

**RFC 準拠テスト。** 個別の RFC セクションに紐づく約 200 の準拠テストが CI でゲートされている。3 サーバー構成の HTTP/3 相互運用マトリクス(Caddy/quic-go、nginx/C、aioquic/Python)が実 UDP 上で動く。ワイヤパーサーはファズテスト済み。スイート全体が `-race` 付きで実行される。

## net/http との比較

`net/http` は「電池同梱」の標準クライアントである。リダイレクト、Cookie、環境変数からのプロキシ設定、HTTP/1.1 + HTTP/2 のネゴシエーションを無設定で処理する。poseidon はその利便性を制御と引き換えにする。HTTP/3、ゼロアロケーションのコーデック、負荷生成向けツールを加える代わりに、ターゲットごとのクライアント構築とレスポンス管理を呼び出し側に求める。汎用の Web クライアントが欲しいなら `net/http` を使うこと。負荷生成器を作る、あるいは HTTP/3 を細かく制御したいなら poseidon を使うこと。

## quic-go との比較

`quic-go` は成熟し広く使われている QUIC / HTTP/3 ライブラリで、サーバーとクライアントの両方をカバーする。poseidon は依存フリーと負荷生成特化を保つために QUIC を再実装した。より若く、より狭い — クライアントのみで、サーバーはない。実績のある QUIC スタックやサーバーが必要なら、`quic-go` が確立された選択肢である。

## 1.0 の非目標

このリリースでは以下を意図的にスコープ外としている:

- **0-RTT / セッション再開。** クライアントから開始することはない。
- **QUIC コネクションマイグレーション。** 開始しない。
- **HTTP/3 サーバープッシュ。** 使用しない。

ピアがこれらを提供してきても単に応じないだけであり、何も失敗しない。未対応の TLS 暗号スイートは型付きエラー `ErrCryptoSuite` でクリーンに失敗する。ハングもパニックも起きない。
