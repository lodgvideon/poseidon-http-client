---
title: 機能と利点
weight: 5
---

# 機能と利点

## サポートマトリクス

3 つのプロトコルバージョンはすべて同じリクエスト API を共有する: `client.Do` / `client.DoStream` と、呼び出し側が所有し再利用できる `client.Response`。圧縮も共通である: レスポンスは gzip、deflate、br、zstd の 4 つすべてがデコードされ(クライアントは `accept-encoding: gzip, deflate, br, zstd` を通知する)、`Request.CompressBody` でリクエストボディを圧縮できる — 後述の[圧縮](#圧縮)を参照。異なるのは、各トランスポートがプロトコルのどこまでを公開するかである。

| プロトコル | 実装 | コンストラクタ | 1 コネクションあたりの並行リクエスト | プーリング | サービスディスカバリ | 主な機能 |
|---|---|---|---|---|---|---|
| HTTP/1.1 | フルスクラッチ | `NewH1Client`、`NewH1PoolClient`、`NewManagedH1Client` | 不可 — 1 コネクションにつき同時に 1 交換のみ(パイプラインなし) | `NewH1PoolClient`(排他チェックアウト方式のプール: `MaxConnsPerHost` がそのままリクエスト並行数) | `NewManagedH1Client`(Resolver + Selector) | キープアライブによるコネクション再利用、リクエストボディはストリーミング可能(`Request.BodyReader`、長さが不明ならチャンク転送)だがレスポンスは常に `Response.Body` にバッファされる — `DoStream` と `BodyStream` はエラーを返す、ALPN フォールバック先: サーバーが h2 を提供しない場合、`TransportALPN` は自動的に HTTP/1.1 を選択する |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541)、フルスクラッチ | `NewSingleConnClient`、`NewPoolClient`、`NewManagedClient` | 可 — ストリーム多重化、`MAX_CONCURRENT_STREAMS` で制限 | `NewPoolClient`(ホスト単位のプール、最小負荷のストリーム選択、アイドル削除) | `NewManagedClient`(Resolver + Selector) | `DoStream` とリクエストトレーラ、フロー制御、動的 SETTINGS、GOAWAY ドレイン、PING キープアライブ、サーバープッシュ (PUSH_PROMISE)、リクエスト優先度、拡張 CONNECT(RFC 8441、H2 上の WebSocket)、CONTINUATION、HTTP CONNECT プロキシダイヤラー、h2c prior knowledge |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204)、フルスクラッチ | `NewH3Client`、`NewH3PoolClient`、`NewManagedH3Client` | 可 — 1 本の QUIC コネクション上で複数リクエストを同時進行 | `NewH3PoolClient`(複数コネクションのプール) | `NewManagedH3Client` | `DoStream`、双方向の動的 QPACK(エンコード + デコード)、TLS 1.3 の全 AEAD(AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305)、輻輳制御はデフォルト NewReno でオプトインの BBR、Linux では GSO バッチ送信・GRO バッチ受信・上限付き ACK 集約 |

HTTP/1.1 には他の 2 バージョンと同じコンストラクタ一式があるが、そのプールは性質が異なる。HTTP/1.1 には多重化がない: 1 本のコネクションではリクエストは厳密に直列になるため、プールなしではこのクライアントは HTTP/1.1 の負荷をそもそも生成できない。プールはコネクションを排他チェックアウトで貸し出す — 1 コネクションにつき同時に 1 交換 — ため、`MaxConnsPerHost` がそのままリクエスト並行数になり、`MaxStreamsPerConn` は適用されない。すべてのコネクションが使用中なら、リクエストはどれかが空くまで待つ(リクエストのコンテキストで打ち切られる)。コネクションはキープアライブで再利用される。`Connection: close`、死んだコネクション、交換エラーが起きたコネクションは破棄して再ダイヤルする。パイプラインは意図的に実装していない。ダイヤラーは ALPN トークン `h2` を提供してはならない — 素の TCP ダイヤラーか、`NextProtos` が `"http/1.1"` のみの TLS ダイヤラーを使うこと。

HTTP/3 で BBR を有効にするには:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## 圧縮

圧縮は HTTP/1.1、HTTP/2、HTTP/3 のいずれでも同一に動作する。

**レスポンス。** クライアントは `accept-encoding: gzip, deflate, br, zstd` を通知し、4 つすべてをプール化されたリーダーでデコードする。呼び出し側が accept-encoding ヘッダーを指定すればそちらが優先され、`Request.DisableDecompression` はヘッダーとデコードの両方を抑止する。展開爆弾ガードは `MaxDecompressedSize`(デフォルト 10 MiB)を超えて膨張するボディを `ErrBodyTooLarge` で拒否し、zstd のウィンドウは 8 MiB に制限される。`Content-Encoding` の照合は大文字小文字を区別しない(RFC 9110 §8.4.1)。

**リクエスト。** `Request.CompressBody` を設定すると、クライアントがボディを圧縮し、`content-encoding` も自ら設定する:

```go
var resp client.Response
err := c.Do(ctx, &client.Request{
    Method: "POST", Path: "/ingest",
    Body:   payload,
    CompressBody: client.EncodingZstd, // client sets content-encoding itself
}, &resp)
```

指定できるのは `EncodingGzip`、`EncodingDeflate`、`EncodingBrotli`、`EncodingZstd`。ゼロ値の `EncodingIdentity` はボディを無変更のまま送る — オプトインしない呼び出し側のコストはゼロである(0 アロケーション)。`content-encoding` を手動で設定した場合は従来どおり「このボディは既にエンコード済み」を意味し、ボディには手を付けない(RFC 9110 §8.4 — Content-Encoding はボディを記述するものであって、指示ではない)。`CompressBody` と手動の `content-encoding` を両方設定すると `ErrConflictingContentEncoding` が返る。content-length は、バッファ済みボディなら圧縮後のサイズになり、ストリーミングボディでは省略される(その場合 HTTP/1.1 はチャンク転送を使う)。

## poseidon を選ぶ理由

**1 つのクライアントで 3 つのプロトコルバージョン。** HTTP/1.1、HTTP/2、HTTP/3 を同じ `Do`/`DoStream` API で扱える。Go 標準ライブラリに HTTP/3 はなく、多くのスタックは別ライブラリ・別 API で後付けする。ここでは、負荷試験を h2 から h3 へ切り替えるのに必要なのはコンストラクタの変更であって、書き直しではない。

**サードパーティのプロトコルコードなし。** すべてのプロトコルスタックが、このモジュール内にフルスクラッチで実装されている: QUIC (RFC 9000/9001/9002)、HTTP/3 (RFC 9114)、QPACK (RFC 9204)、HTTP/2 フレーミング (RFC 7540) と HPACK (RFC 7541)、そして HTTP/1.1。`quic-go` も `nghttp2` も `net/http` も cgo も使わない。TLS 1.3 ハンドシェイクは標準ライブラリの `crypto/tls` を使う。4 つの直接依存は暗号と圧縮のプリミティブであり、自作せず意図して採用したものだ: `golang.org/x/net`、`golang.org/x/crypto`(ChaCha20-Poly1305 のパケット保護)、`github.com/andybalholm/brotli`、`github.com/klauspost/compress`(zstd)。Poly1305 や Brotli を再実装しても、得るものはなくセキュリティ上の負債だけが残る — Brotli には 122 KB の静的辞書と 121 種類の変換が必要で、klauspost の zstd には長年のファジングの蓄積があり、そして展開器は格好の攻撃面である。つまり境界はこうだ: プロトコルコードはすべて自前で、1 つのモジュール内で監査できる。暗号と圧縮のプリミティブは借り物である — そこは借りるほうが安全という工学的判断だからだ。

**ゼロアロケーションのコーデック。** ワイヤコーデック全体が、両プロトコルバージョンで 0 B/op、0 allocs/op で動作する — HTTP/2(フレームと HPACK のエンコード/デコード)も、HTTP/3(QUIC のフレームとパケットヘッダーのパース/シリアライズ、HTTP/3 フレーム、QPACK フィールドセクション)も同様であり、退行すると CI のベンチゲートがビルドを落とす。負荷生成器の高リクエストレートでは、フレームごとのアロケーションはそのまま GC 負荷として現れるが、このコーデックは一切寄与しない。`frame`、`hpack`、`qpack` の各パッケージは単体でも使える。正直な境界を 1 つ挙げると、QUIC パケットの送信パス(送信パケットの構築と暗号化)はゼロではなく低アロケーションである — ゼロアロケーションが指すのはコーデックであって、リクエスト全体ではない。

**細かい制御。** ストリーム、フロー制御ウィンドウ、SETTINGS、プーリングポリシー、輻輳制御(NewReno または BBR)、ペーシングに直接アクセスできる — `net/http` がトランスポートの内側に隠しているノブである。ウィンドウを閉じたまま保持する、ストリーム並行数を固定する、輻輳制御アルゴリズムの効果を測る、といった用途にレバーがそのまま公開されている。

**負荷生成向けの機能を標準装備。** コネクションプーリング、DNS サービスディスカバリ(Resolver/Selector)、冪等リクエストに対するオプトインの上限付きリトライ、トークンバケットのレート制限(`WithRateLimit`)、ライフサイクルフック(`Client.Hooks`)、メトリクス(`Client.MetricsSnapshot()`、`Client.PoolStats()`)。これらはすべて HTTP/1.1、HTTP/2、HTTP/3 で共通で、設定は 1 回で済む。プロトコルごとに繰り返す必要はない。

**RFC 準拠テスト。** 個別の RFC セクションに紐づく約 200 の準拠テストが CI でゲートされている。3 サーバー構成の HTTP/3 相互運用マトリクス(Caddy/quic-go、nginx/C、aioquic/Python)が実 UDP 上で動く。ワイヤパーサーはファズテスト済み。スイート全体が `-race` 付きで実行される。

## net/http との比較

`net/http` は「電池同梱」の標準クライアントである。リダイレクト、Cookie、環境変数からのプロキシ設定、HTTP/1.1 + HTTP/2 のネゴシエーションを無設定で処理する。poseidon はその利便性を制御と引き換えにする。HTTP/3、ゼロアロケーションのコーデック、負荷生成向けツールを加える代わりに、ターゲットごとのクライアント構築とレスポンス管理を呼び出し側に求める。汎用の Web クライアントが欲しいなら `net/http` を使うこと。負荷生成器を作る、あるいは HTTP/3 を細かく制御したいなら poseidon を使うこと。

## quic-go との比較

`quic-go` は成熟し広く使われている QUIC / HTTP/3 ライブラリで、サーバーとクライアントの両方をカバーする。poseidon は、プロトコルコードをすべてこのモジュール内に保ち、負荷生成特化であり続けるために QUIC を再実装した。より若く、より狭い — HTTP クライアントである。`quic` パッケージはサーバーロール(`Listen` / `Accept`、ストリームの accept)を公開しているが、これはテストでクライアントに本物の相手を与えるためのものであり、その上に HTTP/3 サーバーはない。実績のある QUIC スタックや HTTP/3 サーバーが必要なら、`quic-go` が確立された選択肢である。

## 1.0 の非目標

このリリースでは以下を意図的にスコープ外としている:

- **0-RTT / セッション再開。** クライアントから開始することはない。
- **QUIC コネクションマイグレーション。** 開始しない。
- **HTTP/3 サーバープッシュ。** 使用しない。

ピアがこれらを提供してきても単に応じないだけであり、何も失敗しない。未対応の TLS 暗号スイートは型付きエラー `ErrCryptoSuite` でクリーンに失敗する。ハングもパニックも起きない。
