---
title: poseidon-http-client
type: docs
---

# poseidon-http-client

HTTP/1.1、HTTP/2、HTTP/3 をフルスクラッチで実装した Go 向け低レベル HTTP クライアントです。フレーミング、HPACK、QPACK、QUIC スタックまで、すべて自前で実装しています。`net/http` もサードパーティのプロトコルライブラリも使用しません。直接依存は `golang.org/x/net` と `golang.org/x/crypto`（ChaCha20-Poly1305 用）のみで、TLS 1.3 は標準ライブラリを利用します。3 つのプロトコルバージョンすべてが `Do` と `DoStream` という単一のリクエスト API を共有します。コネクション、ストリーム、フロー制御をきめ細かく制御したい負荷生成ツール向けに設計されており、`net/http` の汎用的な代替を目指すものではありません。

MIT ライセンス。Go 1.25 以上が必要です。ソース: [github.com/lodgvideon/poseidon-http-client](https://github.com/lodgvideon/poseidon-http-client)

## poseidon を使う理由

- **1 つのクライアントで 3 つのプロトコルバージョン** — HTTP/1.1、/2、/3 を同一の `Do`/`DoStream` API で扱えます。Go 標準ライブラリに HTTP/3 はありません。
- **フルスクラッチ実装、依存はほぼゼロ** — `quic-go` なし、`nghttp2` なし、cgo なし。小さく、監査しやすいコードベースです。
- **ゼロアロケーションのワイヤーコーデック** — HTTP/2（frame、HPACK）と HTTP/3（QPACK、HTTP/3 フレーム、QUIC のフレームとパケットヘッダー）のエンコード/デコードは 0 B/op、0 allocs/op。CI のベンチマークゲートで強制しています。
- **きめ細かな制御** — ストリーム、フロー制御ウィンドウ、SETTINGS、プーリングポリシー、輻輳制御（NewReno または BBR）。`net/http` が隠している調整点に直接触れられます。
- **負荷生成向け機能を標準装備** — コネクションプーリング、DNS サービスディスカバリ、リトライ、レート制限、フックとメトリクス。H2 と H3 で共通です。
- **RFC 準拠テスト済み** — RFC のセクションに紐付いた約 200 の準拠テスト、実 UDP 上での 3 サーバー HTTP/3 相互運用マトリクス（Caddy、nginx、aioquic）、ワイヤーパーサーのファジング、全体を `-race` で検証しています。

## ガイド

- [はじめに]({{< relref "/docs/getting-started" >}})
- [HTTP/1.1]({{< relref "/docs/http1" >}})
- [HTTP/2]({{< relref "/docs/http2" >}})
- [HTTP/3]({{< relref "/docs/http3" >}})
- [機能と利点]({{< relref "/docs/features" >}})
- [免責事項]({{< relref "/docs/disclaimer" >}})

{{< hint warning >}}
**リリース間もないソフトウェアです。** これは初回リリースであり、セキュリティ上重要なプロトコル（TLS 1.3、QUIC、HPACK/QPACK）をフルスクラッチで実装しています。第三者によるセキュリティ監査は受けていません。現状のまま（as is）提供され、利用は自己責任です（MIT — 無保証）。デプロイ前に[免責事項]({{< relref "/docs/disclaimer" >}})をお読みください。
{{< /hint >}}
