---
title: HTTP/3
weight: 4
---

# HTTP/3

Клиент HTTP/3 (RFC 9114) работает поверх стека QUIC (RFC 9000/9001/9002) и кодека QPACK (RFC 9204), реализованных в этом репозитории — без `quic-go`, без cgo. Из стандартной библиотеки берётся только сам хендшейк TLS 1.3 (`crypto/tls`); защита пакетов, восстановление после потерь, управление перегрузкой и flow control — код poseidon.

API запросов — те же `Do` / `DoStream`, что и в HTTP/2. `client.Request` и `client.Response` не меняются при смене транспорта; перевод нагрузочного теста с H2 на H3 — это замена конструктора.

## Пример

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

## Конструкторы

Три конструктора, зеркально повторяющие набор для HTTP/2:

```go
// One QUIC connection. Buffered (Do) and streaming (DoStream) requests.
client.NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error)

// Pool of QUIC connections to one host.
client.NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error)

// Service discovery: a Resolver supplies addresses, requests spread across them.
client.NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error)
```

Для настроек, которые конструкторы не пробрасывают — например, управления перегрузкой, — соберите клиент через `client.NewClient(client.ClientOptions{...})` с `Transport: client.TransportH3`, как в примере выше.

## Параллельные запросы

Одно HTTP/3-соединение обслуживает несколько запросов одновременно — каждый в собственном QUIC-стриме. Вызывайте `Do` / `DoStream` из параллельных горутин на одном клиенте; варианты с пулом и с service discovery дополнительно распределяют стримы по соединениям.

## Шифросьюты

Для защиты пакетов QUIC поддерживаются все три AEAD-сьюта TLS 1.3: AES-128-GCM, AES-256-GCM и ChaCha20-Poly1305. Сьют выбирает сервер во время хендшейка; на клиенте настраивать ничего не нужно. Сьют вне этого набора (например, TLS_AES_128_CCM_8_SHA256) даёт ошибку при установке ключей — типизированную `quic.ErrCryptoSuite` — без зависаний и паник.

## QPACK

Сжатие заголовков — динамический QPACK в обе стороны: клиент кодирует заголовки запросов по динамической таблице и декодирует вставки сервера на decoder-стриме. Всё происходит автоматически; настроек нет.

## Управление перегрузкой

По умолчанию — NewReno. BBR включается явно:

```go
H3ConnOptions: []quic.ConnOption{
	quic.WithCongestionControl(quic.CCBBR),
},
```

BBR реализован корректно и покрыт тестами, но его выигрыш в пропускной способности по сравнению с NewReno виден только на WAN-пути с узким местом и реальной задержкой в очередях. На LAN или loopback разницы вы не намеряете. Если проверить целевой путь бенчмарком негде — оставьте NewReno по умолчанию.

## Пакетный ввод-вывод на Linux

На Linux транспорт QUIC использует GSO (generic segmentation offload), чтобы отдавать ядру пачки исходящих пакетов одним сисколлом, и GRO для приёма склеенных пачек. Оба механизма включаются сами — без конфигурации и build-тегов. На остальных платформах клиент отправляет и принимает по одной датаграмме на сисколл.

## Вне рамок

Сознательно не входит в 1.0:

- **0-RTT / возобновление сессии** — каждое соединение проходит полный хендшейк.
- **Миграция QUIC-соединения** — соединение привязано к пути, на котором открыто.
- **Server push в HTTP/3** — никогда не включается.

Клиент ничего из этого не инициирует. Если пир предлагает такую возможность, она просто не используется; ничего не ломается, ошибок не возникает. Единственный жёсткий отказ в этой области — неподдерживаемый шифросьют, который возвращает типизированную `quic.ErrCryptoSuite`, описанную выше.
