---
title: Начало работы
weight: 1
---

# Начало работы

poseidon-http-client — низкоуровневый HTTP-клиент для Go. Он реализует HTTP/1.1, HTTP/2 и HTTP/3 с нуля — собственный фрейминг, HPACK, QPACK и QUIC-стек — без `net/http` и без сторонних протокольных библиотек. Клиент рассчитан на генераторы нагрузки и инструменты, которым нужен прямой контроль над соединениями, стримами и flow control.

## Установка

Требуется Go 1.25 или новее.

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## Модель запроса

Все три версии протокола используют один и тот же API. Клиент создаётся конструктором, затем вызывается `Do` с контекстом, запросом и принадлежащим вызывающему коду `*client.Response`:

```go
c, err := client.NewSingleConnClient(addr, dialer)   // or any constructor above
defer c.Close()
resp := &client.Response{}                            // caller-owned, reusable
err = c.Do(ctx, client.GET("/path"), resp)            // resp.Status, resp.Body
resp.Reset()                                          // reuse for the next request
```

Два отличия от `net/http`:

- **Ответ принадлежит вызывающему коду.** Вы выделяете один `client.Response`, передаёте его в `Do`, читаете `resp.Status` (`int`) и `resp.Body` (`[]byte`), затем вызываете `resp.Reset()` и используете его для следующего запроса. Объект ответа не выделяется на каждый вызов — в цикле запросов один `Response` обслуживает все итерации.
- **Запросы — значения, а не указатели внутрь транспорта.** `client.GET(path)` строит GET-запрос. Для полного контроля собирайте `client.Request{Method, Scheme, Authority, Path, BodyMode}` напрямую.

## Первый запрос по HTTP/2

Это `examples/http2/main.go` из репозитория, без изменений. Программа выполняет один GET к публичному HTTP/2-эндпоинту:

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

Протокол здесь определяет dialer: `conn.TLSDialer` с `NextProtos: []string{"h2"}` предлагает в ALPN-переговорах только HTTP/2. Клиент держит одно соединение открытым и автоматически переподключается при обрыве.

## Выбор протокола

Версию протокола определяет вызванный конструктор:

| Конструктор | Протокол | Модель соединений |
|---|---|---|
| `client.NewH1Client(addr, dialer, opts...)` | HTTP/1.1 | Одно соединение, запросы последовательно |
| `client.NewH1PoolClient(addr, dialer, pool, opts...)` | HTTP/1.1 | Пул с эксклюзивным захватом, один запрос на соединение |
| `client.NewManagedH1Client(resolver, dialer, opts...)` | HTTP/1.1 | Service discovery (Resolver + Selector) |
| `client.NewSingleConnClient(addr, dialer, opts...)` | HTTP/2 | Одно соединение, автоматический redial |
| `client.NewPoolClient(addr, dialer, pool, opts...)` | HTTP/2 | Пул соединений на хост |
| `client.NewManagedClient(resolver, dialer, opts...)` | HTTP/2 | Service discovery (Resolver + Selector) |
| `client.NewH3Client(addr, tlsConfig, opts...)` | HTTP/3 | Одно QUIC-соединение |
| `client.NewH3PoolClient(addr, tlsConfig, pool, opts...)` | HTTP/3 | Пул из нескольких QUIC-соединений |
| `client.NewManagedH3Client(resolver, tlsConfig, opts...)` | HTTP/3 | Service discovery поверх QUIC |

Замечания:

- Для конструкторов HTTP/2 в `NextProtos` у dialer должен быть `"h2"`. `client.TransportALPN` откатывается на HTTP/1.1, если сервер не предлагает h2.
- Конструкторы HTTP/3 принимают `*tls.Config` напрямую; транспорт — QUIC поверх UDP.
- Все конструкторы возвращают клиент с одними и теми же методами `Do` / `DoStream`.

## Что дальше

У каждого протокола есть свой раздел с полным проверенным примером, стримингом, пулингом и опциями, специфичными для этой версии:

- [Руководство по HTTP/1.1](/docs/http1/)
- [Руководство по HTTP/2](/docs/http2/)
- [Руководство по HTTP/3](/docs/http3/)
