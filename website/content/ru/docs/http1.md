---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Используйте HTTP/1.1, когда целевой сервер не поддерживает HTTP/2, или
когда нужно проверить именно его HTTP/1.1-путь и не дать ALPN-переговорам
поднять соединение до h2.

У HTTP/1.1 тот же набор конструкторов, что у HTTP/2 и HTTP/3:

| Конструктор | Транспорт | Схема |
|---|---|---|
| `NewH1Client(addr, dialer, opts...)` | `TransportH1SingleConn` | Одно соединение, автоматическое переподключение. Запросы выполняются последовательно. |
| `NewH1PoolClient(addr, dialer, pool, opts...)` | `TransportH1Pool` | До `MaxConnsPerHost` соединений. Это число и есть конкурентность запросов. |
| `NewManagedH1Client(resolver, dialer, opts...)` | `TransportH1Managed` | Service discovery через Resolver + Selector; на каждый адрес — свой HTTP/1.1-подпул. |

## Принудительный HTTP/1.1

Протокол фиксируют две вещи:

1. HTTP/1.1-транспорт — один из конструкторов выше или соответствующее
   значение `Transport` в `client.ClientOptions`.
2. Dialer, не предлагающий `h2`: обычный TCP-dialer
   (`&conn.PlaintextDialer{}`) или TLS-dialer, у которого `NextProtos`
   предлагает только `"http/1.1"`, так что сервер не может выбрать `h2`
   во время handshake.

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

`client.NewH1Client(addr, dialer)` строит такой же клиент с одним
соединением за один вызов. API запросов — те же `Do` / `client.GET` /
`client.Response`, что и для HTTP/2 и HTTP/3, поэтому переключение
тестовой цели между версиями протокола — это замена конструктора, а не
переписывание кода.

## Одно соединение — последовательные запросы

В HTTP/1.1 нет мультиплексирования: соединение несёт ровно один обмен
запрос/ответ за раз. Pipelining не поддерживается сознательно. Поэтому
`TransportH1SingleConn` выполняет запросы последовательно — каждый
запрос завершается до отправки следующего, а второй конкурентный `Do`
ждёт окончания первого. На одном соединении HTTP/1.1-нагрузка строго
последовательна. Для конкурентности используйте пул.

## Пул соединений

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

В отличие от пулов HTTP/2 и HTTP/3, это пул с эксклюзивной выдачей.
Пулы H2/H3 отдают одно соединение многим конкурентным стримам и
выбирают наименее нагруженное; пул HTTP/1.1 забирает соединение из
набора простаивающих на время обмена и возвращает его по завершении.
Следствия:

- `MaxConnsPerHost` **и есть** конкурентность запросов. Это
  единственная значимая настройка размера, и пул никогда не открывает
  соединений сверх неё.
- `MaxStreamsPerConn` к HTTP/1.1 не применяется и игнорируется
  (`PoolOptions` общий с пулами H2/H3; лимит на соединение здесь
  всегда 1).
- Запрос, заставший все соединения занятыми, ждёт освобождения одного
  из них — в пределах своего ctx (и `PoolOptions.AcquireTimeout`, если
  задан). На занятое соединение он никогда не сериализуется.

Соединения поддерживаются живыми и переиспользуются между обменами.
Соединение отбрасывается и устанавливается заново, а не
переиспользуется, когда ответ говорит, что оно не сохранится
(`Connection: close` или HTTP/1.0 без keep-alive), когда peer закрыл
сокет, либо после любой ошибки обмена. Вытеснение простаивающих
соединений и health-check-обходы — те же, что в пулах H2/H3.

Для целей с несколькими адресами `NewManagedH1Client(resolver, dialer)`
держит по одному подпулу с эксклюзивной выдачей на каждый resolved-адрес;
`MaxConnsPerHost` тогда задаёт конкурентность на адрес.

## Стриминг

Потоковое чтение ответов на HTTP/1.1 недоступно: ответы всегда
буферизуются в `Response.Body`. `DoStream` на HTTP/1.1-транспорте
возвращает ошибку, как и `Do` с `Request.BodyMode: client.BodyStream`.
Это сделано сознательно — если ответы нужно стримить, используйте
HTTP/2 или HTTP/3. Тела запросов при этом стримятся: `Request.BodyReader`
отправляется с `Transfer-Encoding: chunked`, когда его длина заранее
неизвестна; так же `Request.CompressBody` отправляет сжатое потоковое
тело.

## Автоматический откат

HTTP/1.1-транспорт не нужен только ради того, чтобы работать с
сервером без HTTP/2. `client.TransportALPN` договаривается через ALPN и
сам откатывается на HTTP/1.1, если сервер не предлагает `h2`. Фиксируйте
транспорт только тогда, когда нужна гарантия, что соединение останется
на HTTP/1.1.
