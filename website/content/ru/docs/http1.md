---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Используйте HTTP/1.1, когда целевой сервер не поддерживает HTTP/2, или
когда нужно проверить именно его HTTP/1.1-путь и не дать ALPN-переговорам
поднять соединение до h2.

## Принудительный HTTP/1.1

Протокол фиксируют две вещи:

1. `Transport: client.TransportH1SingleConn` в `client.ClientOptions`.
2. TLS-dialer, у которого `NextProtos` предлагает только `"http/1.1"`,
   так что сервер не может выбрать `h2` во время handshake.

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

API запросов — те же `Do` / `client.GET` / `client.Response`, что и для
HTTP/2 и HTTP/3, поэтому переключение тестовой цели между версиями
протокола — это замена конструктора, а не переписывание кода.

## Сериализация запросов

`TransportH1SingleConn` держит одно соединение и выполняет запросы на
нём последовательно: каждый запрос завершается до отправки следующего.
Pipelining отсутствует. Для конкурентной нагрузки на HTTP/1.1-сервер
запустите несколько клиентов.

## Автоматический откат

`TransportH1SingleConn` не нужен только ради того, чтобы работать с
сервером без HTTP/2. `client.TransportALPN` договаривается через ALPN и
сам откатывается на HTTP/1.1, если сервер не предлагает `h2`. Фиксируйте
транспорт только тогда, когда нужна гарантия, что соединение останется
на HTTP/1.1.
