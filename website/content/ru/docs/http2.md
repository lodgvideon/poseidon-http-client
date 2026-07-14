---
title: HTTP/2
weight: 3
---

# HTTP/2

HTTP/2-клиент реализует RFC 7540 и HPACK (RFC 7541) с нуля — без `net/http` и без `golang.org/x/net/http2`. Три конструктора покрывают типовые топологии: одно соединение, пул на хост или набор бэкендов, управляемый резолвером. Все три возвращают `*client.Client` с одним и тем же API `Do` / `DoStream`.

## Одно соединение

`client.NewSingleConnClient` держит одно соединение и автоматически переподключается при обрыве. Это `examples/http2/main.go`:

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

`Response` принадлежит вызывающему коду: выделите его один раз и вызывайте `resp.Reset()` между запросами. `client.GET(path)` — сокращённая форма; для полного контроля соберите `client.Request{Method, Scheme, Authority, Path, BodyMode}` вручную.

## Пул соединений

`client.NewPoolClient` ведёт пул на каждый хост. Он назначает каждый запрос наименее загруженному соединению, открывает до `MaxConnsPerHost` соединений и вытесняет простаивающие при очередной проверке состояния.

```go
c, err := client.NewPoolClient("api.example.com:443", dialer,
	client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		HealthCheckPeriod: 30 * time.Second,
	})
```

`MaxStreamsPerConn` — мягкий предел; фактический лимит равен минимуму из этого значения и `SETTINGS_MAX_CONCURRENT_STREAMS`, объявленного пиром. `c.Warmup(n)` открывает соединения заранее; `c.PoolStats()` отдаёт текущие счётчики; `c.Shutdown(timeout)` корректно завершает работу с ожиданием активных запросов.

## Потоковый режим

`DoStream` возвращается сразу после прихода HEADERS ответа. Вызывающий код читает события через `Recv` — DATA, трейлеры, сброс потока — и обязан вызвать `Close`.

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

Смежные варианты:

- `c.Stream(ctx, req, fn)` — форма с колбэком, поток всегда закрывается автоматически.
- `req.BodyMode = client.BodyStream` вместе с обычным `Do` — тело ответа приходит лениво через `resp.BodyReader` (`io.ReadCloser`; его нужно закрыть).
- `req.BodyReader` — потоковая отправка тела запроса из `io.Reader`, нарезка на кадры DATA с учётом flow control.
- `sr.WaitTrailers(ctx)` — дочитать тело и вернуть трейлеры ответа (например, `grpc-status` в gRPC).

## Обнаружение сервисов

`client.NewManagedClient` принимает `Resolver` (какие бэкенды существуют) и `Selector` (какой из них получит следующий запрос). На каждый разрешённый адрес — свой под-пул.

```go
resolver := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 443},
	client.Address{Host: "10.0.0.2", Port: 443},
)
c, err := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

`client.DNSResolver(host, port, client.DNSOptions{TTL: 30 * time.Second})` перечитывает A/AAAA-записи по TTL; пул открывает соединения к новым бэкендам и закрывает соединения к исчезнувшим. Селекторы: `RoundRobin()`, `Random(rng)`, `Hash(keyFn)` для привязки сессий. `Resolver` — интерфейс: реализуйте `Resolve`/`Watch`, чтобы подключить собственный механизм обнаружения.

## Устойчивость к сбоям

Повторные попытки включаются явно через `Retryer`. Он повторяет идемпотентные запросы при временных сбоях (REFUSED_STREAM, GOAWAY, ошибки установки соединения) с экспоненциальной задержкой и джиттером, не превышая `MaxAttempts`.

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

Неидемпотентные методы не повторяются, если не задать `req.Idempotency = client.ForceIdempotent` (используйте вместе с ключом идемпотентности). Ограничение частоты по алгоритму token bucket — опция конструктора: `client.WithRateLimit(100, 20)` даёт не более 100 запросов/с со всплесками до 20. Дедлайн на запрос: задайте `req.Timeout`; по истечении `Do` завершается с `context.DeadlineExceeded`, а поток сбрасывается через `RST_STREAM(CANCEL)`.

## Наблюдаемость

`WithHooks` устанавливает колбэки жизненного цикла; `MetricsSnapshot` возвращает зафиксированный срез счётчиков и гистограмм задержек.

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

Другие хуки: `OnRequestStart`, `OnRetry`, `OnDial`, `OnConnClose`, `OnResolverUpdate`. Хуки не должны блокировать. `c.PoolStats()` отдаёт текущее состояние пула — удобно для `/debug`-эндпоинта.

## Расширенные возможности протокола

**Server push (RFC 7540 §8.2).** Зарегистрируйте обработчик через `WithPushHandler` — это включает push в SETTINGS. Клиент вычитывает каждый отправленный сервером поток в `Response` и передаёт его в колбэк. Без обработчика PUSH_PROMISE считается протокольной ошибкой.

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

**Extended CONNECT (RFC 8441).** Туннелирование WebSocket (или любого другого протокола) внутри одного потока HTTP/2. Сервер должен объявить `SETTINGS_ENABLE_CONNECT_PROTOCOL=1`.

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

**H2C (prior knowledge).** HTTP/2 открытым текстом, без TLS и ALPN: используйте `PlaintextDialer` и задайте схему по умолчанию `http`.

```go
c, err := client.NewSingleConnClient("localhost:8080",
	&conn.PlaintextDialer{},
	client.WithDefaultScheme("http"))
```

Также поддерживаются: приоритеты запросов (`req.Priority = &frame.Priority{...}`, RFC 7540 §5.3), трейлеры запроса (`req.TrailerFunc`) и dialer-ы через HTTP CONNECT-прокси (`conn.ProxyTLSDialer`).

## Пример генератора нагрузки

`examples/loadgen` — полный HTTP/2-генератор нагрузки в одном файле: клиент с пулом, N воркеров-горутин, опциональный глобальный лимит QPS и сводка, построенная из `MetricsSnapshot`.

```bash
go run ./examples/loadgen -url https://localhost:8443/ \
    -conns 4 -workers 64 -duration 30s -rps 5000
```
