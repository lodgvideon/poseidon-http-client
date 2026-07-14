---
title: HTTP/2
weight: 3
---

# HTTP/2

El cliente HTTP/2 implementa RFC 7540 y HPACK (RFC 7541) desde cero — sin `net/http`, sin `golang.org/x/net/http2`. Tres constructores cubren las topologías habituales: una sola conexión, un pool por host, o un conjunto de backends gobernado por un resolver. Los tres devuelven un `*client.Client` con la misma API `Do` / `DoStream`.

## Conexión única

`client.NewSingleConnClient` mantiene una conexión y vuelve a marcar automáticamente cuando se cae. Este es `examples/http2/main.go`:

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

`Response` pertenece al llamador: asígnalo una vez y llama a `resp.Reset()` entre peticiones. `client.GET(path)` es una forma abreviada; construye un `client.Request{Method, Scheme, Authority, Path, BodyMode}` directamente para tener control total.

## Pool de conexiones

`client.NewPoolClient` mantiene un pool por host. Asigna cada petición a la conexión menos cargada, marca hasta `MaxConnsPerHost` conexiones y desaloja las inactivas en cada tick del health check.

```go
c, err := client.NewPoolClient("api.example.com:443", dialer,
	client.PoolOptions{
		MaxConnsPerHost:   4,
		MaxStreamsPerConn: 100,
		HealthCheckPeriod: 30 * time.Second,
	})
```

`MaxStreamsPerConn` es un tope blando; el límite efectivo es el mínimo entre este valor y el `SETTINGS_MAX_CONCURRENT_STREAMS` del peer. `c.Warmup(n)` establece conexiones por adelantado; `c.PoolStats()` informa los conteos en vivo; `c.Shutdown(timeout)` drena de forma ordenada.

## Streaming

`DoStream` retorna en cuanto llegan los HEADERS de la respuesta. El llamador bombea `Recv` para recibir DATA, trailers y eventos de reset, y debe llamar a `Close`.

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

Formas relacionadas:

- `c.Stream(ctx, req, fn)` — forma con callback que siempre cierra el stream por ti.
- `req.BodyMode = client.BodyStream` con un `Do` normal — el cuerpo de la respuesta llega de forma perezosa en `resp.BodyReader` (un `io.ReadCloser`; ciérralo).
- `req.BodyReader` — envía el cuerpo de la petición desde un `io.Reader`, troceado en frames DATA bajo control de flujo.
- `sr.WaitTrailers(ctx)` — drena el cuerpo y devuelve los trailers de la respuesta (p. ej. un `grpc-status` de gRPC).

## Descubrimiento de servicios

`client.NewManagedClient` recibe un `Resolver` (qué backends existen) y un `Selector` (cuál atiende la siguiente petición). Un sub-pool por cada dirección resuelta.

```go
resolver := client.StaticResolver(
	client.Address{Host: "10.0.0.1", Port: 443},
	client.Address{Host: "10.0.0.2", Port: 443},
)
c, err := client.NewManagedClient(resolver, dialer,
	client.WithSelector(client.RoundRobin()))
```

`client.DNSResolver(host, port, client.DNSOptions{TTL: 30 * time.Second})` vuelve a resolver los registros A/AAAA según un TTL; el pool marca los backends nuevos y drena los eliminados. Selectores: `RoundRobin()`, `Random(rng)`, `Hash(keyFn)` para afinidad de sesión. `Resolver` es una interfaz — implementa `Resolve`/`Watch` para conectar tu propio mecanismo de descubrimiento.

## Resiliencia

Los reintentos son opt-in vía `Retryer`. Reemite peticiones idempotentes ante fallos transitorios (REFUSED_STREAM, GOAWAY, errores de conexión) con backoff exponencial y jitter, acotado por `MaxAttempts`.

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

Los métodos no idempotentes no se reintentan salvo que establezcas `req.Idempotency = client.ForceIdempotent` (acompáñalo de una clave de idempotencia). El rate limiting por token bucket es una opción del constructor — `client.WithRateLimit(100, 20)` limita a 100 peticiones/s con ráfagas de hasta 20. Deadlines por petición: establece `req.Timeout`; al expirar, `Do` falla con `context.DeadlineExceeded` y el stream se resetea con `RST_STREAM(CANCEL)`.

## Observabilidad

`WithHooks` instala callbacks de ciclo de vida; `MetricsSnapshot` devuelve una vista congelada de contadores e histogramas de latencia.

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

Otros hooks: `OnRequestStart`, `OnRetry`, `OnDial`, `OnConnClose`, `OnResolverUpdate`. Los hooks no deben bloquear. `c.PoolStats()` expone el estado en vivo del pool, útil para un endpoint `/debug`.

## Funciones avanzadas del protocolo

**Server push (RFC 7540 §8.2).** Registra un handler con `WithPushHandler`; esto habilita el push en SETTINGS. El cliente drena cada stream empujado en un `Response` y lo entrega al callback. Sin handler, PUSH_PROMISE es un error de protocolo.

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

**CONNECT extendido (RFC 8441).** Tuneliza un WebSocket (o cualquier protocolo) sobre un único stream HTTP/2. El servidor debe anunciar `SETTINGS_ENABLE_CONNECT_PROTOCOL=1`.

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

**H2C (prior knowledge).** HTTP/2 en claro, sin TLS ni ALPN: usa un `PlaintextDialer` y fija el scheme por defecto en `http`.

```go
c, err := client.NewSingleConnClient("localhost:8080",
	&conn.PlaintextDialer{},
	client.WithDefaultScheme("http"))
```

También soportado: prioridad de peticiones (`req.Priority = &frame.Priority{...}`, RFC 7540 §5.3), trailers en la petición (`req.TrailerFunc`) y dialers de proxy HTTP CONNECT (`conn.ProxyTLSDialer`).

## Ejemplo de generador de carga

`examples/loadgen` es un generador de carga HTTP/2 completo en un solo archivo: un cliente con pool, N goroutines worker, un tope global de QPS opcional y un resumen derivado de `MetricsSnapshot`.

```bash
go run ./examples/loadgen -url https://localhost:8443/ \
    -conns 4 -workers 64 -duration 30s -rps 5000
```
