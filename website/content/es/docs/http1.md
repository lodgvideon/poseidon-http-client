---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Use HTTP/1.1 cuando el servidor de destino no hable HTTP/2, o cuando
quiera probar específicamente su ruta HTTP/1.1 y necesite impedir que
la negociación ALPN promueva la conexión a h2.

HTTP/1.1 tiene el mismo conjunto de constructores que HTTP/2 y HTTP/3:

| Constructor | Transporte | Forma |
|---|---|---|
| `NewH1Client(addr, dialer, opts...)` | `TransportH1SingleConn` | Una conexión, reconexión automática. Las peticiones se serializan. |
| `NewH1PoolClient(addr, dialer, pool, opts...)` | `TransportH1Pool` | Hasta `MaxConnsPerHost` conexiones. Ese número es la concurrencia de peticiones. |
| `NewManagedH1Client(resolver, dialer, opts...)` | `TransportH1Managed` | Descubrimiento de servicios con Resolver + Selector; un sub-pool HTTP/1.1 por dirección reparte la carga. |

## Forzar HTTP/1.1

Dos cosas fijan el protocolo:

1. Un transporte HTTP/1.1: uno de los constructores anteriores, o el
   valor `Transport` correspondiente en `client.ClientOptions`.
2. Un dialer que no ofrezca `h2`: un dialer TCP plano
   (`&conn.PlaintextDialer{}`), o un dialer TLS cuyo `NextProtos`
   ofrezca únicamente `"http/1.1"`, de modo que el servidor no pueda
   seleccionar `h2` durante el handshake.

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

`client.NewH1Client(addr, dialer)` construye el mismo cliente de
conexión única en una sola llamada. La API de peticiones es la misma
`Do` / `client.GET` / `client.Response` que se usa para HTTP/2 y
HTTP/3, así que cambiar el protocolo de un objetivo de prueba es un
cambio de constructor, no una reescritura.

## Una conexión significa peticiones en serie

HTTP/1.1 no tiene multiplexación: una conexión transporta exactamente
un intercambio petición/respuesta a la vez. El pipelining no está
soportado, de forma deliberada. Por eso `TransportH1SingleConn`
serializa las peticiones: cada petición termina antes de que se
escriba la siguiente, y un segundo `Do` concurrente espera a que
acabe el primero. Sobre una sola conexión, la carga HTTP/1.1 es
estrictamente serial. Para concurrencia, use el pool.

## Pool de conexiones

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

A diferencia de los pools de HTTP/2 y HTTP/3, este es un pool de
préstamo exclusivo. Los pools H2/H3 entregan la misma conexión a
muchos streams concurrentes y eligen la menos cargada; el pool
HTTP/1.1 saca una conexión del conjunto de inactivas mientras dura un
intercambio y la devuelve al terminar. Consecuencias:

- `MaxConnsPerHost` **es** la concurrencia de peticiones. Es el único
  parámetro de dimensionamiento que importa aquí, y el pool nunca abre
  conexiones por encima de él.
- `MaxStreamsPerConn` no aplica a HTTP/1.1 y se ignora
  (`PoolOptions` se comparte con los pools H2/H3; aquí el tope por
  conexión es siempre 1).
- Una petición que encuentra todas las conexiones ocupadas espera a
  que se libere una, acotada por su ctx (y por
  `PoolOptions.AcquireTimeout`, si está configurado). Nunca se
  serializa sobre una conexión ocupada.

Las conexiones se mantienen vivas y se reutilizan entre intercambios.
Una conexión se descarta y se vuelve a marcar, en lugar de
reutilizarse, cuando la respuesta indica que no persistirá
(`Connection: close`, o HTTP/1.0 sin keep-alive), cuando el par cerró
el socket, o tras cualquier error de intercambio. La expulsión por
inactividad y los barridos de comprobación de salud coinciden con los
de los pools H2/H3.

Para objetivos con varias direcciones,
`NewManagedH1Client(resolver, dialer)` ejecuta un sub-pool de préstamo
exclusivo por cada dirección resuelta; `MaxConnsPerHost` fija entonces
la concurrencia por dirección.

## Streaming

El streaming de respuestas no está disponible en HTTP/1.1: las
respuestas se almacenan siempre íntegras en `Response.Body`.
`DoStream` devuelve un error sobre un transporte HTTP/1.1, igual que
`Do` con `Request.BodyMode: client.BodyStream`. Esto es deliberado: si
necesita respuestas en streaming, use HTTP/2 o HTTP/3. Los cuerpos de
petición sí se transmiten en streaming: un `Request.BodyReader` se
envía con `Transfer-Encoding: chunked` cuando su longitud no se conoce
de antemano, que es también la forma en que `Request.CompressBody`
envía un cuerpo comprimido en streaming.

## Fallback automático

No necesita un transporte HTTP/1.1 solo para hablar con un servidor
que carece de HTTP/2. `client.TransportALPN` negocia vía ALPN y cae por
sí solo a HTTP/1.1 cuando el servidor no ofrece `h2`. Fije el transporte
únicamente cuando quiera la garantía de que la conexión permanece en
HTTP/1.1.
