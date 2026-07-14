---
title: Primeros pasos
weight: 1
---

# Primeros pasos

poseidon-http-client es un cliente HTTP de bajo nivel para Go. Implementa HTTP/1.1, HTTP/2 y HTTP/3 desde cero — con su propio framing, HPACK, QPACK y stack QUIC — sin `net/http` y sin bibliotecas de protocolo de terceros. Está pensado para generadores de carga y herramientas que necesitan control directo sobre conexiones, streams y control de flujo.

## Instalación

Requiere Go 1.25 o superior.

```bash
go get github.com/lodgvideon/poseidon-http-client
```

## El modelo de peticiones

Las tres versiones del protocolo comparten la misma API. Se construye un cliente con un constructor y después se llama a `Do` con un contexto, una petición y un `*client.Response` que es propiedad del llamador:

```go
c, err := client.NewSingleConnClient(addr, dialer)   // or any constructor above
defer c.Close()
resp := &client.Response{}                            // caller-owned, reusable
err = c.Do(ctx, client.GET("/path"), resp)            // resp.Status, resp.Body
resp.Reset()                                          // reuse for the next request
```

Dos cosas difieren de `net/http`:

- **La respuesta es propiedad del llamador.** Usted asigna un `client.Response`, lo pasa a `Do`, lee `resp.Status` (un `int`) y `resp.Body` (un `[]byte`), y llama a `resp.Reset()` para reutilizarlo en la siguiente petición. No se asigna un objeto de respuesta por llamada — en un bucle de peticiones, un solo `Response` puede servir para todas las iteraciones.
- **Las peticiones son valores, no punteros hacia un transporte.** `client.GET(path)` construye una petición GET. Para control total, construya `client.Request{Method, Scheme, Authority, Path, BodyMode}` directamente.

## Primera petición sobre HTTP/2

Este es `examples/http2/main.go` del repositorio, sin modificar. Emite un GET contra un endpoint público con HTTP/2:

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

Aquí el dialer decide el protocolo: `conn.TLSDialer` con `NextProtos: []string{"h2"}` ofrece únicamente HTTP/2 en la negociación ALPN. El cliente mantiene una conexión abierta y vuelve a marcar automáticamente si se cae.

## Elegir un protocolo

El constructor que se invoca determina la versión del protocolo:

| Constructor | Protocolo | Modelo de conexión |
|---|---|---|
| `client.NewClient(ClientOptions{Transport: client.TransportH1SingleConn, ...})` | HTTP/1.1 | Una conexión, peticiones serializadas |
| `client.NewSingleConnClient(addr, dialer, opts...)` | HTTP/2 | Una conexión, redial automático |
| `client.NewPoolClient(addr, dialer, pool, opts...)` | HTTP/2 | Pool de conexiones por host |
| `client.NewManagedClient(resolver, dialer, opts...)` | HTTP/2 | Service discovery (Resolver + Selector) |
| `client.NewH3Client(addr, tlsConfig, opts...)` | HTTP/3 | Una conexión QUIC |
| `client.NewH3PoolClient(addr, tlsConfig, pool, opts...)` | HTTP/3 | Pool QUIC multi-conexión |
| `client.NewManagedH3Client(resolver, tlsConfig, opts...)` | HTTP/3 | Service discovery sobre QUIC |

Notas:

- Para los constructores de HTTP/2, el `NextProtos` del dialer debe ofrecer `"h2"`. `client.TransportALPN` hace fallback a HTTP/1.1 si el servidor no ofrece h2.
- Los constructores de HTTP/3 reciben un `*tls.Config` directamente; el transporte es QUIC sobre UDP.
- Todos los constructores devuelven un cliente con los mismos métodos `Do` / `DoStream`.

## Siguientes pasos

Cada protocolo tiene su propia guía con el ejemplo verificado completo, streaming, pooling y las opciones específicas de esa versión:

- [Guía de HTTP/1.1](/docs/http1/)
- [Guía de HTTP/2](/docs/http2/)
- [Guía de HTTP/3](/docs/http3/)
