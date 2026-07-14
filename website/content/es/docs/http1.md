---
title: HTTP/1.1
weight: 2
---

# HTTP/1.1

Use HTTP/1.1 cuando el servidor de destino no hable HTTP/2, o cuando
quiera probar específicamente su ruta HTTP/1.1 y necesite impedir que
la negociación ALPN promueva la conexión a h2.

## Forzar HTTP/1.1

Dos cosas fijan el protocolo:

1. `Transport: client.TransportH1SingleConn` en `client.ClientOptions`.
2. Un dialer TLS cuyo `NextProtos` ofrezca únicamente `"http/1.1"`,
   de modo que el servidor no pueda seleccionar `h2` durante el
   handshake.

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

La API de peticiones es la misma `Do` / `client.GET` / `client.Response`
que se usa para HTTP/2 y HTTP/3, así que cambiar el protocolo de un
objetivo de prueba es un cambio de constructor, no una reescritura.

## Serialización

`TransportH1SingleConn` mantiene una sola conexión y serializa las
peticiones sobre ella: cada petición termina antes de que se escriba la
siguiente. No hay pipelining. Para generar carga concurrente contra un
servidor HTTP/1.1, ejecute varios clientes.

## Fallback automático

No necesita `TransportH1SingleConn` solo para hablar con un servidor
que carece de HTTP/2. `client.TransportALPN` negocia vía ALPN y cae por
sí solo a HTTP/1.1 cuando el servidor no ofrece `h2`. Fije el transporte
únicamente cuando quiera la garantía de que la conexión permanece en
HTTP/1.1.
