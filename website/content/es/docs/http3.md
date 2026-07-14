---
title: HTTP/3
weight: 4
---

# HTTP/3

El cliente HTTP/3 (RFC 9114) funciona sobre una pila QUIC (RFC 9000/9001/9002) y un códec QPACK (RFC 9204) implementados en este repositorio — sin `quic-go`, sin cgo. Solo el handshake TLS 1.3 en sí proviene de la biblioteca estándar `crypto/tls`; la protección de paquetes, la recuperación de pérdidas, el control de congestión y el control de flujo son código de poseidon.

La API de peticiones es el mismo `Do` / `DoStream` que se usa con HTTP/2. Un `client.Request` y un `client.Response` no cambian entre transportes; pasar una prueba de carga de H2 a H3 es solo cambiar el constructor.

## Ejemplo

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

## Constructores

Tres constructores, en paralelo al conjunto de HTTP/2:

```go
// One QUIC connection. Buffered (Do) and streaming (DoStream) requests.
client.NewH3Client(addr string, tlsConfig *tls.Config, opts ...Option) (*Client, error)

// Pool of QUIC connections to one host.
client.NewH3PoolClient(addr string, tlsConfig *tls.Config, pool PoolOptions, opts ...Option) (*Client, error)

// Service discovery: a Resolver supplies addresses, requests spread across them.
client.NewManagedH3Client(resolver Resolver, tlsConfig *tls.Config, opts ...Option) (*Client, error)
```

Para los ajustes que los constructores no exponen — como el control de congestión — construya con `client.NewClient(client.ClientOptions{...})` y establezca `Transport: client.TransportH3`, como en el ejemplo anterior.

## Peticiones concurrentes

Una conexión HTTP/3 transporta varias peticiones en vuelo a la vez, cada una en su propio stream QUIC. Puede invocar `Do` / `DoStream` desde goroutines concurrentes contra el mismo cliente; las variantes de pool y managed además reparten los streams entre varias conexiones.

## Suites de cifrado

Las tres suites AEAD de TLS 1.3 están soportadas para la protección de paquetes QUIC: AES-128-GCM, AES-256-GCM y ChaCha20-Poly1305. El servidor elige durante el handshake; no hace falta configurar nada en el cliente. Una suite fuera de este conjunto (por ejemplo TLS_AES_128_CCM_8_SHA256) falla al instalar las claves con el error tipado `quic.ErrCryptoSuite` — sin bloqueos, sin panics.

## QPACK

La compresión de cabeceras usa QPACK con tabla dinámica en ambas direcciones: el cliente codifica las cabeceras de la petición contra la tabla dinámica y decodifica las inserciones del servidor en el stream del decodificador. Ocurre de forma automática; no hay nada que configurar.

## Asignaciones de memoria

El códec de wire de HTTP/3 no asigna memoria: los frames y cabeceras de paquete QUIC, los frames HTTP/3 y las secciones de campos QPACK se codifican y decodifican a 0 B/op, 0 allocs/op. La misma puerta de benchmarks en CI que lo exige para el códec de frames y HPACK de HTTP/2 cubre los paquetes `qpack`, `quic` y `http3`. La excepción es la ruta de envío de paquetes QUIC: construir y sellar un paquete saliente asigna una cantidad pequeña y acotada por paquete. Una petición sobre HTTP/3 es, por tanto, de pocas asignaciones, no de cero.

## Control de congestión

El valor por defecto es NewReno. BBR está disponible de forma opcional:

```go
H3ConnOptions: []quic.ConnOption{
	quic.WithCongestionControl(quic.CCBBR),
},
```

BBR está implementado correctamente y con tests, pero su ventaja de rendimiento sobre NewReno solo aparece en un enlace WAN con cuello de botella y retardo de cola real. En una LAN o en loopback no medirá ninguna diferencia. Si no puede medir su enlace objetivo, quédese con NewReno.

## E/S por lotes en Linux

En Linux el transporte QUIC usa GSO (generic segmentation offload) para entregar al kernel lotes de paquetes salientes en una sola syscall, y GRO para recibir lotes coalescidos. Ambos son automáticos — sin configuración, sin build tags. En otras plataformas el cliente envía y recibe un datagrama por syscall.

## Fuera de alcance

Deliberadamente fuera del alcance de la versión 1.0:

- **0-RTT / reanudación de sesión** — cada conexión hace el handshake completo.
- **Migración de conexión QUIC** — una conexión queda ligada a la ruta en la que se abrió.
- **Server push de HTTP/3** — nunca se habilita.

El cliente nunca inicia ninguna de estas funciones. Si el servidor las ofrece, simplemente no se usan; nada falla, no aparece ningún error. El único fallo duro en este terreno es una suite de cifrado no soportada, que devuelve el error tipado `quic.ErrCryptoSuite` descrito arriba.
