---
title: poseidon-http-client
type: docs
---

# poseidon-http-client

Un cliente HTTP de bajo nivel para Go que implementa HTTP/1.1, HTTP/2 y HTTP/3 desde cero: framing propio, HPACK, QPACK y una pila QUIC escrita desde cero. No usa `net/http` ni bibliotecas de protocolo de terceros; las únicas dependencias directas son `golang.org/x/net` y `golang.org/x/crypto` (ChaCha20-Poly1305), con TLS 1.3 de la biblioteca estándar. Las tres versiones del protocolo comparten una misma API de peticiones: `Do` y `DoStream`. Está pensado para generadores de carga y herramientas que necesitan control fino sobre conexiones, streams y control de flujo — no como reemplazo de propósito general de `net/http`.

Licencia MIT. Requiere Go 1.25. Código fuente: [github.com/lodgvideon/poseidon-http-client](https://github.com/lodgvideon/poseidon-http-client).

## Por qué poseidon

- **Un cliente, tres versiones del protocolo** — HTTP/1.1, /2 y /3 con la misma API `Do`/`DoStream`; la biblioteca estándar de Go no tiene HTTP/3.
- **Desde cero, casi sin dependencias** — sin `quic-go`, sin `nghttp2`, sin cgo; una superficie pequeña y auditable.
- **Códec de wire sin asignaciones** — HTTP/2 (frames, HPACK) y HTTP/3 (QPACK, frames HTTP/3, frames QUIC y cabeceras de paquete) codifican y decodifican a 0 B/op y 0 allocs/op, garantizado por un bench gate en CI.
- **Control fino** — streams, ventanas de control de flujo, SETTINGS, política de pooling, control de congestión (NewReno o BBR); mandos que `net/http` oculta.
- **Funciones de generación de carga incorporadas** — pooling de conexiones, descubrimiento de servicios por DNS, reintentos, limitación de tasa, hooks y métricas, compartidas entre H2 y H3.
- **Probado por conformidad** — ~200 tests de conformidad ligados a secciones de RFC, una matriz de interoperabilidad HTTP/3 con 3 servidores (Caddy, nginx, aioquic) sobre UDP real, parsers de wire con fuzzing, `-race` en todo el proyecto.

## Guías

- [Primeros pasos]({{< relref "/docs/getting-started" >}})
- [HTTP/1.1]({{< relref "/docs/http1" >}})
- [HTTP/2]({{< relref "/docs/http2" >}})
- [HTTP/3]({{< relref "/docs/http3" >}})
- [Funciones y ventajas]({{< relref "/docs/features" >}})
- [Aviso legal]({{< relref "/docs/disclaimer" >}})

{{< hint warning >}}
**Software joven.** Esta es una primera versión. Implementa protocolos sensibles a la seguridad (TLS 1.3, QUIC, HPACK/QPACK) desde cero y no ha pasado una auditoría de seguridad de terceros. Se proporciona tal cual, úselo bajo su propia responsabilidad (MIT — sin garantía). Lea el [Aviso legal]({{< relref "/docs/disclaimer" >}}) antes de desplegarlo.
{{< /hint >}}
