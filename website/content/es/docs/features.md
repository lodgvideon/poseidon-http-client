---
title: Características y ventajas
weight: 5
---

# Características y ventajas

## Matriz de soporte

Las tres versiones del protocolo comparten la misma API de peticiones: `client.Do` / `client.DoStream` con un `client.Response` reutilizable, propiedad del llamador. Lo que varía es cuánto del protocolo expone cada transporte.

| Protocolo | Implementación | Constructores | Peticiones concurrentes por conexión | Pooling | Descubrimiento de servicios | Capacidades destacadas |
|---|---|---|---|---|---|---|
| HTTP/1.1 | Desde cero | `NewClient` con `TransportH1SingleConn` | No — una conexión, peticiones serializadas (sin pipelining) | No | No | Destino del fallback ALPN: `TransportALPN` selecciona HTTP/1.1 automáticamente cuando el servidor no ofrece h2 |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541), desde cero | `NewSingleConnClient`, `NewPoolClient`, `NewManagedClient` | Sí — multiplexación de streams, limitada por `MAX_CONCURRENT_STREAMS` | `NewPoolClient` (pool por host, selección del stream menos cargado, expulsión de conexiones ociosas) | `NewManagedClient` (Resolver + Selector) | `DoStream` y trailers de petición; control de flujo; SETTINGS dinámicos; drenaje ante GOAWAY; keepalive con PING; server push (PUSH_PROMISE); prioridad de peticiones; CONNECT extendido (RFC 8441, WebSockets sobre H2); CONTINUATION; dialers de proxy HTTP CONNECT; h2c con conocimiento previo |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204), desde cero | `NewH3Client`, `NewH3PoolClient`, `NewManagedH3Client` | Sí — peticiones concurrentes en vuelo sobre una conexión QUIC | `NewH3PoolClient` (pool de varias conexiones) | `NewManagedH3Client` | `DoStream`; QPACK dinámico en ambas direcciones (codificación + decodificación); todos los AEAD de TLS 1.3 (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305); control de congestión NewReno por defecto, BBR opcional; en Linux: envío por lotes con GSO, recepción por lotes con GRO, coalescencia acotada de ACK |

El soporte de HTTP/1.1 es deliberadamente mínimo: existe para que una prueba de carga pueda atacar el mismo objetivo con las tres versiones desde una sola base de código, y como fallback de `TransportALPN`. HTTP/2 y HTTP/3 son los transportes completos.

Cómo activar BBR en HTTP/3:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## Por qué poseidon

**Un cliente, tres versiones del protocolo.** HTTP/1.1, HTTP/2 y HTTP/3 a través de la misma API `Do`/`DoStream`. La biblioteca estándar de Go no tiene HTTP/3; la mayoría de los stacks lo añaden con una biblioteca aparte y una API aparte. Aquí, pasar una prueba de carga de h2 a h3 es cambiar de constructor, no reescribir.

**Desde cero, casi sin dependencias.** Sin `quic-go`, sin `nghttp2`, sin cgo. Las dependencias directas son `golang.org/x/net` y `golang.org/x/crypto` (esta última solo para la protección de paquetes con ChaCha20-Poly1305); el handshake de TLS 1.3 usa `crypto/tls` de la biblioteca estándar. Todo el código de protocolo está en este módulo — auditable, con una superficie pequeña y sin cadena de dependencias transitivas.

**Códec sin asignaciones.** La codificación y decodificación de frames y HPACK corre a 0 B/op y 0 allocs/op, y una compuerta de benchmarks en CI rompe la build si eso regresa. A tasas altas de peticiones en un generador de carga, cada asignación por frame se traduce directamente en presión sobre el GC; este códec no aporta ninguna. Los paquetes `frame`, `hpack` y `qpack` pueden usarse de forma independiente.

**Control de grano fino.** Acceso directo a streams, ventanas de control de flujo, SETTINGS, política de pooling, control de congestión (NewReno o BBR) y pacing — mandos que `net/http` esconde detrás de su transporte. Si tu herramienta necesita mantener una ventana cerrada, fijar la concurrencia de streams o medir el efecto de un controlador de congestión, las palancas están expuestas.

**Funciones de generación de carga incorporadas.** Pooling de conexiones, descubrimiento de servicios por DNS (Resolver/Selector), reintentos acotados opcionales de peticiones idempotentes, limitación de tasa con token bucket (`WithRateLimit`), hooks de ciclo de vida (`Client.Hooks`) y métricas (`Client.MetricsSnapshot()`, `Client.PoolStats()`). Todo compartido entre HTTP/2 y HTTP/3 — se configura una vez, no por protocolo.

**Probado por conformidad.** Alrededor de 200 tests de conformidad ligados a secciones concretas de los RFC, con compuerta en CI. Una matriz de interoperabilidad HTTP/3 con tres servidores (Caddy/quic-go, nginx/C, aioquic/Python) corre sobre UDP real. Los parsers de wire están fuzzeados. Toda la suite se ejecuta con `-race`.

## Comparado con net/http

`net/http` es el cliente estándar con todo incluido. Gestiona redirecciones, cookies, proxies desde el entorno y la negociación HTTP/1.1 + HTTP/2 sin configuración. poseidon cambia esa comodidad por control: añade HTTP/3, un códec sin asignaciones y herramientas de generación de carga, y a cambio te pide construir clientes por objetivo y gestionar las respuestas tú mismo. Si quieres un cliente web de propósito general, usa `net/http`. Si construyes generadores de carga o necesitas HTTP/3 con control fino, usa poseidon.

## Comparado con quic-go

`quic-go` es una biblioteca de QUIC y HTTP/3 madura y muy usada, que cubre servidor y cliente. poseidon reimplementa QUIC para mantenerse libre de dependencias y centrado en la generación de carga. Es más joven y más acotado: solo cliente, sin servidor. Si necesitas un stack QUIC curtido en producción o un servidor, `quic-go` es la opción establecida.

## Fuera del alcance de 1.0

Lo siguiente queda deliberadamente fuera de esta versión:

- **0-RTT / reanudación de sesión.** El cliente nunca la inicia.
- **Migración de conexión QUIC.** No se inicia.
- **Server push de HTTP/3.** No se atiende.

Si un peer ofrece cualquiera de estas capacidades, simplemente no se utilizan — nada falla. Un cipher suite de TLS no soportado falla de forma limpia con el error tipado `ErrCryptoSuite`; no hay cuelgue ni panic.
