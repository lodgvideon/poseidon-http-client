---
title: Características y ventajas
weight: 5
---

# Características y ventajas

## Matriz de soporte

Las tres versiones del protocolo comparten la misma API de peticiones: `client.Do` / `client.DoStream` con un `client.Response` reutilizable, propiedad del llamador. La compresión también es compartida: las respuestas se decodifican para gzip, deflate, br y zstd (el cliente anuncia `accept-encoding: gzip, deflate, br, zstd`), y `Request.CompressBody` comprime los cuerpos de las peticiones — véase [Compresión](#compresión) más abajo. Lo que varía es cuánto del protocolo expone cada transporte.

| Protocolo | Implementación | Constructores | Peticiones concurrentes por conexión | Pooling | Descubrimiento de servicios | Capacidades destacadas |
|---|---|---|---|---|---|---|
| HTTP/1.1 | Desde cero | `NewH1Client`, `NewH1PoolClient`, `NewManagedH1Client` | No — un intercambio por conexión a la vez (sin pipelining) | `NewH1PoolClient` (pool de checkout exclusivo: `MaxConnsPerHost` es la concurrencia de peticiones) | `NewManagedH1Client` (Resolver + Selector) | Reutilización de conexiones con keep-alive; los cuerpos de las peticiones sí se envían en streaming (`Request.BodyReader`, chunked cuando la longitud no se conoce de antemano), pero las respuestas siempre se guardan en búfer en `Response.Body` — `DoStream` y `BodyStream` devuelven un error; destino del fallback ALPN: `TransportALPN` selecciona HTTP/1.1 automáticamente cuando el servidor no ofrece h2 |
| HTTP/2 | RFC 7540 + HPACK (RFC 7541), desde cero | `NewSingleConnClient`, `NewPoolClient`, `NewManagedClient` | Sí — multiplexación de streams, limitada por `MAX_CONCURRENT_STREAMS` | `NewPoolClient` (pool por host, selección del stream menos cargado, expulsión de conexiones ociosas) | `NewManagedClient` (Resolver + Selector) | `DoStream` y trailers de petición; control de flujo; SETTINGS dinámicos; drenaje ante GOAWAY; keepalive con PING; server push (PUSH_PROMISE); prioridad de peticiones; CONNECT extendido (RFC 8441, WebSockets sobre H2); CONTINUATION; dialers de proxy HTTP CONNECT; h2c con conocimiento previo |
| HTTP/3 | RFC 9114 + QUIC (RFC 9000/9001/9002) + QPACK (RFC 9204), desde cero | `NewH3Client`, `NewH3PoolClient`, `NewManagedH3Client` | Sí — peticiones concurrentes en vuelo sobre una conexión QUIC | `NewH3PoolClient` (pool de varias conexiones) | `NewManagedH3Client` | `DoStream`; QPACK dinámico en ambas direcciones (codificación + decodificación); todos los AEAD de TLS 1.3 (AES-128-GCM, AES-256-GCM, ChaCha20-Poly1305); control de congestión NewReno por defecto, BBR opcional; en Linux: envío por lotes con GSO, recepción por lotes con GRO, coalescencia acotada de ACK |

HTTP/1.1 tiene el mismo juego de constructores que las otras dos versiones, pero su pool es de otra naturaleza. HTTP/1.1 no tiene multiplexación: una sola conexión significa peticiones estrictamente en serie, así que sin pool el cliente no puede generar carga HTTP/1.1 en absoluto. El pool entrega conexiones por checkout exclusivo — un intercambio por conexión a la vez — lo que hace de `MaxConnsPerHost` la concurrencia de peticiones; `MaxStreamsPerConn` no aplica. Una petición que encuentra todas las conexiones ocupadas espera a que se libere una, con el límite del contexto de la petición. Las conexiones se mantienen vivas y se reutilizan; un `Connection: close`, una conexión muerta o un error en el intercambio descarta la conexión y vuelve a marcar. El pipelining no está implementado, deliberadamente. El dialer no debe ofrecer el token ALPN `h2` — usa un dialer TCP plano, o un dialer TLS cuyo `NextProtos` sea solo `"http/1.1"`.

Cómo activar BBR en HTTP/3:

```go
client.ClientOptions{
    Transport: client.TransportH3,
    H3ConnOptions: []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)},
}
```

## Compresión

La compresión funciona de forma idéntica sobre HTTP/1.1, HTTP/2 y HTTP/3.

**Respuestas.** El cliente anuncia `accept-encoding: gzip, deflate, br, zstd` y decodifica los cuatro, con readers reutilizados desde pools. Una cabecera accept-encoding aportada por el llamador tiene prioridad; `Request.DisableDecompression` suprime tanto la cabecera como la decodificación. Una guarda contra bombas de descompresión rechaza los cuerpos que se expanden más allá de `MaxDecompressedSize` (10 MiB por defecto) con `ErrBodyTooLarge`, y la ventana de zstd está limitada a 8 MiB. La comparación de `Content-Encoding` no distingue mayúsculas de minúsculas (RFC 9110 §8.4.1).

**Peticiones.** Asigna `Request.CompressBody` y el cliente comprime el cuerpo y pone `content-encoding` él mismo:

```go
var resp client.Response
err := c.Do(ctx, &client.Request{
    Method: "POST", Path: "/ingest",
    Body:   payload,
    CompressBody: client.EncodingZstd, // client sets content-encoding itself
}, &resp)
```

Se aceptan `EncodingGzip`, `EncodingDeflate`, `EncodingBrotli` y `EncodingZstd`. El valor cero, `EncodingIdentity`, envía el cuerpo sin cambios — quien no opta por comprimir no paga nada (0 asignaciones). Poner `content-encoding` manualmente sigue significando "este cuerpo ya viene codificado", y el cuerpo se deja intacto (RFC 9110 §8.4 — Content-Encoding describe el cuerpo; no es una instrucción). Poner a la vez `CompressBody` y un `content-encoding` manual devuelve `ErrConflictingContentEncoding`. content-length es el tamaño comprimido para un cuerpo en búfer, y se omite para un cuerpo en streaming (HTTP/1.1 usa entonces transfer-encoding chunked).

## Por qué poseidon

**Un cliente, tres versiones del protocolo.** HTTP/1.1, HTTP/2 y HTTP/3 a través de la misma API `Do`/`DoStream`. La biblioteca estándar de Go no tiene HTTP/3; la mayoría de los stacks lo añaden con una biblioteca aparte y una API aparte. Aquí, pasar una prueba de carga de h2 a h3 es cambiar de constructor, no reescribir.

**Sin código de protocolo de terceros.** Todos los stacks de protocolo están escritos desde cero, en este módulo: QUIC (RFC 9000/9001/9002), HTTP/3 (RFC 9114), QPACK (RFC 9204), el framing de HTTP/2 (RFC 7540) con HPACK (RFC 7541) y HTTP/1.1. Sin `quic-go`, sin `nghttp2`, sin `net/http`, sin cgo. El handshake de TLS 1.3 usa `crypto/tls` de la biblioteca estándar. Las cuatro dependencias directas son primitivas de criptografía y compresión, adoptadas deliberadamente en lugar de reimplementarlas: `golang.org/x/net`, `golang.org/x/crypto` (protección de paquetes con ChaCha20-Poly1305), `github.com/andybalholm/brotli` y `github.com/klauspost/compress` (zstd). Reimplementar Poly1305 o Brotli sería un riesgo de seguridad sin ninguna ventaja — Brotli requiere un diccionario estático de 122 KB más 121 transformaciones, el zstd de klauspost lleva años de fuzzing a sus espaldas, y un descompresor es una superficie de ataque de primer orden. La frontera queda así: todo el código de protocolo es nuestro, auditable en un solo módulo; las primitivas de criptografía y compresión se toman prestadas, porque ahí lo prestado es la decisión de ingeniería más segura.

**Códec sin asignaciones.** Todo el códec de wire corre a 0 B/op y 0 allocs/op en ambas versiones del protocolo — HTTP/2 (codificación y decodificación de frames y HPACK) y HTTP/3 (parsing y serialización de frames y cabeceras de paquete de QUIC, frames de HTTP/3, secciones de campos QPACK) — y una compuerta de benchmarks en CI rompe la build si eso regresa. A tasas altas de peticiones en un generador de carga, cada asignación por frame se traduce directamente en presión sobre el GC; este códec no aporta ninguna. Los paquetes `frame`, `hpack` y `qpack` pueden usarse de forma independiente. Un límite honesto: la ruta de envío de paquetes QUIC (construir y cifrar un paquete saliente) tiene pocas asignaciones, no cero — la afirmación de cero asignaciones cubre el códec, no la petición completa.

**Control de grano fino.** Acceso directo a streams, ventanas de control de flujo, SETTINGS, política de pooling, control de congestión (NewReno o BBR) y pacing — mandos que `net/http` esconde detrás de su transporte. Si tu herramienta necesita mantener una ventana cerrada, fijar la concurrencia de streams o medir el efecto de un controlador de congestión, las palancas están expuestas.

**Funciones de generación de carga incorporadas.** Pooling de conexiones, descubrimiento de servicios por DNS (Resolver/Selector), reintentos acotados opcionales de peticiones idempotentes, limitación de tasa con token bucket (`WithRateLimit`), hooks de ciclo de vida (`Client.Hooks`) y métricas (`Client.MetricsSnapshot()`, `Client.PoolStats()`). Todo compartido entre HTTP/1.1, HTTP/2 y HTTP/3 — se configura una vez, no por protocolo.

**Probado por conformidad.** Alrededor de 200 tests de conformidad ligados a secciones concretas de los RFC, con compuerta en CI. Una matriz de interoperabilidad HTTP/3 con tres servidores (Caddy/quic-go, nginx/C, aioquic/Python) corre sobre UDP real. Los parsers de wire están fuzzeados. Toda la suite se ejecuta con `-race`.

## Comparado con net/http

`net/http` es el cliente estándar con todo incluido. Gestiona redirecciones, cookies, proxies desde el entorno y la negociación HTTP/1.1 + HTTP/2 sin configuración. poseidon cambia esa comodidad por control: añade HTTP/3, un códec sin asignaciones y herramientas de generación de carga, y a cambio te pide construir clientes por objetivo y gestionar las respuestas tú mismo. Si quieres un cliente web de propósito general, usa `net/http`. Si construyes generadores de carga o necesitas HTTP/3 con control fino, usa poseidon.

## Comparado con quic-go

`quic-go` es una biblioteca de QUIC y HTTP/3 madura y muy usada, que cubre servidor y cliente. poseidon reimplementa QUIC para mantener todo el código de protocolo en este módulo y seguir centrado en la generación de carga. Es más joven y más acotado: un cliente HTTP. El paquete `quic` sí expone un rol de servidor (`Listen` / `Accept`, aceptación de streams) — existe para dar al cliente un peer real en los tests — pero no hay servidor HTTP/3 encima. Si necesitas un stack QUIC curtido en producción o un servidor HTTP/3, `quic-go` es la opción establecida.

## Fuera del alcance de 1.0

Lo siguiente queda deliberadamente fuera de esta versión:

- **0-RTT / reanudación de sesión.** El cliente nunca la inicia.
- **Migración de conexión QUIC.** No se inicia.
- **Server push de HTTP/3.** No se atiende.

Si un peer ofrece cualquiera de estas capacidades, simplemente no se utilizan — nada falla. Un cipher suite de TLS no soportado falla de forma limpia con el error tipado `ErrCryptoSuite`; no hay cuelgue ni panic.
